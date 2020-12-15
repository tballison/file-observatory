/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.cc;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * wrapper around wget to run it multi-threaded to refetch
 * truncated files from their original urls.
 *
 * This relies on autocommit
 */
public class Refetcher {
    enum REFETCH_STATUS {
        IO_EXCEPTION_TMP_FILE,
        WGET_NON_ZERO,
        WGET_TIMEOUT,
        WGOT_TOO_LONG,
        FETCHED_IO_EXCEPTION_SHA1,
        FETCHED_IO_EXCEPTION_LENGTH,
        ALREADY_IN_REFETCHED,
        FETCHED_EXCEPTION_COPYING_TO_REPOSITORY,
        ADDED_TO_REPOSITORY;
    }

    static Logger LOGGER = Logger.getLogger(Refetcher.class);

    private static final long MAX_FILE_LENGTH_BYTES = 50_000_000;
    private static final int MAX_REFETCH_TIME_SECONDS = 60;
    private static final int MAX_WAIT_SECONDS = 30;
    private static final DigestURLPairPoison POISON = new DigestURLPairPoison();

    static AtomicInteger WGET_COUNTER = new AtomicInteger(0);


    private static Options getOptions() {
        Options options = new Options();

        options.addRequiredOption("j", "jdbc", true,
                "jdbc connection string");
        options.addRequiredOption("o", "outputDir", true,
                "directory to which to write the literal files");
        options.addOption("m", "max", true, "max files to retrieve");
        options.addOption("c", "cleanStart", false,
                "whether or not to delete the cc_refetch and " +
                        "cc_refetch_status tables (default = false)");
        options.addOption("n", "numThreads", true,
                "number of threads to run");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);
        Connection connection = DriverManager.getConnection(line.getOptionValue("j"));
        Path outputDir = Paths.get(line.getOptionValue("o"));
        int max = -1;
        if (line.hasOption("m")) {
            max = Integer.parseInt(line.getOptionValue("m"));
        }

        boolean cleanStart = false;
        if (line.hasOption("c")) {
            cleanStart = true;
        }

        int numThreads = 10;
        Refetcher refetcher = new Refetcher();
        refetcher.execute(connection, outputDir, max, cleanStart, numThreads);
    }

    private static void usage() {
    }

    private void execute(Connection connection, Path rootDir,
                         int max, boolean cleanStart, int numThreads)
            throws SQLException {

        createTables(connection, cleanStart);
        ArrayBlockingQueue<IdUrlPair> queue = new ArrayBlockingQueue<IdUrlPair>(1000);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<Integer>(executorService);

        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new WGetter(queue, connection, rootDir));
        }

        selectFilesToFetch(connection, queue, max, numThreads);

        int completed = 0;
        while (completed < numThreads) {
            try {
                Future<Integer> future =
                        executorCompletionService.poll(1, TimeUnit.SECONDS);
                if (future != null) {
                    completed++;
                    future.get();
                }
            } catch (InterruptedException|ExecutionException e) {
                executorService.shutdownNow();
                throw new RuntimeException(e);
            }
        }
        executorService.shutdown();
        executorService.shutdownNow();
    }

    private void selectFilesToFetch(Connection connection,
                                    ArrayBlockingQueue<IdUrlPair> queue,
                                    int max, int numThreads) throws SQLException {
        String sql = "select u.id, u.url "+
        "from cc_urls u "+
        "join cc_truncated t on u.truncated=t.id "+
        "where u.status = 200 and length(t.name) > 0 ";
        if (max > 0) {
            sql += "limit "+max+";";
        }
        try (Statement st = connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    try {
                        queue.offer(new IdUrlPair(rs.getInt(1), rs.getString(2)),
                                MAX_WAIT_SECONDS, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        for (int i = 0; i < numThreads; i++) {
            queue.offer(POISON);
        }
        return;
    }

    private void createTables(Connection connection,
                              boolean cleanStart) throws SQLException {
        String sql = "select * from cc_refetch limit 1";
        if (!cleanStart) {
            //test to see if the table already exists
            boolean createTable = false;
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {

                    }
                }
            } catch (SQLException e) {
                //table doesn't exist
                createTable = true;
            }
            if (!createTable) {
                return;
            }
        }
        try (Statement st = connection.createStatement()) {
            sql = "drop table if exists cc_refetch";
            st.execute(sql);

            sql = "create table cc_refetch (" +
                    "id integer primary key, " +
                    "refetched_status_id int, " +
                    "refetched_digest varchar(64), " +
                    "refetched_length bigint," +
                    "refetched_timestamp timestamp with time zone);";

            st.execute(sql);

            sql = "drop table if exists cc_refetch_status";
            st.execute(sql);

            sql = "create table cc_refetch_status " +
                    "(id integer primary key, status varchar(64));";
            st.execute(sql);


            for (REFETCH_STATUS status : REFETCH_STATUS.values()) {

                sql = "insert into cc_refetch_status values (" +
                        status.ordinal() + ",'" + status.name() + "');";
                st.execute(sql);
            }
        }
    }

    private class WGetter implements Callable<Integer> {
        private int threadId = WGET_COUNTER.getAndIncrement();
        private final Base32 base32 = new Base32();

        private final ArrayBlockingQueue<IdUrlPair> queue;
        private final PreparedStatement insert;
        private final Path rootDir;

        WGetter(ArrayBlockingQueue<IdUrlPair> q,
                Connection connection, Path rootDir) throws SQLException {
            this.queue = q;
            System.out.println(connection.getMetaData().getURL());
            String sql;
            String connectionUrl = connection.getMetaData().getURL();
            if (connectionUrl.startsWith("jdbc:postgresql")) {
                sql = "insert into cc_refetch values (?,?,?,?, now() at time zone 'utc')";
            } else if (connectionUrl.startsWith("jdbc:sqlite")) {
                sql = "insert into cc_refetch values (?,?,?,?, DATETIME('now', 'utc'))";
            } else {
                throw new IllegalArgumentException("currently only supports postgresql and sqlite");
            }
            insert = connection.prepareStatement(sql);
            this.rootDir = rootDir;
        }

        @Override
        public Integer call() throws Exception {
            int processed = 0;
            while (true) {
                try {
                    IdUrlPair p = queue.poll(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
                    if (p == null) {
                        throw new TimeoutException("waited "
                                + MAX_WAIT_SECONDS + " seconds");
                    }
                    if (p == POISON) {
                        return 1;
                    }
                    LOGGER.debug("About to wget: " + p.id + " : " + p.url);
                    wget(p, rootDir);
                    processed++;
                    if (processed % 100 == 0) {
                        LOGGER.info("Thread #"+threadId+" has processed "+processed);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return 1;
                }
            }
        }

        private void wget(IdUrlPair p, Path rootDir) throws SQLException, IOException {

            Path tmpPath = null;
            try {
                tmpPath = Files.createTempFile("wgetter-", ".tmp");
            } catch (IOException e) {
                writeStatus(p.id, REFETCH_STATUS.IO_EXCEPTION_TMP_FILE, insert);
                return;
            }

            try {
                _wget(p, rootDir, tmpPath);
            } finally {
                deleteTmp(tmpPath);
            }
        }

        private void _wget(IdUrlPair p, Path rootDir, Path tmpPath)
                throws SQLException, IOException {
            LOGGER.debug(p.id + " going to get " + p.url);
            ProcessBuilder pb = new ProcessBuilder();
            pb.inheritIO();
            String[] args = new String[]{
                    "wget",
                    "-nv", //not verbose
                    "-t", "1", //just try once
                    "-O",
                    tmpPath.toString(),
                    p.url
            };
            pb.command(args);
            Process process = pb.start();

            boolean finished = false;
            try {
                finished = process.waitFor(MAX_REFETCH_TIME_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //end the world
                throw new RuntimeException(e);
            } finally {
                process.destroyForcibly();
            }
            if (!finished) {
                writeStatus(p.id, REFETCH_STATUS.WGET_TIMEOUT, insert);
                return;
            }
            int exit = process.exitValue();
            if (exit != 0) {
                writeStatus(p.id, REFETCH_STATUS.WGET_NON_ZERO, insert);
                return;
            }
            //wget is done; now time to get the size+digest+move to repo
            wgot(p, rootDir, tmpPath);
        }

        private void wgot(IdUrlPair p, Path rootDir, Path tmpPath) throws SQLException {
            long length = -1;
            try {
                length = Files.size(tmpPath);
            } catch (IOException e) {
                writeStatus(p.id, REFETCH_STATUS.FETCHED_IO_EXCEPTION_LENGTH, insert);
                return;
            }


            String digest = "";
            try (InputStream is = Files.newInputStream(tmpPath)) {
                digest = base32.encodeToString(DigestUtils.sha1(is));
            } catch (IOException e) {
                writeStatus(p.id, REFETCH_STATUS.FETCHED_IO_EXCEPTION_SHA1, insert);
                return;
            }

            if (length > MAX_FILE_LENGTH_BYTES) {
                writeStatus(p.id, REFETCH_STATUS.WGOT_TOO_LONG, digest, length, insert);
                return;
            }
            Path repoTargetFile = rootDir.resolve(digest.substring(0, 2) + "/" + digest);
            if (Files.exists(repoTargetFile)) {
                writeStatus(p.id, REFETCH_STATUS.ALREADY_IN_REFETCHED, digest, length, insert);
                return;
            }
            try {
                Files.createDirectories(repoTargetFile.getParent());
                Files.copy(tmpPath, repoTargetFile);
            } catch (IOException e) {
                writeStatus(p.id, REFETCH_STATUS.FETCHED_EXCEPTION_COPYING_TO_REPOSITORY,
                        digest, length, insert);
                return;
            }
            writeStatus(p.id, REFETCH_STATUS.ADDED_TO_REPOSITORY, digest, length, insert);
        }
    }



    private void deleteTmp(Path tmp) {
        try {
            Files.delete(tmp);
        } catch (IOException e1) {
            LOGGER.error("Couldn't delete tmp file: " + tmp.toAbsolutePath());
        }
    }

    private void writeStatus(int id, REFETCH_STATUS status, String digest, long length, PreparedStatement insert) throws SQLException {
        insert.clearParameters();
        insert.setInt(1, id);
        insert.setInt(2, status.ordinal());
        insert.setString(3, digest);
        insert.setLong(4, length);
        //insert.setTimestamp(5, Timestamp.from(Instant.now()));
        insert.execute();
    }

    private void writeStatus(int id, REFETCH_STATUS status, PreparedStatement insert) throws SQLException {
        insert.clearParameters();
        insert.setInt(1, id);
        insert.setInt(2, status.ordinal());
        insert.setNull(3, Types.VARCHAR);
        insert.setNull(4, Types.BIGINT);
        //insert.setTimestamp(5, Timestamp.from(Instant.now()));
        insert.execute();
    }

    private static class IdUrlPair {
        final int id;
        final String url;

        IdUrlPair(int id, String url) {
            this.id = id;
            this.url = url;
        }
    }

    private static class DigestURLPairPoison extends IdUrlPair {
        DigestURLPairPoison() {
            super(-1, null);
        }
    }

}
