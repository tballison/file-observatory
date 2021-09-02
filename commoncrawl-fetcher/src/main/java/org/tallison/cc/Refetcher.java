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
import org.apache.http.impl.conn.ConnectionShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.PGIndexer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;
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

import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.TikaTimeoutException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.TikaEmitterException;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;

/**
 * wrapper around wget to run it multi-threaded to refetch
 * truncated files from their original urls.
 *
 * This relies on autocommit
 */
public class Refetcher {

    static Logger LOGGER = LoggerFactory.getLogger(Refetcher.class);

    private static final int MAX_WAIT_SECONDS = 30;

    private static final String COMPLETED_HOST_SEMAPHORE = "";

    static AtomicInteger WGET_COUNTER = new AtomicInteger(0);

    //after you've failed to fetch from a given host this many times
    //abandon all hope for that host
    private static final int MAX_FAILED_FETCHES_PER_HOST = 10;

    //Sleep at least this long between requests to the same host
    private static final long SLEEP_BTWN_FETCHES_PER_HOST_MS = 10000;

    //Randomly add up to this many milliseconds to sleep per host
    private static final int ADD_SLEEP_BTWN_FETCHES_PER_HOST_MS = 20000;

    private static Options getOptions() {
        Options options = new Options();

        options.addRequiredOption("j", "jdbc", true, "jdbc connection string");
        options.addRequiredOption("c", "tikaConfig", true,
                "tika-config.xml file for the pipes iterator, fetcher and emtters");
        options.addOption("f", "freshStart", false, "whether or not to delete the cc_refetch and " +
                "cc_refetch_status tables (default = false)");
        options.addOption("n", "numThreads", true, "number of threads to run");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);
        Connection connection = DriverManager.getConnection(line.getOptionValue("j"));
        Path tikaConfigPath = Paths.get(line.getOptionValue("c"));

        boolean cleanStart = false;
        if (line.hasOption("c")) {
            cleanStart = true;
        }

        int numThreads = line.hasOption("n") ? Integer.parseInt(line.getOptionValue('n')) : 5;
        Refetcher refetcher = new Refetcher();
        refetcher.execute(connection, tikaConfigPath, cleanStart, numThreads);
    }

    private static void usage() {
    }

    private void execute(Connection connection, Path tikaConfigFile, boolean freshStart, int numThreads)
            throws Exception {

        createTables(connection, freshStart);
        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<Integer>(executorService);

        executorCompletionService.submit(
                new Enqueuer(connection, queue));
        Fetcher fetcher = FetcherManager.load(tikaConfigFile).getFetcher("fetcher");
        StreamEmitter emitter = (StreamEmitter) EmitterManager.load(tikaConfigFile).getEmitter(
                "emitter");
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new FetchWorker(queue, connection, fetcher, emitter));
        }

        int completed = 0;
        try {
            while (completed < numThreads + 1) {
                try {
                    Future<Integer> future = executorCompletionService.poll(1, TimeUnit.SECONDS);
                    if (future != null) {
                        completed++;
                        future.get();
                    }
                } catch (InterruptedException | ExecutionException e) {
                    executorService.shutdownNow();
                    throw new RuntimeException(e);
                }
            }
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
    }


    private void createTables(Connection connection, boolean cleanStart) throws SQLException {
        //build these tables for now ... todo fix
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

            sql = "create table cc_refetch (" + "id integer primary key, " + "target_url varchar(" +
                    PGIndexer.MAX_URL_LENGTH + ")," + "http_status integer," +
                    "refetched_timestamp timestamp with time zone);";

            st.execute(sql);
        }
    }

    private class FetchWorker implements Callable<Integer> {
        private int threadId = WGET_COUNTER.getAndIncrement();
        //private final Base32 base32 = new Base32();

        private final ArrayBlockingQueue<String> queue;
        private final PreparedStatement selectHost;
        private final PreparedStatement insertFetchTable;
        private final Fetcher fetcher;
        private final StreamEmitter emitter;
        Random random = new Random();


        FetchWorker(ArrayBlockingQueue<String> q, Connection connection, Fetcher fetcher,
                    StreamEmitter emitter) throws SQLException {
            this.queue = q;
            String sql = "insert into cc_fetch (id, status_id, fetched_digest, fetched_length, " +
                    "http_length, warc_ip_address) values (?,?,?,?,?,?)";
            insertFetchTable = connection.prepareStatement(sql);
            sql = "select u.id, u.url " +
                    "from cc_urls u " +
                    "join cc_hosts h on u.host = h.id " +
                    "join cc_truncated t on u.truncated=t.id " +
                    "left join cc_fetch f on u.id=f.id " +
                    "where h.host = ? and length(t.name) > 0 and f.id is null";
            selectHost = connection.prepareStatement(sql);
            this.fetcher = fetcher;
            this.emitter = emitter;
        }

        @Override
        public Integer call() throws Exception {
            int processed = 0;
            while (true) {
                try {

                    String host = queue.poll(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
                    if (host == null) {
                        throw new TimeoutException("waited " + MAX_WAIT_SECONDS + " seconds");
                    }
                    if (host == COMPLETED_HOST_SEMAPHORE) {
                        queue.put(host);
                        return 1;
                    }
                    LOGGER.info(
                            "About to get host: " + host);
                    selectHost.clearParameters();
                    selectHost.setString(1, host);
                    try (ResultSet rs = selectHost.executeQuery()) {
                        //this counts fetch exceptions which could be a sign that a host
                        //has blocked the crawler
                        AtomicInteger unhappyHosts = new AtomicInteger(0);

                        int i = 0;
                        while (rs.next()) {
                            int urlId = rs.getInt(1);
                            String url = rs.getString(2);
                            if (unhappyHosts.get() > MAX_FAILED_FETCHES_PER_HOST) {
                                LOGGER.warn("Too many failed fetches for {}", host);
                                writeStatus(urlId,
                                        CCFileFetcher.FETCH_STATUS.REFETCH_UNHAPPY_HOST,
                                        insertFetchTable);
                                continue;
                            }
                            if (i > 0) {
                                long sleep = SLEEP_BTWN_FETCHES_PER_HOST_MS +
                                        random.nextInt(ADD_SLEEP_BTWN_FETCHES_PER_HOST_MS);
                                LOGGER.info("about to sleep: {}ms", sleep);
                                Thread.sleep(sleep);
                            }
                            LOGGER.info(
                                    "About to get url: " + url);
                            wget(urlId, url, unhappyHosts);
                            i++;
                        }
                    }
                    processed++;
                    if (processed % 100 == 0) {
                        LOGGER.info("Thread #" + threadId + " has processed " + processed);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return 1;
                }
            }
        }

        private void wget(int urlId, String url, AtomicInteger unhappyHosts) throws SQLException,
                IOException {

            Path tmpPath = null;
            try {
                tmpPath = Files.createTempFile("wgetter-", ".tmp");
            } catch (IOException e) {
                LOGGER.warn(Integer.toString(urlId), e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_IO_EXCEPTION,
                        insertFetchTable);
                return;
            }

            try {
                _wget(urlId, url, tmpPath, unhappyHosts);
            } finally {
                deleteTmp(tmpPath);
            }
        }

        private void _wget(int urlId, String url, Path tmpPath, AtomicInteger unhappyHosts) throws SQLException,
                IOException {
            LOGGER.debug(urlId + " going to get " + url);

            Metadata metadata = new Metadata();

            try (InputStream is = fetcher.fetch(url, metadata)) {
                Files.copy(is, tmpPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (TikaTimeoutException e) {
                LOGGER.warn("timeout {}", urlId);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_TIMEOUT,
                        insertFetchTable);
                return;
            } catch (ConnectionShutdownException e) {
                LOGGER.warn(Integer.toString(urlId), e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_CONNECTION_SHUTDOWN,
                        insertFetchTable);
                unhappyHosts.incrementAndGet();
                return;
            } catch (IOException | TikaException e) {
                LOGGER.warn(Integer.toString(urlId), e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_IO_EXCEPTION_READING_ENTITY,
                        insertFetchTable);
                unhappyHosts.incrementAndGet();
                return;
            }

            int status = metadata.getInt(HttpFetcher.HTTP_STATUS_CODE);
            if (status < 200 || status > 299) {
                unhappyHosts.incrementAndGet();
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_BAD_STATUS,
                        insertFetchTable);
                return;
            }

            long httpLength = getHttpLength(metadata);
            String targetUrl = metadata.get(HttpFetcher.HTTP_TARGET_URL);
            String targetIPAddress = metadata.get(HttpFetcher.HTTP_TARGET_IP_ADDRESS);

            if (metadata.get(HttpFetcher.HTTP_FETCH_TRUNCATED) != null) {
                LOGGER.warn("truncated: {}", urlId);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_TRUNCATED, httpLength,
                        targetUrl, targetIPAddress,
                        insertFetchTable);
                return;
            }
            long fetchedLength = -1;
            try {
                fetchedLength = Files.size(tmpPath);
            } catch (IOException e) {
                LOGGER.warn("file length {}", urlId, e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_IO_EXCEPTION_FILE_LENGTH,
                        httpLength,
                        targetUrl, targetIPAddress,
                        insertFetchTable);
                return;
            }

            String digest = "";
            try (InputStream is = Files.newInputStream(tmpPath)) {
                digest = DigestUtils.sha256Hex(is);
            } catch (IOException e) {
                LOGGER.warn("digesting {}", urlId, e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_IO_EXCEPTION_DIGESTING,
                        httpLength, fetchedLength,
                        targetUrl, targetIPAddress,
                        insertFetchTable);
                return;
            }

            String targetPath =
                    digest.substring(0, 2) + "/" + digest.substring(2, 4) + "/" +
                            digest;

            try (InputStream is = TikaInputStream.get(tmpPath, new Metadata())) {
                emitter.emit(targetPath, is, new Metadata());
            } catch (FileAlreadyExistsException e) {
                //this can happen if two threads are writing to the same file
                //even if "update" is selected
                //swallow
            } catch (TikaEmitterException | IOException e) {
                LOGGER.warn(Integer.toString(urlId), e);
                writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_EXCEPTION_EMITTING, httpLength,
                        fetchedLength, targetUrl, targetIPAddress, digest,
                        insertFetchTable);
                return;
            }
            writeStatus(urlId, CCFileFetcher.FETCH_STATUS.REFETCHED_SUCCESS, httpLength,
                    fetchedLength, targetUrl, targetIPAddress, digest,
                    insertFetchTable);
        }

        private long getHttpLength(Metadata metadata) {
            String len = metadata.get(HttpFetcher.HTTP_HEADER_PREFIX + "Content-Length");
            if (len != null) {
                try {
                    return Long.parseLong(len);
                } catch (NumberFormatException e) {

                }
            }
            return -1l;
        }
    }


    private void deleteTmp(Path tmp) {
        try {
            Files.delete(tmp);
        } catch (IOException e1) {
            LOGGER.error("Couldn't delete tmp file: " + tmp.toAbsolutePath());
        }
    }

    private void writeStatus(int urlId, CCFileFetcher.FETCH_STATUS status,
                             PreparedStatement insert)
            throws SQLException {
        writeStatus(urlId, status, -1, -1, null, null, null, insert);
    }


    private void writeStatus(int urlId, CCFileFetcher.FETCH_STATUS status,
                             long httpLength, long fetchedLength, String targetUrl,
                             String targetIPAddress,
                             String digest,
                             PreparedStatement insert) throws SQLException {

        //do something w target url
        if (fetchedLength < 0 && ! StringUtils.isBlank(digest)) {
            System.out.println(urlId);
        }
        insert.clearParameters();
        int i = 0;
        insert.setInt(++i, urlId);
        insert.setInt(++i, status.ordinal());
        if (StringUtils.isBlank(digest)) {
            insert.setNull(++i, Types.VARCHAR);
        } else {
            insert.setString(++i, digest);
        }
        if (fetchedLength < 0) {
            insert.setNull(++i, Types.BIGINT);
        } else {
            insert.setLong(++i, fetchedLength);
        }
        if (httpLength < 0) {
            insert.setNull(++i, Types.BIGINT);
        } else {
            insert.setLong(++i, httpLength);
        }
        if (StringUtils.isBlank(targetIPAddress)) {
            insert.setNull(++i, Types.VARCHAR);
        } else {
            insert.setString(++i, targetIPAddress);
        }
        insert.execute();
    }

    private void writeStatus(int urlId, CCFileFetcher.FETCH_STATUS status,
                             long length, String targetUrl, String targetIPAddress,
                             PreparedStatement insert)
            throws SQLException {
        writeStatus(urlId, status, length, -1, targetUrl, targetIPAddress, null, insert);
    }

    private void writeStatus(int urlId, CCFileFetcher.FETCH_STATUS status,
                             long length, long httpLength, String targetUrl, String targetIPAddress,
                             PreparedStatement insert)
            throws SQLException {
        writeStatus(urlId, status, length, httpLength, targetUrl, targetIPAddress, null, insert);
    }

    private class Enqueuer implements Callable<Integer> {
        private final PreparedStatement select;
        private final ArrayBlockingQueue<String> queue;

        public Enqueuer(Connection connection, ArrayBlockingQueue<String> queue) throws SQLException {
            this.queue = queue;
            String sql = "select h.host from cc_urls u\n" +
                    "join cc_hosts h on u.host = h.id\n" +
                    "left join cc_fetch f on u.id = f.id\n" +
                    "where f.id is null\n" +
                    "group by h.host\n" +
                    //this shuffles the hosts -- not truly random
                    "order by length(h.host) % 7, h.host";
            select = connection.prepareStatement(sql);
        }

        @Override
        public Integer call() throws Exception {
            try (ResultSet rs = select.executeQuery()) {
                while (rs.next()) {
                    String host = rs.getString(1);
                    if (! host.equals(COMPLETED_HOST_SEMAPHORE)) {
                        //blocking
                        queue.put(host);
                    }
                }
            }
            queue.put(COMPLETED_HOST_SEMAPHORE);
            return 1;
        }
    }
}
