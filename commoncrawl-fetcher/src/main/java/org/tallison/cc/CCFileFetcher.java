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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.CCIndexRecord;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.TikaEmitterException;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.fetcher.RangeFetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;

/**
 * Class to read in an index file or a subset of an index file
 * and to "get" those files from cc to a local directory
 * <p>
 * This relies heavily on centic9's CommonCrawlDocumenDownload.
 * Thank you, Dominik!!!
 */
public class CCFileFetcher {

    private final static String AWS_BASE = "https://commoncrawl.s3.amazonaws.com/";
    static Logger LOGGER = LoggerFactory.getLogger(CCFileFetcher.class);
    private int batchTupleListSize = 500;

    public enum FETCH_STATUS {
        BAD_URL, //0
        FETCHED_IO_EXCEPTION,//1
        FETCHED_NOT_200,//2
        FETCHED_IO_EXCEPTION_READING_ENTITY,//3
        FETCHED_IO_EXCEPTION_DIGESTING,//4
        ALREADY_IN_REPOSITORY,//5
        FETCHED_EXCEPTION_EMITTING,//6
        ADDED_TO_REPOSITORY,//7
        ADDED_TO_REPOSITORY_DIFF_DIGEST,//8
        EMPTY_PAYLOAD,//9
        TRUNCATED,//10
        REFETCHED_SUCCESS,//11
        REFETCHED_BAD_STATUS,//12
        REFETCHED_CONNECTION_SHUTDOWN,//13
        REFETCH_UNHAPPY_HOST,//14
        REFETCHED_TIMEOUT,//15
        REFETCHED_IO_EXCEPTION,//16
        REFETCHED_NOT_200,//17
        REFETCHED_IO_EXCEPTION_READING_ENTITY,//18
        REFETCHED_IO_EXCEPTION_FILE_LENGTH,//19
        REFETCHED_IO_EXCEPTION_DIGESTING,//20
        REFETCHED_EXCEPTION_EMITTING,//21
        REFETCHED_TRUNCATED,//22
    }




    private static Options getOptions() {
        Options options = new Options();

        options.addRequiredOption("j", "jdbc", true, "jdbc connection string");
        options.addRequiredOption("c", "tikaConfig", true,
                "tikaConfig file from which to load the emitter");
        options.addOption("n", "numThreads", true, "number of threads");
        options.addOption("m", "max", true, "max files to retrieve");
        options.addOption("f", "freshStart", false, "whether or not to delete the cc_fetch and " +
                "cc_fetch_status tables (default = false)");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);
        Connection connection = DriverManager.getConnection(line.getOptionValue("j"));

        int max = -1;
        if (line.hasOption("m")) {
            max = Integer.parseInt(line.getOptionValue("m"));
        }
        CCFileFetcher ccFileFetcher = new CCFileFetcher();
        boolean freshStart = false;
        if (line.hasOption("f")) {
            freshStart = true;
        }
        Path tikaConfigPath = Paths.get(line.getOptionValue("c"));
        int numThreads = (line.hasOption("n")) ? Integer.parseInt(line.getOptionValue("n")) : 5;
        ccFileFetcher.execute(connection, tikaConfigPath, numThreads, freshStart, max);
    }

    private void execute(Connection connection, Path tikaConfigPath, int numThreads,
                         boolean cleanStart, int max) throws Exception {
        connection.setAutoCommit(false);
        createFetchTable(connection, cleanStart);
        PipesIterator pipesIterator = PipesIterator.build(tikaConfigPath);

        ExecutorService es = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(es);

        FetchEmitIterator idIterator = new FetchEmitIterator(pipesIterator, numThreads);
        completionService.submit(idIterator);
        RangeFetcher fetcher =
                (RangeFetcher)FetcherManager.load(tikaConfigPath).getFetcher(pipesIterator.getFetcherName());
        StreamEmitter emitter =
                (StreamEmitter) EmitterManager.load(tikaConfigPath).getEmitter(pipesIterator.getEmitterName());

        for (int i = 0; i < numThreads; i++) {
            completionService.submit(new WarcFileFetcher(connection, fetcher,
                    idIterator.getQueue(), emitter,
                    max));
        }

        int finished = 0;
        try {
            while (finished < numThreads + 1) {
                Future fut = completionService.take();
                finished++;
                fut.get();
            }
        } finally {
            es.shutdownNow();
            connection.commit();
        }

    }

    private void createFetchTable(Connection connection, boolean cleanStart) throws SQLException {

        String sql = "select * from cc_fetch limit 1";
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
            sql = "drop table if exists cc_fetch";
            st.execute(sql);

            sql = "create table cc_fetch (" + "id integer primary key, " + "status_id int, " +
                    "fetched_digest varchar(64), " + "fetched_length bigint," +
                    "http_length bigint,"+
                    "warc_ip_address varchar(64));";
            st.execute(sql);

            sql = "drop table if exists cc_fetch_status";
            st.execute(sql);

            sql = "create table cc_fetch_status " + "(id integer primary key, status varchar(64));";
            st.execute(sql);


            for (FETCH_STATUS status : FETCH_STATUS.values()) {

                sql = "insert into cc_fetch_status values (" + status.ordinal() + ",'" +
                        status.name() + "');";
                st.execute(sql);
            }
        }
        connection.commit();
    }



    private static class WarcFileFetcher implements Callable<Integer> {

        private static final AtomicInteger COUNT = new AtomicInteger(0);
        private static final int MAX_EMIT_EXCEPTION = 100;
        private final RangeFetcher fetcher;
        private final ArrayBlockingQueue<List<FetchEmitTuple>> q;
        private final StreamEmitter emitter;
        private final int max;
        private final PreparedStatement insert;
        private int emitException = 0;
        private Base32 base32 = new Base32();


        private WarcFileFetcher(Connection connection, RangeFetcher fetcher,
                                ArrayBlockingQueue<List<FetchEmitTuple>> queue,
                                StreamEmitter emitter, int max) throws IOException, SQLException {
            this.fetcher = fetcher;
            this.q = queue;
            this.emitter = emitter;
            insert = prepareInsert(connection);
            this.max = max;
        }

        @Override
        public Integer call() throws Exception {
            int fetched = 0;
            while (true) {
                List<FetchEmitTuple> tuples = q.poll(5, TimeUnit.MINUTES);
                if (tuples.size() == 0) {
                    insert.close();
                    return fetched;
                }
                if (emitException > MAX_EMIT_EXCEPTION) {
                    LOGGER.error("too many emit exceptions");
                    return 1;
                }
                fetched = fetchFiles(tuples, fetched);
            }
        }

        private int fetchFiles(List<FetchEmitTuple> tuples, int fetched)
                throws SQLException, TikaException, IOException {
            for (FetchEmitTuple t : tuples) {
                try {
                    processTuple(t);
                } catch (IOException e) {
                    LOGGER.warn("problem fetching {}", t.getId(), e);
                    continue;
                }

                fetched++;
                if (fetched % 100 == 0) {
                    insert.executeBatch();
                    insert.getConnection().commit();
                }
                //should add limit command to sql
                int cnt = COUNT.incrementAndGet();
                if (cnt > max && max > -1) {
                    break;
                }
                if (cnt % 1000 == 0) {
                    LOGGER.info("processed {} files", cnt);
                }
            }

            insert.executeBatch();
            insert.getConnection().commit();
            return fetched;
        }


        private PreparedStatement prepareInsert(Connection connection) throws SQLException {
            String sql = "insert into cc_fetch values (?, ?, ?, ?, ?, ?)";
            return connection.prepareStatement(sql);
        }

        private void processTuple(FetchEmitTuple t) throws IOException, TikaException,
                SQLException {

            byte[] warcRecordGZBytes = fetch(t);
            try {
                parseWarc(t, warcRecordGZBytes);
            } catch (IOException e) {
                LOGGER.warn("problem parsing warc file", e);
                writeStatus(t.getId(), FETCH_STATUS.FETCHED_IO_EXCEPTION_READING_ENTITY, insert);
                return;
            }
        }

        private void processRecord(FetchEmitTuple t, WarcRecord record) throws SQLException,
                IOException {

            if (!((record instanceof WarcResponse) &&
                    record.contentType().base().equals(MediaType.HTTP))) {
                return;
            }
            String truncated = t.getMetadata().get("cc_truncated");
            String warcDigest = t.getMetadata().get("cc_index_digest");
            String ipAddress = "";

            Optional<InetAddress> inetAddress = ((WarcResponse) record).ipAddress();

            if (inetAddress.isPresent()) {
                ipAddress = inetAddress.get().getHostAddress();
            }

            Optional<String> httpContentLengthString =
                    ((WarcResponse) record).http().headers().first(
                    "Content-Length");

            long httpContentLength = -1;
            if (httpContentLengthString.isPresent()) {
                try {
                    httpContentLength = Long.parseLong(httpContentLengthString.get());
                } catch (NumberFormatException e) {

                }
            }
            if (!StringUtils.isBlank(truncated)) {
                writeStatus(t.getId(), FETCH_STATUS.TRUNCATED, httpContentLength, ipAddress,
                        insert);
            } else {
                fetchPayload(t, httpContentLength, ipAddress, warcDigest, record);
            }
        }

        private void fetchPayload(FetchEmitTuple t, long httpContentLength,
                                  String ipAddress, String warcDigest,
                                  WarcRecord record)
                throws IOException, SQLException {
            Optional<WarcPayload> payload = ((WarcResponse) record).payload();
            if (!payload.isPresent()) {
                LOGGER.warn("no payload {}", t.getId());
                return;
            }
            if (payload.get().body().size() == 0) {
                LOGGER.warn("empty payload id={}", t.getId());
                writeStatus(t.getId(), FETCH_STATUS.EMPTY_PAYLOAD,
                        httpContentLength,
                        ipAddress, insert);
                return;
            }

            Path tmp = Files.createTempFile("ccfile-fetcher-", "");
            try {

                Files.copy(payload.get().body().stream(), tmp, StandardCopyOption.REPLACE_EXISTING);
                String targetDigest = null;
                long tmpLength = 0l;
                String sha1digest = "";
                try (InputStream is = Files.newInputStream(tmp)) {
                    sha1digest = base32.encodeAsString(DigestUtils.sha1(is));
                } catch (IOException e) {
                    writeStatus(t.getId(), FETCH_STATUS.FETCHED_IO_EXCEPTION_DIGESTING,
                            httpContentLength,
                            ipAddress, insert);
                    LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
                    return;
                }

                try (InputStream is = Files.newInputStream(tmp)) {
                    targetDigest = DigestUtils.sha256Hex(is);
                    tmpLength = Files.size(tmp);
                } catch (IOException e) {
                    writeStatus(t.getId(), FETCH_STATUS.FETCHED_IO_EXCEPTION_DIGESTING,
                            httpContentLength,
                            ipAddress, insert);
                    LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
                    return;
                }

                if (! sha1digest.equals(warcDigest)) {
                    LOGGER.warn("Conflicting digests id={}", t.getId());
                }
                String targetPath =
                        targetDigest.substring(0, 2) + "/" + targetDigest.substring(2, 4) + "/" +
                                targetDigest;
                Metadata metadata = new Metadata();

                try (InputStream is = TikaInputStream.get(tmp, metadata)) {
                    emitter.emit(targetPath, is, metadata);
                } catch (AmazonS3Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Please reduce your " +
                            "request rate")) {
                        LOGGER.warn("throttling -- aws exception -- please reduce your request " +
                                        "rate",
                                t.getId(), e);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ex) {
                            //swallow
                        }
                    } else {
                        LOGGER.warn("problem emitting id={}", t.getId(), e);
                    }
                    writeStatus(t.getId(), FETCH_STATUS.FETCHED_EXCEPTION_EMITTING, targetDigest,
                            tmpLength, httpContentLength, ipAddress, insert);
                    emitException++;
                    return;
                } catch (TikaEmitterException | IOException e) {
                    writeStatus(t.getId(), FETCH_STATUS.FETCHED_EXCEPTION_EMITTING, targetDigest,
                            tmpLength, httpContentLength, ipAddress, insert);
                    LOGGER.warn("problem emitting id={}", t.getId(), e);
                    emitException++;
                    return;
                }
                if (sha1digest.equals(warcDigest)) {
                    writeStatus(t.getId(), FETCH_STATUS.ADDED_TO_REPOSITORY, targetDigest, tmpLength,
                            httpContentLength, ipAddress, insert);
                } else {
                    writeStatus(t.getId(), FETCH_STATUS.ADDED_TO_REPOSITORY_DIFF_DIGEST,
                            targetDigest, tmpLength,
                            httpContentLength, ipAddress, insert);
                }
            } finally {
                deleteTmp(tmp);
            }
        }

        private void parseWarc(FetchEmitTuple t, byte[] warcRecordGZBytes) throws SQLException,
                IOException {
            //need to leave initial inputstream open while parsing warcrecord
            //can't just parse record and return
            try (InputStream is = new GZIPInputStream(
                    new ByteArrayInputStream(warcRecordGZBytes))) {
                try (WarcReader warcreader = new WarcReader(is)) {

                    //should be a single warc per file
                    //return the first
                    for (WarcRecord record : warcreader) {
                        processRecord(t, record);
                        return;
                    }
                }
            }
        }

        private byte[] fetch(FetchEmitTuple t) throws TikaException, IOException {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            FetchKey fetchKey = t.getFetchKey();
            try (InputStream is = fetcher.fetch(fetchKey.getFetchKey(), fetchKey.getRangeStart(),
                    fetchKey.getRangeEnd(), new Metadata())) {
                IOUtils.copy(is, bos);
            }
            return bos.toByteArray();
        }

        private void writeStatus(String id, FETCH_STATUS status, String digest, long length,
                                 long httpLength,
                                 String ipAddress, PreparedStatement insert) throws SQLException {
            insert.setInt(1, Integer.parseInt(id));
            insert.setInt(2, status.ordinal());
            insert.setString(3, digest);
            insert.setLong(4, length);
            if (httpLength > -1) {
                insert.setLong(5, httpLength);
            } else {
                insert.setNull(5, Types.BIGINT);
            }
            insert.setString(6, ipAddress);
            insert.addBatch();
        }

        private void writeStatus(String id, FETCH_STATUS status, long httpLength,
                                 String ipAddress,
                                 PreparedStatement insert) throws SQLException {
            insert.setInt(1, Integer.parseInt(id));
            insert.setInt(2, status.ordinal());
            insert.setNull(3, Types.VARCHAR);//retrieved digest
            insert.setNull(4, Types.BIGINT);//retrived file length
            if (httpLength > -1) {
                insert.setLong(5, httpLength);
            } else {
                insert.setNull(5, Types.BIGINT);
            }
            insert.setString(6, ipAddress);
            insert.addBatch();
        }
        private void writeStatus(String id, FETCH_STATUS status, PreparedStatement insert)
                throws SQLException {
            insert.setInt(1, Integer.parseInt(id));
            insert.setInt(2, status.ordinal());
            insert.setNull(3, Types.VARCHAR);
            insert.setNull(4, Types.BIGINT);
            insert.setNull(5, Types.BIGINT);
            insert.setNull(6, Types.VARCHAR);
            insert.addBatch();
        }
    }

    private class FetchEmitIterator implements Callable<Integer> {
        private final int numThreads;
        private final PipesIterator pipesIterator;
        private ArrayBlockingQueue q = new ArrayBlockingQueue<List<FetchEmitTuple>>(1000);

        private FetchEmitIterator(PipesIterator pipesIterator, int numThreads) {
            this.pipesIterator = pipesIterator;
            this.numThreads = numThreads;
        }

        ArrayBlockingQueue<List<FetchEmitTuple>> getQueue() {
            return q;
        }

        @Override
        public Integer call() throws Exception {
            int added = 0;
            List<FetchEmitTuple> list = new ArrayList<>();
            for (FetchEmitTuple t : pipesIterator) {
                if (list.size() >= batchTupleListSize) {
                    boolean offered = q.offer(list, 30, TimeUnit.MINUTES);
                    if (!offered) {
                        throw new TimeoutException("failed to add after 30 minutes");
                    }
                    list = new ArrayList<>();
                }
                list.add(t);
                added++;
            }
            if (list.size() > 0) {
                boolean offered = q.offer(list, 10, TimeUnit.MINUTES);
                if (!offered) {
                    throw new TimeoutException("failed to add after 30 minutes");
                }
            }
            for (int i = 0; i < numThreads; i++) {
                boolean offered = q.offer(Collections.EMPTY_LIST, 10, TimeUnit.MINUTES);
                if (!offered) {
                    throw new TimeoutException("timed out after 10 minutes");
                }
            }
            LOGGER.info("added {} fetchEmitTuples for processing", added);
            return added;
        }
    }
    private static void deleteTmp(Path tmp) {
        try {
            Files.delete(tmp);
        } catch (IOException e1) {
            LOGGER.error("Couldn't delete tmp file: " + tmp.toAbsolutePath());
        }
    }

}

