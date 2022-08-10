package org.tallison.ingest;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.QueryRequest;
import org.tallison.quaerite.connectors.SearchClientException;
import org.tallison.quaerite.connectors.SearchClientFactory;
import org.tallison.quaerite.core.SearchResultSet;
import org.tallison.quaerite.core.StoredDocument;
import org.tallison.quaerite.core.queries.TermQuery;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

public class IngesterCLI {

    private static final long MAX_WAIT_MS = 1200000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterCLI.class);
    private static final int MAX_ES_RETRIES = 3;
    //how many to grab from the db with each select
    private static final int DEFAULT_LIMIT = 1000;
    //how many records for pg to cache during a select
    private static final int FETCH_SIZE = 100;
    private static final int WORKER_CODE = 0;
    private static final int EMITTER_CODE = 1;
    private static final int PIPES_ITERATOR_CODE = 2;
    private static final int REPORTER_CODE = 2;
    private static final int EMIT_WITHIN_MS = 10000;
    private static StoredDocument STOP_SEMAPHORE = new StoredDocument(null);
    private static Map<String, String> STOP_SEMAPHORE_MAP = new HashMap<>();

    private static final AtomicInteger PROCESSED = new AtomicInteger(0);
    private static final AtomicInteger EMITTED = new AtomicInteger(0);
    private static final AtomicInteger SELECTED = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        String pgConnectionString = null;
        String esConnectionStringTmp = null;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]),
                StandardCharsets.UTF_8)) {
            pgConnectionString = reader.readLine();
            esConnectionStringTmp = reader.readLine();
        }
        final String esConnectionString = esConnectionStringTmp;
        Connection pg = DriverManager.getConnection(pgConnectionString);
        pg.setAutoCommit(false);//necessary for select pagination
        Fetcher fetcher = FetcherManager.load(Paths.get(args[1])).getFetcher("file-obs-fetcher");
        int limit = -1;
        if (args.length > 2) {
            limit = Integer.parseInt(args[2]);
        }
        CompositeFeatureMapper compositeFeatureMapper = new CompositeFeatureMapper();
        String sql = getSelectStar("selectStar-minimal.sql");
        int numWorkers = 50;
        int numEmitters = 10;
        ArrayBlockingQueue<Map<String, String>> rows = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<StoredDocument> docs = new ArrayBlockingQueue<>(500);

        ExecutorService executorService =
                Executors.newFixedThreadPool(numWorkers + numEmitters + 2);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        for (int i = 0; i < numWorkers; i++) {
            executorCompletionService.submit(
                    new RowWorker(rows, compositeFeatureMapper, fetcher, docs));
        }
        for (int i = 0; i < numEmitters; i++) {
            executorCompletionService.submit(
                    new EmitWorker(esConnectionString, docs));
        }
        executorCompletionService.submit(new BigSelector(pgConnectionString, sql, limit, rows));
        Callable<Integer> reporter = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                long start = System.currentTimeMillis();
                try {
                    while (true) {
                        long elapsed = System.currentTimeMillis() - start;
                        LOGGER.info("selected {}, processed {}, emitted {} docs in {}ms",
                                SELECTED.get(),
                                PROCESSED.get(),
                                EMITTED.get(), elapsed);
                        Thread.sleep(1000);
                        if (Thread.currentThread().isInterrupted()) {
                            return REPORTER_CODE;
                        }
                    }
                } catch (InterruptedException e) {
                    return REPORTER_CODE;
                }
            }
        };
        executorCompletionService.submit(reporter);

        int finishedWorkers = 0;
        boolean pipesIteratorFinished = false;
        long start = System.currentTimeMillis();
        try {
            while (finishedWorkers < numWorkers || pipesIteratorFinished == false) {
                Future<Integer> future = executorCompletionService.take();
                if (future != null) {
                    int workerCode = future.get();
                    LOGGER.info("worker code={} finished", workerCode);
                    if (workerCode == PIPES_ITERATOR_CODE) {
                        pipesIteratorFinished = true;
                    } else if (workerCode == WORKER_CODE) {
                        finishedWorkers++;
                    } else if (workerCode == REPORTER_CODE) {
                        //ignore
                    } else {
                        throw new IllegalArgumentException(
                                "shouldn't have finished: " + workerCode);
                    }
                }
            }
            for (int i = 0; i < numEmitters; i++) {
                docs.offer(STOP_SEMAPHORE, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            }

            int finished = 0;
            while (finished < numEmitters) {
                LOGGER.info("waiting for emitters");
                Future<Integer> future = executorCompletionService.take();
                if (future != null) {
                    int emitterCode = future.get();
                    if (emitterCode == EMITTER_CODE) {
                        finished++;
                    }
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.info("finished indexing and emitted {} in {}ms", EMITTED.get(), elapsed);
        } finally {
            executorService.shutdownNow();
        }

    }

    private static boolean esContains(String k, ESClient esClient) {
        QueryRequest request = new QueryRequest(new TermQuery("_id", k));
        SearchResultSet searchResultSet = null;
        try {
            searchResultSet = esClient.search(request);
            LOGGER.debug("exists? {}, {}", k, searchResultSet.size());
            return searchResultSet.size() > 0;
        } catch (SearchClientException | IOException e) {
            LOGGER.warn("problem with es {}", k, e);
        }
        return false;
    }

    protected static Map<String, String> mapify(ResultSet rs) throws SQLException {
        Map<String, String> row = new HashMap<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            row.put(rs.getMetaData().getColumnLabel(i), rs.getString(i));
        }
        return row;
    }

    private static String getSelectStar(String name) throws IOException {
        return IOUtils.toString(IngesterCLI.class.getResourceAsStream("/" + name),
                StandardCharsets.UTF_8.name());
    }

    private static class BigSelector implements Callable<Integer> {
        //this should be robust for huge queries with connection
        //possibly cutting off mid select.
        private final String connectionString;
        private final String rawSelectSql;
        private final ArrayBlockingQueue<Map<String, String>> rows;
        private final int limit;
        private Connection connection;
        private PreparedStatement select;

        public BigSelector(String connectionString, String selectSql, int limit,
                           ArrayBlockingQueue<Map<String, String>> rows) throws SQLException,

                InterruptedException {
            this.connectionString = connectionString;
            this.rawSelectSql = selectSql;
            this.rows = rows;
            this.limit = limit;
            loadConnection();
            prepareSelect();
        }

        private void prepareSelect() throws SQLException {
            String sql = rawSelectSql;
            if (limit > 0) {
                sql += "\nlimit " + limit;
            } else {
                sql += "\nlimit " + DEFAULT_LIMIT;
            }
            sql += "\noffset ?";
            LOGGER.info("SELECT: " + sql);
            if (select != null) {
                try {
                    select.close();
                } catch (SQLException e) {
                    LOGGER.warn("problem closing the select");
                }
            }
            select = connection.prepareStatement(sql);
            select.setFetchSize(FETCH_SIZE);
        }

        private void loadConnection() throws InterruptedException {
            int tries = 0;
            while (tries++ < 5) {
                try {
                    connection = DriverManager.getConnection(connectionString);
                    return;
                } catch (SQLException e) {
                    LOGGER.warn("couldn't connect", e);
                    Thread.sleep(30000);
                }
            }
            throw new RuntimeException("Couldn't load connection after 5 tries");
        }

        @Override
        public Integer call() throws Exception {
            if (limit > 0) {
                trySingleConnection();
            } else {
                iterateAll();
            }
            rows.offer(STOP_SEMAPHORE_MAP, MAX_WAIT_MS, TimeUnit.MILLISECONDS);

            return PIPES_ITERATOR_CODE;
        }

        private void iterateAll() throws SQLException, InterruptedException, TimeoutException {
            int offset = 0;
            //mutable, not multithreaded
            AtomicInteger currOffset = new AtomicInteger(0);
            while (true) {
                try {
                    int selected = selectNext(currOffset);
                    if (selected == 0) {
                        LOGGER.info("added 0 rows. Select is done");
                        return;
                    }
                } catch (SQLException e) {
                    LOGGER.warn("problem selecting", e);
                    loadConnection();
                    prepareSelect();
                }
            }

        }

        private int selectNext(AtomicInteger currOffset) throws InterruptedException,
                SQLException, TimeoutException {
            select.clearParameters();
            select.setInt(1, currOffset.get());
            LOGGER.info("About to select next offset={}", currOffset.get());
            int cnt = 0;
            try (ResultSet rs = select.executeQuery()) {
                LOGGER.info("pg executed query");
                while (rs.next()) {
                    Map<String, String> row = mapify(rs);
                    String k = row.get("id");
                    //if (! esContains(k, esClient)) {
                    LOGGER.debug("About to add: {}", k);
                    boolean offered = rows.offer(row, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (!offered) {
                        throw new TimeoutException();
                    }
                    SELECTED.incrementAndGet();
                    currOffset.incrementAndGet();
                    cnt++;
                }
            }
            return cnt;
        }

        private void trySingleConnection()
                throws SQLException, TimeoutException, InterruptedException {
            LOGGER.info("pg created statement");
            select.clearParameters();
            //set offset = 0
            select.setInt(1, 0);
            try (ResultSet rs = select.executeQuery()) {
                LOGGER.info("pg executed query");
                int cnt = 0;
                while (rs.next()) {
                    Map<String, String> row = mapify(rs);
                    String k = row.get("id");
                    //if (! esContains(k, esClient)) {
                    LOGGER.info("About to add: {}", k);
                    boolean offered = rows.offer(row, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (!offered) {
                        throw new TimeoutException();
                    }
                    SELECTED.incrementAndGet();
                    cnt++;
                    //}
                }
            }
        }
    }

    private static class EmitWorker implements Callable<Integer> {
        private final String esConnectionString;
        private ESClient esClient;
        private final ArrayBlockingQueue<StoredDocument> docs;

        public EmitWorker(String esConnectionString,
                          ArrayBlockingQueue<StoredDocument> docs)
                throws SearchClientException, IOException, InterruptedException {
            this.esConnectionString = esConnectionString;
            this.esClient = getNewClient();
            this.docs = docs;
        }
        private ESClient getNewClient() throws SearchClientException, IOException, InterruptedException {
            int tries = 0;
            if (esClient != null) {
                try {
                    esClient.close();
                } catch (IOException e) {
                    LOGGER.warn("problem trying to close esclient", e);
                }
            }
            while (tries++ < 5) {
                try {
                    return (ESClient) SearchClientFactory.getClient(esConnectionString);
                } catch (SearchClientException | IOException e) {
                    LOGGER.warn("couldn't make es connection", e);
                    Thread.sleep(30000);
                }
            }
            throw new SearchClientException("Couldn't establish connection to es after 5 tries");
        }

        @Override
        public Integer call() throws Exception {
            List<StoredDocument> documentList = new ArrayList<>();
            long lastEmit = -1;
            while (true) {
                StoredDocument sd = docs.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                if (sd == STOP_SEMAPHORE) {
                    if (documentList.size() > 0) {
                        emit(documentList);
                    }
                    docs.offer(STOP_SEMAPHORE);
                    return EMITTER_CODE;
                } else if (sd != null) {
                    documentList.add(sd);
                    //tag the last emit to the first doc added
                    if (lastEmit < 0) {
                        lastEmit = System.currentTimeMillis();
                    }
                } else {
                    LOGGER.warn("sd is null in emitter");
                }

                long elapsed = System.currentTimeMillis() - lastEmit;
                if (documentList.size() > 0 &&
                        (documentList.size() >= 100 ||
                                (lastEmit > -1 && elapsed > EMIT_WITHIN_MS))) {
                    emit(documentList);
                    lastEmit = System.currentTimeMillis();
                }
            }
        }

        private void emit(List<StoredDocument> documentList)
                throws SearchClientException, IOException, InterruptedException {
            int sz = documentList.size();
            LOGGER.info("sending {} docs to es", sz);
            int tries = 0;
            Exception ex = null;
            while (tries++ < MAX_ES_RETRIES) {
                try {
                    esClient.addDocuments(documentList);
                    EMITTED.addAndGet(sz);
                    documentList.clear();
                    return;
                } catch (IOException | SearchClientException e) {
                    LOGGER.warn("failed to send to elastic " + tries, e);
                    ex = e;
                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException exc) {
                        return;
                    }
                    esClient = getNewClient();
                    LOGGER.info("waking up after failing to send to elastic");
                }
            }
            LOGGER.warn("failed to upload docs", ex);
        }
    }

    private static class RowWorker implements Callable<Integer> {

        private final ArrayBlockingQueue<Map<String, String>> rows;
        private final FeatureMapper featureMapper;
        private final Fetcher fetcher;
        private final ArrayBlockingQueue<StoredDocument> docs;

        public RowWorker(ArrayBlockingQueue<Map<String, String>> rows, FeatureMapper featureMapper,
                         Fetcher fetcher, ArrayBlockingQueue<StoredDocument> docs) {
            this.rows = rows;
            this.featureMapper = featureMapper;
            this.fetcher = fetcher;
            this.docs = docs;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                Map<String, String> row = rows.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                if (row == STOP_SEMAPHORE_MAP) {
                    rows.offer(STOP_SEMAPHORE_MAP);
                    return WORKER_CODE;
                } else if (row != null) {
                    StoredDocument sd = new StoredDocument(row.get(FeatureMapper.ID_KEY));
                    featureMapper.addFeatures(row, fetcher, sd);
                    boolean offered = docs.offer(sd, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (!offered) {
                        throw new TimeoutException();
                    }
                    PROCESSED.incrementAndGet();
                }
            }
        }
    }
}
