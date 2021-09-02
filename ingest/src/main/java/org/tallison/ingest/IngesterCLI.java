package org.tallison.ingest;

import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.QueryRequest;
import org.tallison.quaerite.connectors.SearchClientException;
import org.tallison.quaerite.connectors.SearchClientFactory;
import org.tallison.quaerite.core.SearchResultSet;
import org.tallison.quaerite.core.StoredDocument;
import org.tallison.quaerite.core.queries.TermQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class IngesterCLI {

    private static StoredDocument STOP_SEMAPHORE = new StoredDocument(null);
    private static Map<String, String> STOP_SEMAPHORE_MAP = new HashMap<>();
    private static final long MAX_WAIT_MS = 1200000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterCLI.class);
    private static final AtomicInteger CONSIDERED = new AtomicInteger(0);
    private static AtomicInteger EMITTED = new AtomicInteger(0);
    private static final int MAX_ES_RETRIES = 3;

    private static final int WORKER_CODE = 0;
    private static final int EMITTER_CODE = 1;
    private static final int PIPES_ITERATOR_CODE = 2;
    private static final int REPORTER_CODE = 2;

    public static void main(String[] args) throws Exception {

        Connection pg = DriverManager.getConnection(args[0]);
        pg.setAutoCommit(false);//necessary for select pagination
        String esConnectionString = args[1];
        Fetcher fetcher = FetcherManager.load(Paths.get(args[2])).getFetcher("file-obs-fetcher");
        CompositeFeatureMapper compositeFeatureMapper = new CompositeFeatureMapper();
        String sql = getSelectStar();
        int numWorkers = 50;
        int numEmitters = 10;
        ArrayBlockingQueue<Map<String, String>> rows = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<StoredDocument> docs = new ArrayBlockingQueue<>(500);

        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers+numEmitters+2);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        for (int i = 0; i < numWorkers; i++) {
            executorCompletionService.submit(new RowWorker(rows, compositeFeatureMapper, fetcher, docs));
        }
        for (int i = 0; i < numEmitters; i++) {
            executorCompletionService.submit(new EmitWorker(
                    (ESClient) SearchClientFactory.getClient(esConnectionString), docs));
        }
        executorCompletionService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                ESClient esClient = (ESClient)SearchClientFactory.getClient(esConnectionString);
                try (Statement st = pg.createStatement()) {
                    st.setFetchSize(100);
                    LOGGER.info("pg created statement");
                    try (ResultSet rs = st.executeQuery(sql)) {
                        LOGGER.info("pg executed query");
                        int cnt = 0;
                        while (rs.next()) {
                            Map<String, String> row = mapify(rs);
                            String k = row.get("fname");
                            CONSIDERED.incrementAndGet();
                            if (! esContains(k, esClient)) {
                                LOGGER.info("About to add: {}", k);
                                boolean offered = rows.offer(row, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                                if (!offered) {
                                    throw new TimeoutException();
                                }
                                cnt++;
                            }
                        }
                    }
                }
                for (int i = 0; i < numWorkers; i++) {
                    rows.offer(STOP_SEMAPHORE_MAP, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                }
                return PIPES_ITERATOR_CODE;
            }
        });
        Callable<Integer> reporter = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                long start = System.currentTimeMillis();
               try {
                   while (true) {
                       long elapsed = System.currentTimeMillis() - start;
                       LOGGER.info("considered {}, emitted {} docs in {}ms",
                               CONSIDERED.get(), EMITTED.get(), elapsed);
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
                    if (workerCode == PIPES_ITERATOR_CODE) {
                        pipesIteratorFinished = true;
                    } else if (workerCode == WORKER_CODE) {
                        finishedWorkers++;
                    } else if (workerCode == REPORTER_CODE) {
                        //ignore
                    } else {
                        throw new IllegalArgumentException("shouldn't have finished: " + workerCode);
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
            LOGGER.info("finished indexing {} in {}ms",
                    EMITTED.get(), elapsed);
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
        } catch (SearchClientException|IOException e) {
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

    private static String getSelectStar() throws IOException {
        return IOUtils.toString(
                IngesterCLI.class.getResourceAsStream("/selectStar-lite.sql"),
                StandardCharsets.UTF_8.name());
    }

    private static class EmitWorker implements Callable<Integer> {
        private final ESClient esClient;
        private final ArrayBlockingQueue<StoredDocument> docs;

        public EmitWorker(ESClient esClient, ArrayBlockingQueue<StoredDocument> docs) {
            this.esClient = esClient;
            this.docs = docs;
        }

        @Override
        public Integer call() throws Exception {
            List<StoredDocument> documentList = new ArrayList<>();
            while (true) {
                StoredDocument sd = docs.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                if (sd == STOP_SEMAPHORE) {
                    if (documentList.size() > 0) {
                        emit(documentList);
                    }
                    return EMITTER_CODE;
                } else if (sd != null) {
                    documentList.add(sd);
                }
                if (documentList.size() >= 100) {
                    emit(documentList);
                }
            }
        }

        private void emit(List<StoredDocument> documentList) {
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
                } catch (IOException|SearchClientException e) {
                    LOGGER.warn("failed to send to elastic " + tries, e);
                    ex = e;
                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException exc) {
                        return;
                    }
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

        public RowWorker(ArrayBlockingQueue<Map<String, String>> rows,
                         FeatureMapper featureMapper, Fetcher fetcher,
                         ArrayBlockingQueue<StoredDocument> docs) {
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
                    return WORKER_CODE;
                } else if (row != null) {
                    StoredDocument sd = new StoredDocument(row.get(FeatureMapper.ID_KEY));
                    featureMapper.addFeatures(row, fetcher, sd);
                    boolean offered = docs.offer(sd, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (! offered) {
                        throw new TimeoutException();
                    }
                }
            }

        }
    }
}
