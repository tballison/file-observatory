package org.tallison.ingest;

import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.SearchClientFactory;
import org.tallison.quaerite.core.StoredDocument;

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

public class IngesterCLI {

    private static StoredDocument STOP_SEMAPHORE = new StoredDocument(null);
    private static Map<String, String> STOP_SEMAPHORE_MAP = new HashMap<>();
    private static final long MAX_WAIT_MS = 300000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterCLI.class);

    public static void main(String[] args) throws Exception {
        Connection pg = DriverManager.getConnection(args[0]);
        String esConnectionString = args[1];
        Fetcher fetcher = new TikaConfig(Paths.get(args[2])).getFetcherManager()
                .getFetcher("file-obs-fetcher");
        CompositeFeatureMapper compositeFeatureMapper = new CompositeFeatureMapper();
        String sql = getSelectStar();
        int numWorkers = 10;
        int numEmitters = 10;
        ArrayBlockingQueue<Map<String, String>> rows = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<StoredDocument> docs = new ArrayBlockingQueue<>(500);

        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers+numEmitters+1);
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
                try (Statement st = pg.createStatement()) {
                    try (ResultSet rs = st.executeQuery(sql)) {
                        while (rs.next()) {
                            Map<String, String> row = mapify(rs);
                            boolean offered = rows.offer(row, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                            if (! offered) {
                                throw new TimeoutException();
                            }
                        }
                    }
                }
                for (int i = 0; i < numWorkers; i++) {
                    rows.offer(STOP_SEMAPHORE_MAP, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                }
                return 1;
            }
        });

        int finished = 0;
        try {
            while (finished < numWorkers + 1) {
                Future<Integer> future = executorCompletionService.take();
                if (future != null) {
                    future.get();
                    finished++;
                }
                if (finished == numWorkers) {
                    for (int i = 0; i < numEmitters; i++) {
                        docs.offer(STOP_SEMAPHORE, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } finally {
            executorService.shutdownNow();
        }

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
                    esClient.addDocuments(documentList);
                    return 1;
                } else if (sd != null) {
                    documentList.add(sd);
                }
                if (documentList.size() >= 100) {
                    LOGGER.info("sending {} docs to es",documentList.size());
                    esClient.addDocuments(documentList);
                    documentList.clear();
                }
            }
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
                    return 1;
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
