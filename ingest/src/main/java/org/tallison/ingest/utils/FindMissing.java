package org.tallison.ingest.utils;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.tallison.ingest.CompositeFeatureMapper;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.QueryRequest;
import org.tallison.quaerite.connectors.SearchClientException;
import org.tallison.quaerite.connectors.SearchClientFactory;
import org.tallison.quaerite.core.SearchResultSet;
import org.tallison.quaerite.core.StoredDocument;
import org.tallison.quaerite.core.queries.TermQuery;
import org.tallison.quaerite.core.queries.TermsQuery;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

public class FindMissing {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindMissing.class);
    private static final AtomicInteger CONSIDERED = new AtomicInteger(0);
    private static AtomicInteger EMITTED = new AtomicInteger(0);
    private static final int MAX_ES_RETRIES = 3;

    public static void main(String[] args) throws Exception {
        String pgString = "";
        String esString = "";
        Connection pg = DriverManager.getConnection(pgString);
        pg.setAutoCommit(false);//necessary for select pagination

        ESClient esClient = (ESClient)SearchClientFactory.getClient(esString);
        String sql = "select path from profiles";
        List<String> ids = new ArrayList<>();
        try (Statement st = pg.createStatement()) {
            st.setFetchSize(100);
            LOGGER.info("pg created statement");
            try (ResultSet rs = st.executeQuery(sql)) {
                LOGGER.info("pg executed query");
                int cnt = 0;
                while (rs.next()) {
                    String idString = rs.getString(1);
                    idString = "S3_BUCKET_HERE"+idString;
                    ids.add(idString);
                    CONSIDERED.incrementAndGet();
                    if (CONSIDERED.get() % 10000 == 0) {
                        LOGGER.info("considered {}", CONSIDERED.get());
                    }
                    if (ids.size() >= 100) {
                        TermsQuery tq = new TermsQuery("_id", ids);
                        QueryRequest queryRequest = new QueryRequest(tq);
                        queryRequest.setStart(0);
                        queryRequest.setNumResults(1000);
                        SearchResultSet resultSet = esClient.search(queryRequest);
                        if (resultSet.size() == ids.size()) {
                            //all good
                        } else {
                            System.out.println("W00t!");
                            for (String id : ids) {
                                if (!resultSet.getIds().contains(id)) {
                                    System.out.println("missing: " + id);
                                }
                            }
                        }
                        ids.clear();
                    }

                }
            }
        }
    }
}
