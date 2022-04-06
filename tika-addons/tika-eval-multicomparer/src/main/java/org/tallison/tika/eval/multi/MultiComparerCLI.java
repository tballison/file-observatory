package org.tallison.tika.eval.multi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.ConfigBase;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;

public class MultiComparerCLI extends ConfigBase implements Initializable {

    public static final String TABLE_NAME = "multi_comparisons";
    private static Logger LOGGER = LoggerFactory.getLogger(MultiComparerCLI.class);

    public enum EXTRACT_STATUS {
        MISSING,
        EMPTY,
        PARSE_EXCEPTION,
        TOO_LONG,
        OK
    }
    public static void main(String[] args) throws Exception {
        Path configPath = Paths.get(args[0]);
        Class.forName("org.postgresql.Driver");
        boolean isDelta = args.length > 1;
        PipesIterator pipesIterator = PipesIterator.build(configPath);
        FetcherManager fetcherManager = FetcherManager.load(configPath);
        MultiComparerCLI multiComparerCLI = MultiComparerCLI.load(configPath);

        multiComparerCLI.execute(pipesIterator, fetcherManager, isDelta);
    }

    public static MultiComparerCLI load(Path configPath) throws IOException, TikaConfigException {
        try (InputStream is =
                     Files.newInputStream(configPath)) {
            return MultiComparerCLI.buildSingle("multicompare", MultiComparerCLI.class, is);
        }
    }

    private String connectionString = null;
    //parallel arrays tool name + extension -- we need to clean this up later
    private List<String> tools = new ArrayList<>();
    private List<String> extensions = new ArrayList<>();
    private long maxInputStreamLengthBytes = 100_000_000;
    private int numThreads = 10;

    private void execute(PipesIterator pipesIterator, FetcherManager fetcherManager,
                         boolean isDelta) throws Exception {
        checkFetchers(fetcherManager);
        Connection connection = DriverManager.getConnection(connectionString);
        createTable(connection, isDelta);
        ArrayBlockingQueue<FetchEmitTuple> tuples = new ArrayBlockingQueue<>(1000);
        Enqueuer enqueuer = new Enqueuer(pipesIterator, tuples);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        executorCompletionService.submit(enqueuer);
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new MultiCompareWorker(connectionString, tuples,
                    fetcherManager, tools, extensions, maxInputStreamLengthBytes));
        }
        int finished = 0;
        try {
            while (finished < numThreads + 1) {
                Future<Integer> future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private void checkFetchers(FetcherManager fetcherManager) throws Exception {
        for (String tool : tools) {
            Fetcher f = fetcherManager.getFetcher(tool);
            if (f == null) {
                throw new IllegalArgumentException("I regret I didn't find a fetcher with name=" + tool);
            }
        }

    }

    private void createTable(Connection connection, boolean isDelta) throws SQLException {
        if (isDelta) {
            return;
        }
        String sql = "drop table if exists " + TABLE_NAME;
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
        sql = "create table " + TABLE_NAME + "(" + "path varchar(512) primary key";
        for (int i = 0; i < tools.size(); i++) {
            sql += ", extract_status_" + i + " integer,";
            sql += " num_tokens_" + i + " integer,";
            sql += " num_alpha_tokens_" + i + " integer,";
            sql += " num_common_tokens_" + i + " integer,";
            sql += " detected_lang_" + i + " varchar(12),";
            sql += " oov_" + i + " float";
        }
        for (int i = 0; i < tools.size() - 1; i++) {
            for (int j = i + 1; j < tools.size(); j++) {
                sql += ", overlap_tool_" + i + "_v_" + j + " float";
                sql += ", dice_tool_" + i + "_v_" + j + " float";
            }
        }
        sql += ")";

        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
        sql = "drop table if exists multi_compare_tools";
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
        sql = "create table multi_compare_tools (tool_name varchar (64) primary key);";
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        }
        try (Statement st = connection.createStatement()) {
            for (String t : tools) {
                sql = "insert into multi_compare_tools values ('" + t + "')";
                st.execute(sql);
            }
        }
    }

    public void setTools(List<String> tools) {
        this.tools = Collections.unmodifiableList(new ArrayList<>(tools));
    }

    public void setExtensions(List<String> extensions) {
        this.extensions = Collections.unmodifiableList(new ArrayList<>(extensions));
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public void setMaxInputStreamLengthBytes(long maxInputStreamLengthBytes) {
        this.maxInputStreamLengthBytes = maxInputStreamLengthBytes;
    }
    @Override
    public void initialize(Map<String, Param> params) throws TikaConfigException {
        //no-op
    }

    @Override
    public void checkInitialization(InitializableProblemHandler problemHandler)
            throws TikaConfigException {
        if (StringUtils.isBlank(connectionString)) {
            throw new TikaConfigException("must specify a jdbcConnection");
        }
        if (tools.size() == 0) {
            throw new TikaConfigException("must specify a list of tools");
        }
    }

    private class Enqueuer implements Callable<Integer> {
        private final PipesIterator pipesIterator;
        private final ArrayBlockingQueue<FetchEmitTuple> tuples;
        public Enqueuer(PipesIterator pipesIterator, ArrayBlockingQueue<FetchEmitTuple> tuples) {
            this.pipesIterator = pipesIterator;
            this.tuples = tuples;
        }

        @Override
        public Integer call() throws Exception {
            int offered = 0;
            for (FetchEmitTuple tuple : pipesIterator) {
                if (offered % 1000 == 0) {
                    LOGGER.info("enqueuer offered: {}", offered);
                }
                //TODO: time this out with offer
                tuples.put(tuple);
                offered++;
            }
            tuples.offer(PipesIterator.COMPLETED_SEMAPHORE);
            LOGGER.info("done offering {}", offered);
            return 1;
        }
    }
}
