package org.tallison.tika.pipes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.Field;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.PipesReporter;
import org.apache.tika.pipes.PipesResult;

public class TikaPipesReporter extends PipesReporter implements Initializable {

    private static Logger LOGGER = LoggerFactory.getLogger(TikaPipesReporter.class);

    private static String TABLE_NAME = "tika";
    private static ReportData STOP_SEMAPHORE = new ReportData(null, null, -1);
    private static boolean IS_DELTA = false;
    private static int MAX_PATH_LENGTH = 1024;
    private static int MAX_STDERR = 1024;
    private static boolean IS_POSTGRES = true;
    private static int BATCH_SIZE = 1000;
    private String connectionString;
    private ExecutorService executorService;
    private ExecutorCompletionService<Integer> executorCompletionService;
    private ArrayBlockingQueue<ReportData> queue = new ArrayBlockingQueue<>(1000);

    @Override
    public void report(FetchEmitTuple fetchEmitTuple, PipesResult pipesResult, long elapsed) {
        Future<Integer> future = executorCompletionService.poll();
        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            throw new RuntimeException("don't call report after close?!");
        }

        try {
            queue.offer(new ReportData(fetchEmitTuple, pipesResult, elapsed), 5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        queue.offer(STOP_SEMAPHORE);
        LOGGER.debug("in close");
        //hang forever
        Future<Integer> future = null;
        try {
            future = executorCompletionService.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOGGER.debug("in close -- future taken");
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new IOException(e);
        } finally {
            LOGGER.debug("in close -- finally");
            executorService.shutdownNow();
        }
        LOGGER.debug("in close -- final after shutdown");
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public void initialize(Map<String, Param> map) throws TikaConfigException {

        executorService = Executors.newFixedThreadPool(1);
        executorCompletionService = new ExecutorCompletionService<>(executorService);
        try {
            executorCompletionService.submit(new Reporter(queue, connectionString));
        } catch (SQLException e) {
            throw new TikaConfigException("can't create reporter", e);
        }
    }

    @Override
    public void checkInitialization(InitializableProblemHandler initializableProblemHandler)
            throws TikaConfigException {

    }

    private static class Reporter implements Callable<Integer> {
        private final ArrayBlockingQueue<ReportData> queue;
        private PreparedStatement insert;
        private final String connectionString;
        private Connection connection;
        private int reportsSent = 0;

        public Reporter(ArrayBlockingQueue<ReportData> queue, String connectionString)
                throws SQLException {
            this.queue = queue;
            this.connectionString = connectionString;
            this.connection = getNewConnection(connectionString);
            createTable(connection);
            insert = getNewInsert(connection);
        }

        private static Connection getNewConnection(String connectionString) throws SQLException {
            Connection connection = DriverManager.getConnection(connectionString);
            connection.setAutoCommit(false);
            return connection;
        }

        private static PreparedStatement getNewInsert(Connection connection) throws SQLException {
            String sql = "insert into " + TABLE_NAME + " values (?,?,?,?,?,?,?);";
            return connection.prepareStatement(sql);
        }

        private static void createTable(Connection connection) throws SQLException {
            String sql;
            if (!IS_DELTA) {
                LOGGER.info("not delta; dropping table " + TABLE_NAME);
                sql = "drop table if exists " + TABLE_NAME;
                connection.createStatement().execute(sql);
            }
            if (!tableExists(connection, TABLE_NAME)) {
                LOGGER.info("table does not exist. creating a new one");
                sql = "create table " + TABLE_NAME + " (" + "path varchar(" + MAX_PATH_LENGTH +
                        ") primary key," + "exit_value integer," + "timeout boolean," +
                        "process_time_ms BIGINT," + "stderr varchar(" + MAX_STDERR + ")," +
                        "stderr_length bigint," + "stderr_truncated boolean)";
                connection.createStatement().execute(sql);
                connection.commit();
            }
        }

        private static boolean tableExists(Connection connection, String table)
                throws SQLException {
            Savepoint savepoint = connection.setSavepoint();
            try {
                try (Statement st = connection.createStatement();
                     ResultSet rs = st.executeQuery("select * from " + table + " limit 1")) {
                    while (rs.next()) {

                    }
                    return true;
                }
            } catch (SQLException e) {
                connection.rollback(savepoint);
                return false;
            }
        }

        private static String clean(String s, int maxLength) {
            if (s == null) {
                return "";
            }
            if (IS_POSTGRES) {
                s = s.replaceAll("\u0000", " ");
            }
            if (s.length() > maxLength) {
                s = s.substring(0, maxLength);
            }
            return s;
        }

        @Override
        public Integer call() throws Exception {
            List<ReportData> reports = new ArrayList<>();

            while (true) {
                ReportData reportData = queue.poll(1, TimeUnit.SECONDS);
                if (reportData == null) {
                    continue;
                } else if (reportData == STOP_SEMAPHORE) {
                    sendReports(reports);
                    reports.clear();
                    return 1;
                } else {
                    reports.add(reportData);
                    if (reports.size() >= BATCH_SIZE) {
                        sendReports(reports);
                        reports.clear();
                    }
                }
            }
        }

        private void sendReports(List<ReportData> reportData) throws InterruptedException {
            int tries = 0;
            while (tries++ < 10) {
                try {
                    trySendReports(reportData);
                    return;
                } catch (SQLException e) {
                    LOGGER.warn("failed to send reports; trying to start new connection after a " +
                            "sleep", e);
                    tryReconnect();
                }
            }
            LOGGER.error("Couldn't write to db. Shutting down");
            throw new RuntimeException("Couldn't write to db. Shutting down now.");
        }

        private void tryReconnect() throws InterruptedException {
            Thread.sleep(10000);
            try {
                insert.clearBatch();
                insert.close();
            } catch (SQLException e2) {
                LOGGER.info("problem closing insert", e2);
            }

            try {
                connection.close();
            } catch (SQLException e2) {
                //whatevs this should not be surprising
                LOGGER.warn("failed to close connection", e2);
            }

            try {
                connection = getNewConnection(connectionString);
                insert = getNewInsert(connection);
                LOGGER.info("successfully got new connection");
            } catch (SQLException e2) {
                LOGGER.warn("failed to get new connection", e2);
            }

        }

        private void trySendReports(List<ReportData> reportData) throws SQLException {
            for (ReportData d : reportData) {
                addReport(d);
            }
            insert.executeBatch();
            connection.commit();
            reportsSent = 0;
        }

        private void addReport(ReportData reportData) throws SQLException {

            int mockExitCode = getPseudoExitCode(reportData);
            boolean stderrTruncated = false;

            String stderr = reportData.pipesResult.getMessage();
            int stderrLength = (stderr == null) ? 0 : stderr.length();

            if (stderr == null) {
                stderr = "";
            } else if (stderrLength > MAX_STDERR) {
                stderr = stderr.substring(0, MAX_STDERR);
                stderrTruncated = true;
            }
            boolean timeout =
                    (reportData.pipesResult.getStatus() == PipesResult.STATUS.TIMEOUT) ? true :
                            false;

            int col = 0;
            insert.clearParameters();
            insert.setString(++col, clean(reportData.t.getId(), MAX_PATH_LENGTH));
            insert.setInt(++col, mockExitCode);
            insert.setBoolean(++col, timeout);
            insert.setLong(++col, reportData.elapsed);
            insert.setString(++col, clean(stderr, MAX_STDERR));
            insert.setInt(++col, stderrLength);
            insert.setBoolean(++col, stderrTruncated);
            insert.addBatch();
        }

        private int getPseudoExitCode(ReportData reportData) {
            PipesResult.STATUS status = reportData.pipesResult.getStatus();
            switch (status) {
                case PARSE_SUCCESS:
                case EMIT_SUCCESS:
                    return 0;
                case EMIT_SUCCESS_PARSE_EXCEPTION:
                case PARSE_SUCCESS_WITH_EXCEPTION:
                case PARSE_EXCEPTION_NO_EMIT:
                    return 1;
                case OOM:
                    return 2;
                case TIMEOUT:
                    return 3;
                case EMPTY_OUTPUT:
                    return 4;
                case EMIT_EXCEPTION:
                    return 5;
                case FETCH_EXCEPTION:
                    return 6;
                case NO_EMITTER_FOUND:
                    return 7;
                case NO_FETCHER_FOUND:
                    return 8;
                case UNSPECIFIED_CRASH:
                    return 9;
                case PARSE_EXCEPTION_EMIT:
                    return 10;
                case INTERRUPTED_EXCEPTION:
                    return 11;
                case CLIENT_UNAVAILABLE_WITHIN_MS:
                    return 12;
                case FETCHER_INITIALIZATION_EXCEPTION:
                    return 13;
                default:
                    return 14;
            }
        }

    }

    private static class ReportData {
        final FetchEmitTuple t;
        final PipesResult pipesResult;
        final long elapsed;

        public ReportData(FetchEmitTuple t, PipesResult pipesResult, long elapsed) {
            this.t = t;
            this.pipesResult = pipesResult;
            this.elapsed = elapsed;
        }
    }

    @Field
    public void setIsDelta(boolean isDelta) {
        IS_DELTA = isDelta;
    }
}
