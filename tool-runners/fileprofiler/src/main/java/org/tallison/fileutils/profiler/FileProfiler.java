package org.tallison.fileutils.profiler;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


public class FileProfiler {

    private static final Logger LOG = LoggerFactory.getLogger(FileProfiler.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private static long TIMEOUT_MILLIS = 600000;
    private final Connection connection;
    private final Path tikaConfig;
    private final int numThreads;
    public FileProfiler(Connection connection, Path tikaConfig, int numThreads) {
        this.connection = connection;
        this.tikaConfig = tikaConfig;
        this.numThreads = numThreads;
    }

    private void execute()
            throws Exception {
        createTable(connection);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads+1);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        PipesIterator pipesIterator = PipesIterator.build(tikaConfig);
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(1000);

        executorCompletionService.submit(new FetchIteratorWrapper(numThreads, queue,
                pipesIterator));
        Fetcher fetcher = FetcherManager.load(tikaConfig).getFetcher(pipesIterator.getFetcherName());
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new PrimaryProfiler(queue, fetcher, connection));
        }

        int completed = 0;
        while (completed < numThreads+1) {
            Future<Integer> future = executorCompletionService.take();
            Integer i = future.get();
            LOG.info("finished {}", i);
            completed++;
        }
        executorService.shutdownNow();
    }

    private void createTable(Connection connection) throws SQLException {
        String sql = "drop table if exists profiles";
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
            sql = "create table profiles (" +
                    "path varchar(10000)," +
                    "collection varchar(256),"+
                    "size bigint,"+
                    "shasum256 varchar(64))";
            st.execute(sql);
        }
    }

    private class PrimaryProfiler extends AbstractFileProcessor {

        private final PreparedStatement insert;
        private final Fetcher fetcher;
        PrimaryProfiler(ArrayBlockingQueue<FetchEmitTuple> queue,
                        Fetcher fetcher, Connection connection)
                throws SQLException, IOException, TikaException {
            super(queue, null);
            this.fetcher = fetcher;
            String insertSql = "insert into profiles values (?,?,?,?)";
            insert = connection.prepareStatement(insertSql);
        }

        @Override
        public void process(FetchEmitTuple fetchEmitTuple) throws IOException {
            long start = System.currentTimeMillis();
            try (InputStream is =
                         fetcher.fetch(fetchEmitTuple.getFetchKey().getFetchKey(),
                    new Metadata());
                 TikaInputStream tis = TikaInputStream.get(is)) {
                try {
                    Path file = tis.getPath();

                    long sz = Files.size(file);
                    String digest = "";
                    try (InputStream stream = Files.newInputStream(file)) {
                        digest = DigestUtils.sha256Hex(is);
                    }
                    Path rel = Paths.get(fetchEmitTuple.getFetchKey().getFetchKey());
                    String collection = rel.getName(0).toString();
                    String path = rel.toString();
                    insert.clearParameters();
                    insert.setString(1, path);
                    insert.setString(2, collection);
                    insert.setLong(3, sz);
                    insert.setString(4, digest);
                    insert.execute();
                    long elapsed = System.currentTimeMillis() - start;
                    LOG.info("added {}, length {}, digest {}, for {} docs total in {} ms",
                            path, sz, digest, COUNTER.getAndIncrement(), elapsed);
                } catch (SQLException e) {
                    LOG.error("sql exception ", e);
                    throw new IOException(e);
                }
            } catch (TikaException e) {
                e.printStackTrace();
                LOG.warn("tika exception", e);
            }
        }

    }

    private static class FetchIteratorWrapper implements Callable<Integer> {
        private final int numThreads;
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final PipesIterator pipesIterator;
        private FetchIteratorWrapper(int numThreads, ArrayBlockingQueue<FetchEmitTuple> queue,
                                     PipesIterator pipesIterator) {
            this.numThreads = numThreads;
            this.queue = queue;
            this.pipesIterator = pipesIterator;
        }
        @Override
        public Integer call() throws Exception {
            for (FetchEmitTuple t : pipesIterator) {
                boolean offered = queue.offer(t, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (offered == false) {
                    throw new TimeoutException();
                }
            }
            for (int i = 0; i < numThreads; i++) {
                boolean offered = queue.offer(PipesIterator.COMPLETED_SEMAPHORE, TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS);
                LOG.info("adding completed semaphore "+ i);
                if (offered == false) {
                    throw new TimeoutException();
                }
            }
            LOG.info("finished adding files and completed semaphore for processing");
            return 2;
        }
    }

    public static void main(String[] args) throws Exception {
        String cString = System.getenv(ConfigSrc.METADATA_WRITER_STRING);
        LOG.debug("CString: "+ cString);
        Connection connection = DriverManager.getConnection(
                cString);
        Path tikaConfigPath = Paths.get(System.getenv(ConfigSrc.TIKA_CONFIG));
        int numThreads = Integer.parseInt(System.getenv(ConfigSrc.NUM_THREADS));
        FileProfiler fp = new FileProfiler(connection, tikaConfigPath, numThreads);
        fp.execute();
    }
}
