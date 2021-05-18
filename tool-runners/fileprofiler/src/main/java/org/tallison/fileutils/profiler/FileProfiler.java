package org.tallison.fileutils.profiler;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

    private static long TIMEOUT_MILLIS = 600000;
    private final Connection connection;
    private final ConfigSrc configSrc;
    private final int numThreads;
    public FileProfiler(Connection connection, ConfigSrc configSrc, int numThreads) {
        this.connection = connection;
        this.configSrc = configSrc;
        this.numThreads = numThreads;
    }

    private void execute()
            throws Exception {
        createTable(connection);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads+1);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        PipesIterator pipesIterator = PipesIterator.build(configSrc.getTikaConfigPath());
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(1000);

        executorCompletionService.submit(new FetchIteratorWrapper(queue, pipesIterator));
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new PrimaryProfiler(queue, configSrc, connection));
        }

        int completed = 0;
        while (completed < numThreads+1) {
            Future<Integer> future = executorCompletionService.take();
            future.get();
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

        PreparedStatement insert;
        PrimaryProfiler(ArrayBlockingQueue<FetchEmitTuple> queue,
                        ConfigSrc configSrc, Connection connection)
                throws SQLException, IOException, TikaException {
            super(queue, configSrc);
            String insertSql = "insert into profiles values (?,?,?,?)";
            insert = connection.prepareStatement(insertSql);
        }

        @Override
        public void process(FetchEmitTuple fetchEmitTuple) throws IOException {

            try (InputStream is =
                         configSrc.getFetcher().fetch(fetchEmitTuple.getFetchKey().getFetchKey(),
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
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            } catch (TikaException e) {
                e.printStackTrace();
            }
        }

    }

    private static class FetchIteratorWrapper implements Callable<Integer> {
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final PipesIterator pipesIterator;
        private FetchIteratorWrapper(ArrayBlockingQueue<FetchEmitTuple> queue,
                                     PipesIterator pipesIterator) {
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
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(
                System.getenv(ConfigSrc.METADATA_WRITER_STRING));
        Path tikaConfigPath = Paths.get(System.getenv(ConfigSrc.TIKA_CONFIG));
        int numThreads = Integer.parseInt(System.getenv(ConfigSrc.NUM_THREADS));
        ConfigSrc configSrc = ConfigSrc.build(args, "file-profiler", 1000, 1000);
        FileProfiler fp = new FileProfiler(connection, configSrc, numThreads);
        fp.execute();
    }
}
