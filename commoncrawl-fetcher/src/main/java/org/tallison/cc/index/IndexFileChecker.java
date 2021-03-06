package org.tallison.cc.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.tallison.util.MapUtil;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class IndexFileChecker {

    static Logger LOGGER = Logger.getLogger(IndexFileChecker.class);

    private static int DEFAULT_NUM_THREADS = 10;
    private static Path POISON = Paths.get("");
    private static Options getOptions() {

        Options options = new Options();

        options.addRequiredOption("i", "indexDirectory", true,
                "directory with the gz index files");
        options.addOption("n", "numThreads", true,
                "number of threads.  Don't use more than 3!");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);

        Path indexDirectory = Paths.get(line.getOptionValue("i"));

        int numThreads = DEFAULT_NUM_THREADS;
        if (line.hasOption("n")) {
            numThreads = Integer.parseInt(line.getOptionValue("n"));
        }

        Path filterFile = null;
        if (line.hasOption("f")) {
            filterFile = Paths.get(line.getOptionValue("f"));
        }
        IndexFileChecker counter = new IndexFileChecker();
        counter.execute(indexDirectory, filterFile, numThreads);
    }

    private void execute(Path indexDirectory,
                         Path filterFile, int numThreads)
            throws SQLException, IOException {

        RecordFilter filter = CompositeRecordFilter.load(filterFile);

        long start = System.currentTimeMillis();
        AtomicLong totalProcessed = new AtomicLong(0);
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);
            ArrayBlockingQueue<Path> paths = new ArrayBlockingQueue<>(300+numThreads);
            addPaths(indexDirectory, paths, numThreads);

            for (int i = 0; i < numThreads; i++) {
                completionService.submit(new GzipFileChecker(paths, totalProcessed));
            }

            int finished = 0;
            while (finished < numThreads) {
                Future<Integer> future = completionService.poll(3, TimeUnit.MINUTES);
                if (future != null) {
                    finished++;
                    future.get();
                }
            }
            executorService.shutdownNow();
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeException(e);
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("processed " + totalProcessed.get() + " records "+
                " and indexed " + PGIndexer.getAdded() + " in " +elapsed+" ms");

    }


    private void addPaths(Path indexDirectory,
                            ArrayBlockingQueue<Path> paths,
                            int numThreads) throws InterruptedException {

        for (File f : indexDirectory.toFile().listFiles()) {
            if (! POISON.equals(f.toPath())) {
                paths.offer(f.toPath(), 3, TimeUnit.MINUTES);
            }
        }

        LOGGER.trace("about to add poison");
        for (int i = 0; i < numThreads; i++) {
            paths.offer(POISON, 3, TimeUnit.MINUTES);
        }
        LOGGER.trace("finished adding poison");
    }

    private static class GzipFileChecker implements Callable<Integer> {

        private final ArrayBlockingQueue<Path> paths;
        private final AtomicLong totalProcessed;

        GzipFileChecker(ArrayBlockingQueue<Path> paths, AtomicLong processed) {
            this.paths = paths;
            this.totalProcessed = processed;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                Path path = paths.poll(3, TimeUnit.MINUTES);
                if (path == null) {
                    throw new TimeoutException("waited 3 minutes for a new record");
                }

                if (path == POISON) {

                    return 1;
                }
                LOGGER.trace(path);
                processFile(path);
            }
        }

        private void processFile(Path path) {
            long processed = totalProcessed.incrementAndGet();

            try (InputStream is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(path)))) {
                byte[] buffer = new byte[20000];
                int read = is.read(buffer);
                while (read > -1) {
                    read = is.read(buffer);
                }
            } catch (IOException e) {
                LOGGER.warn("problem with: "+path.getFileName());
            }
            LOGGER.info("success: "+path.getFileName());
        }
    }
}
