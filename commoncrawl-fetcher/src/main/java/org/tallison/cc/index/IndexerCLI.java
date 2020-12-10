package org.tallison.cc.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
import java.util.zip.GZIPInputStream;

public class IndexerCLI {

    static Logger LOGGER = Logger.getLogger(IndexerCLI.class);

    private static int DEFAULT_NUM_THREADS = 10;
    private static String POISON = "POISON_POISON_POISON";
    private static Options getOptions() {

        Options options = new Options();

        options.addRequiredOption("i", "indexDirectory", true,
                "directory with the gz index files");
        options.addRequiredOption("j", "jdbc", true,
                "jdbc connection string to the pg db");
        options.addOption("m", "max", true,
                "maximum number of records to index");
        options.addOption("n", "numThreads", true,
                "number of threads.  Don't use more than 3!");
        options.addOption("f", "filterFile",
                true,
                "json file that describes filters");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);

        Path indexDirectory = Paths.get(line.getOptionValue("i"));
        String jdbc = line.getOptionValue("j");

        int numThreads = DEFAULT_NUM_THREADS;
        if (line.hasOption("n")) {
            numThreads = Integer.parseInt(line.getOptionValue("n"));
        }
        int max = -1;
        if (line.hasOption("m")) {
            max = Integer.parseInt(line.getOptionValue("m"));
        }
        Path filterFile = null;
        if (line.hasOption("f")) {
            filterFile = Paths.get(line.getOptionValue("f"));
        }
        IndexerCLI indexer = new IndexerCLI();
        Connection connection = DriverManager.getConnection(jdbc);
        try {
            indexer.execute(indexDirectory, connection, filterFile, numThreads, max);
        } finally {
            connection.commit();
            connection.close();
        }
    }

    private void execute(Path indexDirectory, Connection connection,
                         Path filterFile, int numThreads, int max)
            throws SQLException, IOException {
        PGIndexer.init(connection);

        RecordFilter filter = CompositeRecordFilter.load(filterFile);

        long start = System.currentTimeMillis();
        int crawled = 0;
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);
            ArrayBlockingQueue<String> records = new ArrayBlockingQueue<>(10000);

            for (int i = 0; i < numThreads; i++) {
                completionService.submit(new CallableIndexer(records, new PGIndexer(connection, filter)));
            }
            crawled = crawlFiles(indexDirectory, records, numThreads, max);

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
        } finally {
            PGIndexer.shutDown();
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("processed " + crawled + " records "+
                " and indexed " + PGIndexer.getAdded() + " in " +elapsed+" ms");
    }

    private int crawlFiles(Path indexDirectory,
                            ArrayBlockingQueue<String> records,
                            int numThreads, int max) throws InterruptedException {
        AtomicInteger totalProcessed = new AtomicInteger(0);
        for (File f : indexDirectory.toFile().listFiles()) {
            processFile(f.toPath(), records, totalProcessed, max);
        }

        LOGGER.trace("about to add poison");
        for (int i = 0; i < numThreads; i++) {
            records.offer(POISON, 3, TimeUnit.MINUTES);
        }
        LOGGER.trace("finished adding poison");
        return totalProcessed.get();
    }

    private void processFile(Path p, ArrayBlockingQueue<String> records,
                             AtomicInteger totalProcessed, int max) {
        if (max > -1 && totalProcessed.get() >= max) {
            return;
        }
        try (InputStream is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(p)))) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                int lines = 0;
                while (line != null) {
                    LOGGER.trace("about to add a line");
                    if (line.equals(POISON)) {
                        line = reader.readLine();
                        continue;
                    }
                    records.offer(line, 3, TimeUnit.MINUTES);
                    int processed = totalProcessed.incrementAndGet();
                    if (max > 0 && processed >= max) {
                        return;
                    }
                    if (++lines % 100000 == 0) {
                        LOGGER.info("added "+processed + " records to consider");
                    }
                    line = reader.readLine();
                }

            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static class CallableIndexer implements Callable<Integer> {

        private final ArrayBlockingQueue<String> records;
        private final AbstractRecordProcessor recordProcessor;

        CallableIndexer(ArrayBlockingQueue<String> records, AbstractRecordProcessor recordProcessor) {
            this.records = records;
            this.recordProcessor = recordProcessor;
        }
        @Override
        public Integer call() throws Exception {
            while (true) {
                String json = records.poll(3, TimeUnit.MINUTES);
                if (json == null) {
                    throw new TimeoutException("waited 3 minutes for a new record");
                }

                if (json == POISON) {
                    recordProcessor.close();
                    return 1;
                }
                LOGGER.trace(json);
                try {
                    recordProcessor.process(json);
                } catch (IOException e) {
                    LOGGER.warn("bad json "+json);
                }
            }
        }
    }
}
