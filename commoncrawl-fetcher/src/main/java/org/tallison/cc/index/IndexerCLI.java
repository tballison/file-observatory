package org.tallison.cc.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class IndexerCLI {

    static Logger LOGGER = LoggerFactory.getLogger(IndexerCLI.class);

    private static int DEFAULT_NUM_THREADS = 10;
    private static Path POISON = Paths.get("");
    private static Options getOptions() {

        Options options = new Options();

        options.addRequiredOption("c", "tikaConfig", true,
                "tika config file with a pipesiterator for the gz files and a fetcher" +
                        " (name='fetcher') for getting the bytes for the files");
        options.addRequiredOption("j", "jdbc", true,
                "jdbc connection string to the pg db");
        options.addOption("m", "max", true,
                "maximum number of records to index");
        options.addOption("n", "numThreads", true,
                "number of threads.");
        options.addOption("f", "filterFile",
                true,
                "json file that describes filters");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);

        Path tikaConfigPath = Paths.get(line.getOptionValue("c"));
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
            indexer.execute(tikaConfigPath, connection, filterFile, numThreads, max);
        } finally {
            connection.commit();
            connection.close();
        }
    }

    private void execute(Path tikaConfigPath, Connection connection,
                         Path filterFile, int numThreads, int max)
            throws Exception {
        PGIndexer.init(connection);

        RecordFilter filter = CompositeRecordFilter.load(filterFile);

        long start = System.currentTimeMillis();
        AtomicInteger totalProcessed = new AtomicInteger(0);
        PipesIterator pipesIterator = PipesIterator.build(tikaConfigPath);
        Fetcher fetcher = FetcherManager.load(tikaConfigPath).getFetcher("fetcher");

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);
            ArrayBlockingQueue<FetchEmitTuple> paths = new ArrayBlockingQueue<>(300+numThreads);
            for (FetchEmitTuple fetchEmitTuple : pipesIterator) {
                if (fetchEmitTuple.getFetchKey().getFetchKey().endsWith(".gz")) {
                    paths.offer(fetchEmitTuple);
                }
            }
            for (int i = 0; i < numThreads; i++) {
                paths.offer(PipesIterator.COMPLETED_SEMAPHORE);
            }
            LOGGER.info("added index paths");
            for (int i = 0; i < numThreads; i++) {
                completionService.submit(new CallableIndexer(paths, fetcher,
                                new PGIndexer(connection, filter),
                        max, totalProcessed));
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
        } finally {
            PGIndexer.shutDown();
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("processed " + totalProcessed.get() + " records"+
                " and indexed " + PGIndexer.getAdded() + " in " +elapsed+" ms");
    }

    private static class CallableIndexer implements Callable<Integer> {

        private final ArrayBlockingQueue<FetchEmitTuple> paths;
        private final AbstractRecordProcessor recordProcessor;
        private final int max;
        private final AtomicInteger totalProcessed;
        private final Fetcher fetcher;

        CallableIndexer(ArrayBlockingQueue<FetchEmitTuple> paths, Fetcher fetcher,
                        AbstractRecordProcessor recordProcessor, int max, AtomicInteger processed) {
            this.paths = paths;
            this.fetcher = fetcher;
            this.recordProcessor = recordProcessor;
            this.max = max;
            this.totalProcessed = processed;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                FetchEmitTuple fetchEmitTuple = paths.poll(3, TimeUnit.MINUTES);
                if (fetchEmitTuple == null) {
                    throw new TimeoutException("waited 3 minutes for a new record");
                }

                if (fetchEmitTuple == PipesIterator.COMPLETED_SEMAPHORE) {
                    recordProcessor.close();
                    return 1;
                }
                LOGGER.trace(fetchEmitTuple.getFetchKey().getFetchKey());
                processFile(fetchEmitTuple, recordProcessor);
            }
        }

        private void processFile(FetchEmitTuple fetchEmitTuple, AbstractRecordProcessor recordProcessor) {
            int processed = totalProcessed.incrementAndGet();
            LOGGER.info("processing " + fetchEmitTuple.getFetchKey().getFetchKey());
            if (max > 0 && processed >= max) {
                return;
            }
            try (InputStream is =
                         new BufferedInputStream(new GZIPInputStream(
                                 fetcher.fetch(fetchEmitTuple.getFetchKey().getFetchKey(), new Metadata())))) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    String line = reader.readLine();
                    int lines = 0;
                    while (line != null) {
                        LOGGER.trace("about to add a line");
                        if (line.equals(POISON)) {
                            line = reader.readLine();
                            continue;
                        }
                        try {
                            recordProcessor.process(line);
                        } catch (IOException e) {
                            LOGGER.warn("bad json: "+line);
                        }
                        processed = totalProcessed.incrementAndGet();
                        if (max > 0 && processed >= max) {
                            return;
                        }
                        if (processed % 100000 == 0) {
                            LOGGER.info("Processed " + processed);
                        }
                        line = reader.readLine();
                    }

                }
            } catch (IOException | TikaException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
