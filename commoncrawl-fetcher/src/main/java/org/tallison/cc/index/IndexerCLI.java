package org.tallison.cc.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.util.DBUtil;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class IndexerCLI {

    static Logger LOGGER = LoggerFactory.getLogger(IndexerCLI.class);

    private static int DEFAULT_NUM_THREADS = 1;
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
        options.addOption("s", "schema", true, "db schema");
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
        long max = -1l;
        if (line.hasOption("m")) {
            max = Long.parseLong(line.getOptionValue("m"));
        }
        Path filterFile = null;
        if (line.hasOption("f")) {
            filterFile = Paths.get(line.getOptionValue("f"));
        }
        IndexerCLI indexer = new IndexerCLI();
        DBUtil.driverHint(jdbc);
        Connection connection = DriverManager.getConnection(jdbc);
        //connection.setAutoCommit(false);
        String schema = "";
        if (line.hasOption("schema")) {
            schema = line.getOptionValue("s");
        }
        try {
            indexer.execute(tikaConfigPath, connection, schema, filterFile, numThreads, max);
        } catch (Exception e) {
            LOGGER.error("catastrophe", e);
        } finally {
            connection.close();
        }
    }

    private void execute(Path tikaConfigPath, Connection connection,
                         String schema,
                         Path filterFile, int numThreads, long max)
            throws Exception {
        DBIndexer.init(connection, schema);

        RecordFilter filter = CompositeRecordFilter.load(filterFile);

        long start = System.currentTimeMillis();
        AtomicLong totalProcessed = new AtomicLong(0);
        PipesIterator pipesIterator = PipesIterator.build(tikaConfigPath);

        Fetcher fetcher = FetcherManager.load(tikaConfigPath).getFetcher(pipesIterator.getFetcherName());

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);

        try {
            ArrayBlockingQueue<FetchEmitTuple> paths = new ArrayBlockingQueue<>(300+numThreads);
            completionService.submit(new CallablePipesIterator(pipesIterator, paths));

            for (int i = 0; i < numThreads; i++) {
                completionService.submit(new CallableIndexer(paths, fetcher,
                                new DBIndexer(connection, schema, filter),
                        max, totalProcessed));
            }

            int finished = 0;
            while (finished < numThreads + 1) {
                Future<Integer> future = completionService.poll(3, TimeUnit.MINUTES);
                if (future != null) {
                    finished++;
                    LOGGER.info("finished workers {}", finished);
                    future.get();
                }
            }
        } catch (InterruptedException|ExecutionException e) {
            LOGGER.error("fatal problem", e);
            throw new RuntimeException(e);
        } finally {
            DBIndexer.shutDown();
            executorService.shutdownNow();
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("processed " + totalProcessed.get() + " records"+
                " and indexed " + DBIndexer.getAdded() + " in " +elapsed+" ms");
    }

    //this is called on each index file
    private static class CallableIndexer implements Callable<Integer> {

        private final ArrayBlockingQueue<FetchEmitTuple> paths;
        private final AbstractRecordProcessor recordProcessor;
        private final long max;
        private final AtomicLong totalProcessed;
        private final Fetcher fetcher;

        CallableIndexer(ArrayBlockingQueue<FetchEmitTuple> paths, Fetcher fetcher,
                        AbstractRecordProcessor recordProcessor, long max,
                        AtomicLong processed) {
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
                    //hang forever
                    paths.put(PipesIterator.COMPLETED_SEMAPHORE);
                    return 1;
                }
                LOGGER.trace(fetchEmitTuple.getFetchKey().getFetchKey());
                processFile(fetchEmitTuple, recordProcessor);
            }
        }

        private void processFile(FetchEmitTuple fetchEmitTuple, AbstractRecordProcessor recordProcessor) {
            if (max > 0 && totalProcessed.get() >= max) {
                LOGGER.info("hit max stopping now");
                return;
            }
            LOGGER.info("processing " + fetchEmitTuple.getFetchKey().getFetchKey());

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
                        long processed = totalProcessed.incrementAndGet();
                        if (max > 0 && processed >= max) {
                            LOGGER.info("hit max stopping now");
                            return;
                        }
                        if (processed % 100000 == 0) {
                            LOGGER.info("Processed " + processed);
                        }
                        lines++;
                        line = reader.readLine();
                    }
                }
            } catch (IOException | TikaException e) {
                //TODO revisit this.
                throw new RuntimeException(e);
            }

        }
    }

}
