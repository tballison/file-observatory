package org.tallison.cc.index;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.mutable.MutableLong;
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
import java.sql.Connection;
import java.sql.DriverManager;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class MimeCounter {

    static Logger LOGGER = Logger.getLogger(MimeCounter.class);

    private static int DEFAULT_NUM_THREADS = 10;
    private static Path POISON = Paths.get("");
    private static Options getOptions() {

        Options options = new Options();

        options.addRequiredOption("i", "indexDirectory", true,
                "directory with the gz index files");
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

        int numThreads = DEFAULT_NUM_THREADS;
        if (line.hasOption("n")) {
            numThreads = Integer.parseInt(line.getOptionValue("n"));
        }

        Path filterFile = null;
        if (line.hasOption("f")) {
            filterFile = Paths.get(line.getOptionValue("f"));
        }
        MimeCounter counter = new MimeCounter();
        counter.execute(indexDirectory, filterFile, numThreads);
    }

    private void execute(Path indexDirectory,
                         Path filterFile, int numThreads)
            throws SQLException, IOException {

        RecordFilter filter = CompositeRecordFilter.load(filterFile);

        long start = System.currentTimeMillis();
        AtomicLong totalProcessed = new AtomicLong(0);
        List<MimeCounts> mimeCounts = new ArrayList<>();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);
            ArrayBlockingQueue<Path> paths = new ArrayBlockingQueue<>(300+numThreads);
            addPaths(indexDirectory, paths, numThreads);

            for (int i = 0; i < numThreads; i++) {
                completionService.submit(new MimeCounterWrapper(paths,
                                new MimeProcessor(filter), totalProcessed));
            }

            int finished = 0;
            while (finished < numThreads) {
                Future<MimeCounts> future = completionService.poll(3, TimeUnit.MINUTES);
                if (future != null) {
                    finished++;
                    mimeCounts.add(future.get());
                }
            }
            executorService.shutdownNow();
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeException(e);
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("processed " + totalProcessed.get() + " records "+
                " and indexed " + PGIndexer.getAdded() + " in " +elapsed+" ms");
        report(mimeCounts);
    }

    private void report(List<MimeCounts> mimeCounts) {
        Map<String, Long> mimes = new HashMap<>();
        Map<String, Long> detectedMimes = new HashMap<>();
        Map<String, Long> detectedToMime = new HashMap<>();
        for (MimeCounts m : mimeCounts) {
            update(m.mimes, mimes);
            update(m.detectedMimes, detectedMimes);
            update(m.detectedToMime, detectedToMime);
        }
        dump("mimes", mimes);
        System.out.println("\n\n");
        dump("detected_mimes", detectedMimes);
        System.out.println("\n\n");
        dump("detected->mime", detectedToMime);

    }

    private void dump(String title, Map<String, Long> mimes) {
        System.out.println(title);
        for (Map.Entry<String, Long> e : MapUtil.sortByDescendingValue(mimes).entrySet()) {
            System.out.println("\t"+e.getKey() + "\t"+e.getValue());
        }
    }

    private void update(Map<String, MutableLong> mimeCounts, Map<String, Long> mimes) {
        for (Map.Entry<String, MutableLong> e : mimeCounts.entrySet()) {
            Long val = mimes.get(e.getKey());
            if (val == null) {
                val = e.getValue().longValue();
            } else {
                val += e.getValue().longValue();
            }
            mimes.put(e.getKey(), val);
        }
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

    private static class MimeCounterWrapper implements Callable<MimeCounts> {

        private final ArrayBlockingQueue<Path> paths;
        private final AtomicLong totalProcessed;
        private final MimeProcessor mimeProcessor;

        MimeCounterWrapper(ArrayBlockingQueue<Path> paths,
                        MimeProcessor mimeProcessor, AtomicLong processed) {
            this.paths = paths;
            this.mimeProcessor = mimeProcessor;
            this.totalProcessed = processed;
        }

        @Override
        public MimeCounts call() throws Exception {
            while (true) {
                Path path = paths.poll(3, TimeUnit.MINUTES);
                if (path == null) {
                    throw new TimeoutException("waited 3 minutes for a new record");
                }

                if (path == POISON) {
                    mimeProcessor.close();
                    return mimeProcessor.getMimeCounts();
                }
                LOGGER.trace(path);
                processFile(path, mimeProcessor);
            }
        }

        private void processFile(Path path, AbstractRecordProcessor recordProcessor) {
            long processed = totalProcessed.incrementAndGet();

            try (InputStream is = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(path)))) {
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
                        if (processed % 100000 == 0) {
                            LOGGER.info("Processed " + processed);
                        }
                        line = reader.readLine();
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class MimeProcessor extends AbstractRecordProcessor {

        private final MimeCounts mimeCounts = new MimeCounts();
        @Override
        void usage() {

        }

        private final RecordFilter recordFilter;
        MimeProcessor(RecordFilter recordFilter) {
            this.recordFilter = recordFilter;

        }
        @Override
        public void process(String json) throws IOException {
            List<CCIndexRecord> records = CCIndexRecord.parseRecords(json);
            for (CCIndexRecord r : records) {
                if (!recordFilter.accept(r)) {
                    continue;
                }
                mimeCounts.incrementMimes(r.getNormalizedMime(), r.getNormalizedDetectedMime());
            }
        }

        @Override
        public void close() throws IOException {
            //do nothing
        }

        public MimeCounts getMimeCounts() {
            return mimeCounts;
        }
    }

    private static class MimeCounts {
        Map<String, MutableLong> mimes = new HashMap<>();
        Map<String, MutableLong> detectedMimes = new HashMap<>();
        Map<String, MutableLong> detectedToMime = new HashMap<>();
        void incrementMimes(String mime, String detectedMime) {
            increment(mime, mimes);
            increment(detectedMime, detectedMimes);
            increment(detectedMime+"->"+mime, detectedToMime);
        }

        private void increment(String mime, Map<String, MutableLong> mimes) {
            mime = (mime == null) ? "NULL" : mime;
            MutableLong count = mimes.get(mime);
            if (count == null) {
                count = new MutableLong(0);
                mimes.put(mime, count);
            }
            count.increment();
        }

    }
}
