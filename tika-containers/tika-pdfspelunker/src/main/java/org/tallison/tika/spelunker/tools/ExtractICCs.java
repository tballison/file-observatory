package org.tallison.tika.spelunker.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.drew.metadata.mov.atoms.Atom;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class ExtractICCs {

    private static Object LOCK = new Object();

    public static void main(String[] args) throws Exception {
        Path tikaConfig = Paths.get(args[0]);
        Path byteDir = Paths.get(args[1]);
        int numWorkers = Integer.parseInt(args[2]);
        PipesIterator iterator = PipesIterator.build(tikaConfig);
        Fetcher fetcher = FetcherManager.load(tikaConfig).getFetcher(iterator.getFetcherName());
        Map<String, AtomicInteger> counts = new HashMap<>();
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(10000);
        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers+1);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<>(executorService);
        executorCompletionService.submit(new Enqueuer(iterator, queue));
        for (int i = 0; i < numWorkers; i++) {
            executorCompletionService.submit(new Worker(queue, fetcher, byteDir, counts));
        }

        int finished = 0;
        try {
            while (finished < numWorkers + 1) {
                //blocking
                Future future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdownNow();
        }

        counts.entrySet().stream()
                .sorted((o1, o2) -> Integer.compare(o2.getValue().get(), o1.getValue().get()))
                .forEach(e -> System.out.println(e.getKey() + "\t" + e.getValue()));
    }

    private static void updateCount(Map<String, AtomicInteger> counts, String sha) {
        AtomicInteger cnt = counts.get(sha);
        if (cnt == null) {
            synchronized (LOCK) {
                //check again after the lock
                cnt = counts.get(sha);
                if (cnt == null) {
                    cnt = new AtomicInteger();
                    counts.put(sha, cnt);
                }
            }
        }
        cnt.incrementAndGet();
    }

    private static byte[] decompressAndDecode(String gzipEncoded) {
        Base64 base64 = new Base64();
        byte[] gzip = base64.decode(gzipEncoded);
        try (InputStream is = new GzipCompressorInputStream(new ByteArrayInputStream(gzip));
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            IOUtils.copy(is, bos);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static class Enqueuer implements Callable<Integer> {
        private final PipesIterator pipesIterator;
        private final ArrayBlockingQueue<FetchEmitTuple> queue;

        public Enqueuer(PipesIterator pipesIterator, ArrayBlockingQueue<FetchEmitTuple> queue) {
            this.pipesIterator = pipesIterator;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            for (FetchEmitTuple t : pipesIterator) {
                //blocking
                queue.put(t);
            }
            queue.put(PipesIterator.COMPLETED_SEMAPHORE);
            return 1;
        }
    }

    private static class Worker implements Callable<Integer> {
        private static final AtomicInteger PROCESSED = new AtomicInteger(0);
        private static final long START = System.currentTimeMillis();
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final Fetcher fetcher;
        private final Path byteDir;
        private final Map<String, AtomicInteger> counts;

        public Worker(ArrayBlockingQueue<FetchEmitTuple> queue, Fetcher fetcher, Path byteDir,
                      Map<String, AtomicInteger> counts) {
            this.queue = queue;
            this.fetcher = fetcher;
            this.byteDir = byteDir;
            this.counts = counts;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                FetchEmitTuple t = queue.take();
                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    queue.put(t);
                    return 1;
                }
                process(t);
            }
        }

        private void process(FetchEmitTuple t) {
            int processed = PROCESSED.getAndIncrement();

            if (processed % 1000 == 0) {
                long elapsed = System.currentTimeMillis() - START;
                System.out.println("processed " + processed + " in " +
                        elapsed + " ms");
            }
            List<Metadata> metadataList = null;
            Metadata m = new Metadata();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fetcher.fetch(t.getFetchKey().getFetchKey(), m),
                            StandardCharsets.UTF_8))) {
                metadataList = JsonMetadataList.fromJson(reader);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            for (Metadata metadata : metadataList) {
                String sha = metadata.get("shasum_256");
                if (sha == null) {
                    continue;
                }
                updateCount(counts, sha);
                String gzipEncoded = metadata.get("base64_gzip_bytes");
                if (gzipEncoded == null) {
                    continue;
                }
                byte[] bytes = decompressAndDecode(gzipEncoded);
                if (bytes == null) {
                    return;
                }
                String computedDigest = DigestUtils.sha256Hex(bytes);
                if (!computedDigest.equals(sha)) {
                    System.err.println("bad digest " + computedDigest + " expected " + sha);
                    return;
                }

                Path targ = byteDir.resolve(
                        sha.substring(0, 2) + "/" + sha.substring(2, 4) + "/" + sha);
                if (Files.exists(targ)) {
                    continue;
                }
                try {
                    Files.createDirectories(targ.getParent());
                    Files.write(targ, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
