package org.tallison.ingest.arlington;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitData;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class ArlingtonIngest {
    private static Logger LOGGER = LoggerFactory.getLogger(ArlingtonIngest.class);

    public static void main(String[] args) throws Exception {
        Path tikaConfig = Paths.get(args[0]);
        int numThreads = 10;
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Long> executorCompletionService =
                new ExecutorCompletionService<>(executorService);
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(10000);

        executorCompletionService.submit(new CallablePipesIterator(
                PipesIterator.build(tikaConfig), queue));

        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new ArlingtonExtractor(queue, tikaConfig));
        }
        int finished = 0;
        try {
            while (finished <= numThreads) {
                Future<Long> future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
    }



    private static class ArlingtonExtractor implements Callable<Long> {
        private final ArrayBlockingQueue<FetchEmitTuple> tuples;
        private final Fetcher fetcher;
        private final Emitter emitter;
        private final Map<String, Matcher> captureMatchers = new HashMap<>();
        private final Map<String, Matcher> countMatchers = new HashMap<>();
        private final Matcher tradXref = Pattern.compile("Traditional trailer dictionary " +
                "detected").matcher("");
        private final Matcher xrefStream = Pattern.compile("XRefStream detected\\.").matcher("");


        private ArlingtonExtractor(ArrayBlockingQueue tuples, Path tikaConfig) throws Exception {
            this.tuples = tuples;
            this.fetcher = FetcherManager.load(tikaConfig).getFetcher();
            this.emitter = EmitterManager.load(tikaConfig).getEmitter();
            init(captureMatchers, countMatchers);
        }

        private static void init(Map<String, Matcher> captureMatchers,
                                 Map<String, Matcher> countMatchers) {
            captureMatchers.put("latest_version",
                     Pattern.compile("Latest Arlington feature was PDF (\\d\\.\\d)").matcher(""));
            captureMatchers.put("header_version",
                    Pattern.compile("Header is version PDF (\\d\\.\\d)").matcher(""));
            captureMatchers.put("doc_catalog_version",
                    Pattern.compile("Document Catalog\\/Version is PDF (\\d\\.\\d)")
                            .matcher(""));
            countMatchers.put("warnings", Pattern.compile("Warning: ").matcher(""));
            countMatchers.put("errors", Pattern.compile("Error: ").matcher(""));
        }

        @Override
        public Long call() throws Exception {
            List<EmitData> emitData = new ArrayList<>();
            while (true) {
                //blocking
                FetchEmitTuple t = tuples.take();
                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    tuples.put(PipesIterator.COMPLETED_SEMAPHORE);
                    emitter.emit(emitData);
                    emitData.clear();
                    return 1l;
                }
                process(t, emitData);
                if (emitData.size() >= 100) {
                    try {
                        emitter.emit(emitData);
                        emitData.clear();
                    } catch (IOException e) {
                        LOGGER.warn("Couldn't emit", e);
                    }
                }
            }
        }

        private void process(FetchEmitTuple t, List<EmitData> emitData) {
            Metadata metadata = new Metadata();
            try {
                extract(t, metadata);
            } catch (TikaException | IOException e) {
                LOGGER.warn("failed to fetch " + t.getFetchKey(), e);
                return;
            }
            emitData.add(new EmitData(t.getEmitKey(),
                    Collections.singletonList(metadata)));
        }

        private void extract(FetchEmitTuple t, Metadata metadata) throws IOException,
                TikaException {
            String txt;
            try (InputStream inputStream = fetcher.fetch(t.getFetchKey().getFetchKey(), metadata)) {
                txt = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            }
            for (Map.Entry<String, Matcher> e : captureMatchers.entrySet()) {
                Matcher m = e.getValue();
                m.reset(txt);
                if (m.find()) {
                    metadata.set(e.getKey(), m.group(1));
                }
            }
            for (Map.Entry<String, Matcher> e : countMatchers.entrySet()) {
                Matcher m = e.getValue();
                m.reset(txt);
                int i = 0;
                while (m.find()) {
                    i++;
                }
                metadata.set(e.getKey(), Integer.toString(i));
            }
            if (tradXref.reset(txt).find()) {
                metadata.set("xref", "trad");
            } else if (xrefStream.reset(txt).find()) {
                metadata.set("xref", "stream");
            } else {
                metadata.set("xref", "undef");
            }
        }
    }
}
