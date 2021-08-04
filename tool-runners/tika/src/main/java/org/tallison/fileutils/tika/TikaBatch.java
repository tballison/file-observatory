/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.fileutils.tika;

import static org.apache.tika.pipes.PipesServer.OOM;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.PipesConfig;
import org.apache.tika.pipes.PipesParser;
import org.apache.tika.pipes.PipesResult;
import org.apache.tika.pipes.async.AsyncConfig;
import org.apache.tika.pipes.emitter.EmitData;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.emitter.TikaEmitterException;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TikaBatch {


    private static final Logger LOG = LoggerFactory.getLogger(TikaBatch.class);
    private static int MAX_STDOUT = 10000;
    private static int MAX_STDERR = 10000;

    private final ConfigSrc configSrc;
    public TikaBatch(ConfigSrc configSrc) {
        this.configSrc = configSrc;
    }

    public void execute() throws Exception {
        AsyncConfig config = AsyncConfig.load(configSrc.getTikaConfigPath());
        ArrayBlockingQueue<FetchEmitTuple> tuples = new ArrayBlockingQueue<>(1000);
        PipesIterator pipesIterator = PipesIterator.build(configSrc.getTikaConfigPath());
        String emitterName = pipesIterator.getEmitterName();
        PipesIteratorWrapper pipesIteratorWrapper = new PipesIteratorWrapper(
                pipesIterator, tuples,
                config.getNumClients());


        ExecutorService executorService = Executors.newFixedThreadPool(config.getNumEmitters() + 1);
        ExecutorCompletionService executorCompletionService =
                new ExecutorCompletionService(executorService);

        executorCompletionService.submit(pipesIteratorWrapper);
        PipesConfig pipesConfig = PipesConfig.load(configSrc.getTikaConfigPath());
        PipesParser pipesParser = new PipesParser(pipesConfig);

        for (int i = 0; i < config.getNumClients(); i++) {
            executorCompletionService.submit(new TikaWorker(configSrc, tuples, pipesParser,
                    emitterName));
        }

        int finished = 0;
        try {
            while (finished < config.getNumClients() + 1) {
                //blocking
                Future<Integer> future = executorCompletionService.take();
                Integer val = future.get();
                LOG.info("finished: " + val);
                finished++;
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private static class PipesIteratorWrapper implements Callable<Integer> {
        private final PipesIterator pipesIterator;
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final int numThreads;

        public PipesIteratorWrapper(PipesIterator pipesIterator,
                                    ArrayBlockingQueue<FetchEmitTuple> queue,
                                    int numThreads) {
            this.pipesIterator = pipesIterator;
            this.queue = queue;
            this.numThreads = numThreads;

        }

        @Override
        public Integer call() throws Exception {
            for (FetchEmitTuple t : pipesIterator) {
                //potentially blocks forever
                queue.put(t);
            }
            LOG.info("iterator wrapper has finished adding tuples");
            for (int i = 0; i < numThreads; i ++) {
                queue.put(PipesIterator.COMPLETED_SEMAPHORE);
            }
            LOG.info("iterator wrapper has finished adding completed semaphores");
            return 1;
        }
    }

    private class TikaWorker implements Callable<Integer> {

        private final PipesParser pipesParser;
        private final MetadataWriter metadataWriter;
        private final Emitter emitter;
        private final ArrayBlockingQueue<FetchEmitTuple> tuples;
        public TikaWorker(ConfigSrc configSrc,
                      ArrayBlockingQueue<FetchEmitTuple> tuples,
                          PipesParser pipesParser,
                          String emitterName) throws Exception {
            this.metadataWriter = configSrc.getMetadataWriter();
            this.pipesParser = pipesParser;
            this.tuples = tuples;
            this.emitter =
                    EmitterManager.load(configSrc.getTikaConfigPath()).getEmitter(emitterName);
        }

        @Override
        public Integer call() throws Exception {

            while (true) {
                //hang forever
                FetchEmitTuple t = tuples.take();
                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    return 2;
                }
                PipesResult result = pipesParser.parse(t);

                FileProcessResult fileProcessResult = new FileProcessResult();
                switch (result.getStatus()) {
                    case PARSE_SUCCESS:
                        emit(result.getEmitData(), fileProcessResult);
                        break;
                    case OOM:
                        fileProcessResult.setExitValue(1);
                    case TIMEOUT:
                        fileProcessResult.setExitValue(2);
                        fileProcessResult.setTimeout(true);
                    case UNSPECIFIED_CRASH:
                        fileProcessResult.setExitValue(3);
                    case INTERRUPTED_EXCEPTION:
                        fileProcessResult.setExitValue(4);
                    case CLIENT_UNAVAILABLE_WITHIN_MS:
                        fileProcessResult.setExitValue(5);
                    case EMIT_SUCCESS:
                        fileProcessResult.setExitValue(7);
                    case EMIT_SUCCESS_PARSE_EXCEPTION:
                        fileProcessResult.setExitValue(8);
                    case EMIT_EXCEPTION:
                        fileProcessResult.setExitValue(9);
                    case PARSE_EXCEPTION_NO_EMIT:
                        //this shouldn't happen
                    case PARSE_EXCEPTION_EMIT:
                        fileProcessResult.setExitValue(10);
                        fileProcessResult.setStderr(result.getStatus().name());
                        fileProcessResult.setStderrLength(result.getStatus().name().length());
                        break;
                    case NO_EMITTER_FOUND:
                        throw new RuntimeException("emitter not found" + result.getMessage());
                }
                metadataWriter.write(t.getFetchKey().getFetchKey(), fileProcessResult);
            }
        }

        private void emit(EmitData emitData, FileProcessResult fileProcessResult) {
            try {
                emitter.emit(emitData.getEmitKey().getEmitKey(), emitData.getMetadataList());
            } catch (TikaEmitterException|IOException e) {
                LOG.warn("emit exception for " + emitData.getEmitKey().getEmitKey(),
                        e);
                fileProcessResult.setExitValue(7);
                fileProcessResult.setStderr("emit exception");
            } finally {
                String trace = getStackTrace(emitData.getMetadataList());
                if (!StringUtils.isAllBlank(trace)) {
                    int traceLength = trace.length();
                    if (trace.length() > MAX_STDERR) {
                        trace = trace.substring(0, MAX_STDERR-10);
                        fileProcessResult.setStderrTruncated(true);
                    }
                    fileProcessResult.setStderr(trace);
                    fileProcessResult.setStderrLength(traceLength);
                }
            }
        }

        private String getStackTrace(List<Metadata> metadataList) {
            if (metadataList == null || metadataList.size() == 0) {
                return "";
            }
            String stack = metadataList.get(0).get(TikaCoreProperties.CONTAINER_EXCEPTION);
            if (stack != null) {
                return stack;
            }
            return "";
        }
    }

    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        TikaBatch runner = new TikaBatch(ConfigSrc.build(args, "tika",
                MAX_STDOUT, MAX_STDERR));
        //runner.setMaxFiles(100);
        runner.execute();
        System.out.println("finished in "+(System.currentTimeMillis()-begin) + " ms");
    }
}

