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
package org.tallison.batchlite;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.pipes.fetchiterator.FetchIterator;
import org.apache.tika.pipes.fetchiterator.jdbc.JDBCFetchIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.writer.JDBCMetadataWriter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.List;
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

public abstract class AbstractDirectoryProcessor {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractDirectoryProcessor.class);
    private final AtomicLong numAddedToQueue = new AtomicLong(0);

    static Path POISON = Paths.get("");
    private static long TIMEOUT_MILLIS = 720000;
    private static int QUEUE_SIZE = 1000;
    protected final Path tikaConfigPath;
    protected final TikaConfig tikaConfig;
    private int maxFiles = -1;
    protected int numThreads;
    protected final MetadataWriter metadataWriter;

    public AbstractDirectoryProcessor(ConfigSrc configSrc)
            throws TikaConfigException {
        this.tikaConfigPath = configSrc.getTikaConfig();
        try {
            this.tikaConfig = new TikaConfig(tikaConfigPath);
        } catch (TikaException|IOException|SAXException e) {
            throw new TikaConfigException("bad config", e);
        }
        this.metadataWriter = configSrc.getMetadataWriter();
        this.numThreads = configSrc.getNumThreads();
    }

    public void execute() throws SQLException, IOException, TikaException {
        FetchIterator fetchIterator = tikaConfig.getFetchIterator();
        if (fetchIterator instanceof JDBCFetchIterator) {
            String select = ((JDBCFetchIterator) fetchIterator).getSelect();
            if (select.contains("${name}")) {
                select = select.replaceAll("\\$\\{name\\}", metadataWriter.getName());
                ((JDBCFetchIterator)fetchIterator).setSelect(select);
            }
        }
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(1000);

        List<AbstractFileProcessor> processors = getProcessors(queue);
        int totalThreads = processors.size()+2;

        ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Integer> executorCompletionService
                = new ExecutorCompletionService<>(executorService);

        long start = System.currentTimeMillis();
        executorCompletionService.submit(new FetchIteratorWrapper(queue, fetchIterator, processors.size()));
        executorCompletionService.submit(metadataWriter);

        for (int i = 0; i < processors.size(); i++) {
            executorCompletionService.submit(processors.get(i));
        }

        int completed = 0;
        //this waits for all threads to finish.
        //it assumes that the workers will finish first and then it
        //calls shutdown on the writer, which will cause it to flush
        //and close.
        //If something catastrophic happens in the writer, this
        //will stop early with a runtime exception from future.get()
        try {
            while (completed < totalThreads) {
                Future<Integer> future = null;
                try {
                    future = executorCompletionService.poll(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.warn("interrupted: ", e);
                    break;
                }
                if (future != null) {
                    completed++;
                    try {
                        future.get();
                    } catch (InterruptedException|ExecutionException e) {
                        LOGGER.error("catastrophic failure", e);
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                if (completed == processors.size()+1) {
                    metadataWriter.shutdown();
                }
            }
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
        long elapsed = System.currentTimeMillis()-start;
        LOGGER.info("Finished adding " + numAddedToQueue.get() + " records to the queue;" +
                        " and added "+metadataWriter.getRecordsWritten() + " records in "+
                elapsed + " ms.");

    }

    protected void setMaxFiles(int maxFiles) {
        this.maxFiles = maxFiles;
    }

    public abstract List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException;

    private static class FetchIteratorWrapper implements Callable<Integer> {
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final FetchIterator fetchIterator;
        private final int numWorkers;
        private FetchIteratorWrapper(ArrayBlockingQueue<FetchEmitTuple> queue,
                                     FetchIterator fetchIterator, int numWorkers) {
            this.queue = queue;
            this.fetchIterator = fetchIterator;
            this.numWorkers = numWorkers;
        }
        @Override
        public Integer call() throws Exception {
            for (FetchEmitTuple t : fetchIterator) {
                boolean offered = queue.offer(t, TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS);
                if (offered == false) {
                    throw new TimeoutException();
                }
            }
            for (int i = 0; i < numWorkers; i++) {
                boolean offered = queue.offer(FetchIterator.COMPLETED_SEMAPHORE,
                        TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (offered == false) {
                    throw new TimeoutException();
                }
            }
            return 1;
        }
    }
}
