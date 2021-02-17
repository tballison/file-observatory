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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.writer.JDBCMetadataWriter;

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
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractDirectoryProcessor {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractDirectoryProcessor.class);
    private final AtomicLong numAddedToQueue = new AtomicLong(0);

    static Path POISON = Paths.get("");
    private static long TIMEOUT_MILLIS = 720000;
    private static int QUEUE_SIZE = 1000;
    private int maxFiles = -1;
    private int numThreads;
    protected final Path rootDir;
    protected final MetadataWriter metadataWriter;

    public AbstractDirectoryProcessor(Path rootDir, MetadataWriter metadataWriter) {
        this.rootDir = rootDir.toAbsolutePath();
        if (! Files.isDirectory(rootDir)) {
           throw new RuntimeException(rootDir + " does not exist");
        }
        this.metadataWriter = metadataWriter;
    }

    protected Path getRootDir() {
        return rootDir;
    }

    public void execute() throws SQLException, IOException {
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        List<AbstractFileProcessor> processors = getProcessors(queue);
        numThreads = processors.size()+2;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        long start = System.currentTimeMillis();
        executorCompletionService.submit(metadataWriter);
        executorCompletionService.submit(new DirectoryCrawler(rootDir, queue));

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
            while (completed < numThreads) {
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

    public abstract List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue);

    private class DirectoryCrawler implements Callable<Integer> {
        private final Path rootDir;
        private final ArrayBlockingQueue<Path> queue;

        private DirectoryCrawler(Path rootDir, ArrayBlockingQueue queue) {
            this.rootDir = rootDir;
            this.queue = queue;
        }


        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(rootDir, new PathAdder(queue));

            for (int i = 0; i < numThreads; i++) {
                //TODO: could lock forever...fix this
                queue.put(POISON);
            }

            return 2;
        }

        private class PathAdder implements FileVisitor<Path> {
            private final ArrayBlockingQueue<Path> queue;
            int added = 0;
            public PathAdder(ArrayBlockingQueue<Path> queue) {
                this.queue = queue;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                if (maxFiles > -1 && added >= maxFiles) {
                    return FileVisitResult.TERMINATE;
                }
                if (path.getFileName().toString().startsWith(".")) {
                    LOGGER.info("skipping hidden file: "+path);
                    //skip hidden files
                    return FileVisitResult.CONTINUE;
                }
                try {
                    boolean offered = queue.offer(path, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    if (offered) {
                        numAddedToQueue.incrementAndGet();
                        added++;
                        if (numAddedToQueue.get() % 100 == 0) {
                            LOGGER.debug("added to queue: "+numAddedToQueue.get());
                        }
                    } else {
                        LOGGER.warn("failed to offer file to queue in alloted time");
                        throw new RuntimeException("file adder timed out");
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("interrupted ", e);
                    //swallow
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
                LOGGER.warn("visit file failed: "+path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        }
    }
}
