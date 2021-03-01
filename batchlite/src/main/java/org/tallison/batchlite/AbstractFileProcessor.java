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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.pipes.fetchiterator.FetchIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFileProcessor implements Callable<Integer> {

    public static String FETCHER_NAME = "file-obs-fetcher";
    public static String EMITTER_NAME = "file-obs-emitter";

    private static final long DEFAULT_TIMEOUT_MILLIS = 30000;
    private static final int MAX_BUFFER = 10000;

    private static AtomicInteger THREAD_COUNT = new AtomicInteger();

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractFileProcessor.class);
    private final ArrayBlockingQueue<FetchEmitTuple> queue;
    protected final TikaConfig tikaConfig;
    private final int id;
    private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;

    private long fileTimeoutMillis = DEFAULT_TIMEOUT_MILLIS;

    public AbstractFileProcessor(ArrayBlockingQueue<FetchEmitTuple> queue, TikaConfig tikaConfig) {
        id = THREAD_COUNT.getAndIncrement();
        this.tikaConfig = tikaConfig;
        this.queue = queue;
    }

    public abstract void process(FetchEmitTuple fetchEmitTuple) throws IOException;

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public long getFileTimeoutMillis() {
        return fileTimeoutMillis;
    }

    public void setFileTimeoutMillis(long fileTimeoutMillis) {
        this.fileTimeoutMillis = fileTimeoutMillis;
    }

    @Override
    public Integer call() throws IOException, TimeoutException {
        while (true) {
            FetchEmitTuple t = null;
            try {
                long start = System.currentTimeMillis();
                t = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                long elapsed = System.currentTimeMillis() - start;
                LOGGER.debug("thread (" + id + ") from queue " + elapsed + " : " + queue.size());
            } catch (InterruptedException e) {
                return 0;
            }
            if (t == null) {
                throw new TimeoutException("timed out");
            } else if (t == FetchIterator.COMPLETED_SEMAPHORE) {
                return 1;
            } else {
                long start = System.currentTimeMillis();
                try {
                    System.err.println("about to process "+t);
                    process(t);
                } catch (IOException e) {
                    LOGGER.warn("problem processing "+t, e);
                } catch (Throwable throwable) {
                    LOGGER.error("catastrophe "+t, throwable);
                    throw throwable;
                }
                long elapsed = System.currentTimeMillis() - start;
                LOGGER.debug("thread (" + id + ") took " + elapsed + " to process " +
                        t);
            }
        }
    }
}
