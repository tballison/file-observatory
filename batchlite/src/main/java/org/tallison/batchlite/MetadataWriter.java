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
import org.tallison.batchlite.writer.PathResultPair;
import org.tallison.batchlite.writer.WriterResult;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thread-safe metadata writer.  Implementations
 * don't have to worry about thread safety.
 */
public abstract class MetadataWriter implements Callable<Integer> {

    private static Logger LOGGER = LoggerFactory.getLogger(MetadataWriter.class);

    private static PathResultPair POISON = new PathResultPair(null, null);
    private static final long MAX_POLL_SECONDS = 6000;
    private static final int MAX_BUFFER = 10000;
    int recordsWritten = 0;
    private final int maxStdoutBuffer;
    private final int maxStderrBuffer;
    private final ArrayBlockingQueue<PathResultPair> rows = new ArrayBlockingQueue<>(1000);
    private final String name;
    public MetadataWriter(String name, int maxStdoutBuffer, int maxStderrBuffer) {
        this.name = name;
        this.maxStdoutBuffer = maxStdoutBuffer;
        this.maxStderrBuffer = maxStderrBuffer;
    }

    public String getName() {
        return name;
    }
    abstract protected void write(PathResultPair pathResultPair) throws IOException;

    abstract protected void close() throws IOException;

    public int getMaxStdoutBuffer() {
        return maxStdoutBuffer;
    }

    public int getMaxStderrBuffer() {
        return maxStderrBuffer;
    }

    public void write(String relPath, FileProcessResult result) throws IOException {
        try {
            boolean offered = rows.offer(new PathResultPair(relPath, result), MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (!offered) {
                LOGGER.error("timeout error after ", MAX_POLL_SECONDS);
                throw new IOException(new TimeoutException("timeout after "+ MAX_POLL_SECONDS +
                        " seconds"));
            }
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted ", e);
        }
    }

    public void shutdown() throws IOException {
        try {
            boolean offered = rows.offer(POISON, MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (!offered) {
                LOGGER.error("timeout error after ", MAX_POLL_SECONDS);
                throw new IOException(new TimeoutException("timeout after " + MAX_POLL_SECONDS +
                        " seconds"));
            }
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted ", e);
            throw new IOException(e);
        }

    }

    @Override
    public Integer call() throws IOException, TimeoutException, InterruptedException {
        while (true) {
            PathResultPair pair = rows.poll(MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (pair == null) {
                throw new TimeoutException("waited longer than " + MAX_POLL_SECONDS
                        + " seconds");
            }
            if (pair == POISON) {
                close();
                return recordsWritten;
            }
            write(pair);
            if (++recordsWritten % 1000 == 0) {
                LOGGER.info("processed {} records", recordsWritten);
            }
        }
    }

    public int getRecordsWritten() {
        return recordsWritten;
    }

}
