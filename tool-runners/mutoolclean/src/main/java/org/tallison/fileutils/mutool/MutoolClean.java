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
package org.tallison.fileutils.mutool;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class MutoolClean extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MutoolClean.class);
    private static final int MAX_STDOUT = 100;
    private static final int MAX_STDERR = 20000;

    private final long timeoutMillis = 60000;

    public MutoolClean(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            MutoolCleanProcessor p = new MutoolCleanProcessor(queue, tikaConfig, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class MutoolCleanProcessor extends FileProcessor {

        public MutoolCleanProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                    TikaConfig tikaConfig, MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);
            Path tmp = Files.createTempFile("mutool-clean-", ".pdf");
            try {
                List<String> commandLine = new ArrayList<>();
                commandLine.add("mutool");
                commandLine.add("clean");
                commandLine.add("-s");
                commandLine.add(srcPath.toAbsolutePath().toString());
                commandLine.add(tmp.toAbsolutePath().toString());


                FileProcessResult r = ProcessExecutor.execute(
                        new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                        timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                        metadataWriter.getMaxStderrBuffer());
                metadataWriter.write(relPath, r);
            } finally {
                Files.delete(tmp);
            }
        }
    }

    public static void main(String[] args) throws Exception {
            MutoolClean runner = new MutoolClean(
                    ConfigSrc.build(args, MAX_STDOUT, MAX_STDERR)
            );
            runner.execute();
    }
}
