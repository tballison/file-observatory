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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
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

public class MutoolClean extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MutoolClean.class);
    private final int maxBufferLength = 100000;

    private final MetadataWriter metadataWriter;
    private final int numThreads;
    private final long timeoutMillis = 60000;

    public MutoolClean(Path srcRoot, MetadataWriter metadataWriter, int numThreads) {
        super(srcRoot);
        this.metadataWriter = metadataWriter;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new MutoolCleanProcessor(queue, rootDir, metadataWriter));
        }
        return processors;
    }

    private class MutoolCleanProcessor extends FileProcessor {

        public MutoolCleanProcessor(ArrayBlockingQueue<Path> queue,
                                    Path srcRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, metadataWriter);
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
                        timeoutMillis, maxBufferLength);
                metadataWriter.write(relPath, r);
            } finally {
                Files.delete(tmp);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        String metadataWriterString = args[1];
        int numThreads = 10;
        if (args.length > 2) {
            numThreads = Integer.parseInt(args[2]);
        }
        long start = System.currentTimeMillis();
        MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString);
        try {

            MutoolClean runner = new MutoolClean(srcRoot, metadataWriter, numThreads);
            //runner.setMaxFiles(100);
            runner.execute();
        } finally {
            metadataWriter.close();
            long elapsed = System.currentTimeMillis()-start;
            System.out.println("Processed "+ metadataWriter.getRecordsWritten() + " files in "+
                    elapsed + " ms.");
        }

    }
}
