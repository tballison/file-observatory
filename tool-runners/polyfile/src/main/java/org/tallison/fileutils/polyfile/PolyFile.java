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
package org.tallison.fileutils.polyfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PolyFile extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PolyFile.class);

    private final int maxErrBufferLength = 100;
    private final int numThreads;
    private final long timeoutMillis = 120000;
    private final Path targRoot;
    public PolyFile(Path srcRoot, Path targRoot, MetadataWriter metadataWriter,
                    int numThreads) {
        super(srcRoot, metadataWriter);
        this.targRoot = targRoot;
        this.numThreads = numThreads;

    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new PolyfileProcessor(queue, rootDir, targRoot, metadataWriter));
        }
        return processors;
    }

    private class PolyfileProcessor extends FileToFileProcessor {

        public PolyfileProcessor(ArrayBlockingQueue<Path> queue,
                                 Path srcRoot, Path targRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
        }

        @Override
        public String getExtension() {
            return ".json";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {
            if (Files.isRegularFile(outputPath)) {
                LOG.trace("skipping "+relPath);
                return;
            }

            if (! Files.isDirectory(outputPath.getParent())) {
                Files.createDirectories(outputPath.getParent());
            }

            List<String> commandLine = new ArrayList<>();
            commandLine.add("polyfile");
            commandLine.add(srcPath.toAbsolutePath().toString());

            ProcessBuilder pb = new ProcessBuilder(commandLine.toArray(new String[commandLine.size()]));

            FileProcessResult r = ProcessExecutor.execute(pb,
                        timeoutMillis, outputPath, maxErrBufferLength);
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        String metadataWriterString = args[2];
        int numThreads = 10;
        if (args.length > 3) {
            numThreads = Integer.parseInt(args[3]);
        }
        MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString);
        PolyFile runner = new PolyFile(srcRoot, targRoot, metadataWriter, numThreads);
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
