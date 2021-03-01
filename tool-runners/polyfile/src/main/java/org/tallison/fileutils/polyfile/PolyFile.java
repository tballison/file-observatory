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
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PolyFile extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PolyFile.class);

    private final int maxErrBufferLength = 100;
    private final long timeoutMillis = 120000;

    public PolyFile(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    public static String getName() {
        return "polyfile";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PolyfileProcessor p = new PolyfileProcessor(queue, tikaConfig, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PolyfileProcessor extends FileToFileProcessor {

        public PolyfileProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                 TikaConfig tikaConfig, MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }

        @Override
        public String getExtension() {
            return "json";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {


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

        PolyFile runner = new PolyFile(
                ConfigSrc.build(args,
                        getName(), 10, 1000));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
