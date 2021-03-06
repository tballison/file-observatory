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
import org.apache.tika.pipes.FetchEmitTuple;
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


public class MutoolTextRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MutoolTextRunner.class);
    private static final int MAX_STDOUT = 100;
    private static final int MAX_STDERR = 20000;

    private final long timeoutMillis = 120000;

    public MutoolTextRunner(ConfigSrc configSrc) throws TikaException, IOException {
        super(configSrc);
    }

    public static String getName() {
        return "mutooltext";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue)
            throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            MutoolTextProcessor p = new MutoolTextProcessor(queue, configSrc, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class MutoolTextProcessor extends FileToFileProcessor {

        public MutoolTextProcessor(ArrayBlockingQueue<FetchEmitTuple> queue, ConfigSrc configSrc,
                                   MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        public String getExtension() {
            return "txt";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath, MetadataWriter metadataWriter) throws IOException {

            List<String> commandLine = new ArrayList<>();
            commandLine.add("mutool");
            commandLine.add("convert");
            commandLine.add("-F");
            commandLine.add("text");
            commandLine.add("-o");
            commandLine.add(outputPath.toAbsolutePath().toString());
            commandLine.add(srcPath.toAbsolutePath().toString());
            if (!Files.isDirectory(outputPath.getParent())) {
                Files.createDirectories(outputPath.getParent());
            }
            FileProcessResult r = ProcessExecutor.execute(
                    new ProcessBuilder(commandLine.toArray(new String[0])),
                    timeoutMillis, metadataWriter.getMaxStdoutBuffer(), metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
        MutoolTextRunner runner = new MutoolTextRunner(
                ConfigSrc.build(args, getName(), MAX_STDOUT, MAX_STDERR));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
