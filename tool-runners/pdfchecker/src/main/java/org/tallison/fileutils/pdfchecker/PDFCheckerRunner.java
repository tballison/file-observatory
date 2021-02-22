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
package org.tallison.fileutils.pdfchecker;

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

public class PDFCheckerRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PDFCheckerRunner.class);

    private static final int MAX_BUFFER = 20000;
    private final long timeoutMillis = 60000;

    public PDFCheckerRunner(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFCheckerProcessor p = new PDFCheckerProcessor(queue, tikaConfig, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PDFCheckerProcessor extends FileToFileProcessor {

        public PDFCheckerProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                   TikaConfig tikaConfig, MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }


        @Override
        public String getExtension() {
            return ".json";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {

            List<String> commandLine = new ArrayList<>();
            commandLine.add("/pdfchecker-bin/PDF_Checker/pdfchecker");
            commandLine.add("-j");
            commandLine.add("/pdfchecker-bin/PDF_Checker/CheckerProfiles/everything.json");
            commandLine.add("-i");
            commandLine.add(srcPath.toAbsolutePath().toString());
            commandLine.add("-s");
            commandLine.add(outputPath.toAbsolutePath().toString());
            if (! Files.isDirectory(outputPath.getParent())) {
                Files.createDirectories(outputPath.getParent());
            }
            FileProcessResult r = ProcessExecutor.execute(
                        new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                        timeoutMillis, 0, metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {

        PDFCheckerRunner runner = new PDFCheckerRunner(ConfigSrc.build(args, MAX_BUFFER, MAX_BUFFER));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
