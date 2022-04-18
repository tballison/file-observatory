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
package org.tallison.fileutils.pdfcpu;

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
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class PDFCPURunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(org.tallison.fileutils.pdfcpu.PDFCPURunner.class);
    private static final int MAX_STDOUT = 10000;
    private static final int MAX_STDERR = 10000;

    private final long timeoutMillis = 60000;

    public PDFCPURunner(ConfigSrc config) throws TikaException, IOException {
        super(config);
    }

    public static String getName() {
        return "pdfcpu_relaxed";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFCPUProcessor p = new PDFCPUProcessor(queue, configSrc, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PDFCPUProcessor extends FileProcessor {

        public PDFCPUProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                               ConfigSrc configSrc, MetadataWriter metadataWriter) throws IOException,
                TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);
            FileProcessResult r = null;
            Path tmpDotPDF = null;
            try {
                List<String> commandLine = new ArrayList<>();
                commandLine.add("/root/pdfcpu");
                commandLine.add("validate");
                commandLine.add("-mode");
                commandLine.add("relaxed");

                if (! srcPath.toAbsolutePath().endsWith(".pdf")) {
                    tmpDotPDF = Files.createTempFile("cpu-runner", ".pdf");
                    Files.move(srcPath, tmpDotPDF, REPLACE_EXISTING);
                    commandLine.add(tmpDotPDF.toAbsolutePath().toString());
                } else {
                    commandLine.add(srcPath.toAbsolutePath().toString());
                }



                r = ProcessExecutor.execute(
                        new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                        timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                        metadataWriter.getMaxStderrBuffer());

            } finally {
                if (tmpDotPDF != null) {
                    Files.move(tmpDotPDF, srcPath, REPLACE_EXISTING);
                }
            }
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
        PDFCPURunner runner = new org.tallison.fileutils.pdfcpu.PDFCPURunner(
                ConfigSrc.build(args, getName(), MAX_STDOUT, MAX_STDERR)
        );
        runner.execute();
    }
}
