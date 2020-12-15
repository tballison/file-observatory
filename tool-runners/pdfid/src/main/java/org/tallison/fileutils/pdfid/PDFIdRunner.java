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
package org.tallison.fileutils.pdfid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.ConfigSrcTarg;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PDFIdRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PDFIdRunner.class);
    private static final int MAX_STDOUT = 100000;
    private static final int MAX_STDERR = 20000;

    private final int numThreads;
    private final long timeoutMillis = 60000;

    public PDFIdRunner(ConfigSrc config) {
        super(config.getSrcRoot(), config.getMetadataWriter());
        this.numThreads = config.getNumThreads();
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFIdProcessor p = new PDFIdProcessor(queue, rootDir, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PDFIdProcessor extends FileProcessor {

        public PDFIdProcessor(ArrayBlockingQueue<Path> queue,
                                     Path srcRoot,
                                     MetadataWriter metadataWriter) {
            super(queue, srcRoot, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);

            List<String> commandLine = new ArrayList<>();
            commandLine.add("python");
            commandLine.add("/pdfid/didierstevens/pdfid.py");
            commandLine.add(srcPath.toAbsolutePath().toString());


            FileProcessResult r = ProcessExecutor.execute(
                    new ProcessBuilder(commandLine.toArray(new String[0])),
                    timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                    metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);

        }

    }

    public static void main(String[] args) throws Exception {
        PDFIdRunner runner = new PDFIdRunner(
                ConfigSrc.build(args, MAX_STDOUT, MAX_STDERR)
        );
        runner.execute();
    }
}
