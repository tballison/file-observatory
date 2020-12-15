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
package org.tallison.fileutils.pdfminer;

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

public class PDFMinerText extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PDFMinerText.class);
    private static final int MAX_STDOUT = 100;
    private static final int MAX_STDERR = 20000;

    private final int numThreads;
    private final long timeoutMillis = 60000;
    private final Path targRoot;

    public PDFMinerText(ConfigSrcTarg config) {
        super(config.getSrcRoot(), config.getMetadataWriter());
        this.targRoot = config.getTargRoot();
        this.numThreads = config.getNumThreads();
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFMinerTextProcessor p = new PDFMinerTextProcessor(queue, rootDir, targRoot, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PDFMinerTextProcessor extends FileToFileProcessor {

        public PDFMinerTextProcessor(ArrayBlockingQueue<Path> queue,
                                     Path srcRoot, Path targRoot,
                                     MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, Path targPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);

            if (! Files.isDirectory(targPath.getParent())) {
                Files.createDirectories(targPath.getParent());
            }
            List<String> commandLine = new ArrayList<>();
            commandLine.add("python");
            commandLine.add("/usr/local/bin/pdf2txt.py");
            commandLine.add("--outfile");
            commandLine.add(targPath.toAbsolutePath().toString());

            commandLine.add(srcPath.toAbsolutePath().toString());


            FileProcessResult r = ProcessExecutor.execute(
                    new ProcessBuilder(commandLine.toArray(new String[0])),
                    timeoutMillis, 0,
                    metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);

        }

        @Override
        protected String getExtension() {
            return ".txt";
        }
    }

    public static void main(String[] args) throws Exception {
        PDFMinerText runner = new PDFMinerText(
                ConfigSrcTarg.build(args, MAX_STDOUT, MAX_STDERR)
        );
        runner.execute();
    }
}
