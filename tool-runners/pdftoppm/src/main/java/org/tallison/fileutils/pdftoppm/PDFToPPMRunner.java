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
package org.tallison.fileutils.pdftoppm;

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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PDFToPPMRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PDFToPPMRunner.class);

    private static final int MAX_BUFFER = 20000;
    private final int numThreads;
    private final long timeoutMillis = 60000;

    public PDFToPPMRunner(ConfigSrc config) {
        super(config.getSrcRoot(), config.getMetadataWriter());
        this.numThreads = config.getNumThreads();
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFToPPMProcessor p = new PDFToPPMProcessor(queue,
                    rootDir, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PDFToPPMProcessor extends FileProcessor {

        public PDFToPPMProcessor(ArrayBlockingQueue<Path> queue,
                                 Path srcRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, metadataWriter);
        }


        @Override
        public void process(String relPath, Path srcPath,
                            MetadataWriter metadataWriter) throws IOException {

            Path tmpDir = Files.createTempDirectory("pdftoppm-");
            List<String> commandLine = new ArrayList<>();
            commandLine.add("pdftoppm");
            commandLine.add("-png");
            commandLine.add(srcPath.toAbsolutePath().toString());
            commandLine.add(tmpDir.toAbsolutePath().toString());
            FileProcessResult result = null;
            try {
                result = ProcessExecutor.execute(
                        new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                        timeoutMillis, metadataWriter.getMaxStdoutBuffer(), metadataWriter.getMaxStderrBuffer());
            } finally {
                LOG.debug("starting to delete directory "+tmpDir.toAbsolutePath());
                deleteDirectory(tmpDir);
            }
            LOG.debug("finished deleting directory "+tmpDir.toAbsolutePath());
            LOG.debug(result.toString());
            metadataWriter.write(relPath, result);
        }

        private void deleteDirectory(Path tmpDir) throws IOException {
            Files.walkFileTree(tmpDir,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult postVisitDirectory(
                                Path dir, IOException exc) throws IOException {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(
                                Path file, BasicFileAttributes attrs)
                                throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
    }

    public static void main(String[] args) throws Exception {

        PDFToPPMRunner runner = new PDFToPPMRunner(ConfigSrc.build(args, MAX_BUFFER, MAX_BUFFER));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
