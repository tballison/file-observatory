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
package org.tallison.fileutils.arlington;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrcTarg;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class TestGrammarRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TestGrammarRunner.class);

    private static final int MAX_BUFFER = 20000;
    private final int numThreads;
    private final long timeoutMillis = 120000;
    private final Path targRoot;

    public TestGrammarRunner(ConfigSrcTarg config) {
        super(config.getSrcRoot(), config.getMetadataWriter());
        this.targRoot = config.getTargRoot();
        this.numThreads = config.getNumThreads();
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            TestGrammarProcessor p = new TestGrammarProcessor(queue, rootDir, targRoot, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class TestGrammarProcessor extends FileToFileProcessor {

        public TestGrammarProcessor(ArrayBlockingQueue<Path> queue,
                                    Path srcRoot, Path targRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
        }

        @Override
        public String getExtension() {
            return ".txt";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {
            if (Files.isRegularFile(outputPath)) {
                LOG.info("skipping "+relPath);
                return;
            }
            List<String> commandLine = new ArrayList<>();
            commandLine.add("./TestGrammar");
            commandLine.add(srcPath.toAbsolutePath().toString());
            commandLine.add("/grammar/tsv/latest");
            commandLine.add(outputPath.toAbsolutePath().toString());
            if (! Files.isDirectory(outputPath.getParent())) {
                try {
                    Files.createDirectories(outputPath.getParent());
                } catch (FileAlreadyExistsException e) {
                    //swallow
                }
            }
            ProcessBuilder pb = new ProcessBuilder(commandLine.toArray(new String[commandLine.size()]));
            pb.directory(new File("/grammar/"));
            FileProcessResult r = ProcessExecutor.execute(pb,
                        timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                    metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {

        TestGrammarRunner runner = new TestGrammarRunner(
                ConfigSrcTarg.build(args, 1, MAX_BUFFER));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
