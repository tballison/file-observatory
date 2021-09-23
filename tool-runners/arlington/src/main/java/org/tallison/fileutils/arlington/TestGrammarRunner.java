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
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class TestGrammarRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TestGrammarRunner.class);

    private static final int MAX_BUFFER = 20000;
    private final long timeoutMillis = 120000;

    public static String getName() {
        return "arlington";
    }

    public TestGrammarRunner(ConfigSrc config) throws IOException, TikaException {
        super(config);
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            TestGrammarProcessor p = new TestGrammarProcessor(queue, configSrc, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class TestGrammarProcessor extends FileToFileProcessor {

        public TestGrammarProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                    ConfigSrc configSrc, MetadataWriter metadataWriter) throws IOException,
                TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        public String getExtension() {
            return "txt";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {

            List<String> commandLine = new ArrayList<>();
            commandLine.add("./TestGrammar");
            commandLine.add("-p");
            commandLine.add(srcPath.toAbsolutePath().toString());
            commandLine.add("-t");
            commandLine.add("/arlington-pdf-model/tsv/latest");
            commandLine.add("-o");
            commandLine.add(outputPath.toAbsolutePath().toString());
            commandLine.add("--brief");

            ProcessBuilder pb = new ProcessBuilder(commandLine.toArray(new String[commandLine.size()]));
            pb.directory(new File("/arlington-pdf-model/bin"));
            FileProcessResult r = ProcessExecutor.execute(pb,
                        timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                    metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {

        TestGrammarRunner runner = new TestGrammarRunner(
                ConfigSrc.build(args, getName(), MAX_BUFFER, MAX_BUFFER));

        runner.execute();
    }
}
