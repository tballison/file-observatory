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
package org.tallison.fileutils.qpdf;

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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class QPDFToJson extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(QPDFToJson.class);

    private static final int MAX_STDERR = 10000;
    private static final long TIMEOUT_MILLIS = 30000;

    public QPDFToJson(ConfigSrc config) throws TikaException, IOException {
        super(config);
    }

    public static String getName() {
        return "qpdf";
    }
    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            QPDFJsonProcessor processor = new QPDFJsonProcessor(queue,
                    configSrc, metadataWriter);
            processor.setFileTimeoutMillis(TIMEOUT_MILLIS);
            processors.add(processor);
        }
        return processors;
    }

    private class QPDFJsonProcessor extends FileToFileProcessor {

        public QPDFJsonProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                 ConfigSrc configSrc, MetadataWriter metadataWriter) throws IOException,
                TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        public String getExtension() {
            return "json";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath, MetadataWriter metadataWriter) throws IOException {

            List<String> commandLine = new ArrayList<>();
            commandLine.add("qpdf");
            commandLine.add("--json");
            commandLine.add(srcPath.toAbsolutePath().toString());

            ProcessBuilder pb = new ProcessBuilder(commandLine.toArray(new String[commandLine.size()]));

            FileProcessResult r = ProcessExecutor.execute(pb,
                    getFileTimeoutMillis(), outputPath, metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
        QPDFToJson runner = new QPDFToJson(ConfigSrc.build(
                args, getName(), 1, MAX_STDERR));
        runner.execute();
    }
}
