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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.FetchEmitTuple;

public class PolyFilePolyglot extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PolyFilePolyglot.class);

    private final int maxErrBufferLength = 100;
    private final long timeoutMillis = 300000;

    public PolyFilePolyglot(ConfigSrc config) throws TikaException, IOException {
        super(config);
    }

    public static String getName() {
        return "polyfile";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PolyFilePolyGlotProcessor p = new PolyFilePolyGlotProcessor(queue, configSrc, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class PolyFilePolyGlotProcessor extends FileProcessor {

        public PolyFilePolyGlotProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                              ConfigSrc configSrc,
                              MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);

            List<String> commandLine = new ArrayList<>();
            commandLine.add("polyfile");
            commandLine.add("-I");
            commandLine.add(srcPath.toAbsolutePath().toString());


            FileProcessResult r = ProcessExecutor.execute(
                    new ProcessBuilder(commandLine.toArray(new String[0])),
                    timeoutMillis, metadataWriter.getMaxStdoutBuffer(),
                    metadataWriter.getMaxStderrBuffer());
            metadataWriter.write(relPath, r);

        }

    }


    public static void main(String[] args) throws Exception {

        PolyFilePolyglot runner = new PolyFilePolyglot(
                ConfigSrc.build(args,
                        getName(), 10000, 1000));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
