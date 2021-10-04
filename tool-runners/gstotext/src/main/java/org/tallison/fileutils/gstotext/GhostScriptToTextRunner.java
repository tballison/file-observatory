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
package org.tallison.fileutils.gstotext;

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

public class GhostScriptToTextRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GhostScriptToTextRunner.class);

    private static final int MAX_BUFFER = 20000;
    private final long timeoutMillis = 60000;

    private static final String GS = "/ghostscript-9.55.0-linux-x86_64/gs-9550-linux-x86_64";
    public GhostScriptToTextRunner(ConfigSrc config) throws TikaException, IOException {
        super(config);
    }

    public static String getName() {
        return "gstotext";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            GhostScriptToTextProcessor p = new GhostScriptToTextProcessor(queue, configSrc,
                    metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private class GhostScriptToTextProcessor extends FileToFileProcessor {

        public GhostScriptToTextProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
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
            commandLine.add(GS);
            commandLine.add("-DBATCH");
            commandLine.add("-DNOPAUSE");
            commandLine.add("-sDEVICE=txtwrite");
            commandLine.add("-sOutputFile="+outputPath.toAbsolutePath().toString());
            commandLine.add("-dNEWPDF");//this invokes the new c-based pdf parser as of 9.55.0
            commandLine.add(srcPath.toAbsolutePath().toString());
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

        GhostScriptToTextRunner runner = new GhostScriptToTextRunner(ConfigSrc.build(
                args, getName(),
                MAX_BUFFER, MAX_BUFFER));
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
