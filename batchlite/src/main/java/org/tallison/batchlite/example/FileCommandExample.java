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
package org.tallison.batchlite.example;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.MetadataWriter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is an example of running the file command on a directory
 * of files
 */
public class FileCommandExample extends AbstractDirectoryProcessor {

    private static final int MAX_BUFFER = 10000;


    public FileCommandExample(ConfigSrc config) throws TikaException, IOException, SAXException {
        super(config);
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue)
            throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new FileCommandProcessor(queue, configSrc, metadataWriter));
        }
        return processors;
    }

    private class FileCommandProcessor extends CommandlineFileProcessor {
        public FileCommandProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                    ConfigSrc configSrc,
                                    MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath) {
            return new String[]{
                    "file",
                    "-b", "--mime-type",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString())
            };
        }
    }

    public static void main(String[] args) throws Exception {
        FileCommandExample runner = new FileCommandExample(
                ConfigSrc.build(args, "file", MAX_BUFFER, MAX_BUFFER)
        );
        runner.execute();
    }
}
