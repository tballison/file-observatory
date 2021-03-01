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
package org.tallison.fileutils.pdfinfo;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PDFInfo extends AbstractDirectoryProcessor {

    private static final int MAX_STDOUT = 20000;
    private static final int MAX_STDERR = 20000;
    private static final long TIMEOUT_MILLIS = 60000;

    public PDFInfo(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    public static String getName() {
        return "pdfinfo";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PDFInfoProcessor p = new PDFInfoProcessor(queue, tikaConfig, metadataWriter);
            p.setFileTimeoutMillis(TIMEOUT_MILLIS);
            processors.add(p);
        }
        return processors;
    }

    private class PDFInfoProcessor extends CommandlineFileProcessor {

        public PDFInfoProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                TikaConfig tikaConfig, MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath) throws IOException {
            return new String[] {
                    "pdfinfo",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString())
            };
        }
    }

    public static void main(String[] args) throws Exception {
        PDFInfo runner = new PDFInfo(
                ConfigSrc.build(args, getName(), MAX_STDOUT, MAX_STDERR));
        runner.execute();
    }
}
