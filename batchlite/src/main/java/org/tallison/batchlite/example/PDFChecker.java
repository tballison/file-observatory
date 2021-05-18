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
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileToFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is an example of running PDFChecker on a directory of files
 * see: https://www.datalogics.com/products/pdf-tools/pdf-checker/
 * <p>
 * This currently hardcodes the use of /CheckerProfiles/everything.json.
 * This assumes that PDFChecker has been installed.
 * <p>
 * commandline example: /home/tallison/tools/pdfchecker/PDF_Checker input output pdfchecker_metadata.csv 6
 */
public class PDFChecker extends AbstractDirectoryProcessor {

    private static final int MAX_STDOUT = 10000;
    private static final int MAX_STDERR = 10000;
    private final String pdfcheckerRoot;

    public PDFChecker(String pdfCheckerRoot, ConfigSrc configSrc)
            throws TikaException, IOException, SAXException {
        super(configSrc);
        this.pdfcheckerRoot = pdfCheckerRoot;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue)
            throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new PDFCheckerProcessor(queue, configSrc, metadataWriter));
        }
        return processors;
    }

    private class PDFCheckerProcessor extends CommandlineFileToFileProcessor {
        public PDFCheckerProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                                   ConfigSrc configSrc,
                                   MetadataWriter metadataWriter) throws IOException,
                TikaException {
            super(queue, configSrc, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath, Path targPath) throws IOException {
            if (!Files.isDirectory(targPath.getParent())) {
                Files.createDirectories(targPath.getParent());
            }
            return new String[]{
                    ProcessUtils.escapeCommandLine(pdfcheckerRoot + "/pdfchecker"),
                    "--profile",
                    ProcessUtils.escapeCommandLine(pdfcheckerRoot + "/CheckerProfiles/everything.json"),
                    "--input",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString()),
                    "-s",
                    ProcessUtils.escapeCommandLine(targPath.toAbsolutePath().toString())
            };
        }

        @Override
        protected String getExtension() {
            return ".json";
        }
    }

    public static void main(String[] args) throws Exception {
        String pdfcheckerRoot = args[0];
        String[] newArgs = new String[args.length-1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        ConfigSrc configSrc = ConfigSrc.build(newArgs, "pdfchecker", MAX_STDOUT, MAX_STDERR);
        long start = System.currentTimeMillis();

        PDFChecker runner = new PDFChecker(pdfcheckerRoot, configSrc);
        //runner.setMaxFiles(100);
        runner.execute();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Processed " +
                configSrc.getMetadataWriter().getRecordsWritten() + " records in " + elapsed + "ms");

    }
}
