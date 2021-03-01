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
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineStdoutToFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.MetadataWriter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is an example of running PDFChecker on a directory of files
 * see: https://www.datalogics.com/products/pdf-tools/pdf-checker/
 *
 * This currently hardcodes the use of /CheckerProfiles/everything.json.
 * This assumes that PDFChecker has been installed.
 *
 * commandline example: /home/tallison/tools/pdfchecker/PDF_Checker input output pdfchecker_metadata.csv 6
 *
 * This is an example of redirecting stdout rather than relying on PDFChecker's -s flag
 */
public class PDFStdoutChecker extends AbstractDirectoryProcessor {

    private final String pdfcheckerRoot;

    public PDFStdoutChecker(String pdfcheckerRoot, ConfigSrc config) throws TikaException, IOException, SAXException {
        super(config);
        this.pdfcheckerRoot = pdfcheckerRoot;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue)
            throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new PDFCheckerProcessor(queue, tikaConfig, metadataWriter));
        }
        return processors;
    }

    private class PDFCheckerProcessor extends CommandlineStdoutToFileProcessor {
        public PDFCheckerProcessor(ArrayBlockingQueue<FetchEmitTuple> queue, TikaConfig tikaConfig,
                                   MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath) throws IOException {
            return new String[]{
                    ProcessUtils.escapeCommandLine(pdfcheckerRoot+"/pdfchecker"),
                    "--profile",
                    ProcessUtils.escapeCommandLine(pdfcheckerRoot+"/CheckerProfiles/everything.json"),
                    "--input",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString())
            };
        }

        @Override
        protected String getExtension() {
            return ".txt";
        }
    }

    public static void main(String[] args) throws Exception {
        String pdfCheckerRoot = args[0];
        String[] newArgs = new String[args.length-1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        ConfigSrc configSrc = ConfigSrc.build(newArgs, "pdfchecker",10000, 10000);
        PDFStdoutChecker runner = new PDFStdoutChecker(pdfCheckerRoot, configSrc);
        runner.execute();
    }
}