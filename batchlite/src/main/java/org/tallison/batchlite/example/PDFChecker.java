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

import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private final String pdfcheckerRoot;
    private final Path targRoot;
    private final int numThreads;

    public PDFChecker(String pdfCheckerRoot, Path srcRoot, Path targRoot, MetadataWriter metadataWriter, int numThreads) {
        super(srcRoot, metadataWriter);
        this.pdfcheckerRoot = pdfCheckerRoot;
        this.targRoot = targRoot;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new FileToFileProcessor(queue, getRootDir(), targRoot, metadataWriter));
        }
        return processors;
    }

    private class FileToFileProcessor extends CommandlineFileToFileProcessor {
        public FileToFileProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot, Path targRoot,
                                   MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
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
        Path srcRoot = Paths.get(args[1]);
        Path targRoot = Paths.get(args[2]);
        String metadataWriterString = args[3];
        int numThreads = 10;
        if (args.length > 4) {
            numThreads = Integer.parseInt(args[4]);
        }
        long start = System.currentTimeMillis();
        MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString, 100000, 100000);

        PDFChecker runner = new PDFChecker(pdfcheckerRoot, srcRoot, targRoot, metadataWriter, numThreads);
        //runner.setMaxFiles(100);
        runner.execute();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Proccessed " + metadataWriter.getRecordsWritten() + " records in " + elapsed + "ms");

    }
}
