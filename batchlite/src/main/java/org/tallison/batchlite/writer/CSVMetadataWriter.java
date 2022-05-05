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
package org.tallison.batchlite.writer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CSVMetadataWriter extends MetadataWriter {

    private static String[] HEADER = new String[]{
            "path", "exitValue", "isTimeout", "processTimeMillis",
            "stderr", "stderrLength", "stderrTruncated",
            "stdout", "stdoutLength", "stdoutTruncated"
    };

    private final CSVPrinter printer;

    CSVMetadataWriter(String name,
                      Path csvFile, int stdoutLength, int stderrLength) throws IOException {
        super(name, stdoutLength, stderrLength);
        printer = new CSVPrinter(
                Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8),
                CSVFormat.EXCEL);
        printer.printRecord(HEADER);
    }

    @Override
    protected void close() throws IOException {
        printer.flush();
        printer.close();
    }

    @Override
    public void write(PathResultPair pair) throws IOException {
        //TODO: truncate stdout, stder
        List<String> cols = new ArrayList<>();
        FileProcessResult result = pair.getResult();
        cols.add(pair.getRelPath());
        cols.add(Integer.toString(result.getExitValue()));
        cols.add(Boolean.toString(result.isTimeout()));
        cols.add(Long.toString(result.getProcessTimeMillis()));
        cols.add(result.getStderr());
        cols.add(Long.toString(result.getStderrLength()));
        cols.add(Boolean.toString(result.isStderrTruncated()));
        cols.add(result.getStdout());
        cols.add(Long.toString(result.getStdoutLength()));
        cols.add(Boolean.toString(result.isStdoutTruncated()));
        printer.printRecord(cols);
    }
}
