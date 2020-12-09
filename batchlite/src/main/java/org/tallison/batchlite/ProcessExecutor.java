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
package org.tallison.batchlite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProcessExecutor {

    private static final String EMPTY = "";

    /**
     * This writes stdout and stderr to the FileProcessResult.
     *
     * @param pb
     * @param timeoutMillis
     * @param maxStdoutBuffer
     * @param maxStdErrBuffer
     * @return
     * @throws IOException
     */
    public static FileProcessResult execute(ProcessBuilder pb,
                                            long timeoutMillis,
                                            int maxStdoutBuffer, int maxStdErrBuffer) throws IOException {
        Process p = pb.start();
        long elapsed = -1;
        long start = System.currentTimeMillis();
        StreamEater outGobbler = new StreamEater(p.getInputStream(), maxStdoutBuffer);
        StreamEater errGobbler = new StreamEater(p.getErrorStream(), maxStdErrBuffer);

        Thread outThread = new Thread(outGobbler);
        outThread.start();

        Thread errThread = new Thread(errGobbler);
        errThread.start();
        int exitValue = -1;
        boolean complete = false;
        try {
            complete = p.waitFor(timeoutMillis, TimeUnit.MILLISECONDS);
            elapsed = System.currentTimeMillis() - start;
            if (complete) {
                exitValue = p.exitValue();
                outThread.join(1000);
                errThread.join(1000);
            } else {
                p.destroyForcibly();
                outThread.join(1000);
                errThread.join(1000);
            }
        } catch (InterruptedException e) {
            exitValue = -1000;
        }
        FileProcessResult result = new FileProcessResult();
        result.processTimeMillis = elapsed;
        result.stderrLength = errGobbler.getStreamLength();
        result.stdoutLength = outGobbler.getStreamLength();
        result.isTimeout = ! complete;
        result.exitValue = exitValue;
        result.stdout = joinWith("\n", outGobbler.getLines());
        result.stderr = joinWith("\n", errGobbler.getLines());
        result.stdoutTruncated = outGobbler.getIsTruncated();
        result.stderrTruncated = errGobbler.getIsTruncated();
        return result;
    }

    /**
     * This redirects stdout to stdoutRedirect.
     *
     * @param pb
     * @param timeoutMillis
     * @param stdoutRedirect
     * @param maxStdErrBuffer
     * @return
     * @throws IOException
     */
    public static FileProcessResult execute(ProcessBuilder pb,
                                            long timeoutMillis,
                                            Path stdoutRedirect, int maxStdErrBuffer) throws IOException {

        if (!Files.isDirectory(stdoutRedirect.getParent())) {
            Files.createDirectories(stdoutRedirect.getParent());
        }

        pb.redirectOutput(stdoutRedirect.toFile());
        Process p = pb.start();
        long elapsed = -1;
        long start = System.currentTimeMillis();
        StreamEater errGobbler = new StreamEater(p.getErrorStream(), maxStdErrBuffer);

        Thread errThread = new Thread(errGobbler);
        errThread.start();
        int exitValue = -1;
        boolean complete = false;
        try {
            complete = p.waitFor(timeoutMillis, TimeUnit.MILLISECONDS);
            elapsed = System.currentTimeMillis() - start;
            if (complete) {
                exitValue = p.exitValue();
                errThread.join(1000);
            } else {
                p.destroyForcibly();
                errThread.join(1000);
            }
        } catch (InterruptedException e) {
            exitValue = -1000;
        }
        FileProcessResult result = new FileProcessResult();
        result.processTimeMillis = elapsed;
        result.stderrLength = errGobbler.getStreamLength();
        result.stdoutLength = Files.size(stdoutRedirect);
        result.isTimeout = ! complete;
        result.exitValue = exitValue;
        result.stdout = "";
        result.stderr = joinWith("\n", errGobbler.getLines());
        result.stdoutTruncated = false;
        result.stderrTruncated = errGobbler.getIsTruncated();
        return result;

    }

    private static String joinWith(String delimiter, List<String> lines) {
        if (lines.size() == 0) {
            return EMPTY;
        }
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            if (i++ > 0) {
                sb.append(delimiter);
            }
            sb.append(line);
        }
        return sb.toString();
    }
}
