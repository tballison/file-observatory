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
package org.tallison.fileutils.tika;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileToFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TikaBatch extends AbstractDirectoryProcessor {


    private static final Logger LOG = LoggerFactory.getLogger(TikaBatch.class);
    private static int MAX_STDOUT = 10000;
    private static int MAX_STDERR = 10000;

    private final String[] tikaServerUrls;

    public TikaBatch(ConfigSrc config,
                     String tikaServerHost, int[] ports) throws TikaConfigException {
        super(config);
        this.tikaServerUrls = new String[ports.length];
        for (int i = 0; i < ports.length; i++) {
            tikaServerUrls[i] = tikaServerHost+":"+ports[i];
        }
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        setMaxFiles(1000);
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new TikaProcessor(queue, tikaConfig, metadataWriter, tikaServerUrls));
        }
        return processors;
    }

    private class TikaProcessor extends FileToFileProcessor {

        private final TikaServerClient tikaClient;

        public TikaProcessor(ArrayBlockingQueue<FetchEmitTuple> queue, TikaConfig tikaConfig, MetadataWriter metadataWriter,
                             String[] tikaServerUrls) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
            tikaClient = new TikaServerClient(TikaServerClient.INPUT_METHOD.INPUTSTREAM,
                    tikaServerUrls);
        }

        @Override
        public String getExtension() {
            return ".json";
        }

        @Override
        public void process(String relPath, Path srcPath,
                            Path outputPath, MetadataWriter metadataWriter)
                throws IOException {

            int exitValue = 0;
            long start = System.currentTimeMillis();
            List<Metadata> metadataList = null;
            try (TikaInputStream tis = TikaInputStream.get(srcPath)) {
                metadataList = tikaClient.parse(relPath, tis);
            } catch (IOException | TikaClientException e) {
                LOG.error("error on {}", relPath, e);
                exitValue = 1;
            }

            if (exitValue == 0) {
                if (!Files.isDirectory(outputPath.getParent())) {
                    Files.createDirectories(outputPath.getParent());
                }
                try (Writer writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
                    JsonMetadataList.toJson(metadataList, writer);
                } catch (IOException e) {
                    LOG.warn("problem writing json", e);
                }
            }


            long elapsed = System.currentTimeMillis() - start;
            String stackTrace = getStackTrace(metadataList);
            FileProcessResult r = new FileProcessResult();
            r.setExitValue(exitValue);
            r.setProcessTimeMillis(elapsed);
            r.setStderr(stackTrace);
            r.setStderrLength(stackTrace.length());
            r.setStdout("");
            r.setStdoutLength(0);
            r.setTimeout(false);//fix this
            metadataWriter.write(relPath, r);
        }

        private String getStackTrace(List<Metadata> metadataList) {
            if (metadataList == null || metadataList.size() == 0) {
                return "";
            }
            String stack = metadataList.get(0).get(TikaCoreProperties.CONTAINER_EXCEPTION);
            if (stack != null) {
                return stack;
            }
            return "";
        }
    }

    public static void main(String[] args) throws Exception {
        String tikaServerUrl = args[4];
        String portString = args[5];
        Matcher m = Pattern.compile("\\A(\\d+)-(\\d+)\\Z").matcher(portString);
        int[] ports;
        long begin = System.currentTimeMillis();
        if (m.find()) {
            int start = Integer.parseInt(m.group(1));
            int end = Integer.parseInt(m.group(2));
            ports = new int[end-start+1];
            for (int p = start, i = 0; p <= end; p++, i++) {
                ports[i] = p;
            }
        } else {
            ports = new int[1];
            ports[0] = Integer.parseInt(portString);
        }
        TikaBatch runner = new TikaBatch(ConfigSrc.build(args,
                MAX_STDOUT, MAX_STDERR), tikaServerUrl, ports);
        //runner.setMaxFiles(100);
        runner.execute();
        System.out.println("finished in "+(System.currentTimeMillis()-begin) + " ms");
    }
}

