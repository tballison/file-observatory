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
package org.tallison.pdfutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class PDFByteSniffer extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(
            PDFByteSniffer.class);
    private static final int MAX_STDOUT = 20000;
    private static final int MAX_STDERR = 10000;


    public PDFByteSniffer(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    public static String getName() {
        return "pdfbytes";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws IOException, TikaException {
        List<AbstractFileProcessor> processors = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            ByteProcessor p = new ByteProcessor(queue, tikaConfig, metadataWriter);
            processors.add(p);
        }
        return processors;
    }

    private class ByteProcessor extends FileProcessor {

        public ByteProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                               TikaConfig tikaConfig, MetadataWriter metadataWriter) throws IOException, TikaException {
            super(queue, tikaConfig, metadataWriter);
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.info("processing {}", relPath);
            long start = System.currentTimeMillis();
            FileProcessResult r = new FileProcessResult();
            try {
                Pair<String, Boolean> jsonTruncatedPair = getJson(srcPath);
                long elapsed = System.currentTimeMillis()-start;
                r.setProcessTimeMillis(elapsed);
                r.setExitValue(0);
                r.setStdout(jsonTruncatedPair.getKey());
                r.setStdoutLength(jsonTruncatedPair.getKey().length());
                r.setStderr("");
                r.setStderrLength(0);
                r.setStdoutTruncated(jsonTruncatedPair.getValue());
                r.setStderrTruncated(false);
                LOG.info("processed {}", r);
            } catch (IOException e) {
                long elapsed = System.currentTimeMillis()-start;
                r.setProcessTimeMillis(elapsed);
                r.setExitValue(1);
                String err = ExceptionUtils.getStackTrace(e);
                r.setStderr(err);
                r.setStderrLength(err.length());
                LOG.warn("problem {} {}", relPath, r);
                e.printStackTrace();
            }
            metadataWriter.write(relPath, r);
        }
    }

    public static Pair<String, Boolean> getJson(Path srcPath) throws IOException {
        Map<String, Object> data = new HashMap<>();
        List<Long> eofs = PDFVersionator.getEOFs(srcPath);
        Pair<Long, byte[]> header = PDFVersionator.getHeader(srcPath);
        data.put("eofs", eofs);
        boolean truncated = false;
        if (header != null) {
            data.put("header-offset", header.getKey());
            data.put("preheader", Base64.encodeBase64String(header.getValue()));
            if (header.getKey() > header.getValue().length) {
                truncated = true;
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        StringWriter w = new StringWriter();
        mapper.writeValue(w, data);
        return Pair.of(w.toString(), truncated);
    }

    public static void main(String[] args) throws Exception {
        PDFByteSniffer runner = new PDFByteSniffer(
                ConfigSrc.build(args, getName(), MAX_STDOUT, MAX_STDERR)
        );
        runner.execute();
    }
}
