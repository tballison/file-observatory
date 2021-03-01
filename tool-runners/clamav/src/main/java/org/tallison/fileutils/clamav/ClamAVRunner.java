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
package org.tallison.fileutils.clamav;

import io.sensesecure.clamav4j.ClamAV;
import io.sensesecure.clamav4j.ClamAVException;
import io.sensesecure.clamav4j.ClamAVVersion;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;
import org.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.ConfigSrc;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ClamAVRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ClamAVRunner.class);
    private static final int MAX_STDOUT = 100;
    private static final int MAX_STDERR = 20000;
    //I don't think this actually works
    private static final int TIMEOUT_MILLIS = 120000;


    private final long timeoutMillis = 60000;

    public ClamAVRunner(ConfigSrc config) throws TikaConfigException {
        super(config);
    }

    public static String getName() {
        return "clamav";
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<FetchEmitTuple> queue) throws TikaException, IOException {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            ClamAVProcessor p = new ClamAVProcessor(queue, tikaConfig, metadataWriter);
            p.setFileTimeoutMillis(timeoutMillis);
            processors.add(p);
        }
        return processors;
    }

    private static class ClamAVProcessor extends FileProcessor {
        static AtomicInteger COUNTER = new AtomicInteger(0);



        private final ClamAV clammer = new ClamAV(
                new InetSocketAddress("localhost",3310), TIMEOUT_MILLIS);
        private final int id;
        public ClamAVProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                               TikaConfig tikaConfig, MetadataWriter metadataWriter) throws TikaException, IOException {
            super(queue, tikaConfig, metadataWriter);
            id = COUNTER.getAndIncrement();
            if (id == 0) {
                ClamAVVersion version = clammer.getVersion();
                LOG.info("version={} db={} dbtime={}",
                        version.getClamAvVersion(),
                        version.getDatabaseVersion(),
                        version.getDatabaseTime());
            }
        }

        @Override
        protected void process(String relPath, Path srcPath, MetadataWriter metadataWriter) throws IOException {
            LOG.debug("processing {}", relPath);

            long start = System.currentTimeMillis();
            String val = "";
            String ex = "";
            try {
                val = clammer.scan(FileChannel.open(srcPath, StandardOpenOption.READ));
            } catch (IOException|ClamAVException e) {
                ex = e.getMessage();
                if (ex == null) {
                    ex = e.getClass().toString();
                }
            }
            long elapsed = System.currentTimeMillis()-start;
            FileProcessResult r = new FileProcessResult();
            if (StringUtils.isBlank(ex)) {
                r.setExitValue(0);
            } else {
                r.setExitValue(1);
            }
            r.setStderr(ex);
            r.setStderrLength(ex.length());
            r.setStderrTruncated(false);
            r.setStdout(val);
            r.setStdoutLength(val.length());
            r.setStdoutTruncated(false);
            r.setProcessTimeMillis(elapsed);
            LOG.debug(r.toString());
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
            ClamAVRunner runner = new ClamAVRunner(
                    ConfigSrc.build(args, getName(), MAX_STDOUT, MAX_STDERR)
            );
            runner.execute();
    }
}
