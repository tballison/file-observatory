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

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.FetchEmitTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This takes a fetch emit tuple, spools the fetch inputstream to a
 * local temp file and then emits the temporary output file.
 */
public abstract class FileToFileProcessor extends AbstractFileProcessor {

    private static Logger LOGGER = LoggerFactory.getLogger(FileToFileProcessor.class);

    private final MetadataWriter metadataWriter;
    private final StreamEmitter emitter;
    public FileToFileProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                               ConfigSrc configSrc,
                               MetadataWriter metadataWriter)
            throws IOException, TikaException {
        super(queue, configSrc);
        this.metadataWriter = metadataWriter;

        Emitter tmp = configSrc.getEmitter();
        if (!(tmp instanceof StreamEmitter)) {
            throw new IllegalArgumentException("only supports stream emitter for now");
        }
        this.emitter = (StreamEmitter) tmp;
    }

    @Override
    public void process(FetchEmitTuple tuple) throws IOException {
        String relPath = tuple.getFetchKey().getFetchKey();
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path tmpOutFile = null;
            try (InputStream is = configSrc.getFetcher().fetch(relPath, new Metadata());
                 TikaInputStream tis = TikaInputStream.get(is)) {
                Path tmpSrcFile = tis.getPath();
                //temporary workaround for arlington
                //switch this back to tmp.createTempFile()
                 tmpOutFile = tmpSrcFile.getParent().resolve(
                        tmpSrcFile.getFileName().toString() + "-" + UUID.randomUUID());

                process(relPath, tmpSrcFile, tmpOutFile, metadataWriter);
                if (Files.size(tmpOutFile) > 0) {
                    try (InputStream stream = TikaInputStream.get(tmpOutFile)) {
                        String outPath = metadataWriter.getName()+"/"+relPath+"."+getExtension();
                        emitter.emit(outPath, stream, new Metadata());
                    }
                } else {
                    LOGGER.warn("empty file for: "+relPath + " src=" +
                            tmpSrcFile.toAbsolutePath() + " out=" + tmpOutFile.toAbsolutePath());
                }
            } catch (TikaException e) {
                throw new IOException(e);
            } finally {
                if (tmpOutFile != null) {
                    Files.delete(tmpOutFile);
                }
            }
        }
    }

    protected String getExtension() {
        return "";
    }

    protected abstract void process(String relPath, Path srcPath,
                                    Path outputPath, MetadataWriter metadataWriter) throws IOException;
}
