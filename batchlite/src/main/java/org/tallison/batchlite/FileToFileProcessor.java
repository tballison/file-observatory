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
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This takes a fetch emit tuple, spools the fetch inputstream to a
 * local temp file and then emits the temporary output file.
 */
public abstract class FileToFileProcessor extends AbstractFileProcessor {

    private final MetadataWriter metadataWriter;
    private final Fetcher fetcher;
    private final StreamEmitter emitter;

    public FileToFileProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                               TikaConfig tikaConfig, MetadataWriter metadataWriter)
            throws IOException, TikaException {
        super(queue, tikaConfig);
        this.metadataWriter = metadataWriter;
        this.fetcher = tikaConfig.getFetcherManager().getFetcher(AbstractFileProcessor.FETCHER_NAME);
        Emitter tmp = tikaConfig.getEmitterManager().getEmitter(AbstractFileProcessor.EMITTER_NAME);
        if (!(tmp instanceof StreamEmitter)) {
            throw new IllegalArgumentException("only supports stream emitter for now");
        }
        emitter = (StreamEmitter) tmp;

    }

    @Override
    public void process(FetchEmitTuple tuple) throws IOException {
        String relPath = tuple.getFetchKey().getKey();
        try (TemporaryResources tmp = new TemporaryResources()) {
            try (InputStream is = fetcher.fetch(relPath, new Metadata());
                 TikaInputStream tis = TikaInputStream.get(is)) {
                Path tmpSrcFile = tis.getPath();
                Path tmpOutFile = tmp.createTempFile();
                process(relPath, tmpSrcFile, tmpOutFile, metadataWriter);
                try (InputStream stream = TikaInputStream.get(tmpOutFile)) {
                    emitter.emit(relPath, stream, new Metadata());
                }
            } catch (TikaException e) {
                throw new IOException(e);
            }
        }
    }

    protected String getExtension() {
        return "";
    }

    protected abstract void process(String relPath, Path srcPath,
                                    Path outputPath, MetadataWriter metadataWriter) throws IOException;
}
