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
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetchiterator.FetchEmitTuple;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Extend this if the process doesn't write an output file...
 * if all you care about is the status of the process and the
 * stderr and stdout.
 *
 * See {@link FileToFileProcessor} for an abstract class that
 * will process a file and write output to a new file.
 */
public abstract class FileProcessor extends AbstractFileProcessor {

    private final MetadataWriter metadataWriter;
    private final Fetcher fetcher;

    public FileProcessor(ArrayBlockingQueue<FetchEmitTuple> queue,
                         TikaConfig tikaConfig, MetadataWriter metadataWriter)
            throws IOException, TikaException {
        super(queue, tikaConfig);
        this.metadataWriter = metadataWriter;
        this.fetcher = tikaConfig.getFetcherManager()
                .getFetcher(AbstractFileProcessor.FETCHER_NAME);
    }

    @Override
    public void process(FetchEmitTuple fetchEmitTuple) throws IOException {
        String relPath = fetchEmitTuple.getFetchKey().getKey();
        try (InputStream is = fetcher.fetch(relPath, new Metadata());
             TikaInputStream tis = TikaInputStream.get(is)) {
            process(relPath, tis.getPath(), metadataWriter);
        } catch (TikaException e) {
            throw new IOException(e);
        }
    }

    protected abstract void process(String relPath, Path srcPath,
                                    MetadataWriter metadataWriter) throws IOException;
}
