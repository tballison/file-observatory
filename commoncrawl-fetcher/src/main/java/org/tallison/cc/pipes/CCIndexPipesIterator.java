/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.cc.pipes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.Field;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class CCIndexPipesIterator extends PipesIterator implements Initializable {
    static Logger LOGGER = LoggerFactory.getLogger(CCIndexPipesIterator.class);

    Path indexPathsFile = null;
    List<String> indexPathsUrls = null;

    @Override
    protected void enqueue() throws IOException, TimeoutException, InterruptedException {
        if (indexPathsUrls != null) {
            LOGGER.info("indexPathsUrls size: {}", indexPathsUrls.size());
            int i = 0;
            for (String indexPathUrl : indexPathsUrls) {
                LOGGER.info("Opening {}: {} ", i++, indexPathUrl);
                try (BufferedReader reader = getUrlReader(indexPathUrl)) {
                    String line = reader.readLine();
                    while (line != null) {
                        tryToAdd(line);
                        line = reader.readLine();
                    }
                }
            }
        } else {

        }
    }

    private BufferedReader getUrlReader(String indexPathsUrl) throws IOException {
        return new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new URL(indexPathsUrl).openStream()),
                        StandardCharsets.UTF_8));
    }

    private BufferedReader getFileReader() throws IOException {
        if (indexPathsFile.endsWith(".gz")) {
            return new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(Files.newInputStream(indexPathsFile)),
                            StandardCharsets.UTF_8));
        } else {
            return Files.newBufferedReader(indexPathsFile, StandardCharsets.UTF_8);
        }

    }

    @Field
    public void setIndexPathsFile(String path) {
        indexPathsFile = Paths.get(path);
    }

    @Field
    public void setIndexPathsUrls(List<String> urls) {
        indexPathsUrls = urls;
    }

    @Override
    public void checkInitialization(InitializableProblemHandler problemHandler)
            throws TikaConfigException {
        if (indexPathsUrls == null && indexPathsFile == null) {
            throw new TikaConfigException("must specify an indexPathsFile or an indexPathsUrl");
        }
        if (indexPathsFile != null && !Files.isRegularFile(indexPathsFile)) {
            throw new TikaConfigException("indexPathsFile must exist");
        }

        if (indexPathsUrls != null && indexPathsUrls.size() == 0) {
            throw new TikaConfigException("indexPathsUrls must not be empty");
        }

    }


    private void tryToAdd(String line) throws InterruptedException, TimeoutException {
        if (line.trim().endsWith(".gz")) {
            FetchEmitTuple t = new FetchEmitTuple(line, new FetchKey(getFetcherName(), line),
                    new EmitKey(getEmitterName(), line));
            tryToAdd(t);
        }
    }
}
