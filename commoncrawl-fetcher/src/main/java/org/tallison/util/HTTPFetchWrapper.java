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
package org.tallison.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPFetchWrapper {


    private static Logger LOGGER = LoggerFactory.getLogger(HTTPFetchWrapper.class);

    private HttpFetcher fetcher = new HttpFetcher();
    private int maxTriesOn503 = 3;

    //backoff
    private long[] sleepMs = new long[]{0, 30000, 120000, 600000};

    public HTTPFetchWrapper() throws TikaConfigException {
        fetcher.initialize(Collections.EMPTY_MAP);
    }

    public InputStream openStream(String url) throws IOException, TikaException {
        FetchEmitTuple fetchEmitTuple = new FetchEmitTuple(
                url,
                new FetchKey("", url),
                new EmitKey()
        );
        return fetch(fetchEmitTuple);
    }

    public InputStream fetch(FetchEmitTuple t) throws IOException, TikaException {
        int tries = 0;
        IOException ex = null;
        while (tries++ < maxTriesOn503) {
            try {
                return _fetch(t);
            } catch (IOException e) {
                if (e.getMessage() == null) {
                    throw e;
                }
                Matcher m = Pattern.compile("bad status code: (\\d+)").matcher(e.getMessage());
                if (m.find() && m.group(1).equals("503")) {
                    long sleep = sleepMs[tries];
                    LOGGER.warn("got backoff warning: {}. Will sleep {} ms", e.getMessage(), sleep);
                    //sleep, back off
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException exc) {
                        throw ex;
                    }
                    ex = e;
                } else {
                    throw e;
                }
            }
        }
        throw ex;
    }

    private InputStream _fetch(FetchEmitTuple t) throws IOException, TikaException {
        FetchKey fetchKey = t.getFetchKey();
        Metadata metadata = new Metadata();
        if (fetchKey.getRangeStart() > 0) {
            return fetcher.fetch(fetchKey.getFetchKey(), fetchKey.getRangeStart(),
                    fetchKey.getRangeEnd(), metadata);
        } else {
            return fetcher.fetch(fetchKey.getFetchKey(), metadata);
        }

    }
}
