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
package org.tallison.cc.fetcherlite;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.CCIndexReaderCounter;
import org.tallison.cc.index.AbstractRecordProcessor;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.CompositeRecordFilter;
import org.tallison.cc.index.RecordFilter;

public class FetchLiteRecordProcessor extends AbstractRecordProcessor {
    @Override
    public void usage() {

    }
    private static Logger LOGGER = LoggerFactory.getLogger(FetchLiteRecordProcessor.class);

    private final FetcherLiteConfig fetcherLiteConfig;
    private final ArrayBlockingQueue<String> truncatedUrls;
    private final CCIndexReaderCounter counter;

    private final FileFromCCWarcFetcher fileFromCCWarcFetcher;

    private final RecordFilter recordFilter;

    public FetchLiteRecordProcessor(FetcherLiteConfig fetcherLiteConfig,
                                    ArrayBlockingQueue<String> truncatedUrls,
                                    CCIndexReaderCounter counter) throws TikaConfigException,IOException {
        this.fetcherLiteConfig = fetcherLiteConfig;
        this.truncatedUrls = truncatedUrls;
        this.counter = counter;
        this.fileFromCCWarcFetcher = new FileFromCCWarcFetcher(
                fetcherLiteConfig.getFilesDirectory());
        this.recordFilter = CompositeRecordFilter.load(fetcherLiteConfig.getFilterFile());
    }

    @Override
    public boolean process(String json) throws IOException {
        long totalRead = counter.getRecordsRead().incrementAndGet();
        if (totalRead % 100000 == 0) {
            LOGGER.info("processed: {}", counter);
        }
        if (fetcherLiteConfig.getMaxRecords() > -1 && totalRead >= fetcherLiteConfig.getMaxRecords()) {
            LOGGER.info("hit max read");
            return false;
        }
        //check for hit max
        //return false;

        List<CCIndexRecord> records = CCIndexRecord.parseRecords(json);
        if (records.size() == 0) {
            //log problem
            return true;
        }
        CCIndexRecord r = records.get(0);
        if (!recordFilter.accept(r)) {
            return true;
        }
        if (! StringUtils.isBlank(r.getTruncated())) {
            long truncated = counter.getTruncatedWritten().incrementAndGet();
            if (fetcherLiteConfig.getMaxFilesTruncated() > -1 &&
            truncated >= fetcherLiteConfig.getMaxFilesTruncated()) {
                LOGGER.info("hit max truncated files");
                return false;
            }
            //potentially hangs
            try {
                String url = r.getUrl();
                if (StringUtils.isBlank(url)) {
                    //do nothing
                } else {
                    truncatedUrls.put(url);
                }
            } catch (InterruptedException e) {
                LOGGER.debug("interrupted while trying to put to truncated urls");
                return false;
            }
            return true;
        } else {
            long extracted = counter.getFilesExtracted().incrementAndGet();
            if (fetcherLiteConfig.getMaxFilesExtracted() > -1 &&
                    extracted >= fetcherLiteConfig.getMaxFilesExtracted()) {
                LOGGER.info("hit max extracted files");
                return false;
            }
            fetchBytes(r);
            return true;
        }
    }

    private void fetchBytes(CCIndexRecord r) {
        fileFromCCWarcFetcher.fetchToPath(r);
    }

    @Override
    public void close() throws IOException {

    }
}
