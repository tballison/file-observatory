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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;


import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.util.HTTPFetchWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public class FileFromCCWarcFetcher {
    private static Logger LOGGER = LoggerFactory.getLogger(FetchLiteRecordProcessor.class);

    private HTTPFetchWrapper fetcher;
    private final Path filesDir;

    public FileFromCCWarcFetcher(Path filesDir) throws TikaConfigException {
        this.filesDir = filesDir;
        this.fetcher = new HTTPFetchWrapper();
    }
    public void fetchToPath(CCIndexRecord record) {

        LOGGER.debug("going to fetch {} {}->{}", record.getFilename(),
                record.getOffset(), record.getLength());
        String warcUrl = FetcherLiteConfig.CC_URL_BASE + record.getFilename();
        FetchEmitTuple t = new FetchEmitTuple(record.getFilename(),
                new FetchKey("", warcUrl, record.getOffset(),
                        record.getOffset() + record.getLength()-1),
                new EmitKey()
        );
        byte[] warcRecordGZBytes = new byte[0];
        try {
            warcRecordGZBytes = fetchWarcBytes(t);
        } catch (TikaException|IOException e) {
            LOGGER.warn("couldn't get bytes from cc's warc " + t, e);
            return;
        }
        String id = record.getUrl();
        try {
            parseWarc(id, warcRecordGZBytes);
        } catch (IOException e) {
            LOGGER.warn("problem parsing warc file", e);
        }
    }


    private void fetchPayload(String id, WarcRecord record) throws IOException {
        if (!((record instanceof WarcResponse) &&
                record.contentType().base().equals(MediaType.HTTP))) {
            return;
        }

        Optional<WarcPayload> payload = ((WarcResponse) record).payload();
        if (!payload.isPresent()) {
            LOGGER.debug("no payload {}", id);
            return;
        }
        if (payload.get().body().size() == 0) {
            LOGGER.debug("empty payload id={}", id);
            return;
        }

        Path tmp = Files.createTempFile("ccfile-fetcher-", "");
        try {
            Files.copy(payload.get().body().stream(), tmp, StandardCopyOption.REPLACE_EXISTING);
            String targetDigest = null;

            try (InputStream is = Files.newInputStream(tmp)) {
                targetDigest = DigestUtils.sha256Hex(is);
            } catch (IOException e) {
                LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
                return;
            }
            //TODO -- add back sha1 check ?
/*      String targetPath =
          targetDigest.substring(0, 2) + "/" + targetDigest.substring(2, 4) + "/" +
              targetDigest;
  */
            Metadata metadata = new Metadata();
            Path targetPath = filesDir.resolve(targetDigest);
            if (!Files.isDirectory(targetPath.getParent())) {
                Files.createDirectories(targetPath.getParent());
            }
            if (Files.isRegularFile(targetPath)) {
                LOGGER.info("{} already exists", targetPath);
                return;
            }
            try (InputStream is = TikaInputStream.get(tmp, metadata)) {
                Files.copy(is, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                LOGGER.warn("problem writing id={}", id, e);
            }
        } finally {
            try {
              Files.delete(tmp);
            } catch (IOException e) {
              LOGGER.warn("can't delete " + tmp.toAbsolutePath(), e);
            }
        }
    }

    private void parseWarc(String id, byte[] warcRecordGZBytes) throws IOException {
        //need to leave initial inputstream open while parsing warcrecord
        //can't just parse record and return
        try (InputStream is = new GZIPInputStream(
                new ByteArrayInputStream(warcRecordGZBytes))) {
            try (WarcReader warcreader = new WarcReader(is)) {

                //should be a single warc per file
                //return the first
                for (WarcRecord record : warcreader) {
                    fetchPayload(id, record);
                    return;
                }
            }
        }
    }

    private byte[] fetchWarcBytes(FetchEmitTuple t) throws TikaException, IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = fetcher.fetch(t)) {
            IOUtils.copy(is, bos);
        } catch (Exception e) {
            throw e;
        }
        return bos.toByteArray();
    }

}
