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

package org.tallison.cc.index;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.CCIndexWGetter;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CCIndexRecord {

    private static Pattern INT_PATTERN = Pattern.compile("^\\d+$");
    private static Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES)
            .create();
    private static Logger LOGGER = LoggerFactory.getLogger(CCIndexRecord.class);


    private String url;
    private String mime;
    private String mimeDetected;
    private Integer status;
    private String digest;
    private Integer length;
    private Integer offset;
    private String filename;
    private String charset;
    private String languages;
    private String truncated;
    private int hostId;
    private int mimeId;
    private int detectedMimeId;
    private int primaryLanguageId;
    private int truncatedId;
    private int warcId;

    public int getMimeId() {
        return mimeId;
    }

    public void setMimeId(int mimeId) {
        this.mimeId = mimeId;
    }

    public int getDetectedMimeId() {
        return detectedMimeId;
    }

    public void setDetectedMimeId(int detectedMimeId) {
        this.detectedMimeId = detectedMimeId;
    }

    public int getPrimaryLanguageId() {
        return primaryLanguageId;
    }

    public void setPrimaryLanguageId(int primaryLanguageId) {
        this.primaryLanguageId = primaryLanguageId;
    }

    public int getTruncatedId() {
        return truncatedId;
    }

    public void setTruncatedId(int truncatedId) {
        this.truncatedId = truncatedId;
    }

    public int getWarcId() {
        return warcId;
    }

    public void setWarcId(int warcId) {
        this.warcId = warcId;
    }

    public void setHostId(int id) {
        this.hostId = id;
    }
    public int getHostId() {
        return hostId;
    }
    public String getUrl() {
        return url;
    }

    public String getHost() {
        try {
            URL u = new URL(url);
            return u.getHost();
        } catch (MalformedURLException e) {
            return "";
        }
    }

    public String getMime() {
        return mime;
    }

    public String getNormalizedMime() {
        return CCIndexRecord.normalizeMime(mime);
    }

    public String getNormalizedDetectedMime() {
        return CCIndexRecord.normalizeMime(mimeDetected);
    }

    public Integer getStatus() {
        return status;
    }

    public String getDigest() {
        return digest;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getOffset() {
        return offset;
    }

    public String getFilename() {
        return filename;
    }

    public String getDetectedMime() {
        return mimeDetected;
    }

    public String getCharset() {
        return charset;
    }

    public String getLanguages() {
        return languages;
    }

    public String getTruncated() {
        return truncated;
    }

    public void setTruncated(String truncated) {
        this.truncated = truncated;
    }
    public static String normalizeMime(String s) {
        if (s == null) {
            return null;
        }
        s = s.toLowerCase(Locale.ENGLISH);
        s = s.replaceAll("^\"|\"$", "");
        s = s.replaceAll("\\s+", " ");
        return s.trim();
    }


    public String getOffsetHeader() {
        return "bytes=" + offset + "-" + (offset+length-1);
    }

    /**
     *
     * @param url
     * @return "" if no tld could be extracted
     */
    public static String getTLD(String url) {
        if (url == null) {
            return "";
        }
        Matcher intMatcher = INT_PATTERN.matcher("");

        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host == null) {
                return "";
            }
            int i = host.lastIndexOf(".");
            String tld = "";
            if (i > -1 && i+1 < host.length()) {
                tld = host.substring(i+1);
            } else {
                //bad host...or do we want to handle xyz.com. ?
                return tld;
            }
            if (intMatcher.reset(tld).find()) {
                return "";
            }
            return tld;

        } catch (URISyntaxException e) {
            //swallow
        }
        return "";
    }

    public static List<CCIndexRecord> parseRecords(String row) {
        AtomicInteger i = new AtomicInteger(0);
        List<CCIndexRecord> records = new ArrayList<>();
        //for now turn off multi row splitting
        //while (i.get() < row.length()) {
        CCIndexRecord record = parseRecord(row, i);
        if (record != null) {
            records.add(record);
        }/* else {
                break;
            }*/
        //}
        return records;

    }

    private static CCIndexRecord parseRecord(String row, AtomicInteger i) {

        int urlI = row.indexOf(' ',i.get());
        int dateI = row.indexOf(' ', urlI+1);
        if (row.indexOf("{") == 0) {
            try {
                return GSON.fromJson(row, CCIndexRecord.class);
            } catch (JsonSyntaxException e) {
                LOGGER.warn(row, e);
                return null;
            }
        } else {
            if (dateI < 0) {
                LOGGER.warn("bad record dateI < 0: {}", row);
                return null;
            }
            List<Integer> ends = new ArrayList<>();
            int end = row.indexOf('}', dateI + 1);

            while (end > -1) {
                ends.add(end);
                end = row.indexOf('}', end + 1);
            }
            if (ends.size() == 0) {
                LOGGER.warn("bad record: {}", row);
                return null;
            }

            for (int thisEnd : ends) {
                String json = row.substring(dateI, thisEnd+1);
                try {
                    CCIndexRecord record = GSON.fromJson(json, CCIndexRecord.class);
                    i.set(thisEnd + 1);
                    return record;
                } catch (JsonSyntaxException e) {
                    LOGGER.debug("bad record ({}): {}", thisEnd, row);
                }
            }
            LOGGER.warn("bad record, giving up: {}", row);
            return null;
        }
    }

    @Override
    public String toString() {
        return "CCIndexRecord{" +
                "url='" + url + '\'' +
                ", mime='" + mime + '\'' +
                ", detectedMime='" + mimeDetected + '\'' +
                ", status='" + status + '\'' +
                ", digest='" + digest + '\'' +
                ", length=" + length +
                ", offset=" + offset +
                ", filename='" + filename + '\'' +
                ", charset='" + charset + '\'' +
                ", languages='" + languages + '\'' +
                '}';
    }

    public void setMime(String mime) {
        this.mime = mime;
    }

    public void setDetectedMime(String detectedMime) {
        this.mimeDetected = detectedMime;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public void setFilename(String warcFilename) {
        this.filename = warcFilename;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
