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

import static org.apache.commons.lang3.StringUtils.truncate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.util.HostUpsert;
import org.tallison.util.DBUtil;

public class DBIndexer extends AbstractRecordProcessor {
    public static final int MAX_URL_LENGTH = 10000;

    public static final int MAX_HOST_LENGTH = 256;
    private static final AtomicLong ADDED = new AtomicLong(0);
    private static final AtomicLong CONSIDERED = new AtomicLong(0);
    private static StringCache MIME_CACHE = null;

    private static StringCache DETECTED_MIME_CACHE = null;
    private static StringCache LANGUAGE_CACHE = null;
    private static StringCache TRUNCATED_CACHE = null;
    private static StringCache WARC_FILENAME_CACHE = null;
    private static final long STARTED = System.currentTimeMillis();
    static Logger LOGGER = LoggerFactory.getLogger(DBIndexer.class);
    static AtomicInteger THREAD_COUNTER = new AtomicInteger(-1);
    static AtomicInteger THREAD_CLOSED = new AtomicInteger(-1);
    private final PreparedStatement insert;
    private final Connection connection;
    private final RecordFilter recordFilter;
    private final HostUpsert hostCache;
    private final String schema;
    private long added = 0;
    //sqlite does not like the interleaving of the batch updates
    //and the host upsert.  We need to store the records
    //in memory and do the host upserts during the read, and then we
    //can do the batch writes.
    private List<CCIndexRecord> records = new ArrayList<>();

    public DBIndexer(Connection connection, String schema,
                     RecordFilter recordFilter) throws SQLException {
        this.connection = connection;
        this.recordFilter = recordFilter;
        this.schema = schema;
        this.insert = connection.prepareStatement(
                "insert into " + DBUtil.getTable(schema, "cc_urls") + "(" +
                        "url, host, digest, mime, detected_mime,"+
                        " charset, languages, status, truncated, warc_file_name, "+
                        "warc_offset, warc_length) values" +
                        " (" +
                        "?,?,?,?,?,"+
                        "?,?,?,?,?,"+
                        "?,?)");
        this.hostCache = new HostUpsert(connection,
                schema,
                "cc_hosts", MAX_HOST_LENGTH);
    }

    public static void init(Connection connection, String schema) throws SQLException {
        //connection.setAutoCommit(false);
        DETECTED_MIME_CACHE = new StringCache(schema, "cc_detected_mimes", 2000);
        LANGUAGE_CACHE = new StringCache(schema, "cc_languages", 2000);

        MIME_CACHE = new StringCache(schema, "cc_mimes", 2000);
        TRUNCATED_CACHE = new StringCache(schema, "cc_truncated", 12);
        WARC_FILENAME_CACHE =
                new StringCache(schema, "cc_warc_file_name", 200);
        initTables(connection, schema,
                MIME_CACHE, DETECTED_MIME_CACHE, LANGUAGE_CACHE,
                TRUNCATED_CACHE,
                WARC_FILENAME_CACHE);
    }

    public static void shutDown() throws SQLException {
        for (StringCache cache : new StringCache[]{MIME_CACHE, DETECTED_MIME_CACHE, TRUNCATED_CACHE,
                LANGUAGE_CACHE}) {
            cache.close();
        }

    }

    private static void initTables(Connection connection, String schema, StringCache... caches)
            throws SQLException {

        connection.createStatement().execute("drop table if exists "+
                DBUtil.getTable(schema, "cc_urls"));
        connection.createStatement().execute(
                "create table "+ DBUtil.getTable(schema, "cc_urls") + " (" +
                        DBUtil.getAutoIncrementPrimaryKey(connection, "id") + "," +
                        " url varchar(" + MAX_URL_LENGTH + ")," +
                        " host integer," +
                        " digest varchar(64)," +
                        " mime integer," +
                        " detected_mime integer," +
                        " charset varchar(64)," +
                        " languages integer," +
                        " status integer," +
                        " truncated integer," +
                        " warc_file_name integer," +
                        " warc_offset bigint," +
                        " warc_length bigint);");

        connection.createStatement().execute("drop table if exists "+
                        DBUtil.getTable(schema, "cc_hosts"));
        connection.createStatement().execute(
                "create table "+ DBUtil.getTable(schema, "cc_hosts") +
                        "(" +
                        DBUtil.getAutoIncrementPrimaryKey(connection, "id") + "," +
                        "host varchar(" + MAX_HOST_LENGTH + ") UNIQUE," +
                        "tld varchar(32)," +
                        "ip_address varchar(64)," +
                        "country varchar(20)," +
                        "latitude float, longitude float)");

        for (StringCache cache : caches) {
            connection.createStatement().execute("drop table if exists " +
                    DBUtil.getTable(schema, cache.getTableName()));
            connection.createStatement().execute(
                    "create table " + DBUtil.getTable(schema, cache.getTableName()) +
                            "(id integer primary key," +
                            "name varchar(" + cache.getMaxLength() + "))");
            cache.prepareStatement(connection, schema);
        }

        //connection.commit();

    }

    public static int getConsidered() {
        return CONSIDERED.intValue();
    }

    public static int getAdded() {
        return ADDED.intValue();
    }

    @Override
    void usage() {

    }

    @Override
    public void process(String json) throws IOException {
        List<CCIndexRecord> toProcess = CCIndexRecord.parseRecords(json);
        for (CCIndexRecord r : toProcess) {
            CONSIDERED.incrementAndGet();
            if (!recordFilter.accept(r)) {
                continue;
            }
            try {
                ADDED.getAndIncrement();
                if (records.size() >= 100) {
                    writeRecords();
                }
                int hostId = hostCache.upsert(r.getHost());
                r.setHostId(hostId);
                r.setMimeId(MIME_CACHE.getInt(r.getNormalizedMime()));
                r.setDetectedMimeId(DETECTED_MIME_CACHE.getInt(r.getNormalizedDetectedMime()));
                r.setPrimaryLanguageId(LANGUAGE_CACHE.getInt(getPrimaryLanguage(r.getLanguages())));
                r.setTruncatedId(TRUNCATED_CACHE.getInt(r.getTruncated()));
                r.setWarcId(WARC_FILENAME_CACHE.getInt(r.getFilename()));

                records.add(r);
                LOGGER.trace(
                        StringUtils.joinWith("\t", r.getUrl(), r.getDigest(), r.getNormalizedMime(),
                                r.getNormalizedDetectedMime()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        //}
    }

    private void writeRecords() throws SQLException {
        if (records.size() == 0) {
            return;
        }
        for (CCIndexRecord r : records) {
            int i = 0;
            insert.clearParameters();
            insert.setString(++i, pgsafe(truncate(r.getUrl(), MAX_URL_LENGTH)));
            insert.setInt(++i, r.getHostId());
            insert.setString(++i, pgsafe(r.getDigest()));
            insert.setInt(++i, r.getMimeId());
            insert.setInt(++i, r.getDetectedMimeId());
            if (StringUtils.isEmpty(r.getCharset())) {
                insert.setString(++i, "");
            } else {
                insert.setString(++i, pgsafe(truncate(r.getCharset(), 64)));
            }
            insert.setInt(++i, r.getPrimaryLanguageId());
            insert.setInt(++i, r.getStatus());
            insert.setInt(++i, r.getTruncatedId());
            insert.setInt(++i, r.getWarcId());
            insert.setInt(++i, r.getOffset());
            insert.setInt(++i, r.getLength());
            insert.addBatch();
            added++;
        }
        insert.executeBatch();
        long totalAdded = ADDED.get();
        long considered = CONSIDERED.get();
        long elapsed = System.currentTimeMillis() - STARTED;
        double elapsedSec = (double) elapsed / (double) 1000;
        double addedPer = (double) totalAdded / elapsedSec;
        double consideredPer = (double) considered /elapsedSec;
        LOGGER.debug("considered: " + considered);
        LOGGER.info("committing {} ({}) out of {} in {} ms; {} added recs per second;" +
                        " {} processed recs per second",
                added, totalAdded, considered, elapsed, addedPer,
                consideredPer);
        records.clear();

    }

    private static String pgsafe(String s) {
        if (s == null) {
            return "";
        }
        return s.replaceAll("\u0000", " ");
    }

    private String getPrimaryLanguage(String languages) {
        if (languages == null) {
            return "";
        }
        String[] langs = languages.split(",");
        if (langs.length > 0) {
            return langs[0];
        } else {
            return languages;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            hostCache.close();
            if (records.size() > 0) {
                LOGGER.debug("in close about to execute batch");
                writeRecords();
            }
            insert.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private static class StringCache {

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final Map<String, Integer> map = new ConcurrentHashMap<>();
        private final String schema;
        private final String tableName;
        private final int maxLength;
        private PreparedStatement insert;

        StringCache(String schema, String tableName, int maxLength) {
            this.schema = schema;
            this.tableName = tableName;
            this.maxLength = maxLength;

        }

        private void prepareStatement(Connection connection, String schema) throws SQLException {
            insert = connection.prepareStatement(
                    "insert into " + DBUtil.getTable(schema, tableName) + " (id, name) values (?,?)");
        }


        int getInt(String s) throws SQLException {
            lock.readLock().lock();
            String key = s;
            if (key == null) {
                key = "";
            }
            if (key.length() > maxLength) {
                key = key.substring(0, maxLength);
            }
            try {
                if (map.containsKey(key)) {
                    return map.get(key);
                }
            } finally {
                lock.readLock().unlock();
            }
            try {
                lock.writeLock().lock();
                //need to recheck state
                if (map.containsKey(key)) {
                    return map.get(key);
                } else {
                    int index = map.size();
                    if (index > Integer.MAX_VALUE - 10) {
                        throw new RuntimeException("TOO MANY IN CACHE!");
                    }
                    map.put(key, index);
                    insert.clearParameters();
                    insert.setInt(1, index);
                    insert.setString(2, key);
                    insert.execute();
                }
                return map.get(key);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public String getTableName() {
            return tableName;
        }

        public int getMaxLength() {
            return maxLength;
        }

        public void close() throws SQLException {
            insert.close();
        }
    }
}
