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
package org.tallison.batchlite.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//e.g. /data/docs output jdbc:h2:file:/home/tallison/Desktop/h2_results:file_metadata 10

public class JDBCMetadataWriter extends MetadataWriter {

    private static Logger LOGGER = LoggerFactory.getLogger(JDBCMetadataWriter.class);

    private static final int MAX_PATH_LENGTH = 500;
    private static final int MAX_RETRIES = 10;
    private static final long SLEEP_ON_FAILED_WRITE_MS = 30000;
    private final String connectionString;
    private final String table;
    private final boolean isPostgres;
    private Connection connection;
    private PreparedStatement insert;
    private List<PathResultPair> results = new ArrayList<>();


    JDBCMetadataWriter(String name, String connectionString, boolean isDelta,
                       int maxStdout, int maxStderr) throws IOException {
        super(name);
        this.table = name;
        this.connectionString = connectionString;
        isPostgres = connectionString.startsWith("jdbc:postgresql");
        try {
            this.connection = tryConnect(connectionString);
            createTable(connection, table, isDelta, maxStdout, maxStderr);
            this.insert = buildInsert(connection, table);
        } catch (SQLException e) {
            LOGGER.warn("problem with connection string: >"+connectionString+"<");
            throw new IOException(e);
        }
    }

    private static Connection tryConnect(String connectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString);
        connection.setAutoCommit(false);
        return connection;
    }

    private static PreparedStatement buildInsert(Connection connection, String table) throws SQLException {
        String sql = "insert into "+table+
                " values (?,?,?,?,?," +
                "?,?,?,?,?)" +
                " on conflict (path) do update " +
                " set exit_value = ?, " +
                " timeout=?, " +
                " process_time_ms=?, " +
                "stdout=?, " +
                "stdout_length=?, " +
                "stdout_truncated=?, " +
                "stderr=?, " +
                "stderr_length=?, " +
                "stderr_truncated=?;";
        return connection.prepareStatement(sql);
    }


    private static void createTable(Connection connection, String table, boolean isDelta,
                                    int maxStdout, int maxStderr) throws SQLException {
        String sql;

        if (! isDelta) {
            sql = "drop table if exists " + table;
            connection.createStatement().execute(sql);
        }
        if (! tableExists(connection, table)) {

            sql = "create table " + table + " (" +
                    "path varchar(" + MAX_PATH_LENGTH + ") primary key," +
                    "exit_value integer," +
                    "timeout boolean," +
                    "process_time_ms BIGINT," +
                    "stdout varchar(" + maxStdout + ")," +
                    "stdout_length bigint," +
                    "stdout_truncated boolean," +
                    "stderr varchar(" + maxStderr + ")," +
                    "stderr_length bigint," +
                    "stderr_truncated boolean)";
            connection.createStatement().execute(sql);
            connection.commit();
        }
    }

    private static boolean tableExists(Connection connection, String table) throws SQLException {
        Savepoint savepoint = connection.setSavepoint();
        try {
            try (Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("select * from "+table+" limit 1")) {
                while (rs.next()) {

                }
                return true;
            }
        } catch (SQLException e) {
            connection.rollback(savepoint);
            return false;
        }
    }


    @Override
    protected void write(PathResultPair pair) throws IOException {
        results.add(pair);
        if (results.size() > 20) {
            writeResults();
            results.clear();
        }
    }

    private void writeResults() throws IOException {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                Savepoint savepoint = connection.setSavepoint();
                try {
                    tryWrite();
                    return;
                } catch (SQLException e) {
                    LOGGER.warn("failed write", e);
                    connection.rollback(savepoint);
                    LOGGER.warn("rolled back successfully", e);
                    try {
                        Thread.sleep(SLEEP_ON_FAILED_WRITE_MS);
                    } catch (InterruptedException e3) {
                        LOGGER.warn("interrupted?!", e3);
                    }
                    LOGGER.warn("woke up after sleep after rolling back savepoint");
                }
            } catch (SQLException e) {
                //failed to set the savepoint
                //or failed to rollback -- try to close connection and reconnect
                boolean success = false;
                for (int j = 0; j < MAX_RETRIES && success == false; j++) {
                    try {
                        LOGGER.warn("trying to reconnect");
                        success = reconnect();
                        LOGGER.warn("reconnected successfully: " + success);
                    } catch (SQLException e2) {
                        LOGGER.error("failed to reconnect", e2);
                        try {
                            Thread.sleep(SLEEP_ON_FAILED_WRITE_MS);
                        } catch (InterruptedException e3) {
                            LOGGER.warn("interrupted?!", e3);
                        }
                        LOGGER.warn("woke up after sleep after trying to connect");
                    }
                }
            }
        }
        LOGGER.error("completely failed all attempts to write data");
        throw new RuntimeException("catastrophically failed to write results");
    }

    private boolean reconnect() throws SQLException {

        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                LOGGER.warn("rollback failed", e);
            }
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.warn("connection failed to close", e);
            }
        }
        if (insert != null) {
            try {
                insert.clearBatch();
            } catch (SQLException e) {
                LOGGER.warn("insert failed to clear batch", e);
            }
            try {
                insert.close();
            } catch (SQLException e) {
                LOGGER.warn("failed to close insert", e);
            }
        }
        connection = tryConnect(connectionString);
        insert = buildInsert(connection, table);
        return true;
    }

    private void tryWrite() throws SQLException {

        for (PathResultPair pair : results) {
            int i = 0;
            LOGGER.debug("about to write {}", pair);
                FileProcessResult result = pair.getResult();
                insert.clearParameters();
                insert.setString(++i, clean(pair.getRelPath(), MAX_PATH_LENGTH));
                insert.setInt(++i, result.getExitValue());
                insert.setBoolean(++i, result.isTimeout());
                insert.setLong(++i, result.getProcessTimeMillis());
                insert.setString(++i, clean(result.getStdout(), getMaxStdoutBuffer()));
                insert.setLong(++i, result.getStdoutLength());
                insert.setBoolean(++i, result.isStdoutTruncated());
                insert.setString(++i, clean(result.getStderr(), getMaxStderrBuffer()));
                insert.setLong(++i, result.getStderrLength());
                insert.setBoolean(++i, result.isStderrTruncated());
                //do update
                insert.setInt(++i, result.getExitValue());
                insert.setBoolean(++i, result.isTimeout());
                insert.setLong(++i, result.getProcessTimeMillis());
                insert.setString(++i, clean(result.getStdout(), getMaxStdoutBuffer()));
                insert.setLong(++i, result.getStdoutLength());
                insert.setBoolean(++i, result.isStdoutTruncated());
                insert.setString(++i, clean(result.getStderr(), getMaxStderrBuffer()));
                insert.setLong(++i, result.getStderrLength());
                insert.setBoolean(++i, result.isStderrTruncated());

                insert.addBatch();

        }
        insert.executeBatch();
        connection.commit();
    }

    private String clean(String s, int maxLength) {
        if (s == null) {
            return "";
        }
        if (isPostgres) {
            s = s.replaceAll("\u0000", " ");
        }
        if (s.length() > maxLength) {
            s = s.substring(0, maxLength);
        }
        return s;
    }

    @Override
    protected void close() throws IOException {
        writeResults();
        try {
            insert.executeBatch();
            insert.close();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            LOGGER.warn("problem closing jdbc writer", e);
            throw new IOException(e);
        }
    }
}
