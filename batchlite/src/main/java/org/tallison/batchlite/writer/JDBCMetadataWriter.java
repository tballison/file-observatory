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
import java.sql.SQLException;

//e.g. /data/docs output jdbc:h2:file:/home/tallison/Desktop/h2_results:file_metadata 10

public class JDBCMetadataWriter extends MetadataWriter {

    private static Logger LOGGER = LoggerFactory.getLogger(JDBCMetadataWriter.class);

    private static final int MAX_PATH_LENGTH = 500;
    private final boolean isPostgres;
    private final Connection connection;
    private final PreparedStatement insert;


    JDBCMetadataWriter(String jdbcString, int maxStdout, int maxStderr) throws IOException {
        int tableIndex = jdbcString.lastIndexOf(":");
        if (tableIndex < 0) {
            throw new RuntimeException("must specify table name after :");
        }
        isPostgres = jdbcString.startsWith("jdbc:postgresql");
        String table = jdbcString.substring(tableIndex+1);
        jdbcString = jdbcString.substring(0, tableIndex);
        String sql = "insert into "+table+" values (?,?,?,?,?," +
                "?,?,?,?,?);";
        try {
            connection = DriverManager.getConnection(jdbcString);
            connection.setAutoCommit(false);
            createTable(connection, table, maxStdout, maxStderr);
            insert = connection.prepareStatement(sql);
        } catch (SQLException e) {
            LOGGER.warn("problem with connection string: >"+jdbcString+"<");
            throw new IOException(e);
        }
    }

    private static void createTable(Connection connection, String table,
                                    int maxStdout, int maxStderr) throws SQLException {
        String sql = "drop table if exists "+table;
        connection.createStatement().execute(sql);

        sql = "create table "+table+" ("+
                "path varchar("+MAX_PATH_LENGTH+") primary key,"+
                "exit_value integer," +
                "timeout boolean,"+
                "process_time_ms BIGINT,"+
                "stdout varchar("+maxStdout+"),"+
                "stdout_length bigint,"+
                "stdout_truncated boolean," +
                "stderr varchar("+maxStderr+"),"+
                "stderr_length bigint,"+
                "stderr_truncated boolean)";
        connection.createStatement().execute(sql);
        connection.commit();
    }


    @Override
    protected void write(PathResultPair pair) throws IOException {
        int i = 0;
        LOGGER.debug("about to write {}", pair);
        try {
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
            insert.addBatch();

            if (getRecordsWritten() % 20 == 0) {
                insert.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            LOGGER.warn("Can't execute batch insert", e);
            e.printStackTrace();
            throw new IOException(e);
        }
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
