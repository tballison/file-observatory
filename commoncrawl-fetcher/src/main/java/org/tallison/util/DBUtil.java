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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;

public class DBUtil {


    public static String getTable(String schema, String table) {
        if (StringUtils.isAllBlank(schema)) {
            return table;
        }
        return schema + "." + table;
    }

    public static void safelySetString(PreparedStatement insert, int colNumber, String val,
                                 int maxLength) throws SQLException {
        if (val == null) {
            insert.setNull(colNumber, Types.VARCHAR);
            return;
        }
        if (val.length() > maxLength) {
            val = val.substring(0, maxLength);
        }
        //pg does NOT like \u0000
        val = val.replaceAll("\u0000", " ");
        insert.setString(colNumber, val);
    }

    public static void safelySetInteger(PreparedStatement insert, int colNumber,
                                        Integer val) throws SQLException {
        if (val == null) {
            insert.setNull(colNumber, Types.INTEGER);
        } else {
            insert.setInt(colNumber, val);
        }
    }

    public static void driverHint(String connectionUrl) {
        if (connectionUrl.contains("sqlite")) {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String getCurrentTimestampString(Connection connection) throws SQLException {
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return "datetime('now')";
        }
        return "current_timestamp(0)";
    }

    public static String getAutoIncrementPrimaryKey(Connection connection, String colName)
            throws SQLException {
        //this is an ugly, ugly hack
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return colName + " integer primary key autoincrement";
        } else if (productName.equals("postgresql")) {
            return colName + " serial primary key";
        } else {
            throw new IllegalArgumentException("don't yet support: " + productName);
        }
    }
}
