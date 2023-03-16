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

import static org.apache.commons.lang3.StringUtils.truncate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

public class HostUpsert {

    private final String tableName;
    private final String columnName;
    private final int maxLength;
    private PreparedStatement insert;
    private PreparedStatement select;

    public HostUpsert(Connection connection, String schema, String tableName,
                      int maxLength) throws SQLException {
        this.tableName = StringUtils.isAllBlank(schema) ? tableName : schema+"."+tableName;
        this.columnName = "host";
        this.maxLength = maxLength;

        this.insert = connection.prepareStatement(
                "insert into " + this.tableName + " (" + columnName + ", tld) values (?,?)" +
                        " on CONFLICT (" + columnName + ") DO NOTHING");
        this.select = connection.prepareStatement(
                "select id from " + this.tableName + " where " + columnName + "=?");
    }


    public int upsert(String key) throws SQLException {
        String tld = getTld(key);
        insert.clearParameters();
        insert.setString(1, truncate(key, maxLength));
        insert.setString(2, tld);
        insert.execute();
        //insert.getConnection().commit();
        select.clearParameters();
        select.setString(1, key);
        try (ResultSet rs = select.executeQuery()) {
            while (rs.next()) {
                return rs.getInt(1);
            }
        }
        throw new RuntimeException("id could not be found for: " + key);
    }

    private String getTld(String host) {
        int i = host.lastIndexOf(".");
        if (i > -1 && i < host.length()+1) {
            String tld = host.substring(i+1).toLowerCase(Locale.US);
            if (tld.matches("\\A\\d+\\Z")) {
                return "";
            }
            return tld;
        }
        return "";
    }

    public String getTableName() {
        return tableName;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void close() throws SQLException {
        insert.close();
        select.close();
    }

}
