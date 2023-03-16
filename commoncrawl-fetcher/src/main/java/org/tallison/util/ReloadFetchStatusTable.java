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
import java.sql.DriverManager;
import java.sql.Statement;

import org.tallison.cc.CCFileFetcher;

/**
 * For dev use only.  This loads a new status table for when there are changes
 * to CCFileFetcher.STATUS
 */
public class ReloadFetchStatusTable {

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(args[0]);
        try (Statement st = connection.createStatement()) {
            String sql = "drop table if exists cc_fetch_status";
            st.execute(sql);

            sql = "create table cc_fetch_status " + "(id integer primary key, status varchar(64));";
            st.execute(sql);


            for (CCFileFetcher.FETCH_STATUS status : CCFileFetcher.FETCH_STATUS.values()) {

                sql = "insert into cc_fetch_status values (" + status.ordinal() + ",'" +
                        status.name() + "');";
                st.execute(sql);
            }
        }
    }
}
