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
