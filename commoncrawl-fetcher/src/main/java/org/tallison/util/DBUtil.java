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
