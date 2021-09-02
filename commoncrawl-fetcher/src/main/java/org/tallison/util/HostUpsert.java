package org.tallison.util;

import static org.apache.commons.lang3.StringUtils.truncate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public class HostUpsert {

    private final String tableName;
    private final String columnName;
    private final int maxLength;
    private PreparedStatement insert;
    private PreparedStatement select;

    public HostUpsert(Connection connection, String tableName, String columnName,
                      int maxLength) throws SQLException {
        this.tableName = "cc_hosts";
        this.columnName = "host";
        this.maxLength = maxLength;

        this.insert = connection.prepareStatement(
                "insert into " + tableName + " (" + columnName + ", tld) values (?,?)" +
                        " on CONFLICT (" + columnName + ") DO NOTHING");
        this.select = connection.prepareStatement(
                "select id from " + tableName + " where " + columnName + "=?");        }


    public int upsert(String key) throws SQLException {
        String tld = getTld(key);
        insert.clearParameters();
        insert.setString(1, truncate(key, maxLength));
        insert.setString(2, tld);
        insert.execute();
        insert.getConnection().commit();
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
