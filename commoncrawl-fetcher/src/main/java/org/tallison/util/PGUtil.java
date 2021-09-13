package org.tallison.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class PGUtil {

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
}
