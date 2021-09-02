package org.tallison.db;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * don't use this for anything.  this is a one-off script effectively
 */
public class CustomCSVToPG {

    public static void main(String[] args) throws Exception {
        String connectionString = "";
        String dir = "";
        Connection cn = DriverManager.getConnection(connectionString);
        File csvDir = new File(dir);
        String sql = "drop table if exists universes";
        cn.createStatement().execute(sql);
        sql = "create table universes (path varchar(1024) primary key, universe varchar(12), " +
                "validity varchar" +
                "(12));";
        cn.createStatement().execute(sql);
        sql = "insert into universes values (?,?,?)";
        PreparedStatement ps = cn.prepareStatement(sql);

        for (File csv : csvDir.listFiles()) {
            String n = csv.getName();
            Matcher m = Pattern.compile("universe_([A-C])").matcher(n);
            String universe = null;
            if (m.find()) {
                universe = m.group(1);
            }
            try (BufferedReader reader = Files.newBufferedReader(csv.toPath(),
                    StandardCharsets.UTF_8)) {
                String line = reader.readLine();
                line = reader.readLine();
                int lines = 0;
                while (line != null) {
                    String[] data = line.split(",");
                    String k = data[0];
                    k = "eval-three/"+k.substring(0,2)+"/"+k.substring(2,4)+"/"+k;
                    String validity = data[1];
                    ps.clearParameters();
                    ps.setString(1, k);
                    ps.setString(2, universe);
                    ps.setString(3, validity);
                    ps.addBatch();
                    if (lines++ % 10000 == 0) {
                        ps.executeBatch();
                        System.out.println("ingested: "+lines);
                    }
                    line = reader.readLine();
                }
                ps.executeBatch();
            }
        }
        ps.executeBatch();
    }
}
