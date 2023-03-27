package org.tallison.db;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class PGToCSV {

    public static void main(String[] args) throws Exception {
        String sql = "";
        String outputPath = "";

        String connectionString = args[0];

        Path out = Paths.get(outputPath);
        Connection cn = DriverManager.getConnection(connectionString);
        cn.setAutoCommit(false);

        try (BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            try (CSVPrinter printer = new CSVPrinter(writer,CSVFormat.EXCEL)) {
                System.out.println("about to start query");
                try (Statement st = cn.createStatement()) {
                    st.setFetchSize(100000);
                    st.setFetchDirection(ResultSet.FETCH_FORWARD);
                    List<String> data = new ArrayList<>();
                    try (ResultSet rs = st.executeQuery(sql)) {
                        System.out.println("executed query");
                        int cols = rs.getMetaData().getColumnCount();
                        for (int i = 1; i <= cols; i++) {
                            data.add(rs.getMetaData().getColumnName(i));
                        }
                        printer.printRecord(data);
                        int lines = 0;
                        while (rs.next()) {
                            for (int i = 1; i <= cols; i++) {
                                data.add(rs.getString(i));
                            }
                            if (lines > 0 && lines % 1000 == 0) {
                                System.out.println(lines + " processed");
                            }
                            printer.printRecord(data);
                            data.clear();
                            lines++;
                        }
                    }
                }
            }
        }
    }
}
