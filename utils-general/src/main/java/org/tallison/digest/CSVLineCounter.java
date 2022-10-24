package org.tallison.digest;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVLineCounter {

    public static void main(String[] args) throws Exception {
        Path path = Paths.get("/Users/allison/Desktop/size-pages-full.csv");
        int c = 0;
        for (CSVRecord r : CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
            c++;
        }
        System.out.println(c);

    }
}
