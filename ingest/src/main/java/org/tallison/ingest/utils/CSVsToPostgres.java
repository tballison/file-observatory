package org.tallison.ingest.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CSVsToPostgres {

    public static void main (String[] args) throws Exception {
        Path dirs = Paths.get(args[0]);
        String jdbc = args[1];

        for (File file : dirs.toFile().listFiles()) {
            if (file.isDirectory()) {
                processProject(dirs, file.getName(), jdbc);
            }
        }
    }

    private static void processProject(Path dirs, String project, String jdbc) throws Exception {
        try (MetadataWriter writer = MetadataWriterFactory.build(jdbc+":"+project)) {
            Path csv = dirs.resolve(project+"/"+project+".csv");

            int rows = 0;
            for (CSVRecord record : CSVParser.parse(csv,
                    StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
                if (rows++ == 0) {
                    continue;
                }
                int i = 0;
                String relPath = record.get(i++);

                FileProcessResult r = new FileProcessResult();
                r.setExitValue(Integer.parseInt(record.get(i++)));
                r.setTimeout(Boolean.parseBoolean(record.get(i++)));
                r.setProcessTimeMillis(Long.parseLong(record.get(i++)));
                r.setStderr(cleanString(record.get(i++)));
                r.setStderrLength(Integer.parseInt(record.get(i++)));
                r.setStderrTruncated(Boolean.parseBoolean(record.get(i++)));
                r.setStdout(cleanString(record.get(i++)));
                r.setStdoutLength(Integer.parseInt(record.get(i++)));
                r.setStdoutTruncated(Boolean.parseBoolean(record.get(i++)));
                System.out.println("about to : " + project + " "+ rows);
                writer.write(relPath, r);
            }

        }
    }

    private static String cleanString(String s) {
        if (s == null) {
            return "";
        }
        if (s.length() > 20000) {
            s = s.substring(0, 20000);
        }
        return s.replaceAll("\u0000", " ");//*/
    }
}
