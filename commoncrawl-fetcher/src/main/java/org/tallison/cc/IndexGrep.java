package org.tallison.cc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.tallison.cc.index.CCIndexRecord;

/**
 * This crawls through gz index files looking for specific records.
 *
 * To be used in debugging surprises, etc.  Not multithreaded or fully
 * robust.
 */
public class IndexGrep {

    public static void main(String[] args) throws Exception {
        File dir = new File("");
        String url = "http://mesclassesdeneige.be/telecharge" +
                ".php?pdf=fichiers/Pages%2050-51%20Ch%C3%A2tel%20(Clos%20Savoyard).pdf";
        Matcher m = Pattern.compile("Ausschreibung-ZH-Oberlaender-OL-2021.pdf")
                .matcher("");
        File[] files = dir.listFiles();
        Arrays.sort(files);
        for (File gz : files) {
            if (gz.getName().endsWith(".gz")) {
                System.err.println(gz.getName());
                try (BufferedReader reader = getReader(gz)) {
                    String line = reader.readLine();
                    while (line != null) {
                        if (m.reset(line).find()) {
                            System.out.println(gz.getName() + " : >"+line+"<");
                            for (CCIndexRecord r : CCIndexRecord.parseRecords(line)) {
                                System.out.println(r);
                            }
                        }
                        line = reader.readLine();
                    }
                } catch (IOException e) {
                    System.err.println("problem "+ gz.getName());
                }
            }
        }
    }

    private static BufferedReader getReader(File gz) throws IOException {
        return new BufferedReader(new InputStreamReader(
                new GZIPInputStream(
                        new FileInputStream(gz)), StandardCharsets.UTF_8));
    }
}
