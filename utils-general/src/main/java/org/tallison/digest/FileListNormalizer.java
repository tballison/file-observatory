package org.tallison.digest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileListNormalizer {

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get("PATH");
        for (File f : dir.toFile().listFiles()) {
            if (f.getName().endsWith("-normed.txt")) {
                continue;
            }
            Path output = dir.resolve(f.getName().replace(".txt", "-normed.txt"));
            try (BufferedWriter w = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                try (BufferedReader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                    String line = r.readLine();
                    Matcher m =
                            Pattern.compile("([a-f0-9]{2,2}/[a-f0-9]{2,2}/[a-f0-9]+)").matcher("");
                    while (line != null) {
                        m.reset(line);
                        if (m.find()) {
                            System.out.println(m.group(1));
                            w.write(m.group(1) + "\n");
                        } else {
                            System.err.println("wtf: "+line);
                        }
                        line = r.readLine();
                    }
                }
            }
        }
    }
}
