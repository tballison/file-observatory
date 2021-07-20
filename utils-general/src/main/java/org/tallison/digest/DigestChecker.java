package org.tallison.digest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;

public class DigestChecker {

    AtomicInteger totalChecked = new AtomicInteger(0);
    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        try (BufferedWriter writer =
                     Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            DigestChecker digestChecker = new DigestChecker();
            digestChecker.execute(dir, writer);
        }
    }

    private void execute(Path rootDir, BufferedWriter writer) {
        processDir(rootDir, writer);
        System.err.println("completed successfully");
    }

    private void processDir(Path path, BufferedWriter writer) {
        for (File f : path.toFile().listFiles()) {
            if (f.isFile()) {
                processFile(f, writer);
            } else {
                processDir(f.toPath(), writer);
            }
        }
    }

    private void processFile(File f, BufferedWriter writer) {
        String name = f.getName();
        String digest = null;
        try (InputStream is = Files.newInputStream(f.toPath())) {
            digest = DigestUtils.sha256Hex(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (! name.equals(digest)) {
            try {
                writer.write(name + "\t" + digest + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        int checked = totalChecked.incrementAndGet();
        if (checked % 1000 == 0) {
            System.err.println(checked + " files processed");
        }
    }
}
