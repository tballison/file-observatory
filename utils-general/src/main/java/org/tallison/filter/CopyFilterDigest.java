package org.tallison.filter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.tika.Tika;

public class CopyFilterDigest {

    public static void main(String[] args) {
        Path target = Paths.get(args[0]);
        Set<String> seen = new HashSet<>();
        for (int i = 1; i < args.length; i++) {
            Path srcRoot = Paths.get(args[i]);
            Path path = srcRoot;
            processDirectory(srcRoot, path, target, seen);
        }
    }

    private static void processDirectory(Path root, Path path, Path targetRoot,
                                         Set<String> seen) {
        for (File f : path.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(root, f.toPath(), targetRoot, seen);
            } else {
                processFile(root, f.toPath(), targetRoot, seen);
            }
        }
    }

    private static void processFile(Path root, Path path, Path targetRoot, Set<String> seen) {

        String sha256 = null;
        try (InputStream is = Files.newInputStream(path)) {
            sha256 = DigestUtils.sha256Hex(is);
        } catch (IOException e) {
            System.err.println("problem digesting " +path);
            return;
        }

        try {

            Path target = targetRoot.resolve(root.getFileName());
            target = target.resolve(path.getFileName());
            if (seen.contains(sha256)) {
                System.out.println(path + "\t" + sha256 + "\t" + target + "\texists, skipping");
                return;
            }
            if (!Files.isDirectory(target.getParent())) {
                Files.createDirectories(target.getParent());
            }
            if (Files.isRegularFile(target)) {
                System.out.println(path + "\t" + sha256 + "\t" + target + "\tcollision");
                return;
            }
            System.out.println(path + "\t" + sha256 + "\t" + target + "\tcopied");
            Files.copy(path, target);
            seen.add(sha256);
        } catch (IOException e) {
            System.err.println("problem copying " + path);
        }
    }
}
