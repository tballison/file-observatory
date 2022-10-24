package org.tallison.filter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.Tika;

public class CopyByMime {

    public static void main(String[] args) {
        Path src = Paths.get(args[0]);
        Path target = Paths.get(args[1]);
        String mimePart = "nitf";
        Tika tika = new Tika();
        processDirectory(mimePart, src, src, target, tika);

    }

    private static void processDirectory(String mimePart, Path root, Path path, Path targetRoot,
                                         Tika tika) {
        for (File f : path.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(mimePart, root, f.toPath(), targetRoot, tika);
            } else {
                processFile(mimePart, root, f.toPath(), targetRoot, tika);
            }
        }
    }

    private static void processFile(String mimePart, Path root, Path path, Path targetRoot,
                                    Tika tika) {

        try {
            String type = tika.detect(path);
            if (type.contains(mimePart)) {
                Path rel = root.relativize(path);
                Path target= targetRoot.resolve(rel);
                System.out.println(type + " : " + path);
                System.out.println(path + "-> " + target);
                if (!Files.isDirectory(target.getParent())) {
                    Files.createDirectories(target.getParent());
                }
                Files.copy(path, target);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
