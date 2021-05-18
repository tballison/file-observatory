package org.tallison.pdf.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.tallison.batchlite.StreamEater;

public class PDFSplitter {

    public static void main(String[] args) throws Exception {
        Path srcDir = Paths.get(args[0]);
        Path targDir = Paths.get(args[1]);
        int totalPages = Integer.parseInt(args[2]);
        PDFSplitter splitter = new PDFSplitter();
        splitter.execute(srcDir, targDir, totalPages);
    }

    private void execute(Path srcDir, Path targDir, int totalPages) {
        int processed = 0;
        while (processed < totalPages) {
            boolean success = processOne(srcDir, targDir);
            if (success) {
                processed++;
            }
        }
    }

    private boolean processOne(Path srcDir, Path targDir) {
        return processDir(srcDir, srcDir, targDir);
    }

    private boolean processDir(Path currDir, Path srcRoot, Path targRoot) {
        File[] files = currDir.toFile().listFiles();
        List<File> fileList = Arrays.asList(files);
        Collections.shuffle(fileList);
        for (File f : fileList) {
            if (f.isFile()) {
                System.err.println("about to process "+f.toPath());
                return extractRandom(f.toPath(), srcRoot, targRoot);
            } else {
                return processDir(f.toPath(), srcRoot, targRoot);
            }
        }
        return false;
    }

    private static boolean extractRandom(Path p, Path srcRoot, Path targRoot) {
        int numPages = getNumPages(p);
        if (numPages < 1) {
            return false;
        }
        Random r = new Random();
        int page = r.nextInt(numPages);
        String rel = srcRoot.relativize(p).toString();
        Path target = targRoot.resolve(rel+"-"+page);
        try {
            Files.createDirectories(target.getParent());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        String[] args = new String[] {
                "qpdf",
                p.toAbsolutePath().toString(),
                "--pages", ".", Integer.toString(page),
                "--", target.toAbsolutePath().toString()
        };
        ProcessBuilder builder = new ProcessBuilder(args);

        Process process = null;
        try {
            process = builder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        StreamEater err = new StreamEater(process.getErrorStream(), 10000);
        Thread errThread = new Thread(err);
        errThread.start();

        StreamEater out = new StreamEater(process.getInputStream(), 10000);
        Thread outThread = new Thread(out);
        outThread.start();

        boolean completed = false;
        try {
            completed = process.waitFor(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        } finally {
            process.destroyForcibly();
        }
        try {
            errThread.join();
            outThread.join();
        } catch (InterruptedException e) {
            return false;
        }
        if (! completed) {
            //log
            return false;
        }
        return true;
    }

    private static int getNumPages(Path p) {
        String[] args = new String[] {
                "qpdf", "--show-npages", p.toAbsolutePath().toString()
        };
        ProcessBuilder builder = new ProcessBuilder(args);
        Process process = null;
        try {
            process = builder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        StreamEater err = new StreamEater(process.getErrorStream(), 10000);
        Thread errThread = new Thread(err);
        errThread.start();

        StreamEater out = new StreamEater(process.getInputStream(), 10000);
        Thread outThread = new Thread(out);
        outThread.start();

        boolean completed = false;
        try {
            completed = process.waitFor(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return -1;
        } finally {
            process.destroyForcibly();
        }
        try {
            errThread.join();
            outThread.join();
        } catch (InterruptedException e) {
            return -1;
        }
        if (! completed) {
            return -1;
        }
        List<String> lines = out.getLines();
        if (lines.size() > 0) {
            String content = out.getLines().get(0);
            try {
                return Integer.parseInt(content.trim());
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return -1;
            }
        }
        return -1;
    }
}
