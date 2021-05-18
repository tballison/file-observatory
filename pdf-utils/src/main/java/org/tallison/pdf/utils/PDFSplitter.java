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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.tallison.batchlite.StreamEater;

public class PDFSplitter {
    static AtomicInteger PROCESSED = new AtomicInteger(0);
    static int MAX_TO_PROCESS = 0;
    int numThreads = 10;
    public static void main(String[] args) throws Exception {
        Path srcDir = Paths.get(args[0]);
        Path targDir = Paths.get(args[1]);
        MAX_TO_PROCESS = Integer.parseInt(args[2]);
        PDFSplitter splitter = new PDFSplitter();
        splitter.execute(srcDir, targDir);
    }

    private void execute(Path srcDir, Path targDir) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService executorCompletionService =
                new ExecutorCompletionService(executor);
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new Worker(srcDir, targDir));
        }

        int completed = 0;
        try {
            while (completed < numThreads) {
                //blocking
                Future<Integer> future = executorCompletionService.take();
                future.get();
                completed++;
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static class Worker implements Callable<Integer> {

        private final Path srcDir;
        private final Path targDir;

        private Worker(Path srcDir, Path targDir) {
            this.srcDir = srcDir;
            this.targDir = targDir;
        }
        @Override
        public Integer call() throws Exception {
            while (true) {
                int proc = PROCESSED.get();
                if (proc >= MAX_TO_PROCESS) {
                    return 1;
                }
                boolean success = processOne(srcDir, targDir);
                if (success) {
                    PROCESSED.incrementAndGet();
                }
            }
        }
    }
    private static boolean processOne(Path srcDir, Path targDir) {
        return processDir(srcDir, srcDir, targDir);
    }

    private static boolean processDir(Path currDir, Path srcRoot, Path targRoot) {
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
