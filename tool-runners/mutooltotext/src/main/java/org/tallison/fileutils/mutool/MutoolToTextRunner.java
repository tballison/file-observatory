package org.tallison.fileutils.mutool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;


public class MutoolToTextRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MutoolToTextRunner.class);
    private final int maxBufferLength = 100000;

    private final Path targRoot;
    private final int numThreads;
    private final long timeoutMillis = 120000;

    public MutoolToTextRunner(Path srcRoot, Path targRoot, int numThreads) {
        super(srcRoot);
        this.targRoot = targRoot;
        this.numThreads = numThreads;

    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new MutoolToTextProcessor(queue, rootDir, targRoot));
        }
        return processors;
    }

    private class MutoolToTextProcessor extends FileToFileProcessor {

        public MutoolToTextProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot, Path targRoot) {
            super(queue, srcRoot, targRoot);
        }

        @Override
        public String getExtension() {
            return ".txt";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath, Path metadataPath) throws IOException {
            if (Files.isRegularFile(outputPath)) {
                LOG.trace("skipping "+relPath);
                return;
            }
            List<String> commandLine = new ArrayList<>();
            commandLine.add("mutool");
            commandLine.add("convert");
            commandLine.add("-o");
            commandLine.add(outputPath.toAbsolutePath().toString());
            commandLine.add(srcPath.toAbsolutePath().toString());
            if (! Files.isDirectory(outputPath.getParent())) {
                Files.createDirectories(outputPath.getParent());
            }

            FileProcessResult r = ProcessExecutor.execute(
                    new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                    timeoutMillis, maxBufferLength);
            Gson gson = new Gson();
            if (! Files.isDirectory(metadataPath.getParent())) {
                Files.createDirectories(metadataPath.getParent());
            }
            try (Writer writer = Files.newBufferedWriter(metadataPath, StandardCharsets.UTF_8)) {
                gson.toJson(r, writer);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        int numThreads = 10;
        if (args.length > 2) {
            numThreads = Integer.parseInt(args[2]);
        }
        MutoolToTextRunner runner = new MutoolToTextRunner(srcRoot, targRoot, numThreads);
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
