package org.tallison.fileutils.pdftotext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.FileToFileProcessor;
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

public class PDFToTextRunner extends AbstractDirectoryProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PDFToTextRunner.class);

    private final int maxBufferLength = 100000;
    private final int numThreads;
    private final long timeoutMillis = 20000;
    private final Path targRoot;
    private final MetadataWriter metadataWriter;
    public PDFToTextRunner(Path srcRoot, Path targRoot, MetadataWriter metadataWriter,
                           int numThreads) {
        super(srcRoot);
        this.targRoot = targRoot;
        this.metadataWriter = metadataWriter;
        this.numThreads = numThreads;

    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new PDFToTextProcessor(queue, rootDir, targRoot, metadataWriter));
        }
        return processors;
    }

    private class PDFToTextProcessor extends FileToFileProcessor {

        public PDFToTextProcessor(ArrayBlockingQueue<Path> queue,
                                  Path srcRoot, Path targRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
        }

        @Override
        public String getExtension() {
            return ".txt";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath,
                            MetadataWriter metadataWriter) throws IOException {
            if (Files.isRegularFile(outputPath)) {
                LOG.trace("skipping "+relPath);
                return;
            }
            List<String> commandLine = new ArrayList<>();
            commandLine.add("pdftotext");
            commandLine.add(srcPath.toAbsolutePath().toString());
            commandLine.add(outputPath.toAbsolutePath().toString());
            if (! Files.isDirectory(outputPath.getParent())) {
                Files.createDirectories(outputPath.getParent());
            }
            FileProcessResult r = ProcessExecutor.execute(
                        new ProcessBuilder(commandLine.toArray(new String[commandLine.size()])),
                        timeoutMillis, maxBufferLength);
            metadataWriter.write(relPath, r);
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        String metadataWriterString = args[2];
        int numThreads = 10;
        if (args.length > 3) {
            numThreads = Integer.parseInt(args[3]);
        }
        try (MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString) ) {
            PDFToTextRunner runner = new PDFToTextRunner(srcRoot, targRoot, metadataWriter, numThreads);

            //runner.setMaxFiles(100);
            runner.execute();
        }
    }
}
