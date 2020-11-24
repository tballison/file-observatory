package org.tallison.fileutils.pdfinfo;
import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class PDFInfo extends AbstractDirectoryProcessor {

    private final int numThreads;
    private final MetadataWriter metadataWriter;
    public PDFInfo(Path srcRoot, MetadataWriter metadataWriter, int numThreads) {
        super(srcRoot);
        this.metadataWriter = metadataWriter;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new PDFInfoProcessor(queue, rootDir, metadataWriter));
        }
        return processors;
    }

    private class PDFInfoProcessor extends CommandlineFileProcessor {

        public PDFInfoProcessor(ArrayBlockingQueue<Path> queue,
                                Path srcRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath) throws IOException {
            return new String[] {
                    "pdfinfo",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString())
            };
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        String metadataWriterString = args[1];
        int numThreads = 10;
        if (args.length > 2) {
            numThreads = Integer.parseInt(args[2]);
        }
        try (MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString)) {
            PDFInfo runner = new PDFInfo(srcRoot, metadataWriter, numThreads);
            //runner.setMaxFiles(100);
            runner.execute();
        }
    }
}
