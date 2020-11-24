package org.tallison.fileutils.tika;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.sax.AbstractRecursiveParserWrapperHandler;
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

public class TikaBatch extends AbstractDirectoryProcessor {


    private static final Logger LOG = LoggerFactory.getLogger(TikaBatch.class);
    private final int maxBufferLength = 100000;

    private final Path targRoot;
    private final MetadataWriter metadataWriter;
    private final int numThreads;
    private final String[] tikaServerUrls;

    public TikaBatch(Path srcRoot, Path targRoot, MetadataWriter metadataWriter,
                     String[] tikaServerUrls,
                      int numThreads) {
        super(srcRoot);
        this.targRoot = targRoot;
        this.metadataWriter = metadataWriter;
        this.tikaServerUrls = tikaServerUrls;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new TikaProcessor(queue, rootDir, targRoot, metadataWriter, tikaServerUrls));
        }
        return processors;
    }

    private class TikaProcessor extends FileToFileProcessor {

        private final TikaServerClient tikaClient;
        public TikaProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot,
                             Path targRoot, MetadataWriter metadataWriter,
                             String[] tikaServerUrls) {
            super(queue, srcRoot, targRoot, metadataWriter);
            tikaClient = new TikaServerClient(TikaServerClient.INPUT_METHOD.INPUTSTREAM,
                    tikaServerUrls);
        }

        @Override
        public String getExtension() {
            return ".json";
        }

        @Override
        public void process(String relPath, Path srcPath, Path outputPath, MetadataWriter metadataWriter) throws IOException {
            if (Files.isRegularFile(outputPath)) {
                LOG.trace("skipping " + relPath);
                return;
            }
            int exitValue = 0;
            long start = System.currentTimeMillis();
            List<Metadata> metadataList = null;
            try (TikaInputStream tis = TikaInputStream.get(srcPath)) {
                metadataList = tikaClient.parse(relPath, tis);
            } catch (TikaClientException e) {
                LOG.error("error on {}", relPath, e);
                exitValue = 1;
            }

            if (exitValue == 0) {
                if (!Files.isDirectory(outputPath.getParent())) {
                    Files.createDirectories(outputPath.getParent());
                }
                try (Writer writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
                    JsonMetadataList.toJson(metadataList, writer);
                } catch (IOException|TikaException e) {
                    LOG.warn("problem writing json", e);
                }
            }


            long elapsed = System.currentTimeMillis()-start;
            String stackTrace = getStackTrace(metadataList);
            FileProcessResult r = new FileProcessResult();
            r.setExitValue(exitValue);
            r.setProcessTimeMillis(elapsed);
            r.setStderr(stackTrace);
            r.setStderrLength(stackTrace.length());
            r.setStdout("");
            r.setStdoutLength(0);
            r.setTimeout(false);//fix this
            metadataWriter.write(relPath, r);
        }

        private String getStackTrace(List<Metadata> metadataList) {
            if (metadataList == null || metadataList.size() == 0) {
                return "";
            }
            String stack = metadataList.get(0).get(AbstractRecursiveParserWrapperHandler.CONTAINER_EXCEPTION);
            if (stack != null) {
                return stack;
            }
            return "";
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        String metadataWriterString = args[2];
        String tikaServerUrlString = args[3];
        int numThreads = 10;
        if (args.length > 4) {
            numThreads = Integer.parseInt(args[4]);
        }
        try (MetadataWriter metadataWriter = MetadataWriterFactory.build(metadataWriterString)) {
            TikaBatch runner = new TikaBatch(srcRoot, targRoot,
                    metadataWriter,
                    tikaServerUrlString.split(","), numThreads);
            //runner.setMaxFiles(100);
            runner.execute();
        }
    }
}

