package org.tallison.batchlite;

import org.tallison.batchlite.writer.MetadataWriterFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

/**
 * use this for file processors that only have a source directory ...
 * no target directory.
 */
public class ConfigSrc {
    public static final String TIKA_CONFIG = "TIKA_CONFIG";
    public static final String METADATA_WRITER_STRING = "METADATA_WRITER_STRING";
    public static final String NUM_THREADS = "NUM_THREADS";
    public static final String IS_DELTA = "IS_DELTA";

    public static ConfigSrc build(String[] args, String name,
                                  int maxStdout, int maxStderr) throws Exception {
        ConfigSrc config = new ConfigSrc();
        if (args.length > 0) {
            config.tikaConfigPath = Paths.get(args[0]);
            String metadataWriterString = args[1];
            boolean isDelta = false;
            if (args.length > 2) {
                isDelta = Boolean.parseBoolean(args[2]);
            } else if (args.length > 3) {
                config.numThreads = Integer.parseInt(args[3]);
            }
            config.metadataWriter = MetadataWriterFactory.build(
                    metadataWriterString, name, isDelta,
                    maxStdout, maxStderr);
        } else {
            boolean isDelta = Boolean.parseBoolean(System.getenv(IS_DELTA));
            config.tikaConfigPath = Paths.get(System.getenv(TIKA_CONFIG));
            config.metadataWriter = MetadataWriterFactory.build(
                    name,
                    System.getenv(METADATA_WRITER_STRING),
                    isDelta,
                    maxStdout, maxStderr);
            config.numThreads = Integer.parseInt(System.getenv(NUM_THREADS));
        }
        //load the fetcher and tikaconfig now
        config.fetcher =
                FetcherManager.load(config.tikaConfigPath).getFetcher(AbstractFileProcessor.FETCHER_NAME);
        config.tikaConfig = new TikaConfig(config.tikaConfigPath);
        return config;
    }

    private Path tikaConfigPath;
    private MetadataWriter metadataWriter;
    private int numThreads = 10;
    private Fetcher fetcher;
    private Emitter emitter = null;
    private TikaConfig tikaConfig = null;

    public Path getTikaConfigPath() {
        return tikaConfigPath;
    }

    public TikaConfig getTikaConfig() {
        return tikaConfig;
    }
    public MetadataWriter getMetadataWriter() {
        return metadataWriter;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public Fetcher getFetcher() throws TikaException, IOException {
        return fetcher;
    }

    public synchronized Emitter getEmitter() throws TikaException, IOException {
        if (emitter == null) {
            emitter =
                    EmitterManager.load(tikaConfigPath).getEmitter(AbstractFileProcessor.EMITTER_NAME);
        }
        return emitter;
    }
}
