package org.tallison.batchlite;

import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

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
            config.tikaConfig = Paths.get(args[0]);
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
            config.tikaConfig = Paths.get(System.getenv(TIKA_CONFIG));
            config.metadataWriter = MetadataWriterFactory.build(
                    name,
                    System.getenv(METADATA_WRITER_STRING),
                    isDelta,
                    maxStdout, maxStderr);
            config.numThreads = Integer.parseInt(System.getenv(NUM_THREADS));
        }
        return config;
    }

    private Path tikaConfig;
    private MetadataWriter metadataWriter;
    private int numThreads = 10;

    public Path getTikaConfig() {
        return tikaConfig;
    }

    public MetadataWriter getMetadataWriter() {
        return metadataWriter;
    }

    public int getNumThreads() {
        return numThreads;
    }
}
