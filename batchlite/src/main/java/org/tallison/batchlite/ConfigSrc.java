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

    public static ConfigSrc build(String[] args, int maxStdout, int maxStderr) throws Exception {
        ConfigSrc config = new ConfigSrc();
        if (args.length > 0) {
            config.tikaConfig = Paths.get(args[0]);
            String metadataWriterString = args[1];

            if (args.length > 2) {
                config.numThreads = Integer.parseInt(args[2]);
            }
            config.metadataWriter = MetadataWriterFactory.build(metadataWriterString,
                    maxStdout, maxStderr);
        } else {
            config.tikaConfig = Paths.get(System.getenv(TIKA_CONFIG));
            config.metadataWriter = MetadataWriterFactory.build(System.getenv(METADATA_WRITER_STRING),
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
