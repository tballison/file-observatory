package org.tallison.batchlite;

import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * use this for file processors that only have a source directory ...
 * no target directory.
 */
public class ConfigSrc {
    private static final String SRC_ROOT = "SRC_ROOT";
    private static final String METADATA_WRITER_STRING = "METADATA_WRITER_STRING";
    private static final String NUM_THREADS = "NUM_THREADS";

    public static ConfigSrc build(String[] args, int maxStdout, int maxStderr) throws Exception {
        ConfigSrc config = new ConfigSrc();
        if (args.length > 0) {
            config.srcRoot = Paths.get(args[0]);
            String metadataWriterString = args[1];

            if (args.length > 2) {
                config.numThreads = Integer.parseInt(args[2]);
            }
            config.metadataWriter = MetadataWriterFactory.build(metadataWriterString,
                    maxStdout, maxStderr);
        } else {
            config.srcRoot = Paths.get(System.getenv(SRC_ROOT));
            config.metadataWriter = MetadataWriterFactory.build(System.getenv(METADATA_WRITER_STRING),
                    maxStdout, maxStderr);
            config.numThreads = Integer.parseInt(System.getenv(NUM_THREADS));
        }
        return config;
    }

    private Path srcRoot;
    private MetadataWriter metadataWriter;
    private int numThreads = 10;

    public Path getSrcRoot() {
        return srcRoot;
    }

    public MetadataWriter getMetadataWriter() {
        return metadataWriter;
    }

    public int getNumThreads() {
        return numThreads;
    }
}
