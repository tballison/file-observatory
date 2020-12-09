package org.tallison.batchlite;

import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Use this to configure a file processor that has an input directory and an output
 * directory
 */
public class ConfigSrcTarg {
    private static final String SRC_ROOT = "SRC_ROOT";
    private static final String TARG_ROOT = "TARG_ROOT";
    private static final String METADATA_WRITER_STRING = "METADATA_WRITER_STRING";
    private static final String NUM_THREADS = "NUM_THREADS";

    public static ConfigSrcTarg build(String[] args, int maxStdout, int maxStderr) throws Exception {
        ConfigSrcTarg config = new ConfigSrcTarg();
        if (args.length > 0) {
            config.srcRoot = Paths.get(args[0]);
            config.targRoot = Paths.get(args[1]);
            String metadataWriterString = args[2];


            if (args.length > 3) {
                config.numThreads = Integer.parseInt(args[3]);
            }
            config.metadataWriter = MetadataWriterFactory.build(metadataWriterString,
                    maxStdout, maxStderr);
        } else {
            config.srcRoot = Paths.get(System.getenv(SRC_ROOT));
            config.targRoot = Paths.get(System.getenv(TARG_ROOT));
            config.metadataWriter = MetadataWriterFactory.build(System.getenv(METADATA_WRITER_STRING),
                    maxStdout, maxStderr);
            config.numThreads = Integer.parseInt(System.getenv(NUM_THREADS));
        }
        return config;
    }

    private Path srcRoot;
    private Path targRoot;
    private MetadataWriter metadataWriter;
    private int numThreads = 10;

    public Path getSrcRoot() {
        return srcRoot;
    }

    public Path getTargRoot() {
        return targRoot;
    }
    public MetadataWriter getMetadataWriter() {
        return metadataWriter;
    }

    public int getNumThreads() {
        return numThreads;
    }
}
