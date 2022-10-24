package org.tallison.db;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class ExtractsToDB {

    public static void main(String[] args) throws Exception {
        Path tikaConfigFile = Paths.get(args[0]);

        PipesIterator it = PipesIterator.build(tikaConfigFile);

    }
}
