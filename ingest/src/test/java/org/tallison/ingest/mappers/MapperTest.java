package org.tallison.ingest.mappers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import java.nio.file.Files;
import java.nio.file.Paths;

public class MapperTest {

    InputStream getPath(String relPath) throws IOException {
        try {
            String path = "/test-documents/"+relPath;
            return Files.newInputStream(Paths.get(this.getClass().getResource(path).toURI()));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
