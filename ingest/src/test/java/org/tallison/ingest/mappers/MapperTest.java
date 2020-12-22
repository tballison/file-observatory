package org.tallison.ingest.mappers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MapperTest {

    Path getPath(String relPath) throws IOException {
        try {
            String path = "/test-documents/"+relPath;
            return Paths.get(this.getClass().getResource(path).toURI());
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
