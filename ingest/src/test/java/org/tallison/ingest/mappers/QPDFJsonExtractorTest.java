package org.tallison.ingest.mappers;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.tallison.ingest.qpdf.QPDFJsonExtractor;
import org.tallison.ingest.qpdf.QPDFResults;

//these are tests for qpdf 11.x json v2
public class QPDFJsonExtractorTest {

    @Test
    public void testBasic() throws Exception {
        try (Reader reader = getReader("/qpdfv11/qpdf.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            System.out.println(results);
            assertTrue(results.keyValues.contains("/Creator->Microsoft® Office Word 2007"));
            assertTrue(results.keyValues.contains(("/CreationDate->DATE")));
        }
    }

    private Reader getReader(String file) throws IOException {
        return Files.newBufferedReader(getPath(file), StandardCharsets.UTF_8);
    }

    private Path getPath(String file) throws IOException {
        try {
            return Paths.get(this.getClass().getResource("/test-documents/"+file).toURI());
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
