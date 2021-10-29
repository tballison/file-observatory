import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tallison.ingest.qpdf.QPDFJsonExtractor;
import org.tallison.ingest.qpdf.QPDFResults;

import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QPDFJsonExtractorTest {

    @Test
    public void testBasic() throws Exception {
        try (Reader reader = getReader("GHOSTSCRIPT-702993-0.pdf.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            //System.out.println(results);
            //TODO: turn this into an actual test
        }
    }

    @Test
    public void testDebugging2() throws Exception {
        try (Reader reader = getReader("simple.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            System.out.println(results);
            //TODO: turn this into an actual test
        }
    }


    //bugtrackers/GHOSTSCRIPT/
    @Test
    public void testDebugging() throws Exception {
        try (Reader reader = getReader("GHOSTSCRIPT-687771-0.pdf.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            System.out.println(results);
            //TODO: turn this into an actual test
        }
    }

    @Test
    public void testTypeKeys() throws Exception {
        try (Reader reader = getReader("types.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            assertEquals(40, results.typeKeys.size());
            assertTrue(results.typeKeys.contains("/FontDescriptor->/Ascent"));
        }
    }

    @Test
    public void testFontIdentifier() throws Exception {
        try (Reader reader = getReader("tmp.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            System.out.println(results);
            assertEquals(40, results.typeKeys.size());
            assertTrue(results.typeKeys.contains("/FontDescriptor->/Ascent"));
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
