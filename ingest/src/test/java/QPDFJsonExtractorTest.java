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
        try (Reader reader = getReader("GHOSTSCRIPT-690371-0.pdf.json")) {
            QPDFJsonExtractor ex = new QPDFJsonExtractor();
            QPDFResults results = ex.extract("id", reader);
            System.out.println(results);

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
