package org.tallison.ingest.mappers;

import org.apache.tika.io.TikaInputStream;
import org.junit.Test;
import org.tallison.ingest.mappers.PDFCheckerMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class PDFCheckerMapperTest {

    @Test
    public void testBasic() throws Exception {
        PDFCheckerMapper mapper = new PDFCheckerMapper();
        Path p = Paths.get(
                PDFCheckerMapperTest.class.getResource(
                        "/test-documents/pdfchecker/GHOSTSCRIPT-696838-0.zip-0.pdf.json").toURI());
        StoredDocument sd = new StoredDocument("id");
        try (InputStream is = TikaInputStream.get(p)) {
            mapper.processJson(is, sd);
        }
        String summaryInfo = sd.getFields().get("pc_summary_info").toString();
        assertTrue(summaryInfo.contains("can-be-optimized"));
        assertTrue(summaryInfo.contains("born-digital"));
    }

    @Test
    public void testFonts() throws Exception {
        PDFCheckerMapper mapper = new PDFCheckerMapper();
        Path p = Paths.get(
                PDFCheckerMapperTest.class.getResource(
                        "/test-documents/pdfchecker/fonts-PDFBOX-1002-2.pdf.json").toURI());
        StoredDocument sd = new StoredDocument("id");
        try (InputStream is = TikaInputStream.get(p)) {
            mapper.processJson(is, sd);
        }
        String summaryInfo = sd.getFields().get("pc_summary_info").toString();
        assertTrue(summaryInfo.contains("can-be-optimized"));
        assertTrue(summaryInfo.contains("born-digital"));
    }

}
