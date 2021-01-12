package org.tallison.ingest.mappers;

import org.junit.Test;
import org.tallison.ingest.mappers.PDFCheckerMapper;
import org.tallison.quaerite.core.StoredDocument;

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
        mapper.processJson(p, sd);
        String summaryInfo = sd.getFields().get("pc_summary_info").toString();
        assertTrue(summaryInfo.contains("can-be-optimized"));
        assertTrue(summaryInfo.contains("born-digital"));

    }
}
