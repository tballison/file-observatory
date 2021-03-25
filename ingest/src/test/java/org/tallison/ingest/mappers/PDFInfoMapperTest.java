package org.tallison.ingest.mappers;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.tallison.quaerite.core.StoredDocument;

public class PDFInfoMapperTest {

    String info = "Tagged:         no\n" +
            "UserProperties: no\n" +
            "Suspects:       no\n" +
            "Form:           none\n" +
            "JavaScript:     no\n" +
            "Pages:          2\n" +
            "Encrypted:      yes (print:yes copy:yes change:no addNotes:yes algorithm:RC4)\n" +
            "Page size:      502 x 733 pts\n" +
            "Page rot:       0\n" +
            "File size:      182072 bytes\n" +
            "Optimized:      no\n" +
            "PDF version:    1.3";

    String info2 = "Title:          \n" +
            "Subject:        \n" +
            "Keywords:       \n" +
            "Author:         gavas\n" +
            "Creator:        GPL Ghostscript 904 (ps2write)\n" +
            "Producer:       GPL Ghostscript 9.04\n" +
            "CreationDate:   Tue May 29 06:49:39 2012 UTC\n" +
            "ModDate:        Tue May 29 06:49:39 2012 UTC\n" +
            "Tagged:         no\n" +
            "UserProperties: no\n" +
            "Suspects:       no\n" +
            "Form:           none\n" +
            "JavaScript:     no\n" +
            "Pages:          2\n" +
            "Encrypted:      no\n" +
            "Page size:      1684 x 1191 pts (A2)\n" +
            "Page rot:       0\n" +
            "File size:      11509 bytes\n" +
            "Optimized:      no\n" +
            "PDF version:    1.4";
    @Test
    public void testBasic() throws Exception {
        PDFInfoFeatureMapper mapper = new PDFInfoFeatureMapper();
        Map<String, String> row = new HashMap<>();
        row.put("pinfo_stdout", info);
        StoredDocument sd = new StoredDocument("id");
        mapper.addFeatures(row, null, sd);
        assertEquals("true", sd.getFields().get("pinfo_encrypted"));
        assertEquals("2", sd.getFields().get("pinfo_pages"));
    }

    @Test
    public void testCreator() throws Exception {
        PDFInfoFeatureMapper mapper = new PDFInfoFeatureMapper();
        Map<String, String> row = new HashMap<>();
        row.put("pinfo_stdout", info2);
        StoredDocument sd = new StoredDocument("id");
        mapper.addFeatures(row, null, sd);
        assertEquals("false", sd.getFields().get("pinfo_encrypted"));
        assertEquals("2", sd.getFields().get("pinfo_pages"));
        assertEquals("GPL Ghostscript 904 (ps2write)",
                sd.getFields().get("pinfo_creator"));
    }
}
