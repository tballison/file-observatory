package org.tallison.pdfutils;


import org.apache.tika.io.TikaInputStream;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestVersionUnpacker {

    @Test
    public void testVersions() throws Exception {
        Path p = Paths.get(TestVersionUnpacker.class.getResource("/pdf-puzzle.pdf").toURI());
        System.out.println(PDFByteSniffer.getJson(p));
    }


}
