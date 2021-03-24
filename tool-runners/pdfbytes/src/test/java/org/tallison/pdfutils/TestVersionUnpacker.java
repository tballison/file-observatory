package org.tallison.pdfutils;


import org.apache.tika.io.TikaInputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
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

    @Test
    public void testBackTracking() throws Exception {
        byte[] string = "%%%EO%%EOF%%EOF".getBytes(StandardCharsets.UTF_8);
        byte[] pattern = "%%EOF".getBytes(StandardCharsets.UTF_8);
        StreamSearcher streamSearcher = new StreamSearcher(pattern);
        InputStream is = new ByteArrayInputStream(string);
        System.out.println(streamSearcher.search(is));
        System.out.println(streamSearcher.search(is));
    }

}
