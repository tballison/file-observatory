package org.tallison.pdf.utils;


import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;


public class TestPDFSplitter {

    @Test
    public void testSimple() throws Exception {

        PDFSplitter.main(new String[]{
                "/docs",
                "/single-pages",
                "10"});
    }
}
