package org.tallison.tika.parsers.pdf;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import org.apache.tika.TikaTest;
import org.apache.tika.parser.Parser;

public class PDFSpelunkerTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        Parser p = new PDFSpelunker();
        debug(getRecursiveMetadata("testPDF.pdf", p));
    }

    @Test
    public void testDirectory() throws Exception {
        Path dir = Paths.get("/Users/allison/data/cc/small_sample");
        Parser p = new PDFSpelunker();

        for (File f : dir.toFile().listFiles()) {
           if (f.getName().contains(
                   "0006c34ab82fc4489")) {
                System.out.println(f);
                getRecursiveMetadata(f.toPath(), p, false);
            }
        }
    }
}
