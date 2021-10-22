package org.tallison.tika.parsers.pdf;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;

import org.apache.tika.TikaTest;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.sax.RecursiveParserWrapperHandler;

public class PDFSpelunkerTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        String s = "c";
        System.out.println(s.codePointAt(0));
        Parser p = new PDFSpelunker();
        debug(getRecursiveMetadata("testPDF.pdf", p));
    }

    @Test
    public void runTika() throws Exception {
        String[] args = new String[] {
                "-a",
                "--config=/Users/allison/Intellij/file-observatory/tika-containers/tika-pdfspelunker/src/test/resources/config/my-tika-config.xml"
        };
        org.apache.tika.cli.TikaCLI.main(args);
    }

    @Test
    public void testDirectory() throws Exception {
        Path dir = Paths.get("/Users/allison/data/cc/small_sample");
        //dir = Paths.get("/Users/allison/data/safedocs/pngpdfs");

        TikaConfig tikaConfig = null;
        try (InputStream is = getResourceAsStream("/config/my-tika-config.xml")) {
            tikaConfig = new TikaConfig(is);
        }
        Parser p = new AutoDetectParser(tikaConfig);

        for (File f : dir.toFile().listFiles()) {
            //if (f.getName().contains("0005188e9")) {
                //      "0005daade87bed981f113253fb8d5d94cc5a814ea308169c87f00946c1ae1c4b")) {
                System.out.println(f);
                getRecursiveMetadata(f.toPath(), p, false);
            //}

        }
    }

    private List<Metadata> getMetadataList(Parser p, Path path) {

        RecursiveParserWrapper wrapper = new RecursiveParserWrapper(p);
        RecursiveParserWrapperHandler
                handler = new RecursiveParserWrapperHandler(new BasicContentHandlerFactory(
                BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1));

        try (InputStream is = TikaInputStream.get(path)) {
            Metadata metadata = new Metadata();
            ParseContext context = new ParseContext();
            wrapper.parse(is, handler, metadata, context);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return handler.getMetadataList();
    }
}
