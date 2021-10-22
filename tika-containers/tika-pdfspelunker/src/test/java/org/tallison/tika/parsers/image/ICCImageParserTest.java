package org.tallison.tika.parsers.image;

import java.io.InputStream;

import org.junit.Test;

import org.apache.tika.TikaTest;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;

public class ICCImageParserTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        try (InputStream is = this.getClass().getResourceAsStream("/config/my-tika-config.xml")) {
            Parser p = new AutoDetectParser(new TikaConfig(is));
            debug(getRecursiveMetadata("baseball.jpg", p));
        }
    }
}
