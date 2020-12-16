import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.sax.AbstractRecursiveParserWrapperHandler;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class TikaPDFToTextTest extends TikaTest {
    private static String END_POINT = "http://localhost:9998";
    private static final String META_PATH = "/rmeta";

    @Test
    @Ignore("once container is running")
    public void testBasic() throws Exception {
        Response response = WebClient
                .create(END_POINT + META_PATH)
                .accept("application/json")
                .acceptEncoding("gzip")
                .put(ClassLoader.getSystemResourceAsStream("test-documents/testPDF.pdf"));

        Reader reader = new InputStreamReader(new GzipCompressorInputStream((InputStream) response.getEntity()), UTF_8);
        List<Metadata> metadataList = JsonMetadataList.fromJson(reader);
        assertEquals(1, metadataList.size());
        assertContains("Toolkit\nApache Tika is a toolkit", metadataList.get(0).get(AbstractRecursiveParserWrapperHandler.TIKA_CONTENT));
    }

    @Test
    @Ignore("wait for docker to start")
    public void testTruncated() throws Exception {
        byte[] bytes = IOUtils.readBytesFromStream(ClassLoader.getSystemResourceAsStream("test-documents/testPDF.pdf"));
        int length = bytes.length;
        int step = 723;
        length -= step;
        while (length > 0) {
            byte[] data = new byte[length];
            System.arraycopy(bytes, 0, data, 0, length);
            Response response = WebClient
                    .create(END_POINT + META_PATH)
                    .accept("application/json")
                    .put(new ByteArrayInputStream(data));

            Reader reader = new InputStreamReader((InputStream) response.getEntity(), UTF_8);
            List<Metadata> metadataList = JsonMetadataList.fromJson(reader);
            assertEquals(1, metadataList.size());
            assertContains("Bad exit value: 1 ::",
                    metadataList.get(0).get("X-TIKA:EXCEPTION:runtime"));
            length -= step;
        }
    }
}
