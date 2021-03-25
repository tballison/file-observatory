package org.tallison.ingest.mappers;


import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.tallison.quaerite.core.StoredDocument;

public class PDFFontsMapperTest extends MapperTest {

    @Test
    public void testBasic() throws Exception {
        String stdout = IOUtils.toString(
                getPath("pdffonts/test-basic.txt"), StandardCharsets.UTF_8);

        PDFFontsMapper mapper = new PDFFontsMapper();
        StoredDocument sd = new StoredDocument("id");
        Map<String, String> row = new HashMap<>();
        row.put("pdffonts_stdout", stdout);
        mapper.addFeatures(row, null, sd);
        System.out.println(sd);
    }
}
