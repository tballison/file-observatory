package org.tallison.ingest.mappers;

import org.junit.Test;
import org.tallison.quaerite.core.StoredDocument;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArlingtonMapperTest extends MapperTest {

    @Test
    public void testBasic() throws Exception {
        ArlingtonMapper mapper = new ArlingtonMapper();
        StoredDocument sd = new StoredDocument("");
        mapper._processFile(getPath("arlington/GHOSTSCRIPT-687647-0.pdf.txt"), sd);
        assertEquals("Can't select any link", sd.getFields().get("a_warn"));
    }

    @Test
    public void testFailedToOpen() throws Exception {
        ArlingtonMapper mapper = new ArlingtonMapper();
        StoredDocument sd = new StoredDocument("");
        mapper._processFile(getPath("arlington/GHOSTSCRIPT-688076-1.pdf.txt"), sd);
        assertEquals("Failed to open", sd.getFields().get("a_warn"));
    }

    @Test
    public void testDiffContexts() throws Exception {
        //GHOSTSCRIPT-687499-0.pdf.txt
        ArlingtonMapper mapper = new ArlingtonMapper();
        StoredDocument sd = new StoredDocument("");
        mapper._processFile(getPath("arlington/GHOSTSCRIPT-687499-0.pdf.txt"), sd);
        boolean success = false;
        for (String s : (List<String>)sd.getFields().get("a_warn")) {
            if (s.equals("object validated in two different contexts")) {
                success = true;
            }
        }
        assertTrue(success);
    }
}
