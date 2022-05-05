import org.junit.Test;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.CompositeRecordFilter;
import org.tallison.cc.index.RecordFilter;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompositeRecordFilterTest {
    @Test
    public void testBasic() throws Exception {
        RecordFilter filter = loadFilter("mime-filters.json");
        assertTrue(accept(filter, "blahpDf", "blahPdF"));
        assertFalse(accept(filter, "blahpDfa", "blahPdFa"));

        assertTrue(accept(filter, "application/pDf", "application/pDf"));
    }

    @Test
    public void testOne() throws Exception {
        RecordFilter filter = loadFilter("mime-filters-av.json");
        System.out.println(accept(filter, "file", "image/vnd.zbrush.pcx"));
    }
    //TODO: add more unit tests

    public static boolean accept(RecordFilter filter, String mime, String detectedMime) {
        CCIndexRecord r = new CCIndexRecord();
        r.setMime(mime);
        r.setDetectedMime(detectedMime);
        r.setStatus(200);
        return filter.accept(r);
    }

    private RecordFilter loadFilter(String filterName) throws Exception {
        Path p = Paths.get(
                this.getClass().getResource("test-documents/"+filterName).toURI());
        return CompositeRecordFilter.load(p);
    }
}
