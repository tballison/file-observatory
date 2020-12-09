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

    //TODO: add more unit tests

    public static boolean accept(RecordFilter filter, String mime, String detectedMime) {
        CCIndexRecord r = new CCIndexRecord();
        r.setMime(mime);
        r.setDetectedMime(detectedMime);
        return filter.accept(r);
    }

    private RecordFilter loadFilter(String filterName) throws Exception {
        Path p = Paths.get(
                this.getClass().getResource("/test-documents/"+filterName).toURI());
        return CompositeRecordFilter.load(p);
    }
}
