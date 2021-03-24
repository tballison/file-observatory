package org.tallison.ingest.mappers;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

public class PDFResurrectMapper  implements FeatureMapper {
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument)
            throws SQLException {
        String stdout = row.get("pr");
        if (stdout == null) {
            return;
        }
        Matcher m = Pattern.compile(": (\\d+)").matcher(stdout);
        if (m.find()) {
            storedDocument.addNonBlankField("pr_updates", m.group(1));
        }
    }
}
