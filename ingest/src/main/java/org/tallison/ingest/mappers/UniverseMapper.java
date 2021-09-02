package org.tallison.ingest.mappers;

import java.sql.SQLException;
import java.util.Map;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import org.apache.tika.pipes.fetcher.Fetcher;

public class UniverseMapper implements FeatureMapper {
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument)
            throws SQLException {
        storedDocument.addNonBlankField("universe", row.get("universe"));
        storedDocument.addNonBlankField("universe_validity",
                row.get("universe_validity"));
    }
}
