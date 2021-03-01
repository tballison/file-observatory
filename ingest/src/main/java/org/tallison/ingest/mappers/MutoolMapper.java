package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * this should cover both mutool clean -s and mutool text
 * we aren't currently indexing text as extrated by mutool text
 */
public class MutoolMapper implements FeatureMapper {

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        String val = row.get("mc_warn");
        storedDocument.addNonBlankField("mc_warn", val);
        val = row.get("mt_warn");
        storedDocument.addNonBlankField("mt_warn", val);

    }
}
