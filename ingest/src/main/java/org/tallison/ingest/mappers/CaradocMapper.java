package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class CaradocMapper implements FeatureMapper {

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher,
                            StoredDocument storedDocument) throws SQLException {
        String val = row.get("cd");
        storedDocument.addNonBlankField("cd", val);
        val = row.get("cd_warn");
        storedDocument.addNonBlankField("cd_warn", val);
    }
}
