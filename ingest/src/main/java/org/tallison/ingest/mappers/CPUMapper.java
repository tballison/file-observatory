package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class CPUMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        String val = resultSet.getString("cpu_warn");
        storedDocument.addNonBlankField("cpu_warn", val);

    }
}
