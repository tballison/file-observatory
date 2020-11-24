package org.tallison.ingest;

import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface FeatureMapper {

    void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException;
}
