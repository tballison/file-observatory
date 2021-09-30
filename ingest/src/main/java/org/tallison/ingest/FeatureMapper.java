package org.tallison.ingest;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface FeatureMapper {

    public static final String REL_PATH_KEY = "relpath";
    public static final String ID_KEY = "id";
    void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException;
}
