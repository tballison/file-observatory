package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CaradocMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir,
                            StoredDocument storedDocument) throws SQLException {
        String val = resultSet.getString("cd");
        storedDocument.addNonBlankField("cd", val);
        val = resultSet.getString("cd_warn");
        storedDocument.addNonBlankField("cd_warn", val);
    }
}
