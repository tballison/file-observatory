package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * this should cover both mutool clean -s and mutool text
 * we aren't currently indexing text as extrated by mutool text
 */
public class MutoolMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        String val = resultSet.getString("mc_warn");
        storedDocument.addNonBlankField("mc_warn", val);
        val = resultSet.getString("mt_warn");
        storedDocument.addNonBlankField("mt_warn", val);

    }
}
