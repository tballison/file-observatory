package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * this should cover both pdfminer dump and pdfminer text
 * we aren't currently indexing anything but the warning msgs
 */
public class PDFMinerMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        String val = resultSet.getString("pmd_warn");
        storedDocument.addNonBlankField("pmd_warn", val);
        val = resultSet.getString("pmt_warn");
        storedDocument.addNonBlankField("pmt_warn", val);

    }
}
