package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * this should cover both pdfminer dump and pdfminer text
 * we aren't currently indexing anything but the warning msgs
 */
public class PDFMinerMapper implements FeatureMapper {

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        String val = row.get("pmd_warn");
        storedDocument.addNonBlankField("pmd_warn", val);
        val = row.get("pmt_warn");
        storedDocument.addNonBlankField("pmt_warn", val);

    }
}
