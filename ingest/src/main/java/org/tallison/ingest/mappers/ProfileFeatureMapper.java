package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.tallison.ingest.mappers.QPDFFeatureMapper.joinWith;

public class ProfileFeatureMapper implements FeatureMapper {
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {

        storedDocument.addNonBlankField("fname", row.get("fname"));
        storedDocument.addNonBlankField("original_fname", row.get("fname"));
        storedDocument.addNonBlankField("shasum_256", row.get("shasum_256"));
        storedDocument.addNonBlankField("size", row.get("size"));
        storedDocument.addNonBlankField("collection", row.get("collection"));
    }
}
