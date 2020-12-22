package org.tallison.ingest.mappers;

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
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {

        storedDocument.addNonBlankField("fname", resultSet.getString("fname"));
        storedDocument.addNonBlankField("original_fname", resultSet.getString("fname"));
        storedDocument.addNonBlankField("shasum_256", resultSet.getString("shasum_256"));
        storedDocument.addNonBlankField("size", Long.toString(resultSet.getLong("size")));
        storedDocument.addNonBlankField("collection", resultSet.getString("collection"));
    }
}
