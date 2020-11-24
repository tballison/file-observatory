package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.ingest.qpdf.QPDFJsonExtractor;
import org.tallison.ingest.qpdf.QPDFResults;
import org.tallison.quaerite.core.StoredDocument;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class QPDFFeatureMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument)
            throws SQLException {
        try {
            processJson(resultSet.getString(1), rootDir, storedDocument);
        } catch (IOException e) {
            //log
        }
    }

    private void processJson(String relPath, Path rootDir,
                             StoredDocument storedDocument) throws IOException {
        Path p = rootDir.resolve("qpdf/json/"+relPath+".json");
        try (Reader r = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            QPDFResults results = new QPDFJsonExtractor().extract(r);
            storedDocument.addNonBlankField("q_keys", sort(results.keys));
        } catch (IllegalStateException e) {
            //log
        }
    }

    private String sort(Set<String> keySet) {
        List<String> list = new ArrayList<>();
        list.addAll(keySet);
        Collections.sort(list);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(list.get(i));
        }
        return sb.toString();
    }
}
