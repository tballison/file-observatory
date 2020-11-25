package org.tallison.ingest.mappers;

import org.apache.tika.io.IOUtils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QPDFFeatureMapper implements FeatureMapper {

    //if a key matches this regex, do not put it in the out of spec key
    public static final Pattern IN_SPEC = Pattern.compile("\\A\\/(?:R|CS|GS)\\d+\\Z");
    Set<String> commonKeys = new HashSet<>();
    public QPDFFeatureMapper() {

        try {
            commonKeys.addAll(
                    IOUtils.readLines(
                            this.getClass().getResourceAsStream("/common_keys.txt"),
                            StandardCharsets.UTF_8.name()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
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
            List<String> filtered = filter(results.keys);
            storedDocument.addNonBlankField("q_keys_oos",
                    joinWith(" ", filtered));

            storedDocument.addNonBlankField("q_keys_oos_multi",
                    filtered);
        } catch (IllegalStateException e) {
            //log
        }
    }

    private List<String> filter(Set<String> keys) {
        List<String> list = new ArrayList<>();
        Matcher m = IN_SPEC.matcher("");
        for (String k : keys) {
            if (! commonKeys.contains(k)) {
                if (! m.reset(k).find()) {
                    list.add(k);
                }
            }
        }
        Collections.sort(list);
        return list;
    }

    private String sort(Set<String> keySet) {
        List<String> list = new ArrayList<>();
        list.addAll(keySet);
        Collections.sort(list);
        return joinWith(" ", list);

    }

    public static String joinWith(String delimiter, Collection<String> collection) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String s : collection) {
            if (i++ > 0) {
                sb.append(delimiter);
            }
            sb.append(s);
        }
        return sb.toString();
    }
}
