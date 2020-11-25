package org.tallison.ingest.mappers;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PDFInfoFeatureMapper implements FeatureMapper {
    Matcher producer = Pattern.compile("Producer: ([^\r\n]+)").matcher("");
    Matcher creator = Pattern.compile("Creator: ([^\r\n]+)").matcher("");
    Matcher version = Pattern.compile("PDF version: ([^\r\n]+)").matcher("");
    Matcher js = Pattern.compile("JavaScript: ([^\r\n]+)").matcher("");
    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        String stdout = resultSet.getString("pi_stdout");
        //todo add created and modified date
        for (String line : stdout.split("[\r\n]")) {
            if (producer.reset(line).find()) {
                storedDocument.addNonBlankField("pi_producer", producer.group(1).trim());
            } else if (creator.reset(line).find()) {
                storedDocument.addNonBlankField("pi_creator", creator.group(1).trim());
            } else if (version.reset(line).find()) {
                storedDocument.addNonBlankField("pi_version", version.group(1).trim());
            } else if (js.reset(line).find()) {
                String val = js.group(1).trim();
                String bool = (val.equals("yes")) ? "true" : "false";
                storedDocument.addNonBlankField("pi_javascript", bool);
            }
        }

    }
}
