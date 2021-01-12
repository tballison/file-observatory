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
    Matcher encrypted = Pattern.compile("Encrypted:\\s+([^\r\n ]+)").matcher("");
    Matcher pages = Pattern.compile("Pages:\\s+(\\d+)").matcher("");
    Matcher tagged = Pattern.compile("Tagged:\\s+([^\n\r]+)").matcher("");
    Matcher optimized = Pattern.compile("Optimized:\\s+([^\n\r]+)").matcher("");

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        String stdout = resultSet.getString("pinfo_stdout");
        //todo add created and modified date
        for (String line : stdout.split("[\r\n]")) {
            if (producer.reset(line).find()) {
                storedDocument.addNonBlankField("pinfo_producer", producer.group(1).trim());
            } else if (creator.reset(line).find()) {
                storedDocument.addNonBlankField("pinfo_creator", creator.group(1).trim());
            } else if (version.reset(line).find()) {
                storedDocument.addNonBlankField("pinfo_version", version.group(1).trim());
            } else if (js.reset(line).find()) {
                String val = js.group(1).trim();
                String bool = (val.equals("yes")) ? "true" : "false";
                storedDocument.addNonBlankField("pinfo_javascript", bool);
            } else if (encrypted.reset(line).find()) {
                String val = encrypted.group(1).trim();
                String bool = (val.equals("yes")) ? "true" : "false";
                storedDocument.addNonBlankField("pinfo_encrypted", bool);
            } else if (pages.reset(line).find()) {
                int pageCount = Integer.parseInt(pages.group(1));
                storedDocument.addNonBlankField("pinfo_pages", Integer.toString(pageCount));
            } else if (tagged.reset(line).find()) {
                String val = tagged.group(1).trim();
                String bool = (val.equals("yes")) ? "true" : "false";
                storedDocument.addNonBlankField("pinfo_tagged", bool);
            } else if (optimized.reset(line).find()) {
                String val = optimized.group(1).trim();
                String bool = (val.equals("yes")) ? "true" : "false";
                storedDocument.addNonBlankField("pinfo_optimized", bool);
            }
        }
        storedDocument.addNonBlankField("pinfo", stdout);

    }
}
