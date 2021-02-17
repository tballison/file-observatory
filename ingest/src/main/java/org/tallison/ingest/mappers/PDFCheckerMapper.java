package org.tallison.ingest.mappers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PDFCheckerMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        try {
            addFromJSON(resultSet.getString(1), rootDir, storedDocument);
        } catch (IOException e) {
            //log
            e.printStackTrace();
        }
    }

    private void addFromJSON(String relPath, Path rootDir,
                             StoredDocument storedDocument) throws IOException {
        Path p = rootDir.resolve("pdfchecker/output/" + relPath + ".json");
        if (!Files.isRegularFile(p)) {
            //log
            return;
        }
        processJson(p, storedDocument);
    }

    protected void processJson(Path p, StoredDocument storedDocument) throws IOException {
        try (Reader reader = Files.newBufferedReader(p)) {
            JsonElement rootElement = JsonParser.parseReader(reader);
            if (rootElement.isJsonNull()) {
                //log
                return;
            }
            JsonObject root = rootElement.getAsJsonObject();
            StringBuilder sb = new StringBuilder();
            if (root.has("analysis-summary")) {

                JsonObject summary = root.getAsJsonObject("analysis-summary");
                if (summary.has("can-be-optimized")) {
                    boolean canBeOptimized = summary.getAsJsonPrimitive("can-be-optimized").getAsBoolean();
                    if (canBeOptimized) {
                        sb.append("can-be-optimized").append(" ");
                    }
                }
                if (summary.has("information")) {
                    JsonArray info = summary.getAsJsonArray("information");
                    for (JsonElement el : info) {
                        sb.append(el.getAsString()).append(" ");
                    }
                    storedDocument.addNonBlankField("pc_summary_info", sb.toString().trim());
                }
                if (summary.has("errors")) {
                    List<String> errors = new ArrayList<>();
                    for (JsonElement el : summary.getAsJsonArray("errors")) {
                        errors.add(el.getAsString());
                    }
                    if (errors.size() > 0) {
                        storedDocument.addNonBlankField("pc_summary_errors", errors);
                    }
                }
            }
            if (root.has("fonts-results")) {
                JsonObject fontsResults = root.getAsJsonObject("fonts-results");
                if (fontsResults.has("errors")) {
                    JsonObject fontErrors = fontsResults.getAsJsonObject("errors");
                    List<String> fontErrorKeys = new ArrayList<>();
                    for (String n : fontErrors.keySet()) {
                        fontErrorKeys.add(n);
                    }
                    if (fontErrorKeys.size() > 0) {
                        storedDocument.addNonBlankField("pc_font_errors", fontErrorKeys);
                    }
                }
            }
        }
    }
}
