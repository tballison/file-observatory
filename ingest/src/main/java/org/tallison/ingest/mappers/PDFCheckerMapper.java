package org.tallison.ingest.mappers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PDFCheckerMapper implements FeatureMapper {

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        try {
            addFromJSON(row.get(REL_PATH_KEY), fetcher, storedDocument);
        } catch (IOException e) {
            //log
            e.printStackTrace();
        }
    }

    private void addFromJSON(String relPath, Fetcher fetcher,
                             StoredDocument storedDocument) throws IOException {
        String k = "pdfchecker/" + relPath + ".json";
        try (InputStream is = fetcher.fetch(k, new Metadata())) {
            try {
                processJson(is, storedDocument);
            } catch (IOException e) {
                storedDocument.addNonBlankField("pc_status", "bad_extract");
            }
        } catch (IOException | TikaException e) {
            storedDocument.addNonBlankField("pc_status", "missing");
        }

    }

    protected void processJson(InputStream is, StoredDocument storedDocument) throws IOException {
        try (Reader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.US_ASCII))) {
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
