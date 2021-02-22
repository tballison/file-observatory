package org.tallison.ingest.mappers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.ingest.FeatureMapper;
import org.tallison.ingest.qpdf.QPDFJsonExtractor;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArlingtonMapper implements FeatureMapper {

    private static Pattern FAILED_TO_OPEN = Pattern.compile("Failed to open document");
    private static Pattern ERROR_PATTERN = Pattern.compile("Error: ([^(]+)");
    private static Logger LOGGER = LoggerFactory.getLogger(ArlingtonMapper.class);


    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument)
            throws SQLException {
        boolean timeout = resultSet.getBoolean("arlington_timeout");
        int exit = resultSet.getInt("arlington_exit_value");
        if (timeout) {
            storedDocument.addNonBlankField("a_status", "timeout");
            return;
        } else if (exit != 0) {
            storedDocument.addNonBlankField("a_status", "crash");
            return;
        }
        String relPath = resultSet.getString(1);
        try {
            processFile(relPath, rootDir, storedDocument);
        } catch (IOException e) {
            LOGGER.warn(relPath, e);
        }

    }

    private void processFile(String relPath, Path rootDir, StoredDocument storedDocument)
            throws IOException {

        Path p = rootDir.resolve("arlington/output/" + relPath + ".txt");
        if (!Files.isRegularFile(p)) {
            storedDocument.addNonBlankField("a_status", "missing");
            return;
        }
        try {
            _processFile(p, storedDocument);
        } catch (IOException e) {
            storedDocument.addNonBlankField("a_status", "bad_extract");

        }
    }

    protected void _processFile(Path p, StoredDocument storedDocument) throws IOException {
        Matcher error = ERROR_PATTERN.matcher("");
        Matcher cantOpen = FAILED_TO_OPEN.matcher("");
        Set<String> errors = new TreeSet<>();
        try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.US_ASCII)) {
            String line = reader.readLine();
            //skip first line
            line = reader.readLine();
            if (line == null) {
                //log
                return;
            }
            if (cantOpen.reset(line).find()) {
                storedDocument.addNonBlankField("a_status", "fail");
                return;
            }
            while (line != null) {
                if (line.startsWith("Error: ")) {
                    appendError(line, error, errors);
                }
                line = reader.readLine();
            }
        }
        List<String> errorList = new ArrayList<>();
        errorList.addAll(errors);
        if (errors.size() > 0) {
            storedDocument.addNonBlankField("a_status", "warn");
        }
        storedDocument.addNonBlankField("a_warn", errorList);
    }

    private void appendError(String line, Matcher errorMatcher, Set<String> errors) {
        if (line.contains("Can't select any link")) {
            errors.add("Can't select any link");
        } else if (line.contains("object validated in two different contexts")) {
            errors.add("object validated in two different contexts");
        }else if (errorMatcher.reset(line).find()) {
            errors.add(ESUtil.stripIllegalUnicode(errorMatcher.group(1).trim()));
        } else {
            errors.add(ESUtil.stripIllegalUnicode(line.trim()));
        }
    }

}
