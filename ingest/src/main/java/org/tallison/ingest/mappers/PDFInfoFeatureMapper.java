package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PDFInfoFeatureMapper implements FeatureMapper {


    private static Logger LOGGER = LoggerFactory.getLogger(PDFInfoFeatureMapper.class);

    Pattern KEY_VALUE = Pattern.compile("^([^:]+):\\s+(.*?)\\Z");
    Pattern INTEGER = Pattern.compile("^\\s*(\\d+)");
    private static Map<String, String> KEY_VALUE_MAP = new HashMap<>();
    private static Set<String> BOOLEAN_VALS = new HashSet<>();
    private static Set<String> DATE_VALS = new HashSet<>();

    static {
        KEY_VALUE_MAP.put("Producer", "pinfo_producer");
        KEY_VALUE_MAP.put("Creator", "pinfo_creator");
        KEY_VALUE_MAP.put("PDF version", "pinfo_version");
        KEY_VALUE_MAP.put("Pages", "pinfo_pages");
        KEY_VALUE_MAP.put("Tagged", "pinfo_tagged");
        KEY_VALUE_MAP.put("Optimized", "pinfo_optimized");
        KEY_VALUE_MAP.put("JavaScript", "pinfo_javascript");
        KEY_VALUE_MAP.put("Encrypted", "pinfo_encrypted");
        KEY_VALUE_MAP.put("CreationDate", "pinfo_created");
        KEY_VALUE_MAP.put("ModDate",  "pinfo_modified");
        BOOLEAN_VALS.add("Tagged");
        BOOLEAN_VALS.add("Optimized");
        BOOLEAN_VALS.add("JavaScript");
        BOOLEAN_VALS.add("Encrypted");
        DATE_VALS.add("CreationDate");
        DATE_VALS.add("ModDate");

    }
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        String stdout = row.get("pinfo_stdout");
        if (StringUtils.isBlank(stdout)) {
            return;
        }
        //todo add created and modified date
        Matcher matcher = KEY_VALUE.matcher("");
        for (String line : stdout.split("[\r\n]")) {
            if (matcher.reset(line).find()) {
                String k = matcher.group(1);
                String v = matcher.group(2);
                if (KEY_VALUE_MAP.containsKey(k)) {
                    if (BOOLEAN_VALS.contains(k)) {
                        v = (v.startsWith("yes")) ? "true" : "false";
                    } else if (DATE_VALS.contains(k)) {
                        v = convertDate(v);
                    }
                    if (k.equals("Pages")) {
                        //very rarely, this can be: "24 (including covers)"
                        //which doesn't make elastic happy because
                        //it expects an integer
                        Matcher p = INTEGER.matcher(v);
                        if (p.find()) {
                            v = p.group(1);
                        }
                    }
                    storedDocument.addNonBlankField(KEY_VALUE_MAP.get(k), v);
                }
            }
        }
        storedDocument.addNonBlankField("pinfo", stdout);
    }

    private String convertDate(String v) {
        if (StringUtils.isBlank(v)){
            return "";
        }
        v = v.replaceAll("\\s+", " ").trim();
        try {
            Instant instant = LocalDateTime.parse(v,
                    DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss yyyy z", Locale.US)).atZone(ZoneId.of("UTC")).toInstant();
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        } catch (DateTimeParseException e) {
            LOGGER.warn("couldn't parse >{}<", v);
            return "";
        }
    }
}
