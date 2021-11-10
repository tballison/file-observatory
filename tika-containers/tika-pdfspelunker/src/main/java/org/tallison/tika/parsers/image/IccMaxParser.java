package org.tallison.tika.parsers.image;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;

public class IccMaxParser extends AbstractParser {

    private static final String ICC_MAX_PREFIX = "icc-max:";

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return null;
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            Matcher fileId = Pattern.compile("(Profile:|Unable to parse)\\s+'([^']+)").matcher("");

            boolean parseable = false;
            while (line != null) {
                line = line.trim();
                if (fileId.reset(line).find()) {
                    if (fileId.group(1).startsWith("Profile")) {
                        parseable = true;
                    }
                }
                if (line.equals("Header")) {
                    processHeader(reader, metadata);
                } else if (line.equals("Profile Tags")) {
                    processTags(reader, metadata);
                } else if (line.equals("Validation Report")) {
                    processReport(reader, metadata);
                }
                line = reader.readLine();
            }
            metadata.set(ICC_MAX_PREFIX+"parseable", Boolean.toString(parseable));
        }
    }

    private void processReport(BufferedReader reader, Metadata metadata) throws IOException {
        String line = reader.readLine();
        Matcher validMatcher = Pattern.compile("Profile is valid").matcher("");
        Matcher violatesMatcher = Pattern.compile("Profile violates ICC specification").matcher("");
        Matcher warningsMatcher = Pattern.compile("Profile has warning\\(s\\)").matcher("");
        Matcher errorsMatcher = Pattern.compile("Profile has Critical Error\\(s\\)").matcher("");

        Matcher error = Pattern.compile("^Error! (.*)").matcher("");
        Matcher nonCompliant = Pattern.compile("^NonCompliant! - (.*)").matcher("");
        Matcher warningMatcher = Pattern.compile("^Warning! - (.*)").matcher("");
        Set<String> errors = new HashSet<>();
        Set<String> nonCompliances = new HashSet<>();
        Set<String> warnings = new HashSet<>();
        String validity = null;
        StringBuilder sb = new StringBuilder();
        while (line != null) {
            if (validMatcher.reset(line).find()) {
                validity = "valid";
            } else if (violatesMatcher.reset(line).find()) {
                validity = "noncompliant";
            } else if (warningsMatcher.reset(line).find()) {
                validity = "warnings";
            } else if (errorsMatcher.reset(line).find()) {
                validity = "errors";
            }

            if (error.reset(line).find()) {
                String s = error.group(1);
                s = s.replaceAll("^[ -]+", "");
                errors.add(s);
            }
            if (nonCompliant.reset(line).find()) {
                nonCompliances.add(nonCompliant.group(1).trim());
            }
            if (warningMatcher.reset(line).find()) {
                warnings.add(warningMatcher.group(1));
            }
            sb.append(line + "\n");
            line = reader.readLine();
        }
        System.out.println("VALIDITY: " + validity);
        metadata.set(ICC_MAX_PREFIX+"validity", validity);
        if (errors.size() > 0) {
            System.out.println("errors: " + errors);
            metadata.set(ICC_MAX_PREFIX+"errors", joinWith("||", errors));
        }
        if (nonCompliances.size() > 0) {
            System.out.println("noncompliances: " + nonCompliances);
            metadata.set(ICC_MAX_PREFIX+"noncompliances", joinWith("||", nonCompliances));
        }
        if (warnings.size() > 0) {
            metadata.set(ICC_MAX_PREFIX+"warnings", joinWith("||", warnings));
        }
        System.out.println("<------>");
        System.out.println(sb);
        System.out.println("<-------------------->");
        return;
    }

    private static String joinWith(String delimiter, Set<String> vals) {
        if (vals.size() == 0) {
            return "";
        }
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (String val : vals) {
            if (i++ > 0) {
                sb.append(delimiter);
            }
            sb.append(val);
        }
        return sb.toString();
    }
    private void processTags(BufferedReader reader, Metadata metadata) throws IOException {
        String line = reader.readLine();
        while (!StringUtils.isAllBlank(line)) {

            line = reader.readLine();
        }
        return;
    }

    private void processHeader(BufferedReader reader, Metadata metadata) throws IOException {
        String line = reader.readLine();
        Matcher m = Pattern.compile("^(Flags\\s|[^:]+):?\\s+(.*)\\Z").matcher("");
        while (!StringUtils.isAllBlank(line)) {

            if (m.reset(line).find()) {
                String k = m.group(1).replaceAll(" ", "");
                String v = m.group(2);
                metadata.set(ICC_MAX_PREFIX+k, v);
                //System.out.println(k+ "->" + v);
            } else {
                //System.out.println("bad header: " + line);
            }
            line = reader.readLine();
        }
    }
}
