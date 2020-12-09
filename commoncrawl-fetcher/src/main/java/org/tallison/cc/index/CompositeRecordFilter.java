package org.tallison.cc.index;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Only implements "include lists" filter on mime and detected mime for now
 */
public class CompositeRecordFilter implements RecordFilter {

    private static final boolean DEFAULT_CASE_SENSITIVE = false;
    private StringFilter mimesInclude;
    private StringFilter detectedMimesInclude;

    private StringFilter regexMimesInclude;
    private StringFilter regexDetectedMimesInclude;

    public static RecordFilter load(Path jsonFile) throws IOException {
        if (jsonFile == null) {
            return new AcceptAll();
        }
        CompositeRecordFilter filter = new CompositeRecordFilter();
        JsonElement rootEl;
        try (Reader reader = Files.newBufferedReader(jsonFile, StandardCharsets.UTF_8)) {
            rootEl = JsonParser.parseReader(reader);
        }
        if (! rootEl.isJsonObject()) {
            throw new IOException("Exepected json object as root object");
        }
        JsonObject root = rootEl.getAsJsonObject();
        boolean caseSensitive = DEFAULT_CASE_SENSITIVE;
        if (root.has("case_sensitive")) {
            caseSensitive = root.get("case_sensitive").getAsBoolean();
        }
        filter.mimesInclude = new ExactFilter(caseSensitive);
        filter.detectedMimesInclude = new ExactFilter(caseSensitive);
        filter.regexMimesInclude = new RegexFilter();
        filter.regexDetectedMimesInclude = new RegexFilter();
        if (root.has("exact")) {
            JsonObject exact = root.getAsJsonObject("exact");
            if (exact.has("mimes")) {
                for (JsonElement el : exact.getAsJsonArray("mimes")) {
                    filter.mimesInclude.addPattern(el.getAsString());
                }
            }
            if (exact.has("detected_mimes")) {
                for (JsonElement el : exact.getAsJsonArray("detected_mimes")) {
                    filter.detectedMimesInclude.addPattern(el.getAsString());
                }
            }
        }
        if (root.has("regex")) {
            JsonObject regex = root.getAsJsonObject("regex");
            if (regex.has("mimes")) {
                for (JsonElement el : regex.getAsJsonArray("mimes")) {
                    filter.regexMimesInclude.addPattern(el.getAsString());
                }
            }
            if (regex.has("detected_mimes")) {
                for (JsonElement el : regex.getAsJsonArray("detected_mimes")) {
                    filter.regexDetectedMimesInclude.addPattern(el.getAsString());
                }
            }
        }
        return filter;
    }

    @Override
    public boolean accept(CCIndexRecord record) {
        if (mimesInclude.accept(record.getNormalizedMime())) {
            return true;
        } else if (detectedMimesInclude.accept(record.getNormalizedDetectedMime())) {
            return true;
        } else {
            if (regexMimesInclude.accept(record.getNormalizedMime())) {
                return true;
            } else if (regexDetectedMimesInclude.accept(record.getNormalizedDetectedMime())) {
                return true;
            }
        }
        return false;
    }

    private static class AcceptAll implements RecordFilter {

        @Override
        public boolean accept(CCIndexRecord record) {
            return true;
        }
    }

    private interface StringFilter {
        boolean accept(String s);
        void addPattern(String s);
    }

    private static class ExactFilter implements StringFilter {

        private final boolean caseSensitive;
        private Set<String> include = new HashSet<>();
        ExactFilter(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
        }

        @Override
        public void addPattern(String pattern) {
            if (caseSensitive) {
                include.add(pattern);
            } else {
                include.add(pattern.toLowerCase(Locale.US));
            }
        }
        @Override
        public boolean accept(String s) {
            if (s == null) {
                return false;
            }
            if (caseSensitive) {
                return include.contains(s);
            } else {
                return include.contains(s.toLowerCase(Locale.US));
            }
        }
    }

    private static class RegexFilter implements StringFilter {

        Set<Pattern> patterns = new HashSet<>();
        @Override
        public boolean accept(String s) {
            if (s == null) {
                return false;
            }
            for (Pattern p : patterns) {
                if (p.matcher(s).find()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void addPattern(String s) {
            patterns.add(Pattern.compile(s));
        }
    }
}
