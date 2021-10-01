package org.tallison.cc.index;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Only implements "include lists" filter on mime and detected mime
 * and status for now.
 */
public class CompositeRecordFilter implements RecordFilter {

    private static final boolean DEFAULT_CASE_SENSITIVE = false;
    private static final float UNSPECIFIED_PROBABILITY = -1.0f;
    private IntFilter statusInclude;
    private StringFilter mimesInclude;
    private StringFilter detectedMimesInclude;

    private StringFilter regexMimesInclude;
    private StringFilter regexDetectedMimesInclude;

    private boolean defaultInclude = false;
    private float defaultProbability = 1.0f;

    private Random random = new Random(314159);
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
            throw new IOException("Expected json object as root object");
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
                    addPattern(el, filter.mimesInclude);
                }
            }
            if (exact.has("detected_mimes")) {
                for (JsonElement el : exact.getAsJsonArray("detected_mimes")) {
                    addPattern(el, filter.detectedMimesInclude);
                }
            }
        }
        if (root.has("regex")) {
            JsonObject regex = root.getAsJsonObject("regex");
            if (regex.has("mimes")) {
                for (JsonElement el : regex.getAsJsonArray("mimes")) {
                    addPattern(el, filter.regexMimesInclude);
                }
            }
            if (regex.has("detected_mimes")) {
                for (JsonElement el : regex.getAsJsonArray("detected_mimes")) {
                    addPattern(el, filter.regexDetectedMimesInclude);
                }
            }
        }
        filter.statusInclude = new IntFilter();
        if (root.has("status")) {
            JsonElement statusEl = root.get("status");
            if (statusEl.isJsonArray()) {
                for (JsonElement statusIntEl : statusEl.getAsJsonArray()) {
                    filter.statusInclude.addInt(statusIntEl.getAsInt());
                }
            } else {
                filter.statusInclude.addInt(root.get("status").getAsInt());
            }
        }
        if (root.has("defaultInclude")) {
            filter.defaultInclude = root.get("defaultInclude").getAsBoolean();
        }
        if (root.has("defaultProbability")) {
            filter.defaultProbability = root.get("defaultProbability").getAsFloat();
        }
        return filter;
    }

    private static void addPattern(JsonElement el, StringFilter filter) {
        if (el.isJsonObject()) {
            String pattern = el.getAsJsonObject().get("pattern").getAsString();
            float probability = el.getAsJsonObject().get("probability").getAsFloat();
            filter.addPattern(pattern, probability);
        } else {
            filter.addPattern(el.getAsString(), UNSPECIFIED_PROBABILITY);
        }
    }

    @Override
    public boolean accept(CCIndexRecord record) {

        if (!statusInclude.accept(record.getStatus())) {
            return false;
        }
        //TODO -- clean this up
        if (defaultProbability < 1.0f && defaultProbability >= 0.0f) {
            if (random.nextFloat() < defaultProbability) {
                return true;
            } else {
                return false;
            }
        }

        RESULT r = mimesInclude.accept(record.getNormalizedMime());
        if (r == RESULT.MATCH_SELECT) {
            return true;
        } else if (r == RESULT.MATCH_DONT_SELECT) {
            return false;
        }

        r = detectedMimesInclude.accept(record.getNormalizedDetectedMime());
        if (r == RESULT.MATCH_SELECT) {
            return true;
        } else if (r == RESULT.MATCH_DONT_SELECT) {
            return false;
        }

        r = regexMimesInclude.accept(record.getNormalizedMime());
        if (r == RESULT.MATCH_SELECT) {
            return true;
        } else if (r == RESULT.MATCH_DONT_SELECT) {
            return false;
        }
        r = regexDetectedMimesInclude.accept(record.getNormalizedDetectedMime());
        if (r == RESULT.MATCH_SELECT) {
            return true;
        } else if (r == RESULT.MATCH_DONT_SELECT) {
            return false;
        }

        return defaultInclude;
    }

    private static class AcceptAll implements RecordFilter {

        @Override
        public boolean accept(CCIndexRecord record) {
            return true;
        }
    }

    //returns accept if it is empty or if
    //the int has been added via addInt
    private static class IntFilter {
        Set<Integer> ints = new HashSet<>();
        boolean accept(int i) {
            return ints.isEmpty() || ints.contains(i);
        }
        void addInt(int i) {
            ints.add(i);
        }
    }

    private interface StringFilter {
        RESULT accept(String s);
        void addPattern(String s, float probability);
    }
    private static class ExactFilter implements StringFilter {

        private final Random random = new Random();
        private final boolean caseSensitive;
        private Map<String, Float> include = new HashMap<>();
        ExactFilter(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
        }

        @Override
        public void addPattern(String pattern, float probability) {
            if (caseSensitive) {
                include.put(pattern, probability);
            } else {
                include.put(pattern.toLowerCase(Locale.US), probability);
            }
        }

        @Override
        public RESULT accept(String s) {
            if (s == null) {
                return RESULT.DONT_SELECT;
            }
            if (caseSensitive) {
                if (include.containsKey(s)) {
                    Float prob = include.get(s);
                    if (prob > 0.0f) {
                        if (random.nextFloat() < prob) {
                            return RESULT.MATCH_SELECT;
                        } else {
                            return RESULT.MATCH_DONT_SELECT;
                        }
                    } else {
                        return RESULT.MATCH_SELECT;
                    }
                }
            } else {
                String lc = s.toLowerCase(Locale.US);
                if (include.containsKey(lc)) {
                    Float prob = include.get(lc);
                    if (prob > 0.0f) {
                        if (random.nextFloat() < prob) {
                            return RESULT.MATCH_SELECT;
                        } else {
                            return RESULT.MATCH_DONT_SELECT;
                        }
                    }
                    return RESULT.MATCH_SELECT;
                }
            }
            return RESULT.DONT_SELECT;
        }
    }

    private static class RegexFilter implements StringFilter {

        Map<Pattern, Float> patterns = new HashMap<>();
        private final Random random = new Random();

        @Override
        public RESULT accept(String s) {
            if (s == null) {
                return RESULT.DONT_SELECT;
            }
            for (Map.Entry<Pattern, Float> e : patterns.entrySet()) {
                if (e.getKey().matcher(s).find()) {
                    if (e.getValue() > 0.0f) {
                        if (random.nextFloat() < e.getValue()) {
                            return RESULT.MATCH_SELECT;
                        } else {
                            return RESULT.MATCH_DONT_SELECT;
                        }
                    } else {
                        return RESULT.MATCH_SELECT;
                    }
                }
            }
            return RESULT.DONT_SELECT;
        }

        @Override
        public void addPattern(String s, float probability) {
            patterns.put(Pattern.compile(s), probability);
        }
    }

    private enum RESULT {
        MATCH_SELECT,
        MATCH_DONT_SELECT,
        DONT_SELECT
    }
}
