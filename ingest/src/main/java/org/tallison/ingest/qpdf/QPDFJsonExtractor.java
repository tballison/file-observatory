package org.tallison.ingest.qpdf;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.ingest.mappers.QPDFFeatureMapper;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QPDFJsonExtractor {

    private static Logger LOGGER = LoggerFactory.getLogger(QPDFJsonExtractor.class);

    private static final int MAX_DEPTH = 30;
    private final Set<String> ignoreValues = new HashSet<>();

    private Matcher refMatcher = Pattern.compile("^\\d+ \\d+ R$").matcher("");
    private Matcher dateMatcher = Pattern.compile("^D:\\d{14,}").matcher("");
    public QPDFJsonExtractor() {
        ignoreValues.add("/CharSet");
        ignoreValues.add("/Title");
        ignoreValues.add("/DA");
    }
    private JsonObject objects;
    private String fileId;

    public QPDFResults extract(String fileId, Reader reader) {
        JsonElement root = JsonParser.parseReader(reader);
        QPDFResults results = new QPDFResults();
        this.objects = root.getAsJsonObject().get("objects").getAsJsonObject();
        this.fileId = fileId;
        parseObjects(root.getAsJsonObject().get("objects").getAsJsonObject(), results);
        return results;
    }

    private void parseObjects(JsonObject objects, QPDFResults results) {
        for (String k : objects.keySet()) {
            JsonElement obj = objects.get(k);
            if (obj.isJsonObject()) {
                parseObject(k, obj.getAsJsonObject(), results);
            } else if (obj.isJsonArray()) {
                parseArr(k, obj.getAsJsonArray(), results);
            }
        }
    }

    private void parseObject(String parent, JsonObject obj, QPDFResults results) {
        results.keys.addAll(obj.keySet());

        for (String k : obj.keySet()) {
            if (! refMatcher.reset(parent).find()) {
                results.parentAndKeys.add(parent + "->" + k);
            }
            JsonElement el = obj.get(k);
            if (el.isJsonObject()) {
                parseObject(k, el.getAsJsonObject(), results);
            } else if (el.isJsonArray()) {
                parseArr(k, el.getAsJsonArray(), results);
            } else if (el.isJsonPrimitive()) {
                JsonPrimitive primitive = el.getAsJsonPrimitive();
                if (primitive.isBoolean()) {
                    results.keyValues.add(k+"->"+
                            Boolean.toString(primitive.getAsBoolean()).toUpperCase(Locale.US));
                } else if (primitive.isNumber()) {
                    results.keyValues.add(k + "->NUMBER");
                } else if (primitive.isString()) {
                    String normalized = getNormalizedValue(k, primitive.getAsString());
                    if (normalized != null) {
                        results.keyValues.add(k + "->" + normalized);
                    }
                }
            }

            if (k.equals("/Filter")) {
                processFilters(el, results);
            }
        }
    }

    private void processFilters(JsonElement el, QPDFResults results) {
        JsonElement dereferenced = dereference(el, 0);

        if (dereferenced.isJsonArray()) {
            List<String> filters = new ArrayList<>();
            for (JsonElement child : dereferenced.getAsJsonArray()) {
                JsonElement dereffedChild = dereference(child, 0);
                if (dereffedChild.isJsonPrimitive()) {
                    if (dereffedChild.getAsJsonPrimitive().isString()) {
                        filters.add(dereffedChild.getAsString());
                    }
                }
            }
            if (results.maxFilterCount < filters.size()) {
                results.maxFilterCount = filters.size();
            }
            results.filters.add(QPDFFeatureMapper.joinWith("->", filters));
            return;
        }
        if (! el.isJsonPrimitive()) {
            LOGGER.warn("non primitive filter: ("+fileId+"):"+el.toString());
            return;
        }
        results.filters.add(el.getAsString());
        if (results.maxFilterCount < 1) {
            results.maxFilterCount = 1;
        }
    }

    private void parseArr(String parent, JsonArray arr, QPDFResults results) {
        results.parentAndKeys.add(parent+"->ARRAY");
        for (JsonElement el : arr) {
            if (el.isJsonObject()) {
                parseObject(parent, el.getAsJsonObject(), results);
            } else if (el.isJsonArray()) {
                parseArr(parent, el.getAsJsonArray(), results);
            }
        }
    }

    private String getNormalizedValue(String key, String rawValue) {
        if (rawValue == null || rawValue.trim().length() == 0) {
            return null;
        }

        if (refMatcher.reset(rawValue).find()) {
            return "REF";
        } else if (dateMatcher.reset(rawValue).find()) {
            return "DATE";
        } else if (ignoreValues.contains(key)) {
            return "STRING";
        }
        return rawValue;
    }

    JsonElement dereference(JsonElement el, int depth) {
        if (! el.isJsonPrimitive()) {
            return el;
        }

        JsonPrimitive elPrim = el.getAsJsonPrimitive();
        if (! elPrim.isString()) {
            return el;
        }

        if (!refMatcher.reset(elPrim.getAsString()).find()) {
            return el;
        }
        String key = el.getAsString();
        if (depth > MAX_DEPTH) {
            return null;
        }
        JsonElement refVal = objects.get(key);
        return dereference(refVal, depth+1);
    }
}
