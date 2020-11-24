package org.tallison.ingest.qpdf;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Reader;

public class QPDFJsonExtractor {

    public QPDFResults extract(Reader reader) {
        JsonElement root = JsonParser.parseReader(reader);
        QPDFResults results = new QPDFResults();
        parseObjects(root.getAsJsonObject().get("objects").getAsJsonObject(), results);
        return results;
    }

    private void parseObjects(JsonObject objects, QPDFResults results) {
        for (String k : objects.keySet()) {
            JsonElement obj = objects.get(k);
            if (obj.isJsonObject()) {
                parseObject(obj.getAsJsonObject(), results);
            } else if (obj.isJsonArray()) {
                parseArr(obj.getAsJsonArray(), results);
            }
        }
    }

    private void parseObject(JsonObject obj, QPDFResults results) {
        results.keys.addAll(obj.keySet());
        for (String k : obj.keySet()) {
            JsonElement el = obj.get(k);
            if (el.isJsonObject()) {
                parseObject(el.getAsJsonObject(), results);
            } else if (el.isJsonArray()) {
                parseArr(el.getAsJsonArray(), results);
            }
        }
    }

    private void parseArr(JsonArray arr, QPDFResults results) {
        for (JsonElement el : arr) {
            if (el.isJsonObject()) {
                parseObject(el.getAsJsonObject(), results);
            } else if (el.isJsonArray()) {
                parseArr(el.getAsJsonArray(), results);
            }
        }
    }
}
