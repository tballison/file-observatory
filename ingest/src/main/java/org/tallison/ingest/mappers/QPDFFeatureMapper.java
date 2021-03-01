package org.tallison.ingest.mappers;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.ingest.qpdf.QPDFJsonExtractor;
import org.tallison.ingest.qpdf.QPDFResults;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QPDFFeatureMapper implements FeatureMapper {

    //if a key matches this regex, do not put it in the out of spec key
    public static final Pattern IN_SPEC = Pattern.compile("\\A\\/(?:(?:R|CS|Cs|cs|GS|Gs|gs|P|p|SH|Sh|sh|F|FM|Fm|fm|I|IM|Im|XO|Xo|TT|MC)\\d+)|TT\\d+_\\d+\\Z");
    private static final int MAX_STRING_LENGTH = 100;
    private static Set<String> COMMON_KEYS = new HashSet<>();


    public QPDFFeatureMapper() {
        QPDFJsonExtractor.loadKeys("/common-keys.txt", COMMON_KEYS);
    }



    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument)
            throws SQLException {
        processJson(row.get(FeatureMapper.REL_PATH_KEY), fetcher, storedDocument);
    }

    private void processJson(String relPath, Fetcher fetcher,
                             StoredDocument storedDocument) {
        String k = "qpdf/" + relPath + ".json";
        try (InputStream is = fetcher.fetch(k, new Metadata())) {
            try {
                processJson(relPath, is, storedDocument);
            } catch (IOException e) {
                storedDocument.addNonBlankField("q_status", "bad_extract");
            }
        } catch (Exception e) {
            storedDocument.addNonBlankField("q_status", "missing");

        }
    }

    private void processJson(String relPath, InputStream is,
                             StoredDocument storedDocument) throws IOException {
        try (Reader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            QPDFResults results = new QPDFJsonExtractor().extract(relPath, r);
            results = normalize(results);
            storedDocument.addNonBlankField("q_keys", toList(results.keys));
            List<String> filtered = filter(results.keys);

            storedDocument.addNonBlankField("q_keys_oos",
                    filtered);
            storedDocument.addNonBlankField("q_parent_and_keys",
                    toList(results.parentAndKeys));

            storedDocument.addNonBlankField("q_filters", toList(results.filters));
            storedDocument.addNonBlankField("q_keys_and_values", toList(results.keyValues));
            storedDocument.addNonBlankField("q_max_filter_count",
                    Integer.toString(results.maxFilterCount));
            //storedDocument.addNonBlankField("q_keys", sort(results.keys));
            //storedDocument.addNonBlankField("q_filters", sort(results.filters));
            //storedDocument.addNonBlankField("q_keys_and_values", sort(results.keyValues));
            //storedDocument.addNonBlankField("q_parent_and_keys",
            //      sort(results.parentAndKeys));
            //storedDocument.addNonBlankField("q_keys_oos",
            //      joinWith(" ", filtered));


        } catch (IllegalStateException e) {
            //log
            throw new IOException(e);
        }
    }

    private QPDFResults normalize(QPDFResults results) {
        QPDFResults ret = new QPDFResults();
        ret.maxFilterCount = results.maxFilterCount;
        ret.filters = normalize(results.filters);
        ret.keyValues = normalize(results.keyValues);
        ret.parentAndKeys = normalize(results.parentAndKeys);
        ret.keys = normalize(results.keys);
        return ret;
    }

    private Set<String> normalize(Set<String> strings) {
        Set<String> ret = new HashSet<>();
        for (String s : strings) {
            ret.add(truncate(ESUtil.stripIllegalUnicode(s)));
        }
        return ret;
    }

    private List<String> toList(Set<String> keys) {
        List<String> list = new ArrayList<>();
        for (String val : keys) {
            list.add(val);
        }
        Collections.sort(list);
        return list;
    }

    private List<String> filter(Set<String> keys) {
        List<String> list = new ArrayList<>();
        Matcher m = IN_SPEC.matcher("");
        for (String k : keys) {
            if (! COMMON_KEYS.contains(k)) {
                if (! m.reset(k).find()) {
                    list.add(k);
                }
            }
        }
        Collections.sort(list);
        return list;
    }

    private String truncate(String s) {
        if (s.length() > MAX_STRING_LENGTH) {
            return s.substring(0, MAX_STRING_LENGTH)+"...";
        } else {
            return s;
        }
    }


    private String sort(Set<String> keySet) {
        List<String> list = new ArrayList<>();
        list.addAll(keySet);
        Collections.sort(list);
        return joinWith(" ", list);
    }

    public static String joinWith(String delimiter, Collection<String> collection) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String s : collection) {
            if (i++ > 0) {
                sb.append(delimiter);
            }
            sb.append(s);
        }
        return sb.toString();
    }
}
