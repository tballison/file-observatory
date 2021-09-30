package org.tallison.ingest.mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.utils.StringUtils;

public class MultiCompareMapper implements FeatureMapper {

    static Map<String, String> COLS_TO_ES = new ConcurrentHashMap<>();
    static {
        COLS_TO_ES.put("num_tokens_0", "tk_num_tokens");
        COLS_TO_ES.put("oov_0", "tk_oov");
        COLS_TO_ES.put("detected_lang_0", "tk_lang_detected");
        COLS_TO_ES.put("num_tokens_1", "mt_num_tokens");
        COLS_TO_ES.put("oov_1", "mt_oov");
        COLS_TO_ES.put("detected_lang_1", "mt_lang_detected");
        COLS_TO_ES.put("num_tokens_2", "ptt_num_tokens");
        COLS_TO_ES.put("oov_2", "ptt_oov");
        COLS_TO_ES.put("detected_lang_2", "ptt_lang_detected");
        COLS_TO_ES.put("overlap_tool_0_v_1", "overlap_tk_v_mt");
        COLS_TO_ES.put("overlap_tool_0_v_2", "overlap_tk_v_ptt");
        COLS_TO_ES.put("overlap_tool_1_v_2", "overlap_mt_v_ptt");
    }
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        for (Map.Entry<String, String> e : COLS_TO_ES.entrySet()) {
            String v = row.get(e.getKey());
            if (StringUtils.isBlank(v) || "NaN".equals(v)) {
                continue;
            }
            if (! storedDocument.getFields().containsKey(e.getValue())) {
                storedDocument.addNonBlankField(e.getValue(), row.get(e.getKey()));
            }
        }
    }


}
