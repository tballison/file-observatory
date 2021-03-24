package org.tallison.ingest.mappers;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.tallison.ingest.mappers.QPDFFeatureMapper.joinWith;

public class StatusFeatureMapper implements FeatureMapper {
    Pattern toolPattern = Pattern.compile("^([a-zA-Z]+)_status");
    Map<String, String> labelTools = new ConcurrentHashMap<>();

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        if (labelTools.size() == 0) {
            loadLabelTools(row);
        }
        List<String> crashes = new ArrayList<>();
        List<String> warns = new ArrayList<>();
        List<String> timeouts = new ArrayList<>();
        List<String> overall = new ArrayList<>();
        for (Map.Entry<String, String> e : labelTools.entrySet()) {
            String status = row.get(e.getKey());
            if ("crash".equals(status) || "timeout".equals(status)) {
                crashes.add(e.getValue());
            } else if ("warn".equals(status)) {
                warns.add(e.getValue());
            }
            if ("timeout".equals(status)) {
                timeouts.add(e.getValue());
            }
            overall.add(e.getValue()+"_"+status);
            //storedDocument.addNonBlankField(e.getKey(), status);
        }
        Collections.sort(crashes);
        Collections.sort(warns);
        Collections.sort(timeouts);
        Collections.sort(overall);
        storedDocument.addNonBlankField("tools_status_timeout", joinWith(" ", timeouts));
        storedDocument.addNonBlankField("tools_status_fail", joinWith(" ", crashes));
        storedDocument.addNonBlankField("tools_status_warn", joinWith(" ", warns));
        storedDocument.addNonBlankField("tools_status", joinWith(" ", overall));
    }

    private synchronized void loadLabelTools(Map<String, String> row) throws SQLException {
        //now check again inside the synchronized block
        if (labelTools.size() > 0) {
            return;
        }
        Matcher m = toolPattern.matcher("");
        for (String label : row.keySet()) {
            if (m.reset(label).find()) {
                labelTools.put(label, m.group(1));
            }
        }
    }
}
