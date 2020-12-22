package org.tallison.ingest.mappers;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.tallison.ingest.mappers.QPDFFeatureMapper.joinWith;

public class StatusFeatureMapper implements FeatureMapper {
    Pattern toolPattern = Pattern.compile("^([a-zA-Z]+)_status");
    Map<String, String> labelTools = new HashMap<>();
    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        if (labelTools.size() == 0) {
            loadLabelTools(resultSet);
        }
        List<String> crashes = new ArrayList<>();
        List<String> warns = new ArrayList<>();
        List<String> timeouts = new ArrayList<>();
        List<String> overall = new ArrayList<>();
        for (Map.Entry<String, String> e : labelTools.entrySet()) {
            String status = resultSet.getString(e.getKey());
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

    private void loadLabelTools(ResultSet resultSet) throws SQLException {
        Matcher m = toolPattern.matcher("");
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            String label = resultSet.getMetaData().getColumnLabel(i);
            if (m.reset(label).find()) {
                labelTools.put(label, m.group(1));
            }
        }
    }
}
