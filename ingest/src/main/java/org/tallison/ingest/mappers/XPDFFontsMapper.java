package org.tallison.ingest.mappers;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

public class XPDFFontsMapper implements FeatureMapper {
    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument)
            throws SQLException {
        String stdout = row.get("xpdffonts_stdout");
        String stderr = row.get("xpdffonts_stderr");
        storedDocument.addNonBlankField("xpf_stderr", ESUtil.stripIllegalUnicode(stderr));
        storedDocument.addNonBlankField("xpf_stdout", ESUtil.stripIllegalUnicode(stdout));
        if (stdout == null) {
            return;
        }
        String[] lines = stdout.split("[\r\n]+");
        if (lines.length < 2) {
            return;
        }
        int[] colLengths = new int[7];
        String[] dashes = lines[1].split(" ");
        int len = 0;
        for (int i = 0; i < dashes.length; i++) {
            colLengths[i] = dashes[i].trim().length();
            len += colLengths[i];
        }
        Set<String> names = new HashSet<>();
        Set<String> types = new HashSet<>();
        Set<String> embedded = new HashSet<>();
        Set<String> unicode = new HashSet<>();
        for (int i = 2; i < lines.length; i++) {
            String line = lines[i];
            if (line.length() < len) {
                continue;
            }
            int col = 0;
            int start = 0;
            String name = line.substring(start, start+colLengths[col]).trim();
            names.add(name);
            start += colLengths[col]+1;
            String type = line.substring(start, start+colLengths[++col]).trim();
            types.add(type);
            //xpdf doesn't have encodings
//            start += colLengths[col]+1;
  //          String encoding = line.substring(start, start+colLengths[++col]).trim();
    //        encodings.add(encoding);
            start += colLengths[col]+1;
            String emb = line.substring(start, start+colLengths[++col]).trim();
            embedded.add(emb);
            start += colLengths[col]+1;
            String sub = line.substring(start, start+colLengths[++col]).trim();
            start += colLengths[col]+1;
            String uni = line.substring(start, start+colLengths[++col]).trim();
            unicode.add(uni);
        }
        storedDocument.addNonBlankField("xpf_font_names", toList(names));
        storedDocument.addNonBlankField("xpf_font_types", toList(types));
        storedDocument.addNonBlankField("xpf_fonts_embedded", multiBinary(embedded));
        storedDocument.addNonBlankField("xpf_fonts_uni", multiBinary(unicode));
    }

    private String multiBinary(Set<String> set) {
        StringBuilder sb = new StringBuilder();
        if (set.contains("no")) {
            sb.append("no");
        }
        if (set.contains("yes")) {
            if (sb.length() > 0) {
                sb.append("-");
            }
            sb.append("yes");
        }
        return sb.toString();
    }

    private List<String> toList(Set<String> set) {
        List<String> list = new ArrayList<>();
        for (String s : set) {
            list.add(ESUtil.stripIllegalUnicode(s));
        }
        Collections.sort(list);
        return list;
    }
}
