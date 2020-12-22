package org.tallison.ingest.mappers;

public class ESUtil {
    public static String stripIllegalUnicode(String s) {
        if (s == null) {
            return "";
        }
        return s.replaceAll("\u0000", "u0000")
                .replaceAll("\u001f", "u001f")
                .replaceAll("\u001e", "u001e")
                ;
    }
}
