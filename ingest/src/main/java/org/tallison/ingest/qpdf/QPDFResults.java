package org.tallison.ingest.qpdf;

import java.util.HashSet;
import java.util.Set;

public class QPDFResults {

    public Set<String> keys = new HashSet<>();
    public Set<String> parentAndKeys = new HashSet<>();
    public Set<String> keyValues = new HashSet<>();
    public Set<String> filters = new HashSet<>();
    public int maxFilterCount = 0;

    @Override
    public String toString() {
        return "QPDFResults{" +
                "keys=" + keys +
                ", parentAndKeys=" + parentAndKeys +
                ", keyValues=" + keyValues +
                ", filters=" + filters +
                ", maxFilterCount=" + maxFilterCount +
                '}';
    }
}
