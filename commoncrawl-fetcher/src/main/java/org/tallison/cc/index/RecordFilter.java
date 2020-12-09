package org.tallison.cc.index;

public interface RecordFilter {

    boolean accept(CCIndexRecord record);
}
