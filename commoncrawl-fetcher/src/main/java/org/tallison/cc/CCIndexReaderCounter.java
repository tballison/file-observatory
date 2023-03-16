package org.tallison.cc;

import java.util.concurrent.atomic.AtomicLong;

public class CCIndexReaderCounter {
    AtomicLong recordsRead = new AtomicLong(0);
    AtomicLong filesExtracted = new AtomicLong(0);
    AtomicLong truncatedWritten = new AtomicLong(0);

    public AtomicLong getRecordsRead() {
        return recordsRead;
    }

    public AtomicLong getFilesExtracted() {
        return filesExtracted;
    }

    public AtomicLong getTruncatedWritten() {
        return truncatedWritten;
    }

    @Override
    public String toString() {
        return "CCIndexReaderCounter{" +
                "recordsRead=" + recordsRead +
                ", filesExtracted=" + filesExtracted +
                ", truncatedWritten=" + truncatedWritten +
                '}';
    }
}
