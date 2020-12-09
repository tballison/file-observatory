package org.tallison.batchlite.writer;

public class WriterResult {

    private final int recordsWritten;
    public WriterResult(int recordsWritten) {
        this.recordsWritten = recordsWritten;
    }
    public int getRecordsWritten() {
        return recordsWritten;
    }
}
