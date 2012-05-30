package com.yahoo.omid.tso.persistence;

public interface LogExecutor {

    void snapshot(int snapshot);

    void logStart();

    void fullAbort(long timestamp);

    void abort(long timestamp);

    void largestDeletedTimestamp(long timestamp);

    void commit(long startTimestamp, long commitTimestamp);

    void timestampOracle(long timestamp);

}
