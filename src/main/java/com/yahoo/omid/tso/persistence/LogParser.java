package com.yahoo.omid.tso.persistence;

import static com.yahoo.omid.tso.persistence.LoggerProtocol.ABORT;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.COMMIT;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.FULLABORT;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.LARGESTDELETEDTIMESTAMP;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.LOGSTART;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.SNAPSHOT;
import static com.yahoo.omid.tso.persistence.LoggerProtocol.TIMESTAMPORACLE;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogParser {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerProtocol.class);

    LogParser() {
    }
    
    void parse(ByteBuffer bb, LogExecutor le){
        boolean done = !bb.hasRemaining();
        while(!done){
            byte op = bb.get();
            long timestamp, startTimestamp, commitTimestamp;
            if(LOG.isTraceEnabled()){
                LOG.trace("Operation: " + op);
            }
            switch(op){
            case TIMESTAMPORACLE:
                timestamp = bb.getLong();
                le.timestampOracle(timestamp);
                break;
            case COMMIT:
                startTimestamp = bb.getLong();
                commitTimestamp = bb.getLong();
                le.commit(startTimestamp, commitTimestamp);
                break;
            case LARGESTDELETEDTIMESTAMP:
                timestamp = bb.getLong();
                le.largestDeletedTimestamp(timestamp);
                break;
            case ABORT:
                timestamp = bb.getLong();
                le.abort(timestamp);
                break;
            case FULLABORT:
                timestamp = bb.getLong();
                le.fullAbort(timestamp);
                break;
            case LOGSTART:
                le.logStart();
                break;
            case SNAPSHOT:
                int snapshot = (int) bb.getLong();
                le.snapshot(snapshot);
                break;
            }
            if(bb.remaining() == 0) done = true;
        }
    }
}
