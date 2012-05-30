/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso.persistence;

import java.nio.ByteBuffer;

import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TimestampOracle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoggerProtocol extends TSOState{
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(LoggerProtocol.class);
    
    /*
     * Protocol flags. Used to identify fields of the logger records.
     */
    public final static byte TIMESTAMPORACLE = (byte) -1;
    public final static byte COMMIT = (byte) -2;
    public final static byte LARGESTDELETEDTIMESTAMP = (byte) -3;
    public final static byte ABORT = (byte) -4;
    public final static byte FULLABORT = (byte) -5;
    public final static byte LOGSTART = (byte) -6;
    public final static byte SNAPSHOT = (byte) -7;

    private boolean commits;
    private boolean oracle;
    private boolean aborts;
    private boolean consumed;
    private boolean hasSnapshot;

    private LogParser parser = new LogParser();

    private LogExecutor scanner = new LogExecutor() {
        private long loggedLargestDeletedTimestamp;
        private int snapshot = -1;

        @Override
        public void timestampOracle(long timestamp) {
            oracle = true;
        }

        @Override
        public void snapshot(int snapshot) {
            if (snapshot > this.snapshot) {
                this.snapshot = snapshot;
                hasSnapshot = true;
            }
            if (hasSnapshot && snapshot < snapshot) {
                aborts = true;
            }
        }

        @Override
        public void logStart() {
            consumed = true;
        }

        @Override
        public void largestDeletedTimestamp(long timestamp) {
            loggedLargestDeletedTimestamp = Math.max(timestamp, loggedLargestDeletedTimestamp);
        }

        @Override
        public void commit(long startTimestamp, long commitTimestamp) {
            if (commitTimestamp < loggedLargestDeletedTimestamp) {
                commits = true;
            }
        }

        @Override
        public void fullAbort(long timestamp) {
        }

        @Override
        public void abort(long timestamp) {
        }
    };

    private LogExecutor executor = new LogExecutor() {

        @Override
        public void timestampOracle(long timestamp) {
            getSO().initialize(timestamp);
            initialize();
        }

        @Override
        public void largestDeletedTimestamp(long timestamp) {
            processLargestDeletedTimestamp(timestamp);
        }

        @Override
        public void fullAbort(long timestamp) {
            processFullAbort(timestamp);
        }

        @Override
        public void commit(long startTimestamp, long commitTimestamp) {
            processCommit(startTimestamp, commitTimestamp);
        }

        @Override
        public void abort(long timestamp) {
            processAbort(timestamp);
        }

        @Override
        public void snapshot(int snapshot) {
        }

        @Override
        public void logStart() {
        }
    };
    
    /**
     * Logger protocol constructor. Currently it only constructs the
     * super class, TSOState.
     * 
     * @param logger
     * @param largestDeletedTimestamp
     */
    LoggerProtocol(TimestampOracle timestampOracle){
        super(timestampOracle);
    }

    /**
     * Execute a logged entry (several logged ops)
     * @param bb Serialized operations
     */
    void execute(ByteBuffer bb){
        parser.parse(bb, executor);
    }

    /**
     * Read a logged entry scanning for all information needed to reconstruct the state
     * @param bb Serialized operations
     */
    void scan(ByteBuffer bb){
        parser.parse(bb, scanner);
    }

    /**
     * Checks whether all the required information has been read
     * from the log.
     * 
     * @return true if the scanning has finished
     */
    boolean finishedScan() {
        return (oracle && commits && aborts) || consumed;        
    }
    
    /**
     * Returns a TSOState object based on this object.
     * 
     * @return
     */
    TSOState getState(){
        return ((TSOState) this);
    }

}
