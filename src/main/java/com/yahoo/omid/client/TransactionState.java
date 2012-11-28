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

package com.yahoo.omid.client;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import com.yahoo.omid.tso.RowKey;

public class TransactionState {
    private long startTimestamp;
    private long commitTimestamp;
    private Set<Cell> cells;
    
    private static Comparator<byte[]> COMPARATOR = new ByteArrayComparator();

    public TSOClient tsoclient;

    TransactionState() {
        startTimestamp = 0;
        commitTimestamp = 0;
        this.cells = new HashSet<Cell>();
    }

    TransactionState(long startTimestamp, TSOClient client) {
        this();
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = 0;
        this.tsoclient = client;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public Set<RowKey> getRowKeys() {
        Set<RowKey> rows = new HashSet<RowKey>();
        for (Cell cell : cells) {
            rows.add(new RowKey(cell.getTable(), cell.getRowKey()));
        }
        return rows;
    }
    
    public Set<Cell> getCells() {
        return cells;
    }

    void addCell(Cell cell) {
        cells.add(cell);
    }

    @Override
    public String toString() {
        return "TransactionState [startTimestamp=" + startTimestamp + ", commitTimestamp=" + commitTimestamp
                + ", cells=" + cells + "]";
    }
}
