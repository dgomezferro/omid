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

package com.yahoo.omid.tso;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;
import org.jboss.netty.buffer.ChannelBuffer;

public class RowKey {
    private byte[] table;
    private byte[] row;
    private int hash = 0;

    public RowKey(byte[] table, byte[] row) {
        this.table = table;
        this.row = row;
    }

    public byte[] getTable() {
        return table;
    }

    public byte[] getRow() {
        return row;
    }

    @Override
    public String toString() {
        return "RowKey [table=" + Arrays.toString(table) + ", rowKey=" + Arrays.toString(row) + ", hash=" + hash + "]";
    }

    public static RowKey readObject(ChannelBuffer aInputStream) {
        int hash = aInputStream.readInt();
        short len = aInputStream.readByte();
        byte[] rowId = new byte[len];
        aInputStream.readBytes(rowId, 0, len);
        len = aInputStream.readByte();
        byte[] tableId = new byte[len];
        aInputStream.readBytes(tableId, 0, len);
        RowKey rk = new RowKey(rowId, tableId);
        rk.hash = hash;
        return rk;
    }

    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        hashCode();
        aOutputStream.writeInt(hash);
        aOutputStream.writeByte(row.length);
        aOutputStream.write(row, 0, row.length);
        aOutputStream.writeByte(table.length);
        aOutputStream.write(table, 0, table.length);
    }

    public boolean equals(Object obj) {
        if (obj instanceof RowKey) {
            RowKey other = (RowKey) obj;

            return Bytes.equals(other.row, row) && Bytes.equals(other.table, table);
        }
        return false;
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        // hash is the xor or row and table id
        /*
         * int h = 0; for(int i =0; i < Math.min(8, rowId.length); i++){ h <<= 8; h ^= (int)rowId[i] & 0xFF; } hash = h;
         * h = 0; for(int i =0; i < Math.min(8,tableId.length); i++){ h <<= 8; h ^= (int)tableId[i] & 0xFF; } hash ^= h;
         * return hash;
         */
        byte[] key = Arrays.copyOf(table, table.length + row.length);
        System.arraycopy(row, 0, key, table.length, row.length);
        hash = MurmurHash.getInstance().hash(key, 0, key.length, 0xdeadbeef);
        // return MurmurHash3.MurmurHash3_x64_32(rowId, 0xDEADBEEF);
        // return (31*Arrays.hashCode(tableId)) + Arrays.hashCode(rowId);
        return hash;
    }
}
