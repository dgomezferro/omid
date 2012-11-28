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

package com.yahoo.omid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.thrift.ThriftServerHandler;
import com.yahoo.omid.thrift.generated.TColumnValue;
import com.yahoo.omid.thrift.generated.TDelete;
import com.yahoo.omid.thrift.generated.TGet;
import com.yahoo.omid.thrift.generated.TOmidService.Iface;
import com.yahoo.omid.thrift.generated.TPut;
import com.yahoo.omid.thrift.generated.TResult;
import com.yahoo.omid.thrift.generated.TResultGet;
import com.yahoo.omid.thrift.generated.TResultScanner;
import com.yahoo.omid.thrift.generated.TScan;
import com.yahoo.omid.thrift.generated.TTransaction;

public class TestThrift extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestThrift.class);
    
    private static ByteBuffer table = ByteBuffer.wrap(Bytes.toBytes(TEST_TABLE));

    @Test
    public void runTestSimple() throws Exception {
        Iface th = ThriftServerHandler.newInstance(conf);

        TTransaction t1 = th.startTransaction();
        LOG.info("Transaction created " + t1);

        ByteBuffer row = ByteBuffer.wrap(Bytes.toBytes("test-simple"));
        ByteBuffer fam = ByteBuffer.wrap(Bytes.toBytes(TEST_FAMILY));
        ByteBuffer col = ByteBuffer.wrap(Bytes.toBytes("testdata"));
        ByteBuffer data1 = ByteBuffer.wrap(Bytes.toBytes("testWrite-1"));
        ByteBuffer data2 = ByteBuffer.wrap(Bytes.toBytes("testWrite-22"));

        TPut p = new TPut(row, Arrays.asList(new TColumnValue(fam, col, data1)));
        t1 = th.putMultiple(table, Arrays.asList(p), t1);
        th.commitTransaction(t1);

        TTransaction tread = th.startTransaction();
        TTransaction t2 = th.startTransaction();
        p = new TPut(row, Arrays.asList(new TColumnValue(fam, col, data2)));
        t2 = th.putMultiple(table, Arrays.asList(p), t2);
        th.commitTransaction(t2);

        TGet g = new TGet(row);
        TResultGet r = th.getMultiple(table, Arrays.asList(g), tread);
        assertEquals("Unexpected value for SI read", data1,
                ByteBuffer.wrap(r.getResults().get(0).getColumnValues().get(0).getValue()));
    }

    @Test
    public void runTestDeleteRow() throws Exception {
        Iface th = ThriftServerHandler.newInstance(conf);

        TTransaction t1 = th.startTransaction();
        LOG.info("Transaction created " + t1);

        int rowcount = 10;
        int count = 0;

        ByteBuffer fam = ByteBuffer.wrap(Bytes.toBytes(TEST_FAMILY));
        ByteBuffer col = ByteBuffer.wrap(Bytes.toBytes("testdata"));
        ByteBuffer data1 = ByteBuffer.wrap(Bytes.toBytes("testWrite-1"));

        ByteBuffer modrow = ByteBuffer.wrap(Bytes.toBytes("test-del" + 3));
        for (int i = 0; i < rowcount; i++) {
            ByteBuffer row = ByteBuffer.wrap(Bytes.toBytes("test-del" + i));

            TPut p = new TPut(row, Arrays.asList(new TColumnValue(fam, col, data1)));
            t1 = th.putMultiple(table, Arrays.asList(p), t1);
        }
        th.commitTransaction(t1);

        TTransaction t2 = th.startTransaction();
        TDelete d = new TDelete(modrow);
        t2 = th.deleteMultiple(table, Arrays.asList(d), t2);

        TTransaction tscan = th.startTransaction();
        TResultScanner rs = th.openScanner(table, new TScan(), tscan);
        tscan = rs.getTransaction();
        int scannerId = rs.getScannerId();
        List<TResult> results = th.getScannerRows(scannerId, 100);
        count = 0;
        for (TResult r : results) {
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
        }
        assertTrue("Expected " + rowcount + " rows but " + count + " found", count == rowcount);
        th.closeScanner(scannerId);

        boolean committed = th.commitTransaction(t2);
        assertTrue("Delete failed", committed);

        tscan = th.startTransaction();
        rs = th.openScanner(table, new TScan(), tscan);
        tscan = rs.getTransaction();
        scannerId = rs.getScannerId();
        results = th.getScannerRows(scannerId, 100);
        count = 0;
        for (TResult r : results) {
            System.out.println(r);
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
        }
        th.closeScanner(scannerId);
        assertTrue("Expected " + (rowcount - 1) + " rows but " + count + " found", count == (rowcount - 1));
    }
}
