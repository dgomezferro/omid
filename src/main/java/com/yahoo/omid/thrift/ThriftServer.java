package com.yahoo.omid.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.thrift.TException;

import com.yahoo.omid.thrift.generated.AlreadyExists;
import com.yahoo.omid.thrift.generated.BatchMutation;
import com.yahoo.omid.thrift.generated.ColumnDescriptor;
import com.yahoo.omid.thrift.generated.IOError;
import com.yahoo.omid.thrift.generated.IllegalArgument;
import com.yahoo.omid.thrift.generated.Mutation;
import com.yahoo.omid.thrift.generated.Omid;
import com.yahoo.omid.thrift.generated.TCell;
import com.yahoo.omid.thrift.generated.TRegionInfo;
import com.yahoo.omid.thrift.generated.TRowResult;
import com.yahoo.omid.thrift.generated.TScan;

public class ThriftServer implements Omid.Iface {
    
    HBaseAdmin hbaseAdmin;

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError, TException {
        try {
            hbaseAdmin.enableTable(tableName.array());
        } catch (IOException e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError, TException {
        try {
            hbaseAdmin.disableTable(tableName.array());
        } catch (IOException e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError, TException {
        try {
            return hbaseAdmin.isTableEnabled(tableName.array());
        } catch (IOException e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public void compact(ByteBuffer tableNameOrRegionName) throws IOError, TException {
        try {
            hbaseAdmin.compact(tableNameOrRegionName.array());
        } catch (Exception e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError, TException {
        try {
            hbaseAdmin.majorCompact(tableNameOrRegionName.array());
        } catch (Exception e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError, TException {
        return null;
    }

    @Override
    public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(ByteBuffer tableName) throws IOError, TException {
        return null;
    }

    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName) throws IOError, TException {
        return null;
    }

    @Override
    public void createTable(ByteBuffer tableName, List<ColumnDescriptor> columnFamilies) throws IOError,
            IllegalArgument, AlreadyExists, TException {
        try {
            hbaseAdmin.enableTable(tableName.array());
        } catch (IOException e) {
            throw new IOError(e.getLocalizedMessage());
        }
        
    }

    @Override
    public void deleteTable(ByteBuffer tableName) throws IOError, TException {
        try {
            hbaseAdmin.enableTable(tableName.array());
        } catch (IOException e) {
            throw new IOError(e.getLocalizedMessage());
        }
    }

    @Override
    public List<TCell> get(ByteBuffer tableName, ByteBuffer row, ByteBuffer column, long transactionId) throws IOError,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row, long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName, ByteBuffer row, List<ByteBuffer> columns,
            long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName, List<ByteBuffer> rows, long transactionId) throws IOError,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName, List<ByteBuffer> rows, List<ByteBuffer> columns,
            long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row, List<Mutation> mutations, long transactionId)
            throws IOError, IllegalArgument, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches, long transactionId) throws IOError,
            IllegalArgument, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteAll(ByteBuffer tableName, ByteBuffer row, ByteBuffer column, long transactionId) throws IOError,
            TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteAllRow(ByteBuffer tableName, ByteBuffer row, long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int scannerOpenWithScan(ByteBuffer tableName, TScan scan, long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow, List<ByteBuffer> columns, long transactionId)
            throws IOError, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow, ByteBuffer stopRow,
            List<ByteBuffer> columns, long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName, ByteBuffer startAndPrefix, List<ByteBuffer> columns,
            long transactionId) throws IOError, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<TRowResult> scannerGet(int id) throws IOError, IllegalArgument, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TRowResult> scannerGetList(int id, int nbRows) throws IOError, IllegalArgument, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void scannerClose(int id) throws IOError, IllegalArgument, TException {
        // TODO Auto-generated method stub
        
    }

}
