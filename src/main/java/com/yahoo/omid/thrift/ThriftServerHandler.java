package com.yahoo.omid.thrift;

import static com.yahoo.omid.thrift.ThriftUtilities.deletesFromHBase;
import static com.yahoo.omid.thrift.ThriftUtilities.deletesFromThrift;
import static com.yahoo.omid.thrift.ThriftUtilities.getFromThrift;
import static com.yahoo.omid.thrift.ThriftUtilities.putsFromThrift;
import static com.yahoo.omid.thrift.ThriftUtilities.resultFromHBase;
import static com.yahoo.omid.thrift.ThriftUtilities.resultsFromHBase;
import static com.yahoo.omid.thrift.ThriftUtilities.scanFromThrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.thrift.TException;

import com.yahoo.omid.client.Cell;
import com.yahoo.omid.client.CommitUnsuccessfulException;
import com.yahoo.omid.client.TransactionException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.thrift.generated.TCell;
import com.yahoo.omid.thrift.generated.TColumnValue;
import com.yahoo.omid.thrift.generated.TDelete;
import com.yahoo.omid.thrift.generated.TGet;
import com.yahoo.omid.thrift.generated.TIOError;
import com.yahoo.omid.thrift.generated.TIllegalArgument;
import com.yahoo.omid.thrift.generated.TOmidService;
import com.yahoo.omid.thrift.generated.TPut;
import com.yahoo.omid.thrift.generated.TResult;
import com.yahoo.omid.thrift.generated.TResultDelete;
import com.yahoo.omid.thrift.generated.TResultExists;
import com.yahoo.omid.thrift.generated.TResultGet;
import com.yahoo.omid.thrift.generated.TResultScanner;
import com.yahoo.omid.thrift.generated.TScan;
import com.yahoo.omid.thrift.generated.TTransaction;

public class ThriftServerHandler implements TOmidService.Iface {

    // TODO: Size of pool configuraple
    private final Configuration conf;
    private static final Log LOG = LogFactory.getLog(ThriftServerHandler.class);
    private final TransactionManager transactionManager;

    private final AtomicInteger nextScannerId = new AtomicInteger(0);
    private final Map<Integer, ResultScanner> scannerMap = new ConcurrentHashMap<Integer, ResultScanner>();

    public static TOmidService.Iface newInstance(Configuration conf) throws TIOError {
        try {
            return new ThriftServerHandler(conf);
        } catch (IOException e) {
            throw getTIOError(e);
        }
    }

    ThriftServerHandler(Configuration conf) throws IOException {
        this.conf = conf;
        this.transactionManager = new TransactionManager(conf);
    }

    private TransactionalTable getTable(byte[] tableName) throws IOException {
        return new TransactionalTable(conf, tableName);
    }

    private void closeTable(HTableInterface table) throws TIOError {
        if (table == null) {
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            throw getTIOError(e);
        }
    }

    private static TIOError getTIOError(IOException e) {
        TIOError err = new TIOError();
        err.setMessage(e.getMessage());
        return err;
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal HashMap.
     * 
     * @param scanner
     *            to add
     * @return Id for this Scanner
     */
    private int addScanner(ResultScanner scanner) {
        int id = nextScannerId.getAndIncrement();
        scannerMap.put(id, scanner);
        return id;
    }

    /**
     * Returns the Scanner associated with the specified Id.
     * 
     * @param id
     *            of the Scanner to get
     * @return a Scanner, or null if the Id is invalid
     */
    private ResultScanner getScanner(int id) {
        return scannerMap.get(id);
    }

    private TransactionState transactionFromThrift(TTransaction transaction) {
        List<Cell> cells = new ArrayList<Cell>();
        for (TCell tcell : transaction.getCells()) {
            Cell cell = new Cell(tcell.getTable(), tcell.getRow(), tcell.getFamily(), tcell.getQualifier());
            cells.add(cell);
        }
        return transactionManager.createTransactionState(transaction.getId(), cells);
    }

    private TTransaction transactionFromOmid(TransactionState ts) {
        TTransaction transaction = new TTransaction(ts.getStartTimestamp());
        List<TCell> tcells = new ArrayList<TCell>();

        for (Cell cell : ts.getCells()) {
            ByteBuffer table = ByteBuffer.wrap(cell.getTable());
            ByteBuffer row = ByteBuffer.wrap(cell.getRowKey());
            ByteBuffer family = ByteBuffer.wrap(cell.getColumnFamily());
            ByteBuffer qualifier = ByteBuffer.wrap(cell.getColumnQualifier());
            TCell tcell = new TCell(table, row, family, qualifier);
            tcells.add(tcell);
        }

        transaction.setCells(tcells);
        return transaction;
    }

    /**
     * Removes the scanner associated with the specified ID from the internal HashMap.
     * 
     * @param id
     *            of the Scanner to remove
     * @return the removed Scanner, or <code>null</code> if the Id is invalid
     */
    protected ResultScanner removeScanner(int id) {
        return scannerMap.remove(id);
    }

    @Override
    public TResultExists exists(ByteBuffer table, TGet get, TTransaction transaction) throws TIOError, TException {
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            TResultGet results = getMultiple(table, Arrays.asList(get), transaction);
            boolean exists = false;

            for (TResult result : results.getResults()) {
                if (result.getRow() != null) {
                    exists = true;
                    break;
                }
            }
            return new TResultExists(exists, transaction);
        } catch (IOException e) {
            throw getTIOError(e);
        } finally {
            closeTable(htable);
        }
    }

    public TResult get(ByteBuffer table, TGet get, TTransaction transaction) throws TIOError, TException {
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            return resultFromHBase(htable.get(transactionFromThrift(transaction), getFromThrift(get)));
        } catch (IOException e) {
            throw getTIOError(e);
        } finally {
            closeTable(htable);
        }
    }

    @Override
    public TResultGet getMultiple(ByteBuffer table, List<TGet> gets, TTransaction transaction) throws TIOError, TException {
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            List<TResult> results = new ArrayList<TResult>();
            for (TGet get : gets) {
                results.add(get(table, get, transaction));
            }
            return new TResultGet(results, transaction);
        } catch (IOException e) {
            throw getTIOError(e);
        } finally {
            closeTable(htable);
        }
    }

    @Override
    public TTransaction putMultiple(ByteBuffer table, List<TPut> puts, TTransaction transaction) throws TIOError, TException {
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            TransactionState ts = transactionFromThrift(transaction);
            for (Put put : putsFromThrift(puts)) {
                htable.put(ts, put);
            }
            return transactionFromOmid(ts);
        } catch (IOException e) {
            throw getTIOError(e);
        } finally {
            closeTable(htable);
        }
    }

    @Override
    public TResultDelete deleteMultiple(ByteBuffer table, List<TDelete> deletes, TTransaction transaction) throws TIOError, TException {
        List<Delete> tempDeletes = deletesFromThrift(deletes);
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            htable.delete(tempDeletes);
        } catch (IOException e) {
            throw getTIOError(e);
        } finally {
            closeTable(htable);
        }
        return new TResultDelete(deletesFromHBase(tempDeletes), transaction);
    }

    @Override
    public TResultScanner openScanner(ByteBuffer table, TScan scan, TTransaction transaction) throws TIOError, TException {
        ResultScanner resultScanner = null;
        TransactionalTable htable = null;
        try {
            htable = getTable(table.array());
            resultScanner = htable.getScanner(scanFromThrift(scan));
        } catch (IOException e) {
            closeTable(htable);
            throw getTIOError(e);
        }
        return new TResultScanner(addScanner(resultScanner), transaction);
    }

    @Override
    public List<TResult> getScannerRows(int scannerId, int numRows) throws TIOError, TIllegalArgument, TException {
        ResultScanner scanner = getScanner(scannerId);
        if (scanner == null) {
            TIllegalArgument ex = new TIllegalArgument();
            ex.setMessage("Invalid scanner Id");
            throw ex;
        }

        try {
            return resultsFromHBase(scanner.next(numRows));
        } catch (IOException e) {
            throw getTIOError(e);
        }
    }

    @Override
    public void closeScanner(int scannerId) throws TIOError, TIllegalArgument, TException {
        if (removeScanner(scannerId) == null) {
            TIllegalArgument ex = new TIllegalArgument();
            ex.setMessage("Invalid scanner Id");
            throw ex;
        }
    }

    @Override
    public TTransaction startTransaction() throws TIOError, TException {
        try {
            TransactionState ts = transactionManager.beginTransaction();
            return new TTransaction(ts.getStartTimestamp());
        } catch (TransactionException e) {
            throw new TException(e);
        }
    }

    @Override
    public boolean commitTransaction(TTransaction transaction) throws TIOError, TException {
        try {
            TransactionState ts = transactionManager.beginTransaction();
            transactionManager.tryCommit(ts);
            return true;
        } catch (CommitUnsuccessfulException e) {
            return false;
        } catch (TransactionException e) {
            throw new TException(e);
        }
    }

}
