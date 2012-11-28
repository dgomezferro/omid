package com.yahoo.omid.client;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Represents a Cell, composed of Table, Row Key, Column Family and Column Qualifier. 
 *
 */
public class Cell implements Comparable<Cell> {
    private byte[] table;
    private byte[] rowKey;
    private byte[] columnFamily;
    private byte[] columnQualifier;
    
    private static Comparator<byte[]> COMPARATOR = new ByteArrayComparator();

    public Cell(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] columnQualifier) {
        super();
        this.table = table;
        this.rowKey = rowKey;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
    }

    public byte[] getTable() {
        return table;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(columnFamily);
        result = prime * result + Arrays.hashCode(columnQualifier);
        result = prime * result + Arrays.hashCode(rowKey);
        result = prime * result + Arrays.hashCode(table);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Cell other = (Cell) obj;
        if (!Arrays.equals(columnFamily, other.columnFamily))
            return false;
        if (!Arrays.equals(columnQualifier, other.columnQualifier))
            return false;
        if (!Arrays.equals(rowKey, other.rowKey))
            return false;
        if (!Arrays.equals(table, other.table))
            return false;
        return true;
    }

    @Override
    public int compareTo(Cell o) {
        int comparison;
        if ((comparison = COMPARATOR.compare(table, o.table)) != 0)
            return comparison;
        if ((comparison = COMPARATOR.compare(rowKey, o.rowKey)) != 0)
            return comparison;
        if ((comparison = COMPARATOR.compare(columnFamily, o.columnFamily)) != 0)
            return comparison;
        comparison = COMPARATOR.compare(columnQualifier, o.columnQualifier);
        return comparison;
    }

    @Override
    public String toString() {
        return "Cell [table=" + Arrays.toString(table) + ", rowKey=" + Arrays.toString(rowKey) + ", columnFamily="
                + Arrays.toString(columnFamily) + ", columnQualifier=" + Arrays.toString(columnQualifier) + "]";
    }
}
