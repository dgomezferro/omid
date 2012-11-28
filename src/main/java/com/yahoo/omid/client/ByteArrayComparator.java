package com.yahoo.omid.client;

import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] a, byte[] b) {
        return Bytes.compareTo(a, b);
    }
}
