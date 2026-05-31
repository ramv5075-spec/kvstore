package com.kvstore.wal;

public record WALEntry(
    long lsn,
    OpType opType,
    String key,
    byte[] value,
    long timestamp
) {
    public enum OpType { PUT, DELETE }
}
