package com.kvstore.core;

public record StoreEntry(
    byte[] value,
    long timestamp,
    boolean tombstone
) {}
