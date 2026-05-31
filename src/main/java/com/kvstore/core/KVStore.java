package com.kvstore.core;

import java.util.Optional;

public interface KVStore {
    void put(String key, byte[] value);
    Optional<byte[]> get(String key);
    void delete(String key);
    void close();
}
