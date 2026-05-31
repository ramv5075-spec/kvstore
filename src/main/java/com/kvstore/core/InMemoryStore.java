package com.kvstore.core;

import com.kvstore.wal.WAL;
import com.kvstore.wal.WALEntry;
import com.kvstore.wal.WALReplay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStore implements KVStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryStore.class);

    private final ConcurrentHashMap<String, StoreEntry> map = new ConcurrentHashMap<>();
    private final WAL wal;

    public InMemoryStore(Path walPath) throws IOException {
        recoverFromWAL(walPath);
        this.wal = new WAL(walPath);
        log.info("InMemoryStore ready, {} keys loaded", map.size());
    }

    @Override
    public void put(String key, byte[] value) {
        try {
            wal.append(WALEntry.OpType.PUT, key, value);
            map.put(key, new StoreEntry(value, System.currentTimeMillis(), false));
            log.debug("PUT key={} valueSize={}", key, value.length);
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed for PUT key=" + key, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        StoreEntry entry = map.get(key);
        if (entry == null || entry.tombstone()) {
            log.debug("GET key={} -> MISS", key);
            return Optional.empty();
        }
        log.debug("GET key={} -> HIT", key);
        return Optional.of(entry.value());
    }

    @Override
    public void delete(String key) {
        try {
            wal.append(WALEntry.OpType.DELETE, key, null);
            map.put(key, new StoreEntry(null, System.currentTimeMillis(), true));
            log.debug("DELETE key={}", key);
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed for DELETE key=" + key, e);
        }
    }

    public int size() {
        return (int) map.values().stream()
            .filter(e -> !e.tombstone())
            .count();
    }

    public void applyPut(String key, byte[] value) {
        map.put(key, new StoreEntry(value, System.currentTimeMillis(), false));
    }

    public void applyDelete(String key) {
        map.put(key, new StoreEntry(null, System.currentTimeMillis(), true));
    }

    private void recoverFromWAL(Path walPath) throws IOException {
        List<WALEntry> entries = WALReplay.replay(walPath);
        for (WALEntry entry : entries) {
            if (entry.opType() == WALEntry.OpType.PUT) {
                applyPut(entry.key(), entry.value());
            } else {
                applyDelete(entry.key());
            }
        }
        log.info("Recovery complete: {} WAL entries applied", entries.size());
    }

    @Override
    public void close() {
        try {
            wal.close();
        } catch (IOException e) {
            log.error("Error closing WAL", e);
        }
    }
}
