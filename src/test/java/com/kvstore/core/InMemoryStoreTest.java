package com.kvstore.core;

import org.junit.jupiter.api.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    private static final Path WAL_PATH = Path.of("data/test.wal");

    @BeforeEach
    void cleanup() throws Exception {
        Files.deleteIfExists(WAL_PATH);
        WAL_PATH.getParent().toFile().mkdirs();
    }

    @Test
    void basicPutAndGet() throws Exception {
        InMemoryStore store = new InMemoryStore(WAL_PATH);
        store.put("k1", "hello".getBytes());
        assertTrue(store.get("k1").isPresent());
        assertEquals("hello", new String(store.get("k1").get()));
        store.close();
    }

    @Test
    void deleteShouldReturnEmpty() throws Exception {
        InMemoryStore store = new InMemoryStore(WAL_PATH);
        store.put("k1", "hello".getBytes());
        store.delete("k1");
        assertTrue(store.get("k1").isEmpty());
        store.close();
    }

    @Test
    void crashRecoveryShouldRestoreState() throws Exception {
        InMemoryStore store = new InMemoryStore(WAL_PATH);
        store.put("name", "ram".getBytes());
        store.put("goal", "faang".getBytes());
        store.delete("goal");
        store.close();

        InMemoryStore recovered = new InMemoryStore(WAL_PATH);
        assertEquals("ram", new String(recovered.get("name").get()));
        assertTrue(recovered.get("goal").isEmpty());
        assertEquals(1, recovered.size());
        recovered.close();
    }
}
