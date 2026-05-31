package com.kvstore;

import com.kvstore.core.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Path walPath = Path.of("data/kvstore.wal");
        walPath.getParent().toFile().mkdirs();

        InMemoryStore store = new InMemoryStore(walPath);

        log.info("--- putting keys ---");
        store.put("name", "ram".getBytes());
        store.put("role", "engineer".getBytes());
        store.put("goal", "faang".getBytes());

        log.info("name  = {}", new String(store.get("name").orElse("NOT FOUND".getBytes())));
        log.info("role  = {}", new String(store.get("role").orElse("NOT FOUND".getBytes())));
        log.info("goal  = {}", new String(store.get("goal").orElse("NOT FOUND".getBytes())));

        log.info("--- deleting role ---");
        store.delete("role");
        log.info("role  = {}", new String(store.get("role").orElse("NOT FOUND".getBytes())));

        log.info("total keys = {}", store.size());
        store.close();

        log.info("--- restarting store (crash recovery test) ---");
        InMemoryStore recovered = new InMemoryStore(walPath);
        log.info("name  = {}", new String(recovered.get("name").orElse("NOT FOUND".getBytes())));
        log.info("role  = {}", new String(recovered.get("role").orElse("NOT FOUND".getBytes())));
        log.info("goal  = {}", new String(recovered.get("goal").orElse("NOT FOUND".getBytes())));
        log.info("total keys after recovery = {}", recovered.size());
        recovered.close();
    }
}
