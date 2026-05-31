package com.kvstore;

import com.kvstore.cluster.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("=== starting distributed KV store - phase 2 ===");

        Node node1 = new Node("node-1", "localhost", 7001);
        Node node2 = new Node("node-2", "localhost", 7002);
        Node node3 = new Node("node-3", "localhost", 7003);

        ClusterManager cluster = new ClusterManager();
        cluster.addNode(node1);
        cluster.addNode(node2);
        cluster.addNode(node3);

        log.info("--- writing 6 keys ---");
        cluster.put("name",    "ram".getBytes());
        cluster.put("role",    "engineer".getBytes());
        cluster.put("goal",    "faang".getBytes());
        cluster.put("lang",    "java".getBytes());
        cluster.put("phase",   "two".getBytes());
        cluster.put("project", "kvstore".getBytes());

        log.info("--- reading keys ---");
        List<String> keys = List.of("name","role","goal","lang","phase","project");
        for (String key : keys) {
            Node primary = cluster.getPrimaryNode(key);
            String value = new String(cluster.get(key).orElse("NOT FOUND".getBytes()));
            log.info("key={} value={} node={}", key, value, primary.getNodeId());
        }

        log.info("--- key distribution across nodes ---");
        List<String> allKeys = List.of("name","role","goal","lang","phase","project",
            "user1","user2","user3","user4","user5","user6","user7","user8","user9","user10");
        Map<String, Integer> dist = cluster.getRing().getKeyDistribution(allKeys);
        dist.forEach((nodeId, count) ->
            log.info("node={} keys={}", nodeId, count));

        log.info("--- simulating node-2 failure ---");
        cluster.removeNode(node2);
        for (String key : keys) {
            String value = new String(cluster.get(key).orElse("NOT FOUND".getBytes()));
            log.info("key={} value={}", key, value);
        }

        log.info("=== phase 2 complete ===");
    }
}
