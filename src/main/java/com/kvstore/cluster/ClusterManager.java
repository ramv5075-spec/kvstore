package com.kvstore.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);
    private static final int REPLICA_COUNT = 2;

    private final ConsistentHashRing ring = new ConsistentHashRing();

    public void addNode(Node node) {
        ring.addNode(node);
        log.info("Cluster now has {} nodes", ring.getTotalNodes());
    }

    public void removeNode(Node node) {
        node.deactivate();
        ring.removeNode(node);
        log.info("Node {} removed, cluster has {} nodes", node.getNodeId(), ring.getTotalNodes());
    }

    public void put(String key, byte[] value) {
        List<Node> replicas = ring.getReplicaNodes(key, REPLICA_COUNT);
        for (Node node : replicas) {
            node.getStore().put(key, value);
            log.debug("PUT key={} on node={}", key, node.getNodeId());
        }
    }

    public Optional<byte[]> get(String key) {
        Node primary = ring.getNodeForKey(key);
        if (primary.isActive()) {
            return primary.getStore().get(key);
        }
        List<Node> replicas = ring.getReplicaNodes(key, REPLICA_COUNT);
        for (Node replica : replicas) {
            Optional<byte[]> value = replica.getStore().get(key);
            if (value.isPresent()) return value;
        }
        return Optional.empty();
    }

    public void delete(String key) {
        List<Node> replicas = ring.getReplicaNodes(key, REPLICA_COUNT);
        for (Node node : replicas) {
            node.getStore().delete(key);
            log.debug("DELETE key={} on node={}", key, node.getNodeId());
        }
    }

    public Node getPrimaryNode(String key) {
        return ring.getNodeForKey(key);
    }

    public ConsistentHashRing getRing() {
        return ring;
    }
}
