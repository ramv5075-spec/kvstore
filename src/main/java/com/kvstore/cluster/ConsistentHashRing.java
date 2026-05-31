package com.kvstore.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashRing {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRing.class);
    private static final int VIRTUAL_NODES = 150;

    private final TreeMap<Long, Node> ring = new TreeMap<>();
    private final Map<String, Node> nodes = new HashMap<>();

    public void addNode(Node node) {
        nodes.put(node.getNodeId(), node);
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = hash(node.getNodeId() + "-vnode-" + i);
            ring.put(hash, node);
        }
        log.info("Added {} to ring with {} virtual nodes", node.getNodeId(), VIRTUAL_NODES);
    }

    public void removeNode(Node node) {
        nodes.remove(node.getNodeId());
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = hash(node.getNodeId() + "-vnode-" + i);
            ring.remove(hash);
        }
        log.info("Removed {} from ring", node.getNodeId());
    }

    public Node getNodeForKey(String key) {
        if (ring.isEmpty()) throw new IllegalStateException("Ring is empty");
        long hash = hash(key);
        SortedMap<Long, Node> tail = ring.tailMap(hash);
        Long nodeHash = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(nodeHash);
    }

    public List<Node> getReplicaNodes(String key, int replicaCount) {
        if (ring.isEmpty()) throw new IllegalStateException("Ring is empty");
        List<Node> replicas = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        long hash = hash(key);
        SortedMap<Long, Node> tail = ring.tailMap(hash);
        Iterator<Node> iter = tail.values().iterator();

        while (replicas.size() < replicaCount) {
            if (!iter.hasNext()) iter = ring.values().iterator();
            Node candidate = iter.next();
            if (seen.add(candidate.getNodeId()) && candidate.isActive()) {
                replicas.add(candidate);
            }
            if (seen.size() >= nodes.size()) break;
        }
        return replicas;
    }

    public Map<String, Integer> getKeyDistribution(List<String> keys) {
        Map<String, Integer> distribution = new HashMap<>();
        for (Node node : nodes.values()) {
            distribution.put(node.getNodeId(), 0);
        }
        for (String key : keys) {
            Node node = getNodeForKey(key);
            distribution.merge(node.getNodeId(), 1, Integer::sum);
        }
        return distribution;
    }

    public int getTotalNodes()    { return nodes.size(); }
    public int getRingSize()      { return ring.size(); }

    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes());
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
