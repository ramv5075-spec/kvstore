package com.kvstore.raft;

import com.kvstore.core.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

public class RaftCluster {

    private static final Logger log = LoggerFactory.getLogger(RaftCluster.class);

    private final List<RaftNode> nodes = new ArrayList<>();

    public void addNode(String nodeId) throws Exception {
        Path walPath = Path.of("data/raft-" + nodeId + ".wal");
        walPath.getParent().toFile().mkdirs();
        InMemoryStore store = new InMemoryStore(walPath);
        RaftNode node = new RaftNode(nodeId, store);
        nodes.add(node);
        log.info("Added raft node {}", nodeId);
    }

    public void connectPeers() {
        for (RaftNode node : nodes) {
            for (RaftNode peer : nodes) {
                if (!peer.getNodeId().equals(node.getNodeId())) {
                    node.addPeer(peer);
                }
            }
        }
        log.info("Connected {} nodes as peers", nodes.size());
    }

    public void startAll() {
        for (RaftNode node : nodes) node.start();
        log.info("All {} raft nodes started", nodes.size());
    }

    public void stopAll() {
        for (RaftNode node : nodes) node.stop();
        log.info("All raft nodes stopped");
    }

    public RaftNode waitForLeader(int timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : nodes) {
                if (node.isLeader()) return node;
            }
            Thread.sleep(20);
        }
        throw new RuntimeException("No leader elected within " + timeoutMs + "ms");
    }

    public RaftNode getLeader() {
        return nodes.stream()
            .filter(RaftNode::isLeader)
            .findFirst()
            .orElse(null);
    }

    public void printStatus() {
        log.info("=== cluster status ===");
        for (RaftNode node : nodes) {
            log.info("  node={} role={} term={} leader={}",
                node.getNodeId(), node.getRole(),
                node.getCurrentTerm(), node.getLeaderId());
        }
    }

    public List<RaftNode> getNodes() { return nodes; }
}
