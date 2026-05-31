package com.kvstore.api;

import com.kvstore.raft.RaftCluster;
import com.kvstore.raft.RaftNode;
import com.kvstore.raft.RaftRole;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class KVStoreService {

    private static final Logger log = LoggerFactory.getLogger(KVStoreService.class);

    private RaftCluster cluster;

    @PostConstruct
    public void init() throws Exception {
        log.info("Initializing KV store cluster...");
        cluster = new RaftCluster();
        cluster.addNode("raft-1");
        cluster.addNode("raft-2");
        cluster.addNode("raft-3");
        cluster.connectPeers();
        cluster.startAll();

        log.info("Waiting for leader election...");
        cluster.waitForLeader(5000);
        log.info("Cluster ready!");
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down cluster...");
        cluster.stopAll();
    }

    public boolean put(String key, String value) {
        RaftNode leader = cluster.getLeader();
        if (leader == null) return false;
        return leader.clientPut(key, value.getBytes());
    }

    public Optional<String> get(String key) {
        RaftNode leader = cluster.getLeader();
        if (leader == null) return Optional.empty();
        return leader.get(key).map(String::new);
    }

    public boolean delete(String key) {
        RaftNode leader = cluster.getLeader();
        if (leader == null) return false;
        return leader.clientDelete(key);
    }

    public ClusterStatus getClusterStatus() {
        return new ClusterStatus(cluster.getNodes().stream()
            .map(n -> new NodeStatus(
                n.getNodeId(),
                n.getRole().name(),
                n.getCurrentTerm(),
                n.getLeaderId(),
                n.isLeader(),
                n.getCommitIndex()
            )).toList());
    }

    public record NodeStatus(
        String nodeId,
        String role,
        int term,
        String leaderId,
        boolean isLeader,
        int commitIndex
    ) {}

    public record ClusterStatus(java.util.List<NodeStatus> nodes) {}
}
