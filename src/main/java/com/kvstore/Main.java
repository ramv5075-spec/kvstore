package com.kvstore;

import com.kvstore.raft.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("=== phase 3: raft consensus ===");

        RaftCluster cluster = new RaftCluster();
        cluster.addNode("raft-1");
        cluster.addNode("raft-2");
        cluster.addNode("raft-3");
        cluster.connectPeers();
        cluster.startAll();

        log.info("waiting for leader election...");
        RaftNode leader = cluster.waitForLeader(3000);
        log.info("leader elected: {}", leader.getNodeId());
        cluster.printStatus();

        log.info("--- writing via leader ---");
        leader.clientPut("name",    "ram".getBytes());
        leader.clientPut("goal",    "faang".getBytes());
        leader.clientPut("lang",    "java".getBytes());
        leader.clientPut("project", "kvstore".getBytes());

        log.info("--- reading from all nodes ---");
        for (RaftNode node : cluster.getNodes()) {
            String name = new String(node.get("name").orElse("NOT FOUND".getBytes()));
            String goal = new String(node.get("goal").orElse("NOT FOUND".getBytes()));
            log.info("node={} role={} name={} goal={}",
                node.getNodeId(), node.getRole(), name, goal);
        }

        log.info("--- deleting key via leader ---");
        leader.clientDelete("lang");
        Thread.sleep(100);
        for (RaftNode node : cluster.getNodes()) {
            String lang = new String(node.get("lang").orElse("DELETED".getBytes()));
            log.info("node={} lang={}", node.getNodeId(), lang);
        }

        log.info("--- simulating leader crash ---");
        String oldLeaderId = leader.getNodeId();
        leader.stop();
        log.info("node {} crashed", oldLeaderId);

        Thread.sleep(500);
        RaftNode newLeader = cluster.waitForLeader(3000);
        log.info("new leader elected: {}", newLeader.getNodeId());
        cluster.printStatus();

        log.info("--- writing after leader crash ---");
        newLeader.clientPut("recovery", "success".getBytes());
        Thread.sleep(100);

        for (RaftNode node : cluster.getNodes()) {
            if (node.getNodeId().equals(oldLeaderId)) continue;
            String recovery = new String(node.get("recovery").orElse("NOT FOUND".getBytes()));
            log.info("node={} recovery={}", node.getNodeId(), recovery);
        }

        log.info("=== phase 3 complete ===");
        cluster.stopAll();
    }
}
