package com.kvstore.raft;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class RaftClusterTest {

    private RaftCluster cluster;

    @BeforeEach
    void setup() throws Exception {
        cluster = new RaftCluster();
        cluster.addNode("raft-1");
        cluster.addNode("raft-2");
        cluster.addNode("raft-3");
        cluster.connectPeers();
        cluster.startAll();
    }

    @AfterEach
    void teardown() {
        cluster.stopAll();
    }

    @Test
    void leaderElected() throws Exception {
        RaftNode leader = cluster.waitForLeader(3000);
        assertNotNull(leader);
        assertEquals(RaftRole.LEADER, leader.getRole());
        long followerCount = cluster.getNodes().stream()
            .filter(n -> n.getRole() == RaftRole.FOLLOWER)
            .count();
        assertEquals(2, followerCount);
    }

    @Test
    void writesReplicateToAllNodes() throws Exception {
        RaftNode leader = cluster.waitForLeader(3000);
        leader.clientPut("name", "ram".getBytes());
        leader.clientPut("goal", "faang".getBytes());
        Thread.sleep(100);

        for (RaftNode node : cluster.getNodes()) {
            assertEquals("ram",   new String(node.get("name").orElse("NOT FOUND".getBytes())));
            assertEquals("faang", new String(node.get("goal").orElse("NOT FOUND".getBytes())));
        }
    }

    @Test
    void deleteReplicatesToAllNodes() throws Exception {
        RaftNode leader = cluster.waitForLeader(3000);
        leader.clientPut("lang", "java".getBytes());
        leader.clientDelete("lang");
        Thread.sleep(100);

        for (RaftNode node : cluster.getNodes()) {
            assertTrue(node.get("lang").isEmpty());
        }
    }

    @Test
    void newLeaderElectedAfterCrash() throws Exception {
        RaftNode leader = cluster.waitForLeader(3000);
        String oldLeaderId = leader.getNodeId();
        leader.stop();
        Thread.sleep(500);

        RaftNode newLeader = cluster.waitForLeader(3000);
        assertNotNull(newLeader);
        assertNotEquals(oldLeaderId, newLeader.getNodeId());
        assertEquals(RaftRole.LEADER, newLeader.getRole());
    }

    @Test
    void writesWorkAfterLeaderCrash() throws Exception {
        RaftNode leader = cluster.waitForLeader(3000);
        leader.clientPut("before", "crash".getBytes());
        leader.stop();
        Thread.sleep(500);

        RaftNode newLeader = cluster.waitForLeader(3000);
        newLeader.clientPut("after", "recovery".getBytes());
        Thread.sleep(100);

        for (RaftNode node : cluster.getNodes()) {
            if (node.getNodeId().equals(leader.getNodeId())) continue;
            assertEquals("recovery", new String(node.get("after").orElse("NOT FOUND".getBytes())));
        }
    }
}
