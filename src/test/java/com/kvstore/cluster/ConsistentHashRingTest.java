package com.kvstore.cluster;

import org.junit.jupiter.api.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    @Test
    void keyRoutesToCorrectNode() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode(new Node("node-1", "localhost", 7001));
        ring.addNode(new Node("node-2", "localhost", 7002));
        ring.addNode(new Node("node-3", "localhost", 7003));

        String key = "testkey";
        Node node = ring.getNodeForKey(key);
        assertNotNull(node);

        Node sameNode = ring.getNodeForKey(key);
        assertEquals(node.getNodeId(), sameNode.getNodeId());
    }

    @Test
    void keysDistributeAcrossAllNodes() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode(new Node("node-1", "localhost", 7001));
        ring.addNode(new Node("node-2", "localhost", 7002));
        ring.addNode(new Node("node-3", "localhost", 7003));

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) keys.add("key-" + i);

        Map<String, Integer> dist = ring.getKeyDistribution(keys);
        dist.forEach((nodeId, count) -> {
            assertTrue(count > 0, "node " + nodeId + " should have at least 1 key");
            assertTrue(count < 250, "node " + nodeId + " should not hold all keys");
        });
    }

    @Test
    void removeNodeReroutesKeys() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        Node n1 = new Node("node-1", "localhost", 7001);
        Node n2 = new Node("node-2", "localhost", 7002);
        ring.addNode(n1);
        ring.addNode(n2);

        String key = "reroutekey";
        Node before = ring.getNodeForKey(key);

        ring.removeNode(n2);
        ring.removeNode(n1);
        Node n3 = new Node("node-3", "localhost", 7003);
        ring.addNode(n3);

        Node after = ring.getNodeForKey(key);
        assertEquals("node-3", after.getNodeId());
    }

    @Test
    void replicaNodesAreDistinct() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode(new Node("node-1", "localhost", 7001));
        ring.addNode(new Node("node-2", "localhost", 7002));
        ring.addNode(new Node("node-3", "localhost", 7003));

        List<Node> replicas = ring.getReplicaNodes("somekey", 2);
        assertEquals(2, replicas.size());
        assertNotEquals(replicas.get(0).getNodeId(), replicas.get(1).getNodeId());
    }
}
