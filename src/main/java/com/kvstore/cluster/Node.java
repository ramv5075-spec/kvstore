package com.kvstore.cluster;

import com.kvstore.core.InMemoryStore;
import java.io.IOException;
import java.nio.file.Path;

public class Node {

    public enum State { ACTIVE, INACTIVE }

    private final String nodeId;
    private final String host;
    private final int port;
    private State state;
    private final InMemoryStore store;

    public Node(String nodeId, String host, int port) throws IOException {
        this.nodeId = nodeId;
        this.host   = host;
        this.port   = port;
        this.state  = State.ACTIVE;

        Path walPath = Path.of("data/" + nodeId + ".wal");
        walPath.getParent().toFile().mkdirs();
        this.store = new InMemoryStore(walPath);
    }

    public String getNodeId()       { return nodeId; }
    public String getHost()         { return host; }
    public int getPort()            { return port; }
    public State getState()         { return state; }
    public InMemoryStore getStore() { return store; }
    public boolean isActive()       { return state == State.ACTIVE; }

    public void deactivate() { this.state = State.INACTIVE; }
    public void activate()   { this.state = State.ACTIVE; }

    @Override
    public String toString() {
        return String.format("Node[%s %s:%d %s]", nodeId, host, port, state);
    }
}
