package com.kvstore.raft;

import com.kvstore.core.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private static final int ELECTION_TIMEOUT_MIN = 150;
    private static final int ELECTION_TIMEOUT_MAX = 300;
    private static final int HEARTBEAT_INTERVAL   = 50;

    private final String nodeId;
    private final InMemoryStore store;
    private final RaftLog raftLog = new RaftLog();

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private volatile String leaderId = null;

    private final List<RaftNode> peers = new ArrayList<>();
    private final AtomicInteger votesReceived = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    private volatile boolean running = false;

    public RaftNode(String nodeId, InMemoryStore store) {
        this.nodeId = nodeId;
        this.store  = store;
    }

    public void addPeer(RaftNode peer) {
        peers.add(peer);
    }

    public void start() {
        running = true;
        log.info("RaftNode {} starting as FOLLOWER term={}", nodeId, currentTerm.get());
        resetElectionTimer();
    }

    public void stop() {
        running = false;
        if (electionTimer  != null) electionTimer.cancel(true);
        if (heartbeatTimer != null) heartbeatTimer.cancel(true);
        scheduler.shutdownNow();
        log.info("RaftNode {} stopped", nodeId);
    }

    // ─── Election Timer ───────────────────────────────────────────────────────

    private void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        int timeout = ELECTION_TIMEOUT_MIN +
            new Random().nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
        electionTimer = scheduler.schedule(
            this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    // ─── Leader Election ──────────────────────────────────────────────────────

    private synchronized void startElection() {
        if (!running) return;

        role = RaftRole.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor = nodeId;
        votesReceived.set(1); // vote for self

        log.info("Node {} starting election for term={}", nodeId, currentTerm.get());

        for (RaftNode peer : peers) {
            scheduler.submit(() -> requestVote(peer));
        }

        resetElectionTimer();
    }

    private void requestVote(RaftNode peer) {
        if (!running) return;
        boolean granted = peer.handleVoteRequest(
            nodeId,
            currentTerm.get(),
            raftLog.getLastIndex(),
            raftLog.getLastTerm()
        );

        if (granted) {
            int votes = votesReceived.incrementAndGet();
            int majority = (peers.size() + 1) / 2 + 1;
            log.debug("Node {} received vote, total={} majority={}", nodeId, votes, majority);
            if (votes >= majority && role == RaftRole.CANDIDATE) {
                becomeLeader();
            }
        }
    }

    public synchronized boolean handleVoteRequest(
            String candidateId, int term, int lastLogIndex, int lastLogTerm) {

        if (term > currentTerm.get()) {
            currentTerm.set(term);
            role = RaftRole.FOLLOWER;
            votedFor = null;
        }

        boolean logUpToDate = lastLogTerm > raftLog.getLastTerm() ||
            (lastLogTerm == raftLog.getLastTerm() &&
             lastLogIndex >= raftLog.getLastIndex());

        boolean canVote = (votedFor == null || votedFor.equals(candidateId))
            && logUpToDate;

        if (canVote) {
            votedFor = candidateId;
            resetElectionTimer();
            log.debug("Node {} granting vote to {} for term={}", nodeId, candidateId, term);
        }

        return canVote && term >= currentTerm.get();
    }

    // ─── Become Leader ────────────────────────────────────────────────────────

    private synchronized void becomeLeader() {
        if (role != RaftRole.CANDIDATE) return;
        role = RaftRole.LEADER;
        leaderId = nodeId;
        log.info("Node {} became LEADER for term={}", nodeId, currentTerm.get());

        if (electionTimer != null) electionTimer.cancel(false);

        heartbeatTimer = scheduler.scheduleAtFixedRate(
            this::sendHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    // ─── Heartbeats ───────────────────────────────────────────────────────────

    private void sendHeartbeats() {
        if (!running || role != RaftRole.LEADER) return;
        for (RaftNode peer : peers) {
            scheduler.submit(() -> peer.handleHeartbeat(nodeId, currentTerm.get()));
        }
    }

    public synchronized void handleHeartbeat(String fromLeader, int term) {
        if (term >= currentTerm.get()) {
            currentTerm.set(term);
            role = RaftRole.FOLLOWER;
            leaderId = fromLeader;
            resetElectionTimer();
            log.debug("Node {} received heartbeat from leader={} term={}", nodeId, fromLeader, term);
        }
    }

    // ─── Log Replication ──────────────────────────────────────────────────────

    public boolean clientPut(String key, byte[] value) {
        if (role != RaftRole.LEADER) {
            log.warn("Node {} is not leader, rejecting write", nodeId);
            return false;
        }
        return replicateEntry(key, value, false);
    }

    public boolean clientDelete(String key) {
        if (role != RaftRole.LEADER) {
            log.warn("Node {} is not leader, rejecting delete", nodeId);
            return false;
        }
        return replicateEntry(key, null, true);
    }

    private boolean replicateEntry(String key, byte[] value, boolean delete) {
        raftLog.appendEntry(currentTerm.get(), key, value, delete);
        int entryIndex = raftLog.getLastIndex();

        log.debug("Leader {} replicating index={} key={}", nodeId, entryIndex, key);

        int replicated = 1; // leader counts itself
        for (RaftNode peer : peers) {
            boolean success = peer.appendEntry(
                nodeId,
                currentTerm.get(),
                entryIndex,
                key,
                value,
                delete
            );
            if (success) replicated++;
        }

        int majority = (peers.size() + 1) / 2 + 1;
        if (replicated >= majority) {
            raftLog.commit(entryIndex);
            applyCommittedEntries();
            log.info("Entry committed index={} key={} replicated={}/{} nodes",
                entryIndex, key, replicated, peers.size() + 1);
            return true;
        }

        log.warn("Failed to reach majority for key={} replicated={}/{}", key, replicated, peers.size() + 1);
        return false;
    }

    public synchronized boolean appendEntry(
            String leaderId, int term, int index,
            String key, byte[] value, boolean delete) {

        if (term < currentTerm.get()) return false;

        currentTerm.set(term);
        role = RaftRole.FOLLOWER;
        this.leaderId = leaderId;
        resetElectionTimer();

        raftLog.appendEntry(term, key, value, delete);
        raftLog.commit(index);
        applyCommittedEntries();

        log.debug("Follower {} appended index={} key={}", nodeId, index, key);
        return true;
    }

    // ─── Apply to State Machine ───────────────────────────────────────────────

    private void applyCommittedEntries() {
        for (RaftLog.LogEntry entry : raftLog.getUnAppliedEntries()) {
            if (entry.delete()) {
                store.applyDelete(entry.key());
            } else {
                store.applyPut(entry.key(), entry.value());
            }
            raftLog.markApplied(entry.index());
            log.debug("Applied entry index={} key={} delete={}", entry.index(), entry.key(), entry.delete());
        }
    }

    // ─── Getters ──────────────────────────────────────────────────────────────

    public Optional<byte[]> get(String key) { return store.get(key); }
    public String getNodeId()               { return nodeId; }
    public RaftRole getRole()               { return role; }
    public int getCurrentTerm()             { return currentTerm.get(); }
    public String getLeaderId()             { return leaderId; }
    public boolean isLeader()               { return role == RaftRole.LEADER; }
    public int getCommitIndex()             { return raftLog.getCommitIndex(); }
}
