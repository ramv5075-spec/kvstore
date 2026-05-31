package com.kvstore.raft;

public enum RaftRole {
    FOLLOWER,    // default state, copies leader
    CANDIDATE,   // trying to become leader
    LEADER       // accepts writes, replicates to followers
}
