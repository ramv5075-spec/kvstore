package com.kvstore.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftLog {

    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

    public record LogEntry(
        int term,        // which election term this entry was created in
        int index,       // position in the log
        String key,
        byte[] value,
        boolean delete
    ) {}

    private final List<LogEntry> entries = new ArrayList<>();
    private int commitIndex = -1;   // highest entry known to be committed
    private int lastApplied = -1;   // highest entry applied to store

    public void appendEntry(int term, String key, byte[] value, boolean delete) {
        int index = entries.size();
        entries.add(new LogEntry(term, index, key, value, delete));
        log.debug("Log appended index={} term={} key={} delete={}", index, term, key, delete);
    }

    public void commit(int index) {
        if (index > commitIndex && index < entries.size()) {
            commitIndex = index;
            log.debug("Log committed up to index={}", commitIndex);
        }
    }

    public List<LogEntry> getUnAppliedEntries() {
        List<LogEntry> unapplied = new ArrayList<>();
        for (int i = lastApplied + 1; i <= commitIndex; i++) {
            unapplied.add(entries.get(i));
        }
        return unapplied;
    }

    public void markApplied(int index) {
        lastApplied = index;
    }

    public LogEntry getEntry(int index) {
        if (index < 0 || index >= entries.size()) return null;
        return entries.get(index);
    }

    public int getLastIndex()   { return entries.size() - 1; }
    public int getCommitIndex() { return commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public int size()           { return entries.size(); }

    public int getLastTerm() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).term();
    }
}
