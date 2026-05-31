# Distributed Key-Value Store

A distributed key-value store built from scratch in Java, implementing the core concepts behind DynamoDB, Cassandra, and etcd.

## Architecture
Client → REST API (Spring Boot)
↓
Consistent Hash Ring       ← routes keys to shards
↓
Raft Consensus Layer       ← leader election + replication
↓
InMemoryStore + WAL          ← durability + crash recovery

## Features

- **WAL (Write-Ahead Log)** — every write is persisted to disk before touching memory. CRC32 checksums detect torn writes on crash recovery
- **Consistent Hashing** — keys distributed across nodes using MD5 ring with 150 virtual nodes per physical node for even distribution
- **Raft Consensus** — leader election with randomized timeouts (150–300ms), log replication requiring majority acknowledgment before commit
- **Automatic Failover** — new leader elected within 300ms of leader crash, zero data loss
- **REST API** — Spring Boot endpoints for PUT, GET, DELETE, and cluster status
- **React Dashboard** — live cluster status, node roles, operation log, throughput chart

## Benchmark Results

| Operation | Throughput | p50 | p95 | p99 |
|-----------|-----------|-----|-----|-----|
| Write (PUT) | 3,215 req/s | 4ms | 13ms | 18ms |
| Read  (GET) | 7,299 req/s | 2ms | 5ms  | 6ms  |
| Mixed 70/30 | 6,370 req/s | 2ms | 7ms  | 11ms |

Tested with 1000 ops, 20 concurrent threads, running on MacBook Air M1.

## Project Structure
src/main/java/com/kvstore/
├── core/               Phase 1 — WAL + InMemoryStore
│   ├── KVStore.java
│   ├── StoreEntry.java
│   └── InMemoryStore.java
├── wal/                Phase 1 — Write-Ahead Log
│   ├── WAL.java
│   ├── WALEntry.java
│   └── WALReplay.java
├── cluster/            Phase 2 — Consistent Hashing
│   ├── Node.java
│   ├── ConsistentHashRing.java
│   └── ClusterManager.java
├── raft/               Phase 3 — Raft Consensus
│   ├── RaftRole.java
│   ├── RaftLog.java
│   ├── RaftNode.java
│   └── RaftCluster.java
├── api/                Phase 4 — REST API
│   ├── KVStoreService.java
│   └── KVController.java
└── benchmark/          Phase 5 — Load Testing
└── LoadTester.java

## Running Locally

**Backend:**
```bash
mvn spring-boot:run
```

**Frontend dashboard:**
```bash
cd kvstore-dashboard
npm install
npm start
```

**Load tests** (requires backend running):
```bash
mvn exec:java -Dexec.mainClass="com.kvstore.benchmark.LoadTester"
```

## API Endpoints
PUT    /api/keys/{key}       body: {"value": "..."}
GET    /api/keys/{key}
DELETE /api/keys/{key}
GET    /api/cluster/status
GET    /api/health

## Key Design Decisions

**Why consistent hashing over modulo hashing?**
Adding a node with `hash % n` remaps ~100% of keys. Consistent hashing remaps only `1/n` of keys — critical at scale.

**Why Raft over simple replication?**
Simple replication (write to 2 nodes independently) causes split-brain on partial failures. Raft requires majority acknowledgment before commit — no split-brain possible.

**Why WAL before memory write?**
If you write to memory first and crash before the WAL write, data is permanently lost. WAL-first guarantees every committed write survives restarts.

**Why virtual nodes?**
Without virtual nodes, uneven hash distribution leaves some nodes holding 3x more data than others. 150 virtual nodes per physical node gives ~even distribution.

## Tech Stack

- Java 17, Spring Boot 3.2, Maven
- SLF4J + Logback
- React, Recharts, Axios
- JUnit 5

## What's Next

- [ ] Persistent LSM-tree storage engine (replace in-memory map)
- [ ] gRPC for inter-node communication (replace in-process calls)
- [ ] Docker Compose for running actual separate node processes
- [ ] Prometheus metrics + Grafana dashboard
