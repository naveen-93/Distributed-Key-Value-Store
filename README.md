# Distributed Key-Value Store
A scalable, fault-tolerant distributed key-value store implementation with eventual consistency.

## Overview

This distributed key-value store is designed for high availability and reliability, featuring:

- Consistent hashing for load balancing
- Quorum-based operations for consistency
- Write-Ahead Logging (WAL) for durability
- Eventual consistency with anti-entropy synchronization
- Fault tolerance through replication

## System Architecture

The system consists of five main components:

### Storage Layer (`storage.go`)

The storage layer manages persistent data storage on each node:

- In-memory hash map for fast access
- Write-Ahead Log (WAL) for durability
- Periodic snapshots for recovery optimization
- Tombstone marking for eventual consistency
- TTL-based cleanup
- Thread-safe operations

### Protocol Layer (`protocol.go`)

Handles all communication within the system:

- gRPC services for client and inter-node communication
- Write replication across nodes
- Anti-entropy synchronization
- Health monitoring via heartbeats
- Eventual consistency implementation

### Client Library (`client/main.go`)

Provides the application interface:

- gRPC connection management
- Quorum-based operations
- Consistent hash-based routing
- Read repair functionality
- Dynamic ring state updates
- Robust error handling with retries

### Consistent Hashing (`consistent_hash.go`)

Manages key distribution:

- Virtual nodes for balanced distribution
- Minimal-disruption node updates
- Efficient binary search lookups
- Thread-safe ring operations

### Server (`server/main.go`)

Core server implementation:

- Ring state management
- Key rebalancing
- gRPC service registration
- Clean shutdown handling

## Protocol Specification

### Client-Server Protocol

#### KVStore Service

**Methods:**
- `Get(GetRequest) -> GetResponse`
- `Put(PutRequest) -> PutResponse`

**Message Formats:**

```protobuf
message GetRequest {
  string key = 1;
  uint64 client_id = 2;
  uint64 request_id = 3;
}

message GetResponse {
  string value = 1;
  uint64 timestamp = 2;
  bool exists = 3;
}

message PutRequest {
  string key = 1;
  string value = 2;
  uint64 client_id = 3;
  uint64 request_id = 4;
}

message PutResponse {
  string old_value = 1;
  bool had_old_value = 2;
}
```

### Server-Server Protocol

#### NodeInternal Service

**Methods:**
- `Replicate(ReplicateRequest) -> ReplicateResponse`
- `SyncKeys(SyncRequest) -> SyncResponse`
- `Heartbeat(Ping) -> Pong`
- `GetRingState(RingStateRequest) -> RingStateResponse`

**Message Formats:**

```protobuf
message ReplicateRequest {
  string key = 1;
  string value = 2;
  uint64 timestamp = 3;
}

message ReplicateResponse {
  bool success = 1;
  string error = 2;
}

message SyncRequest {
  map<string, uint64> key_timestamps = 1;
  map<string, string> key_ranges = 2;
}

message SyncResponse {
  map<string, KeyValue> missing = 1;
  map<string, uint64> deletions = 2;
}

message Ping {
  uint32 node_id = 1;
}

message Pong {}

message RingStateRequest {}

message RingStateResponse {
  uint64 version = 1;
  map<string, bool> nodes = 2;
  int64 updated_at = 3;
}
```

## Consistency Model

The system implements eventual consistency through:

- Quorum-based operations (R + W > N)
- Last-Write-Wins (LWW) conflict resolution
- Read repair for stale replicas
- Anti-entropy synchronization

## Fault Tolerance

The system handles failures through:

- Heartbeat-based failure detection
- WAL and snapshot-based durability
- N-way replication
- Automatic ring updates on node failures
