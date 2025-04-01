# Distributed Key-Value Store

This project implements a distributed key-value store written in Go, designed to provide a fault-tolerant, scalable, and consistent storage system. It leverages gRPC for communication between nodes, consistent hashing for key distribution across the cluster, and replication to ensure data durability and availability. A Hybrid Logical Clock (HLC) is used to timestamp operations, enabling a total ordering of events across the distributed system.

The system is built to handle node failures, perform read repairs, and rebalance data when the cluster topology changes, making it suitable for distributed environments where reliability and consistency are critical.

## Features

- **Distributed Key-Value Storage**: Keys are distributed across nodes using consistent hashing for load balancing.
- **Replication**: Each key is replicated to multiple nodes (configurable via replication factor) for fault tolerance.
- **Eventual Consistency with Quorum Writes**: Writes require a quorum of replicas to succeed, ensuring strong write consistency, while reads provide eventual consistency with read repair.
- **Hybrid Logical Clock (HLC)**: Combines physical time and logical counters to order operations across nodes.
- **Read Repair**: Automatically resolves inconsistencies during read operations by repairing stale or missing replicas.
- **Rebalancing**: Redistributes data when nodes are added or removed, optimizing for minimal data movement.
- **Fault Tolerance**: Detects node failures via heartbeats and attempts to reconnect to disconnected peers.
- **Peer Gossip**: Nodes share information about new peers to maintain a fully connected mesh.
- **Validation**: Enforces constraints on key and value sizes and allowed characters.

## Architecture

The system is composed of several key components that work together to provide a robust distributed key-value store:

### Key Components

- **Server**: Each node runs an instance of the server struct, which manages local storage, peer connections, and gRPC services.
- **Consistent Hashing Ring**: Implemented via `consistenthash.Ring`, it maps keys to nodes and ensures even distribution and minimal data movement during cluster changes.
- **Hybrid Logical Clock (HLC)**: Provides globally unique timestamps by combining physical time (milliseconds) with a logical counter, ensuring a total ordering of operations.
- **Local Storage**: Managed by the `storage.Storage` package, it stores key-value pairs locally on each node with timestamps.
- **gRPC Services**:
  - **KVStore Service**: Exposes Get and Put methods for client interactions.
  - **NodeInternal Service**: Handles node-to-node communication for replication, heartbeats, and peer management.
- **Peer Management**: Nodes maintain a list of peers, establish gRPC connections, and use gossip to propagate cluster membership changes.
- **Replication**: Ensures data redundancy by replicating writes to a configurable number of nodes, requiring a quorum for write success.
- **Rebalancing**: Adjusts key distribution when the cluster topology changes, replicating only missing or outdated keys.

### How It Works

- **Key Distribution**: When a key is written or read, the consistent hashing ring determines which nodes (replicas) are responsible for it based on the replication factor.
- **Writes**: A Put operation generates an HLC timestamp, stores the value locally, and replicates it to other designated replicas. The write succeeds only if a quorum of replicas acknowledges it.
- **Reads**: A Get operation checks the local store if the node is a replica. If not, or if the key is missing/stale, it queries all replicas, returns the most recent value, and repairs inconsistencies asynchronously.
- **Cluster Management**: Nodes use heartbeats to detect failures and gossip to share information about new or reconnected peers.
- **Rebalancing**: When nodes join or leave, the system rebalances by replicating keys to new replicas or removing them from nodes no longer responsible.

## Installation

### Prerequisites

- **Go**: Version 1.16 or higher.
- **Protocol Buffers Compiler (protoc)**: Required to generate gRPC code from .proto files.
- **Dependencies**: The code relies on external packages (`google.golang.org/grpc`, `github.com/google/uuid`, and custom packages `consistenthash` and `storage`).

### Steps

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/distributed-kv-store.git
   cd distributed-kv-store
   ```

2. **Install Dependencies**:
   Ensure the required Go modules are installed:
   ```bash
   go mod tidy
   ```

3. **Generate gRPC Code**:
   Assuming a kvstore.proto file exists in kvstore/proto/:
   ```bash
   protoc --go_out=. --go-grpc_out=. kvstore/proto/kvstore.proto
   ```

4. **Build the Server**:
   ```bash
   go build -o kvstore-server server/main.go
   ```

## Usage

### Running a Node

Each node in the cluster runs an instance of the server. Start a node with:

```bash
./kvstore-server -id <node-id> -addr <listen-address> -peers <peer-list>
```

### Command-Line Flags

- **-id**: Unique node identifier (optional; defaults to a generated UUID prefixed with `node-`).
- **-addr**: Address to listen on (e.g., `:50051`).
- **-peers**: Comma-separated list of peer nodes in `id@address` format (e.g., `node-1@127.0.0.1:50052,node-2@127.0.0.1:50053`).
- **-sync-interval**: Interval for anti-entropy synchronization (default: `5m`).
- **-heartbeat-interval**: Interval for heartbeat checks (default: `1s`).
- **-replication-factor**: Number of replicas per key (default: `3`).
- **-virtual-nodes**: Number of virtual nodes per physical node in the consistent hashing ring (default: `10`).

### Example: Starting a Three-Node Cluster

Run these commands in separate terminals:

```bash
# Node 1
./kvstore-server -id node-1 -addr :50051 -peers node-2@127.0.0.1:50052,node-3@127.0.0.1:50053

# Node 2
./kvstore-server -id node-2 -addr :50052 -peers node-1@127.0.0.1:50051,node-3@127.0.0.1:50053

# Node 3
./kvstore-server -id node-3 -addr :50053 -peers node-1@127.0.0.1:50051,node-2@127.0.0.1:50052
```

### Interacting with the Store

Clients interact with the system via the KVStore gRPC service (Get and Put methods). You can connect to any node's address (e.g., `127.0.0.1:50051`) using a gRPC client. The node handles routing requests to the appropriate replicas.

#### Example gRPC Client (Pseudo-Code)

```go
// Connect to a node
conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithInsecure())
client := pb.NewKVStoreClient(conn)

// Put a key-value pair
putResp, err := client.Put(context.Background(), &pb.PutRequest{Key: "foo", Value: "bar"})

// Get a value
getResp, err := client.Get(context.Background(), &pb.GetRequest{Key: "foo"})
if getResp.Exists {
    fmt.Println("Value:", getResp.Value)
}
```

## Configuration

The server can be customized via command-line flags (see above). Key settings include:

- **Replication Factor** (`-replication-factor`): Controls how many nodes store each key. Higher values increase fault tolerance but require more resources.
- **Virtual Nodes** (`-virtual-nodes`): Increases the granularity of key distribution in the consistent hashing ring.
- **Heartbeat Interval** (`-heartbeat-interval`): Frequency of peer health checks.
- **Sync Interval** (`-sync-interval`): Frequency of anti-entropy synchronization (though not fully implemented in this code).
- **Reconnect Interval**: Hardcoded to 5 seconds; nodes attempt to reconnect to disconnected peers every 5 seconds, up to 20 attempts.

### Validation Rules

- **Keys**: Must be non-empty, ≤ 256 bytes, and contain only letters, numbers, -, _, ., or /.
- **Values**: Must be ≤ 1MB (1,024,000 bytes).

## Fault Tolerance

The system is designed to handle node failures gracefully:

- **Heartbeats**: Nodes send periodic heartbeats to detect peer failures. Failed peers are marked as disconnected, and reconnection attempts are made every 5 seconds (up to 20 attempts).
- **Reconnection**: If a peer reconnects, it is re-added to the cluster and gossiped to other nodes.
- **Quorum Writes**: Writes succeed only if a majority of replicas (`replicationFactor / 2) + 1` acknowledge, adjusting dynamically if fewer replicas are available.
- **Read Repair**: Fixes missing or stale data during reads, ensuring eventual consistency.

## Consistency Model

- **Writes**: Provide strong consistency via quorum-based replication. A write is successful only if a majority of replicas store it.
- **Reads**: Offer eventual consistency. If a node has an outdated or missing key, it queries all replicas, returns the latest value, and repairs inconsistencies in the background.

## Rebalancing

When nodes join or leave:

- The consistent hashing ring updates, and the `rebalanceOnlyMissingReplicas` function runs.
- Only keys missing from their designated replicas or with outdated timestamps are replicated, minimizing data movement.
- Rebalancing occurs in batches (100 keys by default) with up to 5 concurrent transfers, retrying failed attempts up to 3 times.

## Logging and Monitoring

The server logs critical events (e.g., peer connections, replication status, rebalancing) using Go's log package. Monitor these logs to track system health and debug issues.

Example log output:

```
2023/10/10 12:00:00 Node node-1 listening on :50051
2023/10/10 12:00:01 Successfully connected to peer node-2
2023/10/10 12:00:02 Replicated key foo to node node-3
```.