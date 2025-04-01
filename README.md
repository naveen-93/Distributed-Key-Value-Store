# Distributed Key-Value Store

A high-performance, fault-tolerant distributed key-value store using consistent hashing and quorum-based replication.

## Features

- **Distributed Architecture**: Cluster of servers with even key distribution
- **Consistent Hashing**: Efficient data distribution with virtual nodes
- **Replication**: Configurable replication factor for fault tolerance
- **Read Repair**: Automatic detection and repair of stale data
- **Quorum-Based Writes**: Ensures data durability and consistency
- **Hybrid Logical Clocks (HLC)**: Maintains causal consistency
- **Durability**: Write-ahead logging and snapshot mechanism

## How It Works

### Get Operation

1. **Client Request**: Client sends a Get request with a key
2. **Server Handling**:
   - Server determines if it's a designated replica
   - If yes, retrieves local value and performs read repair
   - If no, queries all designated replicas for most recent value
3. **Response**: Returns the most recent value or "key not found"

### Put Operation

1. **Client Request**: Client sends a Put request with key-value pair
2. **Server Handling**:
   - Generates timestamp using HLC
   - Stores data locally
   - Replicates to designated replicas
   - Ensures quorum acknowledgment
3. **Response**: Returns old value (if any) or error message

## Architecture

### Client Layer
- Multiple clients connect via gRPC
- Can connect to any server in the cluster

### Server Cluster
- Distributed system with inter-server communication
- Consistent Hashing Ring with virtual nodes for load balancing

### Storage Layer (Per Server)
- **In-Memory Store**: Fast read/write access
- **Write-Ahead Log (WAL)**: Operation durability
- **Snapshots**: WAL compaction and faster recovery

### Starting a Cluster
```bash
# First server
go run pkg/server/main.go -addr :50051 -id node1

# Second server (knows about first)
go run pkg/server/main.go -addr :50052 -id node2 -peers "node1@localhost:50051"

# Third server (knows about first and second)
go run pkg/server/main.go -addr :50053 -id node3 -peers "node1@localhost:50051,node2@localhost:50052"
```

### Using the Client Library
```go
client := kvstore.NewClient([]string{"localhost:8001", "localhost:8002", "localhost:8003"})

// Put operation
err := client.Put("key1", "value1")

// Get operation
value, err := client.Get("key1")
```

## Client Library Usage

### Running the Client Library
```bash
# Clean up any existing processes and WAL files
sudo kill $(sudo lsof -t -i :50051)
rm wal*

# Start the cluster (3 nodes)
bash run.sh

# Build and test the client library
cd lib
make clean && make
make test
```

## Testing

### Correctness Testing
```bash
# Clean up any existing processes and WAL files
sudo kill $(sudo lsof -t -i :50051)
rm wal*

# Start the cluster (3 nodes)
bash run.sh

# Run correctness tests
cd tests
go test -v
```

### Performance Testing
```bash
From Root Folder
# Run performance tests for 10 seconds
go test -bench=. -benchtime=10s ./test/performance_test.go
```

### Performance Testing
```bash
# Run performance tests for 10 seconds
cd internal/storage
go test -v
```

## Configuration Options

- `--port`: Server listening port
- `--id`: Unique server identifier
- `--join`: Address of existing server to join the cluster
- `--replication-factor`: Number of replicas (default: 3)
- `--snapshot-interval`: Interval between snapshots in seconds (default: 300)


# System Performance and Testing Results

## Benchmark Performance

### Uniform Distribution Workloads
| Benchmark | Ops/Sec | Latency (ms) |
|-----------|---------|--------------|
| Read Heavy | 6,868 | 0.1454 |
| Write Heavy | 11,107 | 0.08985 |
| Mixed Workload | 4,005 | 0.2493 |

### Hot-Cold Distribution Workloads
| Benchmark | Ops/Sec | Latency (ms) |
|-----------|---------|--------------|
| Read Heavy | 5,548 | 0.1798 |
| Write Heavy | 11,154 | 0.08930 |
| Mixed Workload | 3,872 | 0.2577 |

### Multi-Client Workloads
| Benchmark | Ops/Sec | Latency (ms) |
|-----------|---------|--------------|
| Read Heavy | 15,615 | 0.6401 |
| Write Heavy | 30,777 | 0.3245 |
| Mixed Workload | 27,921 | 0.3578 |

### Replication Latency
| Benchmark | Avg Latency (ms) |
|-----------|------------------|
| Sequential | 0.1921 |
| Random | 0.2028 |

### Large Value Performance
| Value Size | Ops/Sec | Latency (ms) |
|------------|---------|--------------|
| 1 KB | N/A | 0.1013 |
| 10 KB | N/A | 0.1653 |

### Storage Test
## Stress Test Results
- **Total Operations**: 5,000,000
- **Total Time**: 3.74 seconds
- **Throughput**: 1,335,051 ops/sec

### Latency Percentiles
#### Store Operations
- p50: 1 µs
- p95: 20.875 µs
- p99: 38.75 µs

#### Get Operations
- p50: 84 ns
- p95: 1.667 µs
- p99: 3.291 µs

## Long-Running Performance Test
| Duration | Total Ops | Ops/Sec | Avg Latency | Max Latency |
|----------|-----------|---------|-------------|-------------|
| 5s | 6,362,655 | 1,272,531 | 2.98 µs | 6.267 ms |
| 10s | 12,751,760 | 1,277,821 | 2.97 µs | 11.306 ms |
| 15s | 19,087,034 | 1,267,055 | 2.98 µs | 11.306 ms |
| 20s | 25,490,368 | 1,280,667 | 2.97 µs | 11.306 ms |
| 25s | 31,883,304 | 1,278,587 | 2.97 µs | 11.306 ms |

## Test Coverage
- Basic Operations ✓
- Concurrent Operations ✓
- Write-Ahead Log (WAL) Recovery ✓
- Timestamp Ordering ✓
- Tombstone Behavior ✓
- Boundary Conditions ✓
- WAL Corruption Handling ✓
- TTL Cleanup ✓
- Snapshot Management ✓
- High Concurrency ✓

## Key System Design Trade-offs

### Consistency vs. Availability
- **Quorum Writes**: Ensures strong consistency at the cost of potential write availability
- **Eventual Consistency**: Maintains high read availability with background consistency reconciliation

### Performance vs. Durability
- **Write-Ahead Logging (WAL)**: Ensures data durability with minimal write latency
- **In-Memory Storage**: Provides fast access with periodic disk snapshots

## External Libraries
- gRPC
- Protocol Buffers
- Murmur3 Hash Function
- UUID
- Consistent Hashing Library