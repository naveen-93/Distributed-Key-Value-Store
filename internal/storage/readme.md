# Distributed Storage Engine

## Overview
This package implements a high-performance, distributed key-value storage engine with built-in replication, durability guarantees, and automatic data lifecycle management. The storage engine is designed for distributed systems and implements a Write-Ahead Log (WAL) and snapshot mechanism to ensure data durability and quick recovery in case of failures.

## Features
- **Distributed architecture**: Supports multiple storage nodes with configurable replication.
- **Durable write-ahead logging**: Prevents data loss during crashes or restarts.
- **Snapshot mechanism**: Maintains system state with periodic snapshots for efficient recovery.
- **Hybrid logical clock**: Guarantees event ordering in distributed environments.
- **TTL support**: Automatic expiration and cleanup of entries.
- **Garbage collection**: Efficiently manages tombstones and expired entries.
- **Concurrent access**: Thread-safe operations with fine-grained locking.
- **Configurable replication factor**: Adjustable per deployment needs.
- **Automatic WAL compaction**: Prevents unbounded storage growth.

## Architecture
### Storage
Manages the in-memory key-value store and coordinates operations. Each storage node has a unique ID and maintains its own state:
- **In-memory map**: Fast access to current key-value data.
- **Logical clock**: Ensures consistent ordering of operations.
- **Thread-safe access**: Concurrent read/write support.

### Write-Ahead Log (WAL)
Provides durability by recording all operations before they're applied to the in-memory store:
- **Append-only log file**: Records all PUT and DELETE operations.
- **Batched writes**: Buffers operations for improved performance.
- **Background sync**: Ensures data is persisted to disk.
- **Checksum verification**: Ensures data integrity.

### Snapshot Mechanism
Periodically captures the entire store state:
- **Point-in-time snapshots**: Complete state captures at specific timestamps.
- **Retention policy**: Keeps last N snapshots (configurable).
- **Checksum validation**: Ensures snapshot integrity.
- **Atomic writes**: Prevents corruption during snapshot creation.

### TTL Management
Supports automatic expiration of entries:
- **Configurable TTL**: Set per key.
- **Background cleanup**: Periodically removes expired entries.
- **Tombstone management**: Handles deleted entries efficiently.

## API Usage
### Initialization
```go
// Create a new storage instance with node ID and optional replication factor
storage := storage.NewStorage(nodeID, replicationFactor)
```

### Basic Operations
```go
// Store a value with an optional TTL (in seconds)
err := storage.Store(key, value, timestamp, ttl)

// Retrieve a value
value, timestamp, exists := storage.Get(key)

// Delete a value
err := storage.Delete(key)

// Get all keys
keys := storage.GetKeys()
```

### Lifecycle Management
```go
// Shutdown the storage engine gracefully
err := storage.Shutdown()
```

## Implementation Details
### Timestamp Generation
The system uses a hybrid logical clock that combines:
- Node ID (16 bits).
- Physical time (32 bits).
- Logical counter (16 bits).
This ensures unique, monotonically increasing timestamps even when physical clocks are not perfectly synchronized.

### WAL Format
Each WAL entry is encoded as a space-separated string with the format:
```
OPERATION KEY VALUE TIMESTAMP CREATED_AT CHECKSUM TTL
```
Example:
```
PUT mykey myvalue 1234567890 1600000000 3876543210 3600
```

### Snapshot Format
Snapshots are stored as JSON files with the filename pattern:
```
snapshots/snapshot-{nodeID}-{timestamp}.dat
```
Each snapshot includes the complete store state and a checksum for validation.

### Recovery Process
1. Load the most recent valid snapshot (if available).
2. Replay WAL entries with timestamps newer than the snapshot.
3. Apply entries in timestamp order.
4. Remove entries with expired TTLs.

### Compaction Strategy
1. WAL compaction occurs when the log exceeds the configured size limit (default: 100MB).
2. Only the most recent N snapshots are retained (default: 3).
3. Old tombstone entries are eventually removed during compaction.

## Configuration Constants
```go
const (
    maxSnapshots = 3                 // Number of snapshots to retain
    maxWALSize   = 100 * 1024 * 1024 // Maximum WAL size (100MB) before compaction
    snapshotPath = "snapshots"       // Directory for snapshot storage
)
```

## Maintenance Operations
### Garbage Collection
```go
// Remove a key completely from storage (beyond tombstoning)
err := storage.GarbageCollect(key)
```

### Health Metrics
```go
// Storage engine metrics
type Metrics struct {
    TombstoneCount   int64  // Number of tombstoned entries
    ActiveKeyCount   int64  // Number of active entries
    LastCompactionTS int64  // Timestamp of last compaction
    WALSize          int64  // Current WAL size
}
```

## Best Practices
1. Set appropriate TTL values to automatically manage data lifecycle.
2. Monitor WAL size and compaction frequency.
3. Consider storage node capacity when setting replication factor.
4. Ensure sufficient disk space for WAL and snapshots.
5. Implement proper error handling for all storage operations.
6. Perform graceful shutdown to ensure all data is persisted.

## Recovery Scenarios
1. **Node Restart**: Automatically recovers from latest snapshot and WAL.
2. **Corrupted WAL**: Recovers from the latest valid snapshot, potentially losing recent updates.
3. **Corrupted Snapshot**: Falls back to earlier snapshots or rebuilds from WAL if necessary.
