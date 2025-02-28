
### Server

The server struct is the central component that implements:

- gRPC interfaces for client operations (`KVStoreServer`)
- Internal node-to-node communication (`NodeInternalServer`)
- Consistent hashing ring management
- Peer management and heartbeat monitoring
- Data replication and rebalancing logic

```go
type server struct {
    pb.UnimplementedKVStoreServer
    pb.UnimplementedNodeInternalServer

    nodeID string
    store  *node.Node
    ring   *consistenthash.Ring
    mu     sync.RWMutex

    // Node management
    peers   map[string]string
    clients map[string]pb.NodeInternalClient

    // Configuration
    syncInterval      time.Duration
    heartbeatInterval time.Duration
    replicationFactor int

    stopChan chan struct{}

    // Rebalancing configuration
    rebalanceConfig RebalanceConfig
    rebalancing     atomic.Bool
}
```

### Configuration Options

The server supports various configuration options:

- `syncInterval`: Time between anti-entropy synchronization
- `heartbeatInterval`: Frequency of peer health checks
- `replicationFactor`: Number of nodes to replicate each key to
- `virtualNodes`: Number of virtual nodes per physical node in the consistent hash ring

Rebalancing is also configurable:

```go
type RebalanceConfig struct {
    BatchSize     int           // Number of keys per batch
    BatchTimeout  time.Duration // Max time per batch
    MaxConcurrent int           // Max concurrent transfers
    RetryAttempts int           // Number of retry attempts
}
```

## Data Flow

### Key-Value Operations

#### Get Operation

1. When a client calls `Get(key)`:
   - The server retrieves the value, timestamp, and existence status from the local store
   - Returns this information to the client

```go
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
    value, timestamp, exists := s.store.Get(req.Key)
    return &pb.GetResponse{
        Value:     value,
        Timestamp: timestamp,
        Exists:    exists,
    }, nil
}
```

#### Put Operation

1. When a client calls `Put(key, value)`:
   - The server validates the key and value
   - Stores the key-value pair locally with a timestamp
   - Determines which nodes should replicate this data based on the consistent hash ring
   - Sends replication requests to those nodes
   - Returns the old value (if any) to the client

```go
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
    if err := validateKeyValue(req.Key, req.Value); err != nil {
        return nil, err
    }

    err := s.store.Store(req.Key, req.Value, req.Timestamp)
    if err != nil {
        return nil, err
    }

    oldValue, _, hadOldValue := s.store.Get(req.Key)
    return &pb.PutResponse{
        OldValue:    string(oldValue),
        HadOldValue: hadOldValue,
    }, nil
}
```

### Replication

When a key-value pair needs to be replicated:

1. The source node calculates which peers should store the data using the consistent hash ring
2. Replication requests are sent to the designated nodes
3. Receiving nodes validate they should be replicas for the key
4. Receiving nodes store the data if they don't have it or have an older version
5. Acknowledgments are sent back to the source node

```go
func (s *server) replicateToNodes(key, value string, timestamp uint64) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    // Get replica nodes from ring
    hash := s.ring.HashKey(key)
    replicasSent := 1 // Count self as first replica
    currentHash := hash

    req := &pb.ReplicateRequest{
        Key:       key,
        Value:     value,
        Timestamp: timestamp,
    }

    // Send to next nodes in ring until we hit replicationFactor
    for replicasSent < s.replicationFactor {
        nextNodeID := s.ring.GetNextNode(currentHash)
        if nextNodeID == "" || nextNodeID == fmt.Sprintf("node-%s", s.nodeID) {
            break // No more nodes available or wrapped around to self
        }

        // Extract UUID from node ID format "node-{uuid}"
        peerID := strings.TrimPrefix(nextNodeID, "node-")
        if client, ok := s.clients[peerID]; ok {
            go func(c pb.NodeInternalClient, nodeID string) {
                ctx, cancel := context.WithTimeout(context.Background(), time.Second)
                defer cancel()

                resp, err := c.Replicate(ctx, req)
                if err != nil {
                    log.Printf("Failed to replicate to node %s: %v", nodeID, err)
                } else if !resp.Success {
                    log.Printf("Replication rejected by node %s: %s", nodeID, resp.Error)
                }
            }(client, nextNodeID)
            replicasSent++
        }

        currentHash = s.ring.HashKey(nextNodeID)
    }
}
```

### Node Management

#### Adding a Peer

When a peer is added:

1. The server validates the peer's UUID
2. Adds the peer to its peer list and consistent hash ring
3. Establishes a gRPC connection to the peer
4. Triggers rebalancing to ensure proper data distribution

```go
func (s *server) addPeer(peerID string, addr string) error {
    // Validate peer UUID
    if _, err := uuid.Parse(peerID); err != nil {
        return fmt.Errorf("invalid peer UUID: %v", err)
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    // Add to peer list and ring
    s.peers[peerID] = addr
    s.ring.AddNode(fmt.Sprintf("node-%s", peerID))

    // Establish gRPC connection
    conn, err := grpc.Dial(addr, grpc.WithInsecure())
    if err != nil {
        return err
    }

    s.clients[peerID] = pb.NewNodeInternalClient(conn)

    // Trigger ring rebalancing
    s.rebalanceRing()

    return nil
}
```

#### Heartbeat Mechanism

Nodes maintain health checks through a heartbeat mechanism:

1. Each node periodically checks its peers
2. If a peer fails to respond, it's considered down
3. Failed peers are removed from the ring
4. Data is rebalanced to ensure proper replication

```go
func (s *server) startHeartbeat() {
    ticker := time.NewTicker(s.heartbeatInterval)
    go func() {
        for {
            select {
            case <-ticker.C:
                s.checkPeers()
            case <-s.stopChan:
                ticker.Stop()
                return
            }
        }
    }()
}

func (s *server) checkPeers() {
    s.mu.RLock()
    peers := make(map[string]pb.NodeInternalClient)
    for id, client := range s.clients {
        peers[id] = client
    }
    s.mu.RUnlock()

    for peerID, client := range peers {
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        _, err := client.Heartbeat(ctx, &pb.Ping{
            NodeId:    uint32(consistenthash.HashString(s.nodeID)),
            Timestamp: uint64(time.Now().UnixNano()),
        })
        cancel()

        if err != nil {
            s.handlePeerFailure(peerID)
        }
    }
}
```

### Rebalancing

When the topology changes (nodes join/leave), data must be rebalanced:

1. The rebalancing process runs in batches to limit resource usage
2. Keys are sorted and processed in chunks
3. Concurrent batch processing is limited by a semaphore
4. For each key, the system:
   - Determines the new owner node
   - Replicates the key-value pair to the new owner
   - Includes retry logic for fault tolerance

```go
func (s *server) rebalanceRing() {
    if !s.rebalancing.CompareAndSwap(false, true) {
        log.Println("Rebalancing already in progress, skipping")
        return
    }
    defer s.rebalancing.Store(false)

    s.mu.RLock()
    keys := s.store.GetKeys()
    s.mu.RUnlock()

    // Sort keys for deterministic batching
    sort.Strings(keys)

    // Create batches
    var batches [][]string
    for i := 0; i < len(keys); i += s.rebalanceConfig.BatchSize {
        end := i + s.rebalanceConfig.BatchSize
        if end > len(keys) {
            end = len(keys)
        }
        batches = append(batches, keys[i:end])
    }

    // Process batches with concurrency control
    sem := make(chan struct{}, s.rebalanceConfig.MaxConcurrent)
    var wg sync.WaitGroup
    errors := make(chan error, len(batches))

    for _, batch := range batches {
        wg.Add(1)
        sem <- struct{}{} // Acquire semaphore

        go func(batchKeys []string) {
            defer func() {
                <-sem // Release semaphore
                wg.Done()
            }()

            if err := s.processBatch(batchKeys); err != nil {
                errors <- fmt.Errorf("batch processing failed: %v", err)
            }
        }(batch)
    }

    // Wait for all batches to complete
    go func() {
        wg.Wait()
        close(errors)
    }()

    // Collect and log errors
    var errs []error
    for err := range errors {
        errs = append(errs, err)
        log.Printf("Rebalancing error: %v", err)
    }

    if len(errs) > 0 {
        log.Printf("Rebalancing completed with %d errors", len(errs))
    } else {
        log.Println("Rebalancing completed successfully")
    }
}
```

## Validation and Safety

The system implements validation for keys and values:

- Keys must follow specific character constraints
- Keys and values have maximum size limits
- Timestamps are used to resolve conflicts (last-write-wins)

```go
func isValidKey(key string) bool {
    for _, r := range key {
        if !unicode.IsLetter(r) && !unicode.IsNumber(r) &&
            r != '-' && r != '_' && r != '.' && r != '/' {
            return false
        }
    }
    return true
}

func validateKeyValue(key, value string) error {
    // Validate key
    if len(key) == 0 {
        return status.Error(codes.InvalidArgument, "key cannot be empty")
    }
    if len(key) > maxKeyLength {
        return status.Errorf(codes.InvalidArgument, "key length exceeds maximum of %d bytes", maxKeyLength)
    }
    if !isValidKey(key) {
        return status.Error(codes.InvalidArgument, "key contains invalid characters")
    }

    // Validate value
    if len(value) > maxValueLength {
        return status.Errorf(codes.InvalidArgument, "value length exceeds maximum of %d bytes", maxValueLength)
    }

    return nil
}
```

## Server Startup and Configuration

The main function handles:

1. Parsing command-line flags for configuration
2. Creating a new server instance
3. Adding peers from the provided peer list
4. Setting up the gRPC server
5. Handling graceful shutdown on signal

```go
func main() {
    nodeID := flag.String("id", "", "Node ID (optional, must be valid UUID if provided)")
    addr := flag.String("addr", ":50051", "Address to listen on")
    peerList := flag.String("peers", "", "Comma-separated list of peer addresses in format 'uuid@address'")
    syncInterval := flag.Duration("sync-interval", 5*time.Minute, "Anti-entropy sync interval")
    heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "Heartbeat check interval")
    replicationFactor := flag.Int("replication-factor", 3, "Number of replicas per key")
    virtualNodes := flag.Int("virtual-nodes", consistenthash.DefaultVirtualNodes, "Number of virtual nodes per physical node")
    flag.Parse()

    // ... (server creation, peer setup, and server start)
}
```

## Fault Tolerance

The system provides fault tolerance through multiple mechanisms:

1. Data replication across multiple nodes
2. Periodic heartbeat checks to detect node failures
3. Automatic rebalancing when topology changes
4. Retry logic for replication operations
5. Conflict resolution using timestamps (last-write-wins)

## Concurrency Control

The system uses several mechanisms for safe concurrent operation:

1. Read-write mutex for access to shared state
2. Atomic boolean for rebalancing status
3. Semaphore for controlling concurrent batch processing
4. Wait groups for synchronizing concurrent operations
5. Context with timeouts for safe RPC calls