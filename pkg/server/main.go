package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    "syscall"
    "time"
    "unicode"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/reflection"
    "google.golang.org/grpc/status"

    "Distributed-Key-Value-Store/internal/storage"
    pb "Distributed-Key-Value-Store/kvstore/proto"
    "Distributed-Key-Value-Store/pkg/consistenthash"

    "github.com/google/uuid"
)

// Define replicaResponse at the package level
type replicaResponse struct {
    nodeID    string
    value     string
    timestamp uint64
    exists    bool
    err       error
}

// HLC represents a Hybrid Logical Clock
type HLC struct {
    mu           sync.Mutex
    physicalTime uint64 // Physical component (from system clock)
    logicalTime  uint32 // Logical component (counter)
    nodeID       uint32 // Node identifier for uniqueness
}

// NewHLC creates a new hybrid logical clock
func NewHLC(nodeID uint32) *HLC {
    return &HLC{
        physicalTime: uint64(time.Now().UnixNano() / 1000000), // Millisecond precision
        logicalTime:  0,
        nodeID:       nodeID,
    }
}

// Now returns the current timestamp, updating the clock
func (h *HLC) Now() uint64 {
    h.mu.Lock()
    defer h.mu.Unlock()

    // Get current physical time in milliseconds
    now := uint64(time.Now().UnixNano() / 1000000)

    // If physical time went backwards or is the same, increment logical clock
    if now <= h.physicalTime {
        h.logicalTime++
    } else {
        // Physical time advanced, reset logical component
        h.physicalTime = now
        h.logicalTime = 0
    }

    // Encode timestamp: 48 bits physical time + 16 bits logical counter + 32 bits node ID
    // This gives us uniqueness and total ordering
    return h.encode()
}

// Update updates the clock based on a received timestamp
func (h *HLC) Update(receivedTimestamp uint64) uint64 {
    h.mu.Lock()
    defer h.mu.Unlock()

    // Decode received timestamp
    receivedPhysical, receivedLogical, _ := h.decode(receivedTimestamp)

    // Get current physical time
    now := uint64(time.Now().UnixNano() / 1000000)

    // Update physical time to max of all three
    h.physicalTime = max(h.physicalTime, receivedPhysical, now)

    // If physical times are equal, increment logical clock
    if h.physicalTime == receivedPhysical {
        h.logicalTime = maxUint32(h.logicalTime+1, receivedLogical+1)
    } else if h.physicalTime == now {
        // Our physical time is most recent, increment logical clock
        h.logicalTime++
    } else {
        // We took the received physical time, reset logical to received+1
        h.logicalTime = receivedLogical + 1
    }

    return h.encode()
}

// encode creates a 64-bit timestamp from the components
func (h *HLC) encode() uint64 {
    // 48 bits for physical time, 16 bits for logical counter
    return (h.physicalTime << 16) | uint64(h.logicalTime)
}

// decode extracts components from a 64-bit timestamp
func (h *HLC) decode(timestamp uint64) (physical uint64, logical uint32, nodeID uint32) {
    physical = timestamp >> 16
    logical = uint32(timestamp & 0xFFFF)
    return physical, logical, h.nodeID
}

// max returns the maximum of multiple uint values
func max(a uint64, b ...uint64) uint64 {
    result := a
    for _, v := range b {
        if v > result {
            result = v
        }
    }
    return result
}

// maxUint32 returns the maximum of multiple uint32 values
func maxUint32(a uint32, b ...uint32) uint32 {
    result := a
    for _, v := range b {
        if v > result {
            result = v
        }
    }
    return result
}

// Server represents a node in the distributed key-value store
type server struct {
    pb.UnimplementedKVStoreServer
    pb.UnimplementedNodeInternalServer

    nodeID string
    store  *storage.Storage
    ring   *consistenthash.Ring
    mu     sync.RWMutex

    // Hybrid logical clock for timestamp ordering
    hlc *HLC

    // Node management
    peers     map[string]string                // Maps nodeID to address
    clients   map[string]pb.NodeInternalClient // Internal clients for replication
    kvClients map[string]pb.KVStoreClient      // KV clients for fetching data

    // Configuration
    syncInterval      time.Duration
    heartbeatInterval time.Duration
    replicationFactor int

    stopChan chan struct{}

    // Rebalancing configuration
    rebalanceConfig RebalanceConfig
    rebalancing     atomic.Bool

    ctx    context.Context
    cancel context.CancelFunc
}

// Validation constants
const (
    maxKeyLength   = 256         // Maximum key length in bytes
    maxValueLength = 1024 * 1024 // Maximum value length (1MB)
)

// RebalanceConfig defines settings for rebalancing operations
type RebalanceConfig struct {
    BatchSize     int           // Number of keys per batch
    BatchTimeout  time.Duration // Max time per batch
    MaxConcurrent int           // Max concurrent transfers
    RetryAttempts int           // Number of retry attempts
}

// NewServer initializes a new server instance
func NewServer(nodeID string, replicationFactor int, virtualNodes int) (*server, error) {
    // Ensure nodeID is prefixed with "node-" if it's a raw UUID
    if _, err := uuid.Parse(nodeID); err == nil {
        nodeID = fmt.Sprintf("node-%s", nodeID)
    }

    log.Printf("Initializing server with nodeID: %s", nodeID)

    // Initialize the store with a numeric ID for WAL
    numericID := consistenthash.HashString(nodeID)
    store := storage.NewStorage(numericID, replicationFactor)

    // Create hybrid logical clock with node ID as part of the seed
    hlc := NewHLC(uint32(numericID & 0xFFFFFFFF))

    s := &server{
        nodeID:            nodeID,
        store:             store,
        ring:              consistenthash.NewRing(virtualNodes),
        peers:             make(map[string]string),
        clients:           make(map[string]pb.NodeInternalClient),
        kvClients:         make(map[string]pb.KVStoreClient),
        syncInterval:      5 * time.Second,
        heartbeatInterval: 1 * time.Second,
        replicationFactor: replicationFactor,
        stopChan:          make(chan struct{}),
        hlc:               hlc,
        rebalanceConfig: RebalanceConfig{
            BatchSize:     100,
            BatchTimeout:  30 * time.Second,
            MaxConcurrent: 5,
            RetryAttempts: 3,
        },
    }

    // Add self to the consistent hash ring
    s.ring.AddNode(nodeID)
    log.Printf("Added self to ring: %s", nodeID)

    return s, nil
}

// gossipNewNode sends information about a new node to all existing peers
func (s *server) gossipNewNode(newPeerID, newAddr string) {
    // Create a copy of clients to avoid holding the lock during RPC calls
    s.mu.RLock()
    clients := make(map[string]pb.NodeInternalClient, len(s.clients))
    for id, client := range s.clients {
        clients[id] = client
    }
    s.mu.RUnlock()

    log.Printf("Gossiping new node %s@%s to %d peers", newPeerID, newAddr, len(clients))

    // Also gossip self to the new node if it's not already connected
    if newPeerID != s.nodeID {
        peerID := strings.TrimPrefix(newPeerID, "node-")
        s.mu.RLock()
        _, alreadyConnected := s.clients[peerID]
        s.mu.RUnlock()

        if !alreadyConnected {
            log.Printf("Establishing connection with new node %s@%s", newPeerID, newAddr)
            if err := s.addPeer(newPeerID, newAddr); err != nil {
                log.Printf("Failed to connect to new node %s: %v", newPeerID, err)
            }
        }
    }

    // Gossip to all existing peers
    for peerID, client := range clients {
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        _, err := client.AddNode(ctx, &pb.AddNodeRequest{
            NodeId: newPeerID,
            Addr:   newAddr,
        })
        cancel()

        if err != nil {
            log.Printf("Failed to gossip new node %s to peer %s: %v", newPeerID, peerID, err)
        } else {
            log.Printf("Successfully gossiped new node %s to peer %s", newPeerID, peerID)
        }
    }
}

// AddNode handles new node additions from gossip
func (s *server) AddNode(ctx context.Context, req *pb.AddNodeRequest) (*pb.Empty, error) {
    // Don't add ourselves
    if req.NodeId == s.nodeID {
        return &pb.Empty{}, nil
    }

    s.mu.Lock()
    // Check if we already know about this node
    _, exists := s.peers[req.NodeId]
    s.mu.Unlock()

    if !exists {
        log.Printf("Adding new peer via gossip: %s@%s", req.NodeId, req.Addr)

        // Add the new peer
        if err := s.addPeer(req.NodeId, req.Addr); err != nil {
            log.Printf("Failed to add gossiped node %s: %v", req.NodeId, err)
            return &pb.Empty{}, nil
        }

        // Forward this information to all other peers (except the one that sent it to us)
        // This ensures full mesh connectivity
        go s.gossipNewNode(req.NodeId, req.Addr)
    }

    return &pb.Empty{}, nil
}

// addPeer adds a peer node with retry logic for establishing connections
func (s *server) addPeer(peerID string, addr string) error {
    // Format node ID consistently
    if _, err := uuid.Parse(peerID); err == nil {
        peerID = fmt.Sprintf("node-%s", peerID)
    }

    log.Printf("Adding peer %s at address %s", peerID, addr)

    s.mu.Lock()
    defer s.mu.Unlock()

    // Check if we already have this peer
    if _, exists := s.peers[peerID]; exists {
        log.Printf("Peer %s already exists in peer list", peerID)
        return nil
    }

    // Add to peer list and ring
    s.peers[peerID] = addr
    s.ring.AddNode(peerID)

    // Establish gRPC connection with retry logic
    var conn *grpc.ClientConn
    var err error
    for attempt := 0; attempt < 3; attempt++ {
        conn, err = grpc.Dial(addr, grpc.WithInsecure())
        if err == nil {
            break
        }
        log.Printf("Retry %d: Failed to connect to peer %s: %v", attempt+1, peerID, err)
        time.Sleep(time.Second)
    }
    if err != nil {
        // Remove from peers and ring if connection failed
        delete(s.peers, peerID)
        s.ring.RemoveNode(peerID)
        return fmt.Errorf("failed to connect to peer %s after retries: %v", peerID, err)
    }

    // Create clients for both internal and KV operations
    internalClient := pb.NewNodeInternalClient(conn)
    kvClient := pb.NewKVStoreClient(conn)

    // Store in maps using consistent key (UUID without "node-" prefix)
    mapKey := strings.TrimPrefix(peerID, "node-")
    s.clients[mapKey] = internalClient
    s.kvClients[mapKey] = kvClient

    log.Printf("Successfully connected to peer %s", peerID)
    log.Printf("Current ring state: %v", s.ring)

    return nil
}

// Get implements the KVStore Get RPC method
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
    log.Printf("Get request for key %s", req.Key)

    // Get all replicas for this key
    s.mu.RLock()
    replicas := s.ring.GetReplicas(req.Key, s.replicationFactor)
    s.mu.RUnlock()

    // Only log replicas at debug level or when needed
    if log.Default().Writer() == os.Stderr {
        log.Printf("Replicas for key %s: %v", req.Key, replicas)
    }

    // Check local store first
    localValue, localTimestamp, localExists := s.store.Get(req.Key)

    // If we're not a designated replica, we shouldn't have this key
    // Just query all replicas
    if !s.isDesignatedReplica(req.Key) {
        log.Printf("This node is not a designated replica for key %s, querying all replicas", req.Key)
        return s.queryAllReplicasAndRepair(ctx, req.Key, replicas)
    }

    // We are a designated replica
    if localExists {
        
        // Even if we have the key locally, we should still check other replicas
        // to see if they have a newer version, but do it asynchronously
        go s.asyncReadRepair(req.Key, localTimestamp, replicas)

        return &pb.GetResponse{
            Value:     localValue,
            Timestamp: localTimestamp,
            Exists:    true,
        }, nil
    }

    // We're a designated replica but don't have the key
    // This might be due to a node failure or rebalancing
    // Query all replicas and perform read repair
    log.Printf("Key %s not found locally despite being a designated replica, querying all replicas", req.Key)
    return s.queryAllReplicasAndRepair(ctx, req.Key, replicas)
}

// queryAllReplicasAndRepair queries all replicas for a key, returns the most recent value,
// and updates any stale replicas
func (s *server) queryAllReplicasAndRepair(ctx context.Context, key string, replicas []string) (*pb.GetResponse, error) {
    // Create a channel to collect results
    resultChan := make(chan replicaResponse, len(replicas))

    // Query all replicas concurrently
    queriesStarted := 0
    for _, replicaID := range replicas {
        // Skip self as we've already checked locally
        if replicaID == s.nodeID {
            continue
        }

        queriesStarted++
        go func(nodeID string) {
            peerID := strings.TrimPrefix(nodeID, "node-")

            s.mu.RLock()
            kvClient, ok := s.kvClients[peerID]
            s.mu.RUnlock()

            if !ok {
                resultChan <- replicaResponse{
                    nodeID: nodeID,
                    err:    fmt.Errorf("no client connection for node %s", nodeID),
                }
                return
            }

            ctxTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
            defer cancel()

            resp, err := kvClient.Get(ctxTimeout, &pb.GetRequest{Key: key})
            if err != nil {
                resultChan <- replicaResponse{
                    nodeID: nodeID,
                    err:    err,
                }
                return
            }

            resultChan <- replicaResponse{
                nodeID:    nodeID,
                value:     resp.Value,
                timestamp: resp.Timestamp,
                exists:    resp.Exists,
                err:       nil,
            }
        }(replicaID)
    }

    // Process results and find the most recent value
    var mostRecentValue string
    var mostRecentTimestamp uint64
    var mostRecentExists bool
    var staleReplicas []replicaResponse

    // Check if we have the key locally
    localValue, localTimestamp, localExists := s.store.Get(key)
    if localExists {
        mostRecentValue = localValue
        mostRecentTimestamp = localTimestamp
        mostRecentExists = true
    }

    // Wait for all queries to complete
    for i := 0; i < queriesStarted; i++ {
        select {
        case result := <-resultChan:
            if result.err != nil {
                log.Printf("Error querying replica %s: %v", result.nodeID, result.err)
                continue
            }

            if !result.exists {
                continue
            }

            log.Printf("Replica %s has key %s with timestamp %d", result.nodeID, key, result.timestamp)

            // If this is the first result or has a newer timestamp
            if !mostRecentExists || result.timestamp > mostRecentTimestamp {
                // If we already had a value but this one is newer, the previous one was stale
                if mostRecentExists {
                    staleReplicas = append(staleReplicas, replicaResponse{
                        nodeID:    s.nodeID, // Our local copy is stale
                        value:     mostRecentValue,
                        timestamp: mostRecentTimestamp,
                    })
                }

                mostRecentValue = result.value
                mostRecentTimestamp = result.timestamp
                mostRecentExists = true
            } else if result.timestamp < mostRecentTimestamp {
                // This replica has a stale version
                staleReplicas = append(staleReplicas, result)
            }

        case <-ctx.Done():
            log.Printf("Context deadline exceeded while querying replicas for key %s", key)
            break
        }
    }

    // If we found the key, update any stale replicas (including our local copy if needed)
    if mostRecentExists {
        // Update our local copy if it's stale or missing
        if !localExists || localTimestamp < mostRecentTimestamp {
            log.Printf("Updating local copy of key %s with newer timestamp %d", key, mostRecentTimestamp)
            // Update our HLC with the received timestamp
            s.hlc.Update(mostRecentTimestamp)
            s.store.Store(key, mostRecentValue, mostRecentTimestamp)
        }

        // Repair stale replicas asynchronously
        if len(staleReplicas) > 0 {
            go s.repairStaleReplicas(key, mostRecentValue, mostRecentTimestamp, staleReplicas)
        }

        return &pb.GetResponse{
            Value:     mostRecentValue,
            Timestamp: mostRecentTimestamp,
            Exists:    true,
        }, nil
    }

    // Key not found on any replica
    log.Printf("Key %s not found on any replica", key)
    return &pb.GetResponse{Exists: false}, nil
}

// asyncReadRepair checks other replicas for newer versions of a key
// and updates stale replicas asynchronously
func (s *server) asyncReadRepair(key string, localTimestamp uint64, replicas []string) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var staleReplicas []replicaResponse
    var mostRecentValue string
    var mostRecentTimestamp uint64 = localTimestamp

    // Query all replicas except self
    for _, replicaID := range replicas {
        if replicaID == s.nodeID {
            continue
        }

        peerID := strings.TrimPrefix(replicaID, "node-")

        s.mu.RLock()
        kvClient, ok := s.kvClients[peerID]
        s.mu.RUnlock()

        if !ok {
            log.Printf("No client connection for node %s", replicaID)
            continue
        }

        ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
        resp, err := kvClient.Get(ctxTimeout, &pb.GetRequest{Key: key})
        cancel()

        if err != nil {
            log.Printf("Error querying replica %s: %v", replicaID, err)
            continue
        }

        if !resp.Exists {
            // This replica should have the key but doesn't
            staleReplicas = append(staleReplicas, replicaResponse{
                nodeID: replicaID,
                exists: false,
            })
            continue
        }

        // Check if this replica has a newer or older version
        if resp.Timestamp > mostRecentTimestamp {
            // Found a newer version
            log.Printf("Found newer version of key %s on replica %s (timestamp %d > %d)",
                key, replicaID, resp.Timestamp, mostRecentTimestamp)

            // Our local copy is now considered stale
            localValue, _, _ := s.store.Get(key)
            staleReplicas = append(staleReplicas, replicaResponse{
                nodeID:    s.nodeID,
                value:     localValue,
                timestamp: mostRecentTimestamp,
                exists:    true,
            })

            mostRecentValue = resp.Value
            mostRecentTimestamp = resp.Timestamp
        } else if resp.Timestamp < mostRecentTimestamp {
            // This replica has a stale version
            staleReplicas = append(staleReplicas, replicaResponse{
                nodeID:    replicaID,
                value:     resp.Value,
                timestamp: resp.Timestamp,
                exists:    true,
            })
        }
    }

    // If we found a newer version, update our local copy and our HLC
    if mostRecentTimestamp > localTimestamp {
        log.Printf("Updating local copy of key %s with newer timestamp %d", key, mostRecentTimestamp)
        s.hlc.Update(mostRecentTimestamp)
        s.store.Store(key, mostRecentValue, mostRecentTimestamp)
    }

    // Repair any stale replicas
    if len(staleReplicas) > 0 {
        s.repairStaleReplicas(key, mostRecentValue, mostRecentTimestamp, staleReplicas)
    }
}

// repairStaleReplicas updates stale replicas with the most recent value
func (s *server) repairStaleReplicas(key, value string, timestamp uint64, staleReplicas []replicaResponse) {
    log.Printf("Repairing %d stale replicas for key %s", len(staleReplicas), key)

    for _, replica := range staleReplicas {
        // Skip self as we've already updated locally
        if replica.nodeID == s.nodeID {
            continue
        }

        log.Printf("Repairing stale replica %s for key %s (timestamp %d)",
            replica.nodeID, key, replica.timestamp)

        // Use the existing replication mechanism
        go func(nodeID string) {
            ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
            defer cancel()

            if err := s.replicateKey(ctx, key, value, timestamp, nodeID); err != nil {
                log.Printf("Failed to repair replica %s: %v", nodeID, err)
            } else {
                log.Printf("Successfully repaired replica %s for key %s", nodeID, key)
            }
        }(replica.nodeID)
    }
}

// isValidKey checks if a key contains only valid characters
func isValidKey(key string) bool {
    for _, r := range key {
        if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '-' && r != '_' && r != '.' && r != '/' {
            return false
        }
    }
    return true
}

// validateKeyValue ensures key and value meet length and content requirements
func validateKeyValue(key, value string) error {
    if len(key) == 0 {
        return status.Error(codes.InvalidArgument, "key cannot be empty")
    }
    if len(key) > maxKeyLength {
        return status.Errorf(codes.InvalidArgument, "key length exceeds maximum of %d bytes", maxKeyLength)
    }
    if !isValidKey(key) {
        return status.Error(codes.InvalidArgument, "key contains invalid characters")
    }
    if len(value) > maxValueLength {
        return status.Errorf(codes.InvalidArgument, "value length exceeds maximum of %d bytes", maxValueLength)
    }
    return nil
}

// Put implements the KVStore Put RPC method
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
    if err := validateKeyValue(req.Key, req.Value); err != nil {
        return nil, err
    }

    // Get existing value before storing new one
    oldValue, _, hadOldValue := s.store.Get(req.Key)

    // Generate timestamp using hybrid logical clock
    timestamp := s.hlc.Now()

    // Store locally
    if err := s.store.Store(req.Key, req.Value, timestamp); err != nil {
        log.Printf("Error storing key %s: %v", req.Key, err)
        return nil, err
    }

    // Replicate synchronously with quorum
    quorumSize := (s.replicationFactor / 2) + 1

    // Synchronously replicate to other nodes
    if err := s.replicateWithQuorum(ctx, req.Key, req.Value, timestamp, quorumSize); err != nil {
        // If we can't achieve quorum, fail the write
        return nil, status.Errorf(codes.Unavailable, "failed to achieve write quorum: %v", err)
    }

    return &pb.PutResponse{
        OldValue:    string(oldValue),
        HadOldValue: hadOldValue,
    }, nil
}

// replicateWithQuorum replicates data to nodes and waits for quorum acknowledgment
func (s *server) replicateWithQuorum(ctx context.Context, key, value string, timestamp uint64, quorumSize int) error {
    s.mu.RLock()
    replicas := s.ring.GetReplicas(key, s.replicationFactor)
    s.mu.RUnlock()

    log.Printf("Replicating key %s to %d replicas with quorum size %d", key, len(replicas), quorumSize)

    // We already have 1 successful write (local)
    successCount := 1

    // If we only need 1 node for quorum, we're already done (local write succeeded)
    if quorumSize <= 1 {
        return nil
    }

    // Create a channel to collect results
    type replicaResult struct {
        nodeID string
        err    error
    }
    resultChan := make(chan replicaResult, len(replicas))

    // Create a timeout context for the entire replication operation
    replicationCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    // Track how many replications we've initiated
    replicationsStarted := 0

    // Send replication requests to all replicas except self
    for _, nodeID := range replicas {
        if nodeID == s.nodeID {
            continue
        }

        replicationsStarted++

        go func(targetNodeID string) {
            err := s.replicateKey(replicationCtx, key, value, timestamp, targetNodeID)
            resultChan <- replicaResult{nodeID: targetNodeID, err: err}
        }(nodeID)
    }

    // Wait for enough successful replications or context timeout
    for i := 0; i < replicationsStarted; i++ {
        select {
        case result := <-resultChan:
            if result.err == nil {
                successCount++
                log.Printf("Successful replication to node %s, success count: %d/%d",
                    result.nodeID, successCount, quorumSize)

                // If we've reached quorum, we can return success
                if successCount >= quorumSize {
                    log.Printf("Achieved write quorum (%d/%d) for key %s",
                        successCount, quorumSize, key)
                    return nil
                }
            } else {
                log.Printf("Failed to replicate to node %s: %v", result.nodeID, result.err)
            }
        case <-replicationCtx.Done():
            return fmt.Errorf("replication timed out after achieving %d/%d successful replicas",
                successCount, quorumSize)
        }
    }

    // If we get here, we didn't achieve quorum
    return fmt.Errorf("failed to achieve write quorum, only %d/%d successful replicas",
        successCount, quorumSize)
}

// isDesignatedReplica checks if this node should hold a key
func (s *server) isDesignatedReplica(key string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()

    replicas := s.ring.GetReplicas(key, s.replicationFactor)
    for _, replicaID := range replicas {
        if replicaID == s.nodeID {
            return true
        }
    }
    return false
}

// Replicate handles incoming replication requests
func (s *server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
    // Check if this node is a designated replica
    if !s.isDesignatedReplica(req.Key) {
        return &pb.ReplicateResponse{
            Success: false,
            Error:   "node is not a designated replica for this key",
        }, nil
    }

    // Existing validation and storage logic...
    if err := validateKeyValue(req.Key, req.Value); err != nil {
        return nil, err
    }

    // Update our hybrid logical clock based on the received timestamp
    s.hlc.Update(req.Timestamp)

    log.Printf("Received replication for key %s with timestamp %d", req.Key, req.Timestamp)
    if err := s.store.Store(req.Key, req.Value, req.Timestamp); err != nil {
        return &pb.ReplicateResponse{Success: false, Error: err.Error()}, nil
    }

    return &pb.ReplicateResponse{Success: true}, nil
}

// GetRingState returns the current ring state
func (s *server) GetRingState(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    nodes := make(map[string]bool)
    for id := range s.peers {
        nodes[id] = true
    }
    nodes[s.nodeID] = true

    return &pb.RingStateResponse{
        Version:   uint64(time.Now().UnixNano()),
        Nodes:     nodes,
        UpdatedAt: time.Now().Unix(),
    }, nil
}

// replicateToNodes is now used only for background rebalancing, not for primary writes
func (s *server) replicateToNodes(key, value string, timestamp uint64) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    replicas := s.ring.GetReplicas(key, s.replicationFactor)
    log.Printf("Background replicating key %s to %d replicas: %v", key, len(replicas), replicas)

    req := &pb.ReplicateRequest{
        Key:       key,
        Value:     value,
        Timestamp: timestamp,
    }

    for _, nodeID := range replicas {
        if nodeID == s.nodeID {
            continue
        }

        mapKey := strings.TrimPrefix(nodeID, "node-")
        client, ok := s.clients[mapKey]
        if !ok {
            log.Printf("No client connection for node %s", nodeID)
            continue
        }

        log.Printf("Background replicating key %s to node %s", key, nodeID)
        go func(c pb.NodeInternalClient, targetNodeID string) {
            ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
            defer cancel()

            resp, err := c.Replicate(ctx, req)
            if err != nil {
                log.Printf("Failed to replicate to node %s: %v", targetNodeID, err)
            } else if !resp.Success {
                log.Printf("Replication rejected by node %s: %s", targetNodeID, resp.Error)
            } else {
                log.Printf("Successfully replicated to node %s", targetNodeID)
            }
        }(client, nodeID)
    }
}

// rebalanceRing redistributes keys after ring changes
func (s *server) rebalanceRing() {
    if !s.rebalancing.CompareAndSwap(false, true) {
        log.Println("Rebalancing already in progress")
        return
    }
    defer s.rebalancing.Store(false)

    s.mu.RLock()
    keys := s.store.GetKeys()
    s.mu.RUnlock()

    log.Printf("Starting rebalancing for %d keys", len(keys))

    // Create batches
    sort.Strings(keys)
    var batches [][]string
    for i := 0; i < len(keys); i += s.rebalanceConfig.BatchSize {
        end := i + s.rebalanceConfig.BatchSize
        if end > len(keys) {
            end = len(keys)
        }
        batches = append(batches, keys[i:end])
    }

    // Process batches concurrently
    sem := make(chan struct{}, s.rebalanceConfig.MaxConcurrent)
    var wg sync.WaitGroup
    for i, batch := range batches {
        wg.Add(1)
        sem <- struct{}{}
        go func(batchIndex int, batchKeys []string) {
            defer func() {
                <-sem
                wg.Done()
            }()
            if err := s.processBatch(batchKeys); err != nil {
                log.Printf("Batch %d failed: %v", batchIndex, err)
            }
        }(i, batch)
    }
    wg.Wait()
}

// processBatch handles rebalancing for a batch of keys
func (s *server) processBatch(keys []string) error {
    ctx, cancel := context.WithTimeout(context.Background(), s.rebalanceConfig.BatchTimeout)
    defer cancel()

    for _, key := range keys {
        replicas := s.ring.GetReplicas(key, s.replicationFactor)
        value, timestamp, exists := s.store.Get(key)
        if !exists {
            continue
        }

        for _, replicaID := range replicas {
            if replicaID == s.nodeID {
                continue
            }

            var lastErr error
            for attempt := 0; attempt < s.rebalanceConfig.RetryAttempts; attempt++ {
                if err := s.replicateKey(ctx, key, string(value), timestamp, replicaID); err != nil {
                    lastErr = err
                    time.Sleep(time.Duration(1<<uint(attempt)) * 100 * time.Millisecond)
                    continue
                }
                lastErr = nil
                break
            }
            if lastErr != nil {
                log.Printf("Failed to replicate key %s to %s: %v", key, replicaID, lastErr)
            }
        }
    }
    return nil
}

// replicateKey sends a replication request to a specific node
func (s *server) replicateKey(ctx context.Context, key, value string, timestamp uint64, nodeID string) error {
    if err := validateKeyValue(key, value); err != nil {
        return err
    }

    peerID := strings.TrimPrefix(nodeID, "node-")
    s.mu.RLock()
    client, ok := s.clients[peerID]
    s.mu.RUnlock()

    if !ok {
        return fmt.Errorf("no client connection to node %s", nodeID)
    }

    req := &pb.ReplicateRequest{Key: key, Value: value, Timestamp: timestamp}
    resp, err := client.Replicate(ctx, req)
    if err != nil {
        return err
    }
    if !resp.Success {
        return fmt.Errorf("replication rejected: %s", resp.Error)
    }

    log.Printf("Replicated key %s to node %s", key, nodeID)
    return nil
}

// Heartbeat implements the NodeInternal Heartbeat RPC method
func (s *server) Heartbeat(ctx context.Context, ping *pb.Ping) (*pb.Pong, error) {
    return &pb.Pong{
        NodeId:    uint32(consistenthash.HashString(s.nodeID)),
        Timestamp: uint64(time.Now().UnixNano()),
    }, nil
}

// startHeartbeat periodically checks peer health
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

// checkPeers sends heartbeat requests to all peers
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
            log.Printf("Heartbeat failed for peer %s: %v", peerID, err)
            s.handlePeerFailure(peerID)
        }
    }
}

// handlePeerFailure removes a failed peer and triggers rebalancing
func (s *server) handlePeerFailure(peerID string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    log.Printf("Handling failure of peer %s", peerID)
    delete(s.peers, peerID)
    delete(s.clients, peerID)

    nodeID := peerID
    if !strings.HasPrefix(peerID, "node-") {
        nodeID = fmt.Sprintf("node-%s", peerID)
    }

    s.ring.RemoveNode(nodeID)
    log.Printf("Removed node %s from ring", nodeID)
    go s.rebalanceRing()
}

// Stop gracefully shuts down the server
func (s *server) Stop() {
    log.Println("Shutting down server...")
    close(s.stopChan)
    if s.cancel != nil {
        s.cancel()
    }

    s.mu.Lock()
    for _, client := range s.clients {
        if closer, ok := client.(interface{ Close() error }); ok {
            _ = closer.Close()
        }
    }
    s.mu.Unlock()

    if err := s.store.Shutdown(); err != nil {
        log.Printf("Error during store shutdown: %v", err)
    }
    log.Println("Server shutdown completed")
}

// Start initializes background services and performs initial peer discovery
func (s *server) Start() {
    ctx, cancel := context.WithCancel(context.Background())
    s.ctx = ctx
    s.cancel = cancel

    go s.startHeartbeat()

    // Perform initial peer discovery and gossip
    go func() {
        // Wait a short time for all connections to establish
        time.Sleep(2 * time.Second)

        s.mu.RLock()
        peers := make(map[string]string)
        for id, addr := range s.peers {
            peers[id] = addr
        }
        s.mu.RUnlock()

        // Gossip all known peers to ensure full mesh connectivity
        for peerID, addr := range peers {
            s.gossipNewNode(peerID, addr)
        }

        // After all peers are connected, trigger rebalancing
        s.rebalanceRing()
    }()
}

// Main function to start the server
func main() {
    nodeID := flag.String("id", "", "Node ID (optional, will be formatted as node-{id})")
    addr := flag.String("addr", ":50051", "Address to listen on")
    peerList := flag.String("peers", "", "Comma-separated list of peer addresses in format 'id@address'")
    syncInterval := flag.Duration("sync-interval", 5*time.Minute, "Anti-entropy sync interval")
    heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "Heartbeat check interval")
    replicationFactor := flag.Int("replication-factor", 3, "Number of replicas per key")
    virtualNodes := flag.Int("virtual-nodes", 10, "Number of virtual nodes per physical node")
    flag.Parse()

    // Generate UUID if not provided
    if *nodeID == "" {
        generatedUUID, err := uuid.NewRandom()
        if err != nil {
            log.Fatalf("Failed to generate UUID: %v", err)
        }
        *nodeID = generatedUUID.String()
        log.Printf("Generated node ID: %s", *nodeID)
    }

    srv, err := NewServer(*nodeID, *replicationFactor, *virtualNodes)
    if err != nil {
        log.Fatalf("Failed to create server: %v", err)
    }

    srv.syncInterval = *syncInterval
    srv.heartbeatInterval = *heartbeatInterval

    // Add peers
    if *peerList != "" {
        peers := strings.Split(*peerList, ",")
        for _, peer := range peers {
            parts := strings.Split(peer, "@")
            if len(parts) != 2 {
                log.Printf("Invalid peer format %s, expected 'id@address'", peer)
                continue
            }
            peerID, addr := parts[0], parts[1]
            if err := srv.addPeer(peerID, addr); err != nil {
                log.Printf("Failed to add peer %s at %s: %v", peerID, addr, err)
            }
        }
    }

    // Start gRPC server
    lis, err := net.Listen("tcp", *addr)
    if err != nil {
        log.Fatalf("Failed to listen on %s: %v", *addr, err)
    }

    srv.Start()
    grpcServer := grpc.NewServer()
    pb.RegisterKVStoreServer(grpcServer, srv)
    pb.RegisterNodeInternalServer(grpcServer, srv)
    reflection.Register(grpcServer)

    go func() {
        log.Printf("Node %s listening on %s", *nodeID, *addr)
        if err := grpcServer.Serve(lis); err != nil {
            log.Fatalf("Failed to serve: %v", err)
        }
    }()

    // Handle shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    log.Println("Received shutdown signal")
    grpcServer.GracefulStop()
    srv.Stop()
    os.Exit(0)
}

