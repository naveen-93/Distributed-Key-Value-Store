package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
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

// Add a new type to track disconnected peers for reconnection attempts
type disconnectedPeer struct {
	nodeID      string
	address     string
	lastAttempt time.Time
	attempts    int
}

// ReplicationTask represents a pending replication task
type ReplicationTask struct {
	key         string
	value       string
	timestamp   uint64
	attempts    int
	lastTry     time.Time
	targetNodes []string // List of target nodes for this replication task
}

// ReplicationMetrics tracks statistics about replication operations
type ReplicationMetrics struct {
	mu                     sync.Mutex
	totalReplications      int64
	successfulReplications int64
	failedReplications     int64
	pendingReplications    int64
	avgReplicationTimeMs   int64
	replicationTimes       []time.Duration // Recent replication times for calculating average
	maxRecentTimes         int             // Maximum number of recent times to track
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
	peers              map[string]string                // Maps nodeID to address
	clients            map[string]pb.NodeInternalClient // Internal clients for replication
	kvClients          map[string]pb.KVStoreClient      // KV clients for fetching data
	disconnectedPeers  map[string]disconnectedPeer      // Tracks peers we couldn't connect to
	disconnectedPeerMu sync.Mutex                       // Mutex for disconnected peers map

	// Replication queue and metrics
	replicationQueue     []ReplicationTask
	replicationQueueMu   sync.Mutex
	replicationQueueCond *sync.Cond
	replicationMetrics   ReplicationMetrics

	// Configuration
	syncInterval          time.Duration
	heartbeatInterval     time.Duration
	replicationFactor     int
	reconnectInterval     time.Duration // New setting for reconnect attempts
	maxReconnectAttempts  int           // Maximum number of reconnect attempts
	maxReplicationRetries int           // Maximum number of replication retries

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
		disconnectedPeers: make(map[string]disconnectedPeer),
		replicationQueue:  make([]ReplicationTask, 0),
		replicationMetrics: ReplicationMetrics{
			maxRecentTimes:   100, // Track the last 100 replication times
			replicationTimes: make([]time.Duration, 0, 100),
		},
		syncInterval:          5 * time.Second,
		heartbeatInterval:     1 * time.Second,
		reconnectInterval:     5 * time.Second, // Try to reconnect every 5 seconds
		maxReconnectAttempts:  20,              // Try to reconnect up to 20 times (100 seconds total)
		maxReplicationRetries: 5,               // Try to replicate up to 5 times
		replicationFactor:     replicationFactor,
		stopChan:              make(chan struct{}),
		hlc:                   hlc,
		rebalanceConfig: RebalanceConfig{
			BatchSize:     100,
			BatchTimeout:  30 * time.Second,
			MaxConcurrent: 5,
			RetryAttempts: 3,
		},
	}

	// Initialize the condition variable for the replication queue
	s.replicationQueueCond = sync.NewCond(&s.replicationQueueMu)

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
				// Even if connection fails, still try to gossip about it
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
		go s.rebalanceRing()
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
	// Check if we already have this peer
	if _, exists := s.peers[peerID]; exists {
		log.Printf("Peer %s already exists in peer list", peerID)
		s.mu.Unlock()
		return nil
	}

	// Add to peer list and ring
	s.peers[peerID] = addr
	s.ring.AddNode(peerID)
	s.mu.Unlock()

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
		// Don't remove from peers and ring immediately, add to disconnected peers for later retry
		s.disconnectedPeerMu.Lock()
		s.disconnectedPeers[peerID] = disconnectedPeer{
			nodeID:      peerID,
			address:     addr,
			lastAttempt: time.Now(),
			attempts:    1,
		}
		s.disconnectedPeerMu.Unlock()

		log.Printf("Failed to connect to peer %s after retries, will attempt reconnection later", peerID)
		return fmt.Errorf("failed to connect to peer %s after retries: %v", peerID, err)
	}

	// Create clients for both internal and KV operations
	internalClient := pb.NewNodeInternalClient(conn)
	kvClient := pb.NewKVStoreClient(conn)

	// Store in maps using consistent key (UUID without "node-" prefix)
	mapKey := strings.TrimPrefix(peerID, "node-")

	s.mu.Lock()
	s.clients[mapKey] = internalClient
	s.kvClients[mapKey] = kvClient
	// Make sure it's in the ring
	s.ring.AddNode(peerID)
	s.mu.Unlock()

	log.Printf("Successfully connected to peer %s", peerID)
	return nil
}

// Get implements the KVStore Get RPC method
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Validate the key to prevent nil pointer dereference
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Check local store first - this is fast
	localValue, localTimestamp, localExists := s.store.Get(req.Key)

	// If we have the key locally, return it immediately
	// and trigger read repair asynchronously
	if localExists {
		// Get replicas for this key for potential read repair
		s.mu.RLock()
		replicas := s.ring.GetReplicas(req.Key, s.replicationFactor)
		s.mu.RUnlock()

		// Trigger read repair asynchronously
		go s.asyncReadRepair(req.Key, localTimestamp, replicas)

		return &pb.GetResponse{
			Value:     localValue,
			Timestamp: localTimestamp,
			Exists:    true,
		}, nil
	}

	// We don't have the key locally, check if we should have it
	s.mu.RLock()
	replicas := s.ring.GetReplicas(req.Key, s.replicationFactor)
	isReplica := s.isDesignatedReplica(req.Key)
	s.mu.RUnlock()

	// If we're not a designated replica, we shouldn't have this key
	// Just query all replicas
	if !isReplica {
		log.Printf("This node is not a designated replica for key %s, querying all replicas", req.Key)
		return s.queryAllReplicasAndRepair(ctx, req.Key, replicas)
	}

	// We are a designated replica but don't have the key
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
	resultsReceived := 0
	keyFound := false

	for resultsReceived < queriesStarted {
		select {
		case result := <-resultChan:
			resultsReceived++

			if result.err != nil {
				log.Printf("Error querying replica %s: %v", result.nodeID, result.err)
				continue
			}

			if !result.exists {
				continue
			}

			keyFound = true
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
			log.Printf("Context deadline exceeded while querying replicas for key %s (%d/%d results received)",
				key, resultsReceived, queriesStarted)
			// Don't exit the loop if we found the key on at least one replica
			if keyFound && mostRecentExists {
				resultsReceived = queriesStarted // Force loop exit
			} else {
				// Only continue waiting if we have more results pending
				if resultsReceived < queriesStarted {
					continue
				}
			}
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
	// Skip read repair for very recent writes (less than 100ms old)
	// This reduces unnecessary network traffic for hot keys
	if time.Since(time.Unix(0, int64(localTimestamp>>16)*1000000)) < 100*time.Millisecond {
		return
	}

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
	// Validate key and value
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

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

	// Replicate asynchronously to other nodes
	go s.replicateAsync(context.Background(), req.Key, req.Value, timestamp)

	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
}

// replicateAsync asynchronously replicates data to all designated replica nodes
func (s *server) replicateAsync(ctx context.Context, key, value string, timestamp uint64) {
	s.mu.RLock()
	replicas := s.ring.GetReplicas(key, s.replicationFactor)
	s.mu.RUnlock()

	// Filter out self from replicas
	var targetReplicas []string
	for _, nodeID := range replicas {
		if nodeID != s.nodeID {
			targetReplicas = append(targetReplicas, nodeID)
		}
	}

	if len(targetReplicas) == 0 {
		log.Printf("No replicas to replicate key %s to", key)
		return
	}

	log.Printf("Asynchronously replicating key %s to %d replicas", key, len(targetReplicas))

	// Update metrics
	s.replicationMetrics.mu.Lock()
	s.replicationMetrics.totalReplications += int64(len(targetReplicas))
	s.replicationMetrics.pendingReplications += int64(len(targetReplicas))
	s.replicationMetrics.mu.Unlock()

	// Create a single replication task for all target nodes
	task := ReplicationTask{
		key:         key,
		value:       value,
		timestamp:   timestamp,
		attempts:    0,
		lastTry:     time.Now(),
		targetNodes: targetReplicas,
	}

	// Add to replication queue
	s.replicationQueueMu.Lock()
	s.replicationQueue = append(s.replicationQueue, task)
	s.replicationQueueMu.Unlock()

	// Signal that there's work to do
	s.replicationQueueCond.Signal()
}

// processReplicationTask attempts to replicate a key to all designated replicas
func (s *server) processReplicationTask(task ReplicationTask) {
	// Check if we need to retry
	if task.attempts >= s.maxReplicationRetries {
		log.Printf("Giving up on replicating key %s after %d attempts",
			task.key, task.attempts)

		// Update metrics for failed replications
		s.replicationMetrics.mu.Lock()
		s.replicationMetrics.failedReplications += int64(len(task.targetNodes))
		s.replicationMetrics.pendingReplications -= int64(len(task.targetNodes))
		s.replicationMetrics.mu.Unlock()

		return
	}

	// Increment attempt counter
	task.attempts++

	// Calculate backoff time based on attempt number
	backoffTime := time.Duration(1<<uint(task.attempts-1)) * 100 * time.Millisecond
	if time.Since(task.lastTry) < backoffTime {
		// Not enough time has passed since last attempt, re-queue with delay
		time.AfterFunc(backoffTime-time.Since(task.lastTry), func() {
			s.replicationQueueMu.Lock()
			s.replicationQueue = append(s.replicationQueue, task)
			s.replicationQueueCond.Signal()
			s.replicationQueueMu.Unlock()
		})
		return
	}

	// Update last try time
	task.lastTry = time.Now()
	startTime := time.Now()

	// Try to replicate to all target nodes in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var remainingNodes []string

	for _, nodeID := range task.targetNodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := s.replicateKey(ctx, task.key, task.value, task.timestamp, nodeID)
			cancel()

			if err != nil {
				log.Printf("Failed to replicate key %s to node %s: %v",
					task.key, nodeID, err)

				mu.Lock()
				remainingNodes = append(remainingNodes, nodeID)
				mu.Unlock()
			} else {
				log.Printf("Successfully replicated key %s to node %s on attempt %d",
					task.key, nodeID, task.attempts)

				// Update metrics for successful replication
				s.replicationMetrics.mu.Lock()
				s.replicationMetrics.successfulReplications++
				s.replicationMetrics.pendingReplications--

				// Track replication time
				replicationTime := time.Since(startTime)
				s.replicationMetrics.replicationTimes = append(s.replicationMetrics.replicationTimes, replicationTime)
				if len(s.replicationMetrics.replicationTimes) > s.replicationMetrics.maxRecentTimes {
					// Remove oldest time if we've exceeded our tracking limit
					s.replicationMetrics.replicationTimes = s.replicationMetrics.replicationTimes[1:]
				}

				// Recalculate average replication time
				var totalTime int64
				for _, t := range s.replicationMetrics.replicationTimes {
					totalTime += t.Milliseconds()
				}
				if len(s.replicationMetrics.replicationTimes) > 0 {
					s.replicationMetrics.avgReplicationTimeMs = totalTime / int64(len(s.replicationMetrics.replicationTimes))
				}
				s.replicationMetrics.mu.Unlock()
			}
		}(nodeID)
	}

	// Wait for all replication attempts to complete
	wg.Wait()

	// If there are remaining nodes, requeue the task
	if len(remainingNodes) > 0 {
		// Create a new task with only the remaining nodes
		newTask := ReplicationTask{
			key:         task.key,
			value:       task.value,
			timestamp:   task.timestamp,
			attempts:    task.attempts,
			lastTry:     task.lastTry,
			targetNodes: remainingNodes,
		}

		// Calculate backoff time for next attempt
		nextBackoff := time.Duration(1<<uint(task.attempts)) * 100 * time.Millisecond

		// Requeue with delay
		time.AfterFunc(nextBackoff, func() {
			s.replicationQueueMu.Lock()
			s.replicationQueue = append(s.replicationQueue, newTask)
			s.replicationQueueCond.Signal()
			s.replicationQueueMu.Unlock()
		})
	}
}

// replicateWithQuorum is kept for backward compatibility but now delegates to replicateAsync
// This function is now deprecated and will be removed in future versions
func (s *server) replicateWithQuorum(ctx context.Context, key, value string, timestamp uint64, quorumSize int) error {
	// Start async replication
	go s.replicateAsync(context.Background(), key, value, timestamp)

	// Always return success since we're now using async replication
	return nil
}

// isDesignatedReplica checks if this node should hold a key
func (s *server) isDesignatedReplica(key string) bool {
	// Validate key to prevent nil pointer dereference
	if key == "" {
		return false
	}

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
	// Validate key to prevent nil pointer dereference
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

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

	// Check if we already have this key with a newer or equal timestamp
	_, existingTimestamp, exists := s.store.Get(req.Key)
	if exists && existingTimestamp >= req.Timestamp {
		log.Printf("Ignoring replication for key %s: local timestamp %d >= received timestamp %d",
			req.Key, existingTimestamp, req.Timestamp)
		return &pb.ReplicateResponse{Success: true}, nil
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
	heartbeatTicker := time.NewTicker(s.heartbeatInterval)
	reconnectTicker := time.NewTicker(s.reconnectInterval)

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				s.checkPeers()
			case <-reconnectTicker.C:
				s.tryReconnectDisconnectedPeers()
			case <-s.stopChan:
				heartbeatTicker.Stop()
				reconnectTicker.Stop()
				return
			}
		}
	}()
}

// tryReconnectDisconnectedPeers attempts to reconnect to peers that were disconnected
func (s *server) tryReconnectDisconnectedPeers() {
	s.disconnectedPeerMu.Lock()
	if len(s.disconnectedPeers) == 0 {
		s.disconnectedPeerMu.Unlock()
		return
	}

	// Create a copy of disconnected peers to work with without holding the lock
	peersToTry := make([]disconnectedPeer, 0, len(s.disconnectedPeers))
	for _, peer := range s.disconnectedPeers {
		peersToTry = append(peersToTry, peer)
	}
	s.disconnectedPeerMu.Unlock()

	log.Printf("Attempting to reconnect to %d disconnected peers", len(peersToTry))

	for _, peer := range peersToTry {
		// Check if we should stop trying to reconnect
		if peer.attempts > s.maxReconnectAttempts {
			log.Printf("Giving up on reconnecting to %s after %d attempts", peer.nodeID, peer.attempts)

			s.disconnectedPeerMu.Lock()
			delete(s.disconnectedPeers, peer.nodeID)
			s.disconnectedPeerMu.Unlock()

			// If we're giving up, ensure it's removed from the ring
			s.mu.Lock()
			delete(s.peers, peer.nodeID)
			s.ring.RemoveNode(peer.nodeID)
			s.mu.Unlock()

			continue
		}

		// Try to establish the connection
		conn, err := grpc.Dial(peer.address, grpc.WithInsecure())
		if err != nil {
			// Update the attempt count and last attempt time
			s.disconnectedPeerMu.Lock()
			updatedPeer := s.disconnectedPeers[peer.nodeID]
			updatedPeer.attempts++
			updatedPeer.lastAttempt = time.Now()
			s.disconnectedPeers[peer.nodeID] = updatedPeer
			s.disconnectedPeerMu.Unlock()

			log.Printf("Reconnect attempt %d to %s failed: %v", peer.attempts, peer.nodeID, err)
			continue
		}

		// Successfully reconnected
		log.Printf("Successfully reconnected to peer %s at %s", peer.nodeID, peer.address)

		// Create the clients
		internalClient := pb.NewNodeInternalClient(conn)
		kvClient := pb.NewKVStoreClient(conn)

		mapKey := strings.TrimPrefix(peer.nodeID, "node-")

		// Add to our connected clients
		s.mu.Lock()
		s.clients[mapKey] = internalClient
		s.kvClients[mapKey] = kvClient
		// Make sure it's in the ring
		s.ring.AddNode(peer.nodeID)
		s.mu.Unlock()

		// Remove from disconnected peers
		s.disconnectedPeerMu.Lock()
		delete(s.disconnectedPeers, peer.nodeID)
		s.disconnectedPeerMu.Unlock()

		// Announce to other peers that we've reconnected with this peer
		go s.gossipNewNode(peer.nodeID, peer.address)
	}
}

// checkPeers sends heartbeat requests to all peers
func (s *server) checkPeers() {
	s.mu.RLock()
	peers := make(map[string]pb.NodeInternalClient)
	peerIDs := make(map[string]string) // Map from client ID to node ID
	for id, client := range s.clients {
		peers[id] = client
		nodeID := id
		if !strings.HasPrefix(id, "node-") {
			nodeID = fmt.Sprintf("node-%s", id)
		}
		peerIDs[id] = nodeID
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
			s.handlePeerFailure(peerID, peerIDs[peerID])
		}
	}
}

// handlePeerFailure handles a failed peer temporarily, adding it to disconnected peers
func (s *server) handlePeerFailure(peerID string, nodeID string) {
	s.mu.Lock()
	// Get the address of the peer before removing
	addr, exists := s.peers[nodeID]
	if !exists {
		// If the node ID doesn't match, we need to look for the node ID differently
		for nID, nAddr := range s.peers {
			if strings.TrimPrefix(nID, "node-") == peerID {
				addr = nAddr
				nodeID = nID
				exists = true
				break
			}
		}
	}

	if !exists {
		s.mu.Unlock()
		log.Printf("Cannot find address for peer %s, cannot add to reconnection list", peerID)
		return
	}

	// Remove from active connections but not from peers list yet
	delete(s.clients, peerID)
	delete(s.kvClients, peerID)
	s.mu.Unlock()

	log.Printf("Marking peer %s as disconnected, will attempt to reconnect", nodeID)

	// Add to disconnected peers list for reconnection attempts
	s.disconnectedPeerMu.Lock()
	s.disconnectedPeers[nodeID] = disconnectedPeer{
		nodeID:      nodeID,
		address:     addr,
		lastAttempt: time.Now(),
		attempts:    1,
	}
	s.disconnectedPeerMu.Unlock()
}

// Stop gracefully shuts down the server
func (s *server) Stop() {
	log.Println("Shutting down server...")
	close(s.stopChan)
	if s.cancel != nil {
		s.cancel()
	}

	// Signal replication worker to exit
	s.replicationQueueCond.Broadcast()

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
	go s.startReplicationWorker()

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
		// But only for keys that need rebalancing, not all keys
		go s.rebalanceOnlyMissingReplicas()
	}()
}

// rebalanceOnlyMissingReplicas only rebalances keys that are missing from their designated replicas
// instead of rebalancing all keys on startup
func (s *server) rebalanceOnlyMissingReplicas() {
	if !s.rebalancing.CompareAndSwap(false, true) {
		log.Println("Rebalancing already in progress")
		return
	}
	defer s.rebalancing.Store(false)

	s.mu.RLock()
	keys := s.store.GetKeys()
	s.mu.RUnlock()

	log.Printf("Starting targeted rebalancing check for %d keys", len(keys))

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
			if err := s.processTargetedBatch(batchKeys); err != nil {
				log.Printf("Batch %d failed: %v", batchIndex, err)
			}
		}(i, batch)
	}
	wg.Wait()
}

// processTargetedBatch handles rebalancing for a batch of keys, but only replicates to nodes
// that don't already have the key or have an outdated version
func (s *server) processTargetedBatch(keys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.rebalanceConfig.BatchTimeout)
	defer cancel()

	for _, key := range keys {
		s.mu.RLock()
		replicas := s.ring.GetReplicas(key, s.replicationFactor)
		s.mu.RUnlock()

		value, timestamp, exists := s.store.Get(key)
		if !exists {
			continue
		}

		// Check which replicas need this key
		for _, replicaID := range replicas {
			if replicaID == s.nodeID {
				continue // Skip self
			}

			// Check if this replica already has the key with the same or newer timestamp
			replicaTimestamp, hasKey := s.checkReplicaKeyTimestamp(ctx, key, replicaID)

			if hasKey && replicaTimestamp >= timestamp {
				log.Printf("Replica %s already has key %s with same or newer timestamp (%d >= %d), skipping replication",
					replicaID, key, replicaTimestamp, timestamp)
				continue
			}

			// This replica is missing the key or has an outdated version
			if !hasKey {
				log.Printf("Replica %s is missing key %s, replicating", replicaID, key)
			} else {
				log.Printf("Replica %s has outdated version of key %s (%d < %d), updating",
					replicaID, key, replicaTimestamp, timestamp)
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

// checkReplicaKeyTimestamp checks if a replica has a key and returns its timestamp
// Returns timestamp and whether the key exists
func (s *server) checkReplicaKeyTimestamp(ctx context.Context, key string, replicaID string) (uint64, bool) {
	peerID := strings.TrimPrefix(replicaID, "node-")

	s.mu.RLock()
	kvClient, ok := s.kvClients[peerID]
	s.mu.RUnlock()

	if !ok {
		log.Printf("No client connection for node %s", replicaID)
		return 0, false // Assume it doesn't have the key if we can't connect
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := kvClient.Get(ctxTimeout, &pb.GetRequest{Key: key})
	if err != nil {
		log.Printf("Error checking if replica %s has key %s: %v", replicaID, key, err)
		return 0, false // Assume it doesn't have the key if we can't check
	}

	return resp.Timestamp, resp.Exists
}

// GetReplicationMetrics returns current replication metrics
func (s *server) GetReplicationMetrics(ctx context.Context, req *pb.Empty) (*pb.ReplicationMetricsResponse, error) {
	s.replicationMetrics.mu.Lock()
	defer s.replicationMetrics.mu.Unlock()

	return &pb.ReplicationMetricsResponse{
		TotalReplications:      s.replicationMetrics.totalReplications,
		SuccessfulReplications: s.replicationMetrics.successfulReplications,
		FailedReplications:     s.replicationMetrics.failedReplications,
		PendingReplications:    s.replicationMetrics.pendingReplications,
		AvgReplicationTimeMs:   s.replicationMetrics.avgReplicationTimeMs,
		QueueSize:              int64(len(s.replicationQueue)),
	}, nil
}

// startReplicationWorker starts a background worker to process the replication queue
func (s *server) startReplicationWorker() {
	// Number of workers based on CPU cores, but capped at a reasonable number
	numWorkers := runtime.NumCPU()
	if numWorkers > 8 {
		numWorkers = 8
	}

	log.Printf("Starting %d replication workers", numWorkers)

	// Start multiple workers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for {
				// Get a task from the queue with proper locking
				s.replicationQueueMu.Lock()
				for len(s.replicationQueue) == 0 {
					// Check for shutdown before waiting
					select {
					case <-s.stopChan:
						s.replicationQueueMu.Unlock()
						return
					default:
						// Continue with wait
					}

					// Wait releases the mutex while waiting and reacquires it when woken up
					s.replicationQueueCond.Wait()
				}

				// We have the lock and there's at least one task
				task := s.replicationQueue[0]
				s.replicationQueue = s.replicationQueue[1:]
				s.replicationQueueMu.Unlock()

				// Process the task
				s.processReplicationTask(task)
			}
		}(i)
	}

	// Start a periodic log of replication metrics
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.replicationQueueMu.Lock()
				queueSize := len(s.replicationQueue)
				s.replicationQueueMu.Unlock()

				s.replicationMetrics.mu.Lock()
				log.Printf("Replication metrics: total=%d, success=%d, failed=%d, pending=%d, avg_time=%dms, queue_size=%d",
					s.replicationMetrics.totalReplications,
					s.replicationMetrics.successfulReplications,
					s.replicationMetrics.failedReplications,
					s.replicationMetrics.pendingReplications,
					s.replicationMetrics.avgReplicationTimeMs,
					queueSize)
				s.replicationMetrics.mu.Unlock()
			case <-s.stopChan:
				return
			}
		}
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
	asyncReplication := flag.Bool("async-replication", true, "Use asynchronous replication (default: true)")
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

	// Log replication mode
	if *asyncReplication {
		log.Printf("Using asynchronous replication mode")
	} else {
		log.Printf("Using synchronous replication mode with quorum")
	}

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
