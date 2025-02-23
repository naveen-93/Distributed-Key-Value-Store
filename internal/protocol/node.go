package protocol


import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"Distributed-Key-Value-Store/internal/node"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"google.golang.org/grpc"
	"fmt"
	"strconv"
	"strings"
)

type Node struct {
	ID           uint32
	store        *node.Node
	logicalClock uint64

	storeMu sync.RWMutex

	// Node management
	nodes      map[uint32]string // nodeID -> address
	nodeStatus map[uint32]bool   // nodeID -> isAlive
	statusMu   sync.RWMutex

	// gRPC clients
	clients   map[uint32]pb.NodeInternalClient
	clientsMu sync.RWMutex

	// Configuration
	syncInterval      time.Duration
	heartbeatInterval time.Duration

	// Added for consistent hashing
	ring *consistenthash.Ring

	// Ring state
	ringVersion uint64
	stopChan     chan struct{}
}

func NewNode(id uint32, store *node.Node) *Node {
	return &Node{
		ID:                id,
		store:             store,
		nodes:             make(map[uint32]string),
		nodeStatus:        make(map[uint32]bool),
		clients:           make(map[uint32]pb.NodeInternalClient),
		syncInterval:      5 * time.Minute, //
		heartbeatInterval: time.Second,
		ring:              consistenthash.NewRing(10),
		ringVersion:       0,
		stopChan:  make(chan struct{}),
	}
}

// Replicate handles incoming replication requests
func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	_, timestamp, exists := n.store.Get(req.Key)
	if !exists || (req.Timestamp) > timestamp {
		n.store.Store(req.Key, req.Value, req.Timestamp)
		return &pb.ReplicateResponse{Success: true}, nil
	}
	return &pb.ReplicateResponse{
		Success: false,
		Error:   "older timestamp",
	}, nil
}

// SyncKeys handles anti-entropy sync requests
func (n *Node) SyncKeys(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	missing := make(map[string]*pb.KeyValue)
	deletions := make(map[string]uint64)

	// Process key ranges in batches
	for start, end := range req.KeyRanges {
		n.storeMu.RLock()
		keys := n.store.GetKeys()
		for _, key := range keys {
			if key < start || key >= end {
				continue
			}

			value, timestamp, exists := n.store.Get(key)
			remoteTs := req.KeyTimestamps[key]

			if !exists {
				// Track deletions
				deletions[key] = uint64(timestamp)
			} else if int64(timestamp) > int64(remoteTs) {
				missing[key] = &pb.KeyValue{
					Value:     string(value),
					Timestamp: uint64(timestamp),
					Deleted:   !exists,
				}
			}
		}
		n.storeMu.RUnlock()
	}

	return &pb.SyncResponse{
		Missing:   missing,
		Deletions: deletions,
	}, nil
}

// StartAntiEntropy begins periodic anti-entropy sync
func (n *Node) StartAntiEntropy() {
	go func() {
		ticker := time.NewTicker(n.syncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.performAntiEntropy()
			}
		}
	}()
}

func (n *Node) performAntiEntropy() {
	// Get local key ranges (e.g., divide keyspace into chunks)
	keyRanges := n.getKeyRanges()

	// Send to each node
	for nodeID, client := range n.getClients() {
		if nodeID == n.ID {
			continue
		}

		// Sync each range separately
		for start, end := range keyRanges {
			keyTimestamps := make(map[string]uint64)

			// Get timestamps for keys in range
			n.storeMu.RLock()
			for _, key := range n.store.GetKeys() {
				if key >= start && key < end {
					_, timestamp, exists := n.store.Get(key)
					if exists {
						keyTimestamps[key] = uint64(timestamp)
					}
				}
			}
			n.storeMu.RUnlock()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			resp, err := client.SyncKeys(ctx, &pb.SyncRequest{
				KeyTimestamps: keyTimestamps,
				KeyRanges:     map[string]string{start: end},
			})
			cancel()

			if err != nil {
				log.Printf("Anti-entropy with node %d failed: %v", nodeID, err)
				continue
			}

			// Apply missing updates and deletions
			n.storeMu.Lock()
			for key, kv := range resp.Missing {
				n.store.Store(key, kv.Value, kv.Timestamp)
			}
			for key, ts := range resp.Deletions {
				if _, existing, exists := n.store.Get(key); !exists || (ts) > existing {
					n.store.Store(key, "", ts)
				}
			}
			n.storeMu.Unlock()
		}
	}
}

func (n *Node) getKeyRanges() map[string]string {
	ranges := make(map[string]string)
	rangeSize := 1000 // Adjust based on your needs

	n.storeMu.RLock()
	keys := n.store.GetKeys()
	n.storeMu.RUnlock()
	n.storeMu.RUnlock()

	sort.Strings(keys)

	for i := 0; i < len(keys); i += rangeSize {
		end := i + rangeSize
		if end > len(keys) {
			end = len(keys)
		}
		ranges[keys[i]] = keys[end-1] + "\x00"
	}

	return ranges
}

// getTimestamp generates a new timestamp for writes
func (n *Node) getTimestamp(key string) uint64 {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	n.logicalClock++
	return (uint64(n.ID) << 32) | n.logicalClock
}

// getLocalTimestamp retrieves the stored timestamp without incrementing
func (n *Node) getLocalTimestamp(key string) uint64 {
	_, timestamp, _ := n.store.Get(key)
	return uint64(timestamp)
}

// updateTimestamp updates logical clock based on received timestamp
func (n *Node) updateTimestamp(receivedTS uint64) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	receivedClock := receivedTS & 0xFFFFFFFF
	if receivedClock > n.logicalClock {
		n.logicalClock = receivedClock
	}
}

// Helper methods
func (n *Node) getClients() map[uint32]pb.NodeInternalClient {
	n.clientsMu.RLock()
	defer n.clientsMu.RUnlock()

	clients := make(map[uint32]pb.NodeInternalClient)
	for id, client := range n.clients {
		clients[id] = client
	}
	return clients
}

func (n *Node) StartHeartbeat() {
	go func() {
		ticker := time.NewTicker(n.heartbeatInterval)
		// Track consecutive failures
		failureCount := make(map[uint32]int)
		// Number of consecutive failures before marking a node as down
		failureThreshold := 3

		for range ticker.C {
			anyStateChanged := false
			n.statusMu.Lock()
			for nodeID := range n.nodes {
				if nodeID == n.ID {
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := n.clients[nodeID].Heartbeat(ctx, &pb.Ping{NodeId: n.ID})
				cancel()

				// Update failure count and node status
				if err != nil {
					failureCount[nodeID]++
					// Log increasing failures
					if failureCount[nodeID] > 1 {
						log.Printf("Node %d heartbeat failed %d times consecutively", nodeID, failureCount[nodeID])
					}
				} else {
					failureCount[nodeID] = 0
				}

				// Mark node as down after threshold failures
				newStatus := failureCount[nodeID] < failureThreshold
				if n.nodeStatus[nodeID] != newStatus {
					n.nodeStatus[nodeID] = newStatus
					anyStateChanged = true
					if !newStatus {
						log.Printf("Node %d marked as down after %d consecutive failures", nodeID, failureThreshold)
					} else {
						log.Printf("Node %d is back online", nodeID)
					}
				}
			}
			n.statusMu.Unlock()

			// Trigger ring update and rebalancing if needed
			if anyStateChanged {
				n.storeMu.Lock()
				n.ringVersion++
				// Trigger immediate rebalancing
				go n.rebalanceRing()
				n.storeMu.Unlock()
			}
		}
	}()
}

// rebalanceRing handles the rebalancing of keys after node status changes
func (n *Node) rebalanceRing() {
	// Acquire a read lock to get the current state
	n.statusMu.RLock()
	activeNodes := make([]uint32, 0)
	for nodeID, status := range n.nodeStatus {
		if status {
			activeNodes = append(activeNodes, nodeID)
		}
	}
	n.statusMu.RUnlock()

	// Update the ring with only active nodes
	n.storeMu.Lock()
	for nodeID := range n.nodes {
		n.ring.RemoveNode(fmt.Sprintf("node-%d", nodeID))
	}
	for _, nodeID := range activeNodes {
		n.ring.AddNode(fmt.Sprintf("node-%d", nodeID))
	}
	n.storeMu.Unlock()

	// Rebalance keys
	keys := n.store.GetKeys()
	for _, key := range keys {
		// Calculate the new node for each key
		hash := n.ring.HashKey(key)
		newNodeID := n.ring.GetNode(fmt.Sprintf("%d", hash))

		// If the key should be on a different node, initiate transfer
		if newNodeID != fmt.Sprintf("node-%d", n.ID) {
			value, timestamp, exists := n.store.Get(key)
			if exists {
				// Attempt to replicate with retries
				go func(k, v string, ts uint64, target string) {
					for retries := 0; retries < 3; retries++ {
						if err := n.replicateKey(k, v, ts, target); err != nil {
							log.Printf("Failed to replicate key %s to %s (attempt %d/3): %v", 
								k, target, retries+1, err)
							time.Sleep(time.Second * time.Duration(retries+1))
							continue
						}
						return
					}
				}(key, string(value), timestamp, newNodeID)
			}
		}
	}
}


// replicateKey handles the replication of a single key to a target node
func (n *Node) replicateKey(key, value string, timestamp uint64, targetNode string) error {
	// Extract node ID from target node string
	targetID, err := strconv.ParseUint(strings.TrimPrefix(targetNode, "node-"), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid target node format: %v", err)
	}

	// Get client for target node
	client, ok := n.clients[uint32(targetID)]
	if !ok {
		return fmt.Errorf("no client found for node %d", targetID)
	}

	// Send replication request
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	_, err = client.Replicate(ctx, req)
	return err
}

// ReplicateToNodes sends updates only to designated replica nodes
func (n *Node) replicateToNodes(key string, value string, timestamp uint64) {
	n.clientsMu.RLock()
	defer n.clientsMu.RUnlock()

	// Create replication request
	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	// Get replica nodes using consistent hashing
	replicaCount := 3 // Number of replicas (including primary)
	replicasSent := 1 // Count self as first replica

	// Start from the primary node's hash
	currentHash := n.ring.HashKey(key)

	// Send to next nodes in the ring
	for replicasSent < replicaCount {
		// Get next node in ring
		nextNodeID := n.ring.GetNextNode(currentHash)
		if nextNodeID == "" || nextNodeID == n.nodes[n.ID] {
			break // No more nodes available
		}

		// Convert node address to ID and send if it's not self
		nodeID := uint32(n.ring.HashKey(nextNodeID))
		if client, exists := n.clients[nodeID]; exists {
			go func(c pb.NodeInternalClient) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				_, err := c.Replicate(ctx, req)
				if err != nil {
					log.Printf("Failed to replicate to node %s: %v", nextNodeID, err)
				}
			}(client)
			replicasSent++
		}

		currentHash = n.ring.HashKey(nextNodeID)
	}
}

func (n *Node) IsPrimary(key string) bool {
	hash := n.ring.HashKey(key) % uint32(len(n.nodes))
	return n.ID == hash
}

// Put handles incoming write requests
func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Generate timestamp for this write
	timestamp := n.getTimestamp(req.Key)

	// Store locally
	oldValue, _, hadOldValue := n.store.Get(req.Key)
	n.store.Store(req.Key, req.Value, timestamp)

	// Replicate to other nodes
	n.replicateToNodes(req.Key, req.Value, timestamp)

	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
}

// Node management methods
func (n *Node) AddNode(nodeID uint32, addr string) error {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	// Add to node list
	n.nodes[nodeID] = addr
	n.nodeStatus[nodeID] = true

	// Initialize gRPC connection
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		delete(n.nodes, nodeID)
		return err
	}

	// Create client
	n.clientsMu.Lock()
	n.clients[nodeID] = pb.NewNodeInternalClient(conn)
	n.clientsMu.Unlock()

	return nil
}

func (n *Node) RemoveNode(nodeID uint32) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	// Clean up client connection
	n.clientsMu.Lock()
	if _, exists := n.clients[nodeID]; exists {
		delete(n.clients, nodeID)
	}
	n.clientsMu.Unlock()

	// Remove from node list
	delete(n.nodes, nodeID)
	delete(n.nodeStatus, nodeID)
}

func (n *Node) GetNodes() map[uint32]string {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	nodes := make(map[uint32]string)
	for id, addr := range n.nodes {
		nodes[id] = addr
	}
	return nodes
}

func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Get local value
	value, timestamp, exists := n.store.Get(req.Key)

	// Get replica nodes for this key
	currentHash := n.ring.HashKey(req.Key)
	replicaValues := make([]struct {
		value     string
		timestamp uint64
		nodeID    uint32
	}, 0)

	// Add local value
	if exists {
		replicaValues = append(replicaValues, struct {
			value     string
			timestamp uint64
			nodeID    uint32
		}{string(value), uint64(timestamp), n.ID})
	}

	// Query other replicas
	for i := 0; i < 2; i++ { // Check 2 other replicas
		nextNodeID := n.ring.GetNextNode(currentHash)
		if nextNodeID == "" {
			break
		}

		nodeID := uint32(n.ring.HashKey(nextNodeID))
		if client, ok := n.clients[nodeID]; ok {
			// Use SyncKeys for node-to-node communication
			resp, err := client.SyncKeys(ctx, &pb.SyncRequest{
				KeyTimestamps: map[string]uint64{req.Key: 0}, // Request latest value
			})
			if err == nil {
				if kv, exists := resp.Missing[req.Key]; exists {
					replicaValues = append(replicaValues, struct {
						value     string
						timestamp uint64
						nodeID    uint32
					}{kv.Value, kv.Timestamp, nodeID})
				}
			}
		}
		currentHash = n.ring.HashKey(nextNodeID)
	}

	// Find latest value
	var latest struct {
		value     string
		timestamp uint64
		exists    bool
	}

	for _, rv := range replicaValues {
		if rv.timestamp > latest.timestamp {
			latest.value = rv.value
			latest.timestamp = rv.timestamp
			latest.exists = true
		}
	}

	// Perform read repair if needed
	if latest.exists && (latest.timestamp) > timestamp {
		go n.store.Store(req.Key, latest.value, latest.timestamp)
	}

	return &pb.GetResponse{
		Value:     latest.value,
		Exists:    latest.exists,
		Timestamp: latest.timestamp,
	}, nil
}

// RingState represents the current state of the consistent hash ring
type RingState struct {
	Version   uint64          // Incremented on changes
	Nodes     map[string]bool // node address -> isActive
	UpdatedAt int64           // Unix timestamp
}

func (n *Node) GetRingState(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
	n.storeMu.RLock()
	state := &pb.RingStateResponse{
		Version:   n.ringVersion,
		Nodes:     make(map[string]bool),
		UpdatedAt: time.Now().Unix(),
	}

	// Copy current node states
	for addr, status := range n.nodeStatus {
		state.Nodes[n.nodes[addr]] = status
	}
	n.storeMu.RUnlock()

	return state, nil
}
func (n *Node) Stop() {
    close(n.stopChan)
    if err := n.store.Shutdown(); err != nil {
        log.Printf("Error shutting down store: %v", err)
    }
}