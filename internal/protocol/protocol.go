package protocol

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"Distributed-Key-Value-Store/internal/storage"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"github.com/bits-and-blooms/bloom/v3"
)

type Node struct {
	ID           uint32
	Store        *storage.Storage
	logicalClock uint64
	storeMu      sync.RWMutex

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
	stopChan    chan struct{}

	// Added for replication factor
	replicationFactor int

	// Added for hybrid logical clock
	lastPhysicalTime int64
}

func NewNode(id uint32, store *storage.Storage, replicationFactor int) *Node {
	return &Node{
		ID:                id,
		Store:             store,
		nodes:             make(map[uint32]string),
		nodeStatus:        make(map[uint32]bool),
		clients:           make(map[uint32]pb.NodeInternalClient),
		syncInterval:      5 * time.Minute,
		heartbeatInterval: time.Second,
		ring:              consistenthash.NewRing(10),
		ringVersion:       0,
		stopChan:          make(chan struct{}),
		replicationFactor: replicationFactor,
	}
}

// Replicate handles incoming replication requests
func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	_, timestamp, exists := n.Store.Get(req.Key)
	if !exists || (req.Timestamp) > timestamp {
		n.Store.Store(req.Key, req.Value, req.Timestamp)
		return &pb.ReplicateResponse{Success: true}, nil
	}
	return &pb.ReplicateResponse{
		Success: false,
		Error:   "older timestamp",
	}, nil
}

// Add Bloom filter for efficient sync
type SyncState struct {
	filter        *bloom.BloomFilter
	keyTimestamps map[string]uint64
}

// SyncKeys handles anti-entropy sync requests
func (n *Node) SyncKeys(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	missing := make(map[string]*pb.KeyValue)
	filter := &bloom.BloomFilter{}
	filter.GobDecode(req.BloomFilter)

	// Process key ranges in batches
	for start, end := range req.KeyRanges {
		n.storeMu.RLock()
		keys := n.Store.GetKeys()
		n.storeMu.RUnlock()

		for _, key := range keys {
			if key >= start && key <= end {
				// Only check keys not in remote filter
				if !filter.Test([]byte(key)) {
					value, ts, exists := n.Store.Get(key)
					if exists {
						missing[key] = &pb.KeyValue{
							Value:     string(value),
							Timestamp: ts,
						}
					}
				} else {
					// Key exists in both, check timestamp
					if remoteTs, ok := req.KeyTimestamps[key]; ok {
						value, ts, exists := n.Store.Get(key)
						if exists && ts > remoteTs {
							missing[key] = &pb.KeyValue{
								Value:     string(value),
								Timestamp: ts,
							}
						}
					}
				}
			}
		}
	}

	return &pb.SyncResponse{
		Missing: missing,
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
	// Create Bloom filter for local keys
	filter := bloom.NewWithEstimates(100000, 0.01) // Size and false positive rate
	keyTimestamps := make(map[string]uint64)

	// Add local keys to filter and timestamps
	n.storeMu.RLock()
	for _, key := range n.Store.GetKeys() {
		filter.Add([]byte(key))
		_, ts, _ := n.Store.Get(key)
		keyTimestamps[key] = ts
	}
	n.storeMu.RUnlock()

	// Get key ranges for batch processing
	keyRanges := n.getKeyRanges()

	// Send to each node
	for nodeID, client := range n.getClients() {
		if nodeID == n.ID {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel() // Ensure cancel is called to prevent context leak

		// Process key ranges in batches
		for start, end := range keyRanges {
			// Create a batch of keys to sync
			batch := make(map[string]uint64)
			n.storeMu.RLock()
			for key, ts := range keyTimestamps {
				if key >= start && key <= end {
					batch[key] = ts
				}
			}
			n.storeMu.RUnlock()

			// Send the batch to the remote node
			encoded, err := filter.GobEncode()
			if err != nil {
				log.Printf("Failed to encode bloom filter: %v", err)
				continue
			}
			resp, err := client.SyncKeys(ctx, &pb.SyncRequest{
				KeyTimestamps: batch,
				BloomFilter:   encoded,
			})
			if err != nil {
				log.Printf("Anti-entropy with node %d failed: %v", nodeID, err)
				continue
			}

			// Apply missing updates
			n.storeMu.Lock()
			for key, kv := range resp.Missing {
				if !filter.Test([]byte(key)) || kv.Timestamp > keyTimestamps[key] {
					n.Store.Store(key, kv.Value, kv.Timestamp)
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
	keys := n.Store.GetKeys()
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

// Update getTimestamp with hybrid logical clock implementation
func (n *Node) getTimestamp(key string) uint64 {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	// Get current physical time in milliseconds
	now := time.Now().UnixNano() / 1000000

	// Ensure physical clock is monotonic
	if now > n.lastPhysicalTime {
		n.lastPhysicalTime = now
		n.logicalClock = 0
	} else {
		// Same millisecond, increment logical clock
		n.logicalClock++
	}

	// Combine physical and logical components
	// Format: 48 bits physical time | 16 bits logical clock
	timestamp := (uint64(n.lastPhysicalTime) << 16) | (n.logicalClock & 0xFFFF)

	return timestamp
}

// getLocalTimestamp retrieves the stored timestamp without incrementing
func (n *Node) getLocalTimestamp(key string) uint64 {
	_, timestamp, _ := n.Store.Get(key)
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
	defer n.storeMu.Unlock()

	// Add active nodes to the ring first
	for _, nodeID := range activeNodes {
		n.ring.AddNode(fmt.Sprintf("node-%d", nodeID))
	}

	// Remove old nodes from the ring
	for nodeID := range n.nodes {
		if !contains(activeNodes, nodeID) { // Only remove nodes that are not active
			n.ring.RemoveNode(fmt.Sprintf("node-%d", nodeID))
		}
	}

	// Rebalance keys
	keys := n.Store.GetKeys()
	for _, key := range keys {
		// Calculate the new node for each key
		hash := n.ring.HashKey(key)
		newNodeID := n.ring.GetNode(fmt.Sprintf("%d", hash))

		// If the key should be on a different node, initiate transfer
		if newNodeID != fmt.Sprintf("node-%d", n.ID) {
			value, timestamp, exists := n.Store.Get(key)
			if exists {
				// Attempt to replicate with retries
				success := false
				for retries := 0; retries < 3; retries++ {
					if err := n.replicateKey(key, string(value), timestamp, newNodeID); err == nil {
						success = true
						break
					}
					log.Printf("Failed to replicate key %s to %s (attempt %d/3)", key, newNodeID, retries+1)
					time.Sleep(time.Second * time.Duration(1<<retries)) // Exponential backoff
				}
				if !success {
					// If all retries fail, log the error
					log.Printf("All attempts to replicate key %s to %s failed. Key may remain un-replicated.", key, newNodeID)
					// Optionally, you can implement a notification mechanism here
				}
			}
		}
	}
}

// replicateKey handles the replication of a single key to a target node
func (n *Node) replicateKey(key string, value string, timestamp uint64, targetNode string) error {
	nodeID, err := strconv.ParseUint(strings.TrimPrefix(targetNode, "node-"), 10, 32)
	if err != nil {
		return err
	}

	client, exists := n.clients[uint32(nodeID)]
	if !exists {
		return fmt.Errorf("client for node %s does not exist", targetNode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err = client.Replicate(ctx, &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	})
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
		nodeID, _ := strconv.ParseUint(nextNodeID, 10, 32)
		if client, exists := n.clients[uint32(nodeID)]; exists {
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

func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	writeQuorum := (n.replicationFactor / 2) + 1
	successCount := 1 // Count self as first success

	timestamp := n.getTimestamp(req.Key)

	oldValue, _, hadOldValue := n.Store.Get(req.Key)
	n.Store.Store(req.Key, req.Value, timestamp)

	responses := make(chan error, n.replicationFactor-1)
	var wg sync.WaitGroup         // WaitGroup to wait for all goroutines
	var replicationErrors []error // Slice to collect replication errors

	hash := n.ring.HashKey(req.Key)
	currentHash := hash

	// Replicate to other nodes
	for i := 1; i < n.replicationFactor; i++ {
		nextNodeID := n.ring.GetNextNode(currentHash)
		if nextNodeID == "" {
			break
		}

		nodeID, _ := strconv.ParseUint(strings.TrimPrefix(nextNodeID, "node-"), 10, 32)
		if client, exists := n.clients[uint32(nodeID)]; exists {
			wg.Add(1) // Increment the WaitGroup counter
			go func(c pb.NodeInternalClient) {
				defer wg.Done() // Decrement the counter when the goroutine completes
				_, err := c.Replicate(ctx, &pb.ReplicateRequest{
					Key:       req.Key,
					Value:     req.Value,
					Timestamp: timestamp,
				})
				responses <- err
			}(client)
		}
		currentHash = n.ring.HashKey(nextNodeID)
	}

	// Close the responses channel in a separate goroutine
	go func() {
		wg.Wait()        // Wait for all replication goroutines to finish
		close(responses) // Close the channel after all responses are sent
	}()

	// Wait for responses and check for quorum
	for err := range responses {
		if err == nil {
			successCount++
			if successCount >= writeQuorum {
				// If quorum is reached, we can return early
				break
			}
		} else {
			replicationErrors = append(replicationErrors, err) // Collect errors
			log.Printf("Replication error: %v", err)
		}
	}

	if successCount < writeQuorum {
		return nil, fmt.Errorf("failed to achieve write quorum: got %d, need %d. Errors: %v", successCount, writeQuorum, replicationErrors)
	}

	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
}

// Add a dedicated GetReplica method for more efficient single-key reads
func (n *Node) GetReplica(ctx context.Context, req *pb.GetReplicaRequest) (*pb.GetReplicaResponse, error) {
	value, timestamp, exists := n.Store.Get(req.Key)
	return &pb.GetReplicaResponse{
		Value:     string(value),
		Timestamp: uint64(timestamp),
		Exists:    exists,
	}, nil
}

// Update Get to use GetReplica
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (pb.GetResponse, error) {
	// Use a context with timeout for replica queries
	replicaCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	readQuorum := (n.replicationFactor / 2) + 1
	responses := make(chan struct {
		value     string
		timestamp uint64
		exists    bool
		err       error
	}, n.replicationFactor) // Initially size to replicationFactor

	// Get local value
	value, timestamp, exists := n.Store.Get(req.Key)
	responses <- struct {
		value     string
		timestamp uint64
		exists    bool
		err       error
	}{string(value), uint64(timestamp), exists, nil}

	// Query replicas using GetReplica
	hash := n.ring.HashKey(req.Key)
	currentHash := hash
	queriedNodes := 1 // Start with the local node

	for i := 1; i < n.replicationFactor; i++ {
		nextNodeID := n.ring.GetNextNode(currentHash)
		if nextNodeID == "" {
			break // No more nodes available
		}

		nodeID, _ := strconv.ParseUint(nextNodeID, 10, 32)
		if client, ok := n.clients[uint32(nodeID)]; ok {
			queriedNodes++ // Increment the count of queried nodes
			go func(c pb.NodeInternalClient) {
				replicaReq := &pb.GetReplicaRequest{Key: req.Key}
				resp, err := c.GetReplica(replicaCtx, replicaReq)
				responses <- struct {
					value     string
					timestamp uint64
					exists    bool
					err       error
				}{resp.Value, resp.Timestamp, resp.Exists, err}
			}(client)
		}
		currentHash = n.ring.HashKey(nextNodeID)
	}

	// Wait for responses and find the latest value
	successCount := 0
	var latest struct {
		value     string
		timestamp uint64
		exists    bool
	}

	for i := 0; i < queriedNodes; i++ { // Adjust loop to the number of queried nodes
		select {
		case resp := <-responses:
			if resp.err == nil {
				successCount++
				if resp.exists && resp.timestamp > latest.timestamp {
					latest = struct {
						value     string
						timestamp uint64
						exists    bool
					}{resp.value, resp.timestamp, true}
				}
				if successCount >= readQuorum {
					// Perform read repair if needed
					if latest.exists && latest.timestamp > timestamp {
						go n.Store.Store(req.Key, latest.value, latest.timestamp)
					}
					break // Exit early if quorum is reached
				}
			}
		case <-ctx.Done():
			return pb.GetResponse{}, fmt.Errorf("failed to achieve read quorum: %v", ctx.Err())
		}
	}

	if successCount < readQuorum {
		return pb.GetResponse{}, fmt.Errorf("failed to achieve read quorum: got %d, need %d", successCount, readQuorum)
	}

	return pb.GetResponse{
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
	if err := n.Store.Shutdown(); err != nil {
		log.Printf("Error shutting down store: %v", err)
	}
}

// contains checks if a slice contains a specific uint32 item
func contains(slice []uint32, item uint32) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}
