package protocol

import (
	"context"
	"log"
	"sync"
	"time"

	"Distributed-Key-Value-Store/internal/node"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"google.golang.org/grpc"
)

type Node struct {
	ID           uint32
	store        *node.Node
	logicalClock uint64
	mu           sync.RWMutex

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
	}
}

// Replicate handles incoming replication requests
func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	_, timestamp, exists := n.store.Get(req.Key)
	if !exists || req.Timestamp > timestamp {
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

	for key, remoteTs := range req.KeyTimestamps {
		localValue, localTs, exists := n.store.Get(key)
		if !exists {
			continue
		}

		if localTs > remoteTs {
			missing[key] = &pb.KeyValue{
				Value:     localValue,
				Timestamp: localTs,
			}
		}
	}

	return &pb.SyncResponse{Missing: missing}, nil
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

	// Get local key timestamps
	keyTimestamps := make(map[string]uint64)
	for _, key := range n.store.GetKeys() {
		_, timestamp, exists := n.store.Get(key)
		if exists {
			keyTimestamps[key] = timestamp
		}
	}

	// Send to each node
	for nodeID, client := range n.getClients() {
		if nodeID == n.ID {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		resp, err := client.SyncKeys(ctx, &pb.SyncRequest{
			KeyTimestamps: keyTimestamps,
		})
		cancel()

		if err != nil {
			log.Printf("Anti-entropy with node %d failed: %v", nodeID, err)
			continue
		}

		// Apply missing updates
		for key, kv := range resp.Missing {
			n.store.Store(key, kv.Value, kv.Timestamp)
		}
	}
}

// getTimestamp generates a new timestamp for writes
func (n *Node) getTimestamp(key string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logicalClock++
	return (uint64(n.ID) << 32) | n.logicalClock
}

// getLocalTimestamp retrieves the stored timestamp without incrementing
func (n *Node) getLocalTimestamp(key string) uint64 {
	_, timestamp, _ := n.store.Get(key)
	return timestamp
}

// updateTimestamp updates logical clock based on received timestamp
func (n *Node) updateTimestamp(receivedTS uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
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
		for range ticker.C {
			for nodeID, _ := range n.nodes {
				if nodeID == n.ID {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := n.clients[nodeID].Heartbeat(ctx, &pb.Ping{NodeId: n.ID})
				cancel()
				n.statusMu.Lock()
				n.nodeStatus[nodeID] = (err == nil)
				n.statusMu.Unlock()
			}
		}
	}()
}

// ReplicateToNodes sends updates only to designated replica nodes
func (n *Node) replicateToNodes(key string, value string, timestamp uint64) {
	n.clientsMu.RLock()
	defer n.clientsMu.RUnlock()

	// Get replica nodes using consistent hashing
	hash := consistenthash.HashString(key)
	replicaCount := 3 // Number of replicas (including primary)

	// Create replication request
	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	// Send only to replica nodes
	replicasSent := 1 // Count self as first replica
	current := hash

	for replicasSent < replicaCount && replicasSent < len(n.clients) {
		// Get next node in ring
		nextHash := (current + 1) % uint32(len(n.nodes))
		nodeID := uint32(nextHash)

		if nodeID != n.ID {
			if client, exists := n.clients[nodeID]; exists {
				// Async replication with context timeout
				go func(c pb.NodeInternalClient) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					_, err := c.Replicate(ctx, req)
					if err != nil {
						log.Printf("Failed to replicate to node %d: %v", nodeID, err)
					}
				}(client)
				replicasSent++
			}
		}
		current = nextHash
	}
}

func (n *Node) IsPrimary(key string) bool {
	hash := consistenthash.HashString(key) % uint32(len(n.nodes))
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
		OldValue:    oldValue,
		HadOldValue: hadOldValue,
	}, nil
}

// Node management methods
func (n *Node) AddNode(nodeID uint32, addr string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

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
	n.mu.Lock()
	defer n.mu.Unlock()

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
	n.mu.RLock()
	defer n.mu.RUnlock()

	nodes := make(map[uint32]string)
	for id, addr := range n.nodes {
		nodes[id] = addr
	}
	return nodes
}
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
    value, timestamp, exists := n.store.Get(req.Key)
    if !exists {
        return &pb.GetResponse{Exists: false}, nil
    }
    return &pb.GetResponse{
        Value:     value,
        Timestamp: timestamp,
        Exists:    true,
    }, nil
}
