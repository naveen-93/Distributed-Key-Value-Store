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

	"Distributed-Key-Value-Store/internal/node"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"github.com/google/uuid"
)

// Server represents a node in the distributed key-value store
type server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedNodeInternalServer

	nodeID string
	store  *node.Node
	ring   *consistenthash.Ring
	mu     sync.RWMutex

	// Node management
	peers     map[string]string            // Maps nodeID to address
	clients   map[string]pb.NodeInternalClient // Internal clients for replication
	kvClients map[string]pb.KVStoreClient  // KV clients for fetching data

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
	store := node.NewNode(numericID, replicationFactor)

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

// addPeer adds a peer node with retry logic for establishing connections
func (s *server) addPeer(peerID string, addr string) error {
	// Format node ID consistently
	if _, err := uuid.Parse(peerID); err == nil {
		peerID = fmt.Sprintf("node-%s", peerID)
	}

	log.Printf("Adding peer %s at address %s", peerID, addr)

	s.mu.Lock()
	defer s.mu.Unlock()

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

	// Trigger ring rebalancing asynchronously
	go s.rebalanceRing()
	go s.gossipNewNode(peerID, addr)
	return nil
}
func (s *server) gossipNewNode(newPeerID, newAddr string) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    for _, client := range s.clients {
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        _, err := client.AddNode(ctx, &pb.AddNodeRequest{
            NodeId: newPeerID,
            Addr:   newAddr,
        })
        if err != nil {
            log.Printf("Failed to gossip new node to %s: %v", newPeerID, err)
        }
    }
}
// AddNode handles new node additions from gossip
func (s *server) AddNode(ctx context.Context, req *pb.AddNodeRequest) (*pb.Empty, error) {
    if req.NodeId == s.nodeID {
        return &pb.Empty{}, nil
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    if _, exists := s.peers[req.NodeId]; !exists {
        log.Printf("Adding new peer via gossip: %s@%s", req.NodeId, req.Addr)
        s.peers[req.NodeId] = req.Addr
        s.ring.AddNode(req.NodeId)

        // Establish connection to the new node
        conn, err := grpc.Dial(req.Addr, grpc.WithInsecure())
        if err != nil {
            log.Printf("Failed to connect to gossiped node %s: %v", req.NodeId, err)
            return &pb.Empty{}, nil
        }

        // Create clients for the new node
        mapKey := strings.TrimPrefix(req.NodeId, "node-")
        s.clients[mapKey] = pb.NewNodeInternalClient(conn)
        s.kvClients[mapKey] = pb.NewKVStoreClient(conn)
    }

    return &pb.Empty{}, nil
}

// Get implements the KVStore Get RPC method
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Get request for key %s on node %s", req.Key, s.nodeID)

	// Check local store first
	value, timestamp, exists := s.store.Get(req.Key)
	if exists {
		log.Printf("Key %s found on node %s with value %s", req.Key, s.nodeID, value)
		return &pb.GetResponse{
			Value:     value,
			Timestamp: timestamp,
			Exists:    exists,
		}, nil
	}

	log.Printf("Key %s not found locally, checking if we need to fetch from other nodes", req.Key)

	// If not a designated replica, fetch from other nodes
	if !s.isDesignatedReplica(req.Key) {
		log.Printf("This node is not a designated replica for key %s, fetching from replicas", req.Key)
		s.mu.RLock()
		replicas := s.ring.GetReplicas(req.Key, s.replicationFactor)
		log.Printf("Designated replicas for key %s: %v", req.Key, replicas)

		for _, replicaID := range replicas {
			if replicaID == s.nodeID {
				continue
			}

			peerID := strings.TrimPrefix(replicaID, "node-")
			kvClient, ok := s.kvClients[peerID]
			if !ok {
				log.Printf("No client connection for node %s", replicaID)
				continue
			}

			log.Printf("Fetching key %s from node %s", req.Key, replicaID)
			ctxTimeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := kvClient.Get(ctxTimeout, &pb.GetRequest{Key: req.Key})
			cancel()

			if err != nil {
				log.Printf("Error fetching key from node %s: %v", replicaID, err)
				continue
			}

			if resp.Exists {
				log.Printf("Fetched key %s from node %s", req.Key, replicaID)
				s.store.Store(req.Key, resp.Value, resp.Timestamp)
				s.mu.RUnlock()
				return resp, nil
			}
		}
		s.mu.RUnlock()
	}

	log.Printf("Key %s not found on any node", req.Key)
	return &pb.GetResponse{Exists: false}, nil
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

	// Generate timestamp
	timestamp := uint64(time.Now().UnixNano())
    if err := s.store.Store(req.Key, req.Value, timestamp); err != nil {
        return nil, err
    }

	// Store locally
	if err := s.store.Store(req.Key, req.Value, timestamp); err != nil {
		log.Printf("Error storing key %s: %v", req.Key, err)
		return nil, err
	}

	// Replicate asynchronously
	log.Printf("Starting replication for key %s", req.Key)
	go s.replicateToNodes(req.Key, req.Value, timestamp)

	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
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

// replicateToNodes sends replication requests to designated replicas
func (s *server) replicateToNodes(key, value string, timestamp uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	replicas := s.ring.GetReplicas(key, s.replicationFactor)
	log.Printf("Replicating key %s to %d replicas: %v", key, len(replicas), replicas)

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

		log.Printf("Replicating key %s to node %s", key, nodeID)
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

// Start initializes background services
func (s *server) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	go s.startHeartbeat()
	go s.rebalanceRing()
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