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
	"google.golang.org/grpc/status"

	"Distributed-Key-Value-Store/internal/storage"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"github.com/google/uuid"
)

type server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedNodeInternalServer

	nodeID string
	store  *storage.Node
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

	ctx    context.Context
	cancel context.CancelFunc
}

type ServerConfig struct {
	syncInterval      time.Duration
	heartbeatInterval time.Duration
	replicationFactor int
	virtualNodes      int
}

// Add validation constants
const (
	maxKeyLength   = 256         // Maximum key length in bytes
	maxValueLength = 1024 * 1024 // Maximum value length (1MB)
)

// Add rebalancing configuration
type RebalanceConfig struct {
	BatchSize     int           // Number of keys per batch
	BatchTimeout  time.Duration // Max time per batch
	MaxConcurrent int           // Max concurrent transfers
	RetryAttempts int           // Number of retry attempts
}

func NewServer(nodeID string, config *ServerConfig) (*server, error) {
	if config.heartbeatInterval <= 0 {
		return nil, fmt.Errorf("heartbeatInterval must be positive, got %v", config.heartbeatInterval)
	}
	if config.replicationFactor <= 0 {
		return nil, fmt.Errorf("replicationFactor must be positive, got %d", config.replicationFactor)
	}
	if config.virtualNodes <= 0 {
		return nil, fmt.Errorf("virtualNodes must be positive, got %d", config.virtualNodes)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &server{
		nodeID:            nodeID,
		store:             storage.NewNode(uint32(consistenthash.HashString(nodeID)), config.replicationFactor),
		ring:              consistenthash.NewRing(config.virtualNodes),
		peers:             make(map[string]string),
		clients:           make(map[string]pb.NodeInternalClient),
		syncInterval:      config.syncInterval,
		heartbeatInterval: config.heartbeatInterval,
		replicationFactor: config.replicationFactor,
		stopChan:          make(chan struct{}),
		rebalanceConfig: RebalanceConfig{
			BatchSize:     1000,
			BatchTimeout:  30 * time.Second,
			MaxConcurrent: 5,
			RetryAttempts: 3,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Add self to ring using full UUID
	s.ring.AddNode(fmt.Sprintf("node-%s", nodeID))
	s.startHeartbeat()
	return s, nil
}

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

// Get implements the KVStore Get RPC method
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, timestamp, exists := s.store.Get(req.Key)
	return &pb.GetResponse{
		Value:     value,
		Timestamp: timestamp,
		Exists:    exists,
	}, nil
}

// Add helper function for key validation
func isValidKey(key string) bool {
	for _, r := range key {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) &&
			r != '-' && r != '_' && r != '.' && r != '/' {
			return false
		}
	}
	return true
}

// Add validation helper
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

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validateKeyValue(req.Key, req.Value); err != nil {
		return nil, err
	}

	// Generate timestamp
	timestamp := uint64(time.Now().UnixNano())

	// Store locally first
	err := s.store.Store(req.Key, req.Value, timestamp)
	if err != nil {
		return nil, err
	}

	// Replicate to other nodes asynchronously
	s.replicateToNodes(req.Key, req.Value, timestamp)

	// Return immediately after local store (eventual consistency)
	oldValue, _, hadOldValue := s.store.Get(req.Key)
	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
}

// Add helper method to check if this node is a designated replica for a key
func (s *server) isDesignatedReplica(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get replica nodes from ring
	hash := s.ring.HashKey(key)
	replicaCount := 0
	currentHash := hash

	// Check if we're one of the replica nodes
	for replicaCount < s.replicationFactor {
		nodeID := s.ring.GetNextNode(currentHash)
		if nodeID == "" {
			break // No more nodes available
		}

		// Check if this node is the replica
		if nodeID == fmt.Sprintf("node-%s", s.nodeID) {
			return true
		}

		replicaCount++
		currentHash = s.ring.HashKey(nodeID)
	}

	return false
}

// Update Replicate method to include validation
func (s *server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	// First validate the key/value pair
	if err := validateKeyValue(req.Key, req.Value); err != nil {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Verify this node should be a replica for this key
	if !s.isDesignatedReplica(req.Key) {
		return &pb.ReplicateResponse{
			Success: false,
			Error:   "not a designated replica for this key",
		}, nil
	}

	// Check if we have a newer value
	_, timestamp, exists := s.store.Get(req.Key)
	if !exists || req.Timestamp > timestamp {
		// Store the replicated value
		if err := s.store.Store(req.Key, req.Value, req.Timestamp); err != nil {
			return &pb.ReplicateResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to store replicated value: %v", err),
			}, nil
		}
		return &pb.ReplicateResponse{Success: true}, nil
	}

	return &pb.ReplicateResponse{
		Success: false,
		Error:   "have newer value",
	}, nil
}

// GetRingState implements the NodeInternal GetRingState RPC method
func (s *server) GetRingState(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make(map[string]bool)
	for id, _ := range s.peers {
		nodes[fmt.Sprintf("node-%s", id)] = true
	}
	// Add self
	nodes[fmt.Sprintf("node-%s", s.nodeID)] = true

	return &pb.RingStateResponse{
		Version:   uint64(time.Now().UnixNano()),
		Nodes:     nodes,
		UpdatedAt: time.Now().Unix(),
	}, nil
}

// Update replicateToNodes to be more selective
func (s *server) replicateToNodes(key string, value string, timestamp uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create replication request
	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	// Get replica nodes using consistent hashing
	hash := s.ring.HashKey(key)
	currentHash := hash
	replicasSent := 0

	// Send to next nodes in the ring
	for replicasSent < s.replicationFactor {
		nextNodeID := s.ring.GetNextNode(currentHash)
		if nextNodeID == "" {
			break // No more nodes available
		}

		// Extract UUID from node ID format "node-{uuid}"
		peerID := strings.TrimPrefix(nextNodeID, "node-")
		if client, ok := s.clients[peerID]; ok {
			go func(c pb.NodeInternalClient) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				resp, err := c.Replicate(ctx, req)
				if err != nil {
					log.Printf("Failed to replicate to node %s: %v", nextNodeID, err)
				} else if !resp.Success {
					log.Printf("Replication rejected by node %s: %s", nextNodeID, resp.Error)
				}
			}(client)
			replicasSent++
		}

		currentHash = s.ring.HashKey(nextNodeID)
	}
}

// Update rebalanceRing with batching and concurrency control
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

// Process a batch of keys
func (s *server) processBatch(keys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.rebalanceConfig.BatchTimeout)
	defer cancel()

	for _, key := range keys {
		// Calculate new node for the key
		hash := s.ring.HashKey(key)
		newNodeID := s.ring.GetNode(fmt.Sprintf("%d", hash))

		// Skip if key belongs to current node
		if newNodeID == fmt.Sprintf("node-%s", s.nodeID) {
			continue
		}

		// Get key data
		value, timestamp, exists := s.store.Get(key)
		if !exists {
			continue
		}

		// Try to replicate with retries
		var lastErr error
		for attempt := 0; attempt < s.rebalanceConfig.RetryAttempts; attempt++ {
			if err := s.replicateKey(ctx, key, string(value), timestamp, newNodeID); err != nil {
				lastErr = err
				// Exponential backoff
				time.Sleep(time.Duration(1<<uint(attempt)) * 100 * time.Millisecond)
				continue
			}
			lastErr = nil
			break
		}

		if lastErr != nil {
			return fmt.Errorf("failed to replicate key %s after %d attempts: %v",
				key, s.rebalanceConfig.RetryAttempts, lastErr)
		}
	}

	return nil
}

// Update replicateKey to include validation
func (s *server) replicateKey(ctx context.Context, key, value string, timestamp uint64, nodeID string) error {
	// Validate before sending
	if err := validateKeyValue(key, value); err != nil {
		return fmt.Errorf("validation failed: %v", err)
	}

	// Extract UUID from node ID format "node-{uuid}"
	peerID := strings.TrimPrefix(nodeID, "node-")
	client, ok := s.clients[peerID]
	if !ok {
		return fmt.Errorf("no client connection to node %s", nodeID)
	}

	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	resp, err := client.Replicate(ctx, req)
	if err != nil {
		return fmt.Errorf("replication failed: %v", err)
	}
	if !resp.Success {
		return fmt.Errorf("replication rejected: %s", resp.Error)
	}

	return nil
}

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

func (s *server) handlePeerFailure(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.peers, peerID)
	delete(s.clients, peerID)
	s.ring.RemoveNode(fmt.Sprintf("node-%s", peerID))
	s.rebalanceRing()
}

func (s *server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *server) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	// Start background services
	go s.startHeartbeat()
}

func main() {
	nodeID := flag.String("id", "", "Node ID (optional, must be valid UUID if provided)")
	addr := flag.String("addr", ":50051", "Address to listen on")
	peerList := flag.String("peers", "", "Comma-separated list of peer addresses in format 'uuid@address'")
	syncInterval := flag.Duration("sync-interval", 5*time.Minute, "Anti-entropy sync interval")
	heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "Heartbeat check interval")
	replicationFactor := flag.Int("replication-factor", 3, "Number of replicas per key")
	virtualNodes := flag.Int("virtual-nodes", consistenthash.DefaultVirtualNodes, "Number of virtual nodes per physical node")
	flag.Parse()

	config := &ServerConfig{
		syncInterval:      *syncInterval,
		heartbeatInterval: *heartbeatInterval,
		replicationFactor: *replicationFactor,
		virtualNodes:      *virtualNodes,
	}

	srv, err := NewServer(*nodeID, config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Parse peers in format "uuid@address"
	if *peerList != "" {
		peers := strings.Split(*peerList, ",")
		for _, peer := range peers {
			parts := strings.Split(peer, "@")
			if len(parts) != 2 {
				log.Printf("Invalid peer format %s, expected 'uuid@address'", peer)
				continue
			}
			peerID, addr := parts[0], parts[1]

			// Validate peer UUID
			if _, err := uuid.Parse(peerID); err != nil {
				log.Printf("Invalid peer UUID %s: %v", peerID, err)
				continue
			}

			if peerID == srv.nodeID {
				continue // Skip self
			}
			if err := srv.addPeer(peerID, addr); err != nil {
				log.Printf("Failed to add peer %s at %s: %v", peerID, addr, err)
			}
		}
	}

	// Set up gRPC server
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, srv)
	pb.RegisterNodeInternalServer(grpcServer, srv)

	// Start server
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
	log.Println("Shutting down...")
	grpcServer.GracefulStop()
	if err := srv.store.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
	log.Println("Server stopped")
}

