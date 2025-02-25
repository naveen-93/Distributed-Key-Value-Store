package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"Distributed-Key-Value-Store/internal/node"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"github.com/google/uuid"
)

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
}

type ServerConfig struct {
	syncInterval      time.Duration
	heartbeatInterval time.Duration
	replicationFactor int
	virtualNodes      int
}

func NewServer(nodeID string, config *ServerConfig) (*server, error) {
	// Use provided config or defaults
	if config == nil {
		config = &ServerConfig{
			syncInterval:      5 * time.Minute,
			heartbeatInterval: time.Second,
			replicationFactor: 3,
			virtualNodes:      consistenthash.DefaultVirtualNodes,
		}
	}

	// Validate or generate UUID
	var nodeUUID uuid.UUID
	var err error

	if nodeID == "" {
		// Generate new UUID if none provided
		nodeUUID = uuid.New()
	} else {
		// Parse provided ID as UUID
		nodeUUID, err = uuid.Parse(nodeID)
		if err != nil {
			return nil, fmt.Errorf("invalid node ID format, must be UUID: %v", err)
		}
	}

	// Use UUID string as node identifier
	nodeIDString := nodeUUID.String()

	s := &server{
		nodeID:            nodeIDString,
		store:             node.NewNode(uint32(consistenthash.HashString(nodeIDString))),
		ring:              consistenthash.NewRing(config.virtualNodes),
		peers:             make(map[string]string),
		clients:           make(map[string]pb.NodeInternalClient),
		syncInterval:      config.syncInterval,
		heartbeatInterval: config.heartbeatInterval,
		replicationFactor: config.replicationFactor,
		stopChan:          make(chan struct{}),
	}

	// Add self to ring using full UUID
	s.ring.AddNode(fmt.Sprintf("node-%s", nodeIDString))
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

// Put implements the KVStore Put RPC method
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Call the appropriate method on the store
	err := s.store.Store(req.Key, req.Value, req.Timestamp)
	if err != nil {
		return nil, err
	}

	// Get old value for response
	oldValue, _, hadOldValue := s.store.Get(req.Key)

	return &pb.PutResponse{
		OldValue:    string(oldValue),
		HadOldValue: hadOldValue,
	}, nil
}

// Replicate implements the NodeInternal Replicate RPC method
func (s *server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
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

// replicateToNodes handles asynchronous replication to other nodes
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

		currentHash = s.ring.HashKey(nextNodeID)
	}
}

func (s *server) rebalanceRing() {
	s.mu.RLock()
	keys := s.store.GetKeys() // Assuming GetKeys() returns all keys in the store
	s.mu.RUnlock()

	for _, key := range keys {
		// Calculate the new node for the key based on the updated ring
		hash := s.ring.HashKey(key)
		newNodeID := s.ring.GetNode(fmt.Sprintf("%d", hash))

		// If the new node is different from the current node, move the key
		if newNodeID != fmt.Sprintf("node-%s", s.nodeID) {
			value, timestamp, exists := s.store.Get(key)
			if exists {
				// Replicate the key to the new node
				req := &pb.ReplicateRequest{
					Key:       key,
					Value:     string(value),
					Timestamp: uint64(timestamp),
				}

				if client, ok := s.clients[newNodeID]; ok {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					_, err := client.Replicate(ctx, req)
					if err != nil {
						log.Printf("Failed to replicate key %s to node %s: %v", key, newNodeID, err)
					}
				}
			}
		}
	}
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
