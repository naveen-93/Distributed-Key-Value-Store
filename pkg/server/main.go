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
)

type server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedNodeInternalServer

	nodeID uint32
	store  *node.Node
	ring   *consistenthash.Ring
	mu     sync.RWMutex

	// Node management
	peers   map[uint32]string
	clients map[uint32]pb.NodeInternalClient

	// Configuration
	syncInterval      time.Duration
	heartbeatInterval time.Duration
	replicationFactor int
}

func NewServer(nodeID uint32) *server {
	s := &server{
		nodeID:            nodeID,
		store:             node.NewNode(nodeID),
		ring:              consistenthash.NewRing(consistenthash.DefaultVirtualNodes),
		peers:             make(map[uint32]string),
		clients:           make(map[uint32]pb.NodeInternalClient),
		syncInterval:      5 * time.Minute,
		heartbeatInterval: time.Second,
		replicationFactor: 3,
	}

	// Add self to ring
	s.ring.AddNode(fmt.Sprintf("node-%d", nodeID))
	return s
}

func (s *server) addPeer(peerID uint32, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add to peer list and ring
	s.peers[peerID] = addr
	s.ring.AddNode(fmt.Sprintf("node-%d", peerID))

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
	// Get local value
	value, timestamp, exists := s.store.Get(req.Key)
	if !exists {
		return &pb.GetResponse{
			Exists: false,
		}, nil
	}

	// Query replicas for potentially newer values
	replicaValues := make([]struct {
		value     string
		timestamp uint64
	}, 0)

	// Add local value
	replicaValues = append(replicaValues, struct {
		value     string
		timestamp uint64
	}{string(value), timestamp})

	// Find the latest value among replicas
	var latest struct {
		value     string
		timestamp uint64
	}

	for _, rv := range replicaValues {
		if rv.timestamp > latest.timestamp {
			latest.value = rv.value
			latest.timestamp = rv.timestamp
		}
	}

	// Perform read repair if needed
	if latest.timestamp > timestamp {
		go s.store.Store(req.Key, latest.value, latest.timestamp)
	}

	return &pb.GetResponse{
		Value:     latest.value,
		Exists:    true,
		Timestamp: latest.timestamp,
	}, nil
}

// Put implements the KVStore Put RPC method
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Store value locally first
	err := s.store.Store(req.Key, req.Value, req.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to store value: %v", err)
	}

	// Get the old value for the response
	oldValue, oldTimestamp, hadOld := s.store.Get(req.Key)

	// Replicate to other nodes asynchronously
	go s.replicateToNodes(req.Key, req.Value, req.Timestamp)

	return &pb.PutResponse{
		OldValue:     oldValue,
		HadOldValue:  hadOld,
		OldTimestamp: oldTimestamp,
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
		nodes[fmt.Sprintf("node-%d", id)] = true
	}
	// Add self
	nodes[fmt.Sprintf("node-%d", s.nodeID)] = true

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

	req := &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	for nodeID, client := range s.clients {
		if nodeID == s.nodeID {
			continue // Skip self
		}

		go func(c pb.NodeInternalClient) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err := c.Replicate(ctx, req)
			if err != nil {
				log.Printf("Failed to replicate to node: %v", err)
			}
		}(client)
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
		if newNodeID != fmt.Sprintf("node-%d", s.nodeID) {
			value, timestamp, exists := s.store.Get(key)
			if exists {
				// Replicate the key to the new node
				req := &pb.ReplicateRequest{
					Key:       key,
					Value:     string(value),
					Timestamp: uint64(timestamp),
				}

				if client, ok := s.clients[uint32(s.ring.HashKey(newNodeID))]; ok {
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

func main() {
	// Parse flags
	nodeID := flag.Uint("id", 0, "Node ID (required)")
	addr := flag.String("addr", ":50051", "Address to listen on")
	peerList := flag.String("peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	if *nodeID == 0 {
		log.Fatal("Node ID is required")
	}

	// Create server
	srv := NewServer(uint32(*nodeID))

	// Add peers if provided
	if *peerList != "" {
		peers := strings.Split(*peerList, ",")
		for i, addr := range peers {
			peerID := uint32(i + 1)
			if peerID == uint32(*nodeID) {
				continue
			}
			if err := srv.addPeer(peerID, addr); err != nil {
				log.Printf("Failed to add peer %d at %s: %v", peerID, addr, err)
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
		log.Printf("Node %d listening on %s", *nodeID, *addr)
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
