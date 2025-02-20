package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

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
}

func NewServer(nodeID uint32) *server {
	s := &server{
		nodeID:  nodeID,
		store:   &node.Node{ID: nodeID},
		ring:    consistenthash.NewRing(consistenthash.DefaultVirtualNodes),
		peers:   make(map[uint32]string),
		clients: make(map[uint32]pb.NodeInternalClient),
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
	return nil
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
	log.Println("Server stopped")
}
