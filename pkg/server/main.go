package main

import (
	"Distributed-Key-Value-Store/internal/store"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

// server: defines a struct that implements the pb.KVStoreServer interface.
type server struct {
	pb.UnimplementedKVStoreServer
	mu    sync.RWMutex
	store *store.KVStore
}

// mustEmbedUnimplementedKVStoreServer implements proto.KVStoreServer.
func (s *server) mustEmbedUnimplementedKVStoreServer() {
	panic("unimplemented")
}

func main() {
	var (
		port = flag.String("port", "50051", "Port to listen on")
	)
	flag.Parse()

	// Initialize store
	kvStore := store.NewKVStore()

	// Create server instance
	s := &server{
		store: kvStore,
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, s)

	log.Printf("Starting server on port %s...", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Validate key
	if err := validateKey(req.Key); err != nil {
		return &pb.GetResponse{
			Error: err.Error(),
		}, nil
	}

	value, exists := s.store.Get(req.Key)
	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate key and value
	if err := validateKey(req.Key); err != nil {
		return &pb.PutResponse{
			Error: err.Error(),
		}, nil
	}
	if err := validateValue(req.Value); err != nil {
		return &pb.PutResponse{
			Error: err.Error(),
		}, nil
	}

	oldValue, hadOld := s.store.Put(req.Key, req.Value)
	return &pb.PutResponse{
		OldValue:    oldValue,
		HadOldValue: hadOld,
	}, nil
}

func validateKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > 128 {
		return fmt.Errorf("key too long (max 128 bytes)")
	}
	for _, r := range key {
		if r < 32 || r > 126 || r == '[' || r == ']' {
			return fmt.Errorf("invalid character in key")
		}
	}
	return nil
}

func validateValue(value string) error {
	if len(value) > 2048 {
		return fmt.Errorf("value too long (max 2048 bytes)")
	}
	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value")
		}
	}
	return nil
}
