package main

import (
	"Distributed-Key-Value-Store/internal/raft"
	"Distributed-Key-Value-Store/internal/store"
	"flag"
	"log"
	"net"
	"strings"

	pb "Distributed-Key-Value-Store/kvstore/proto"

	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server: defines a struct that implements the pb.KVStoreServer interface.
type server struct {
	pb.UnimplementedKVStoreServer // Embed the unimplemented server.
	node                          *raft.Node
	store                         *store.KVStore
}

// mustEmbedUnimplementedKVStoreServer implements proto.KVStoreServer.
func (s *server) mustEmbedUnimplementedKVStoreServer() {
	panic("unimplemented")
}

func main() {
	var (
		id    = flag.String("id", "", "Node ID")
		port  = flag.String("port", "50051", "Port to listen on")
		peers = flag.String("peers", "", "Comma-separated list of peer addresses")
	)
	flag.Parse()

	// Initialize node
	node := raft.NewNode(*id, strings.Split(*peers, ","))

	// Initialize store
	kvStore := store.NewKVStore()

	// Start server
	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, NewServer(node, kvStore))

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewServer(node *raft.Node, store *store.KVStore) pb.KVStoreServer {

	server := &server{
		node:  node,
		store: store,
	}
	return server
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	panic("unimplemented")
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	panic("unimplemented")
}

// AppendEntries is a stub implementation to satisfy pb.KVStoreServer.
func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "AppendEntries is not implemented")
}

// RequestVote is a stub implementation to satisfy the KVStoreServer interface.
func (s *server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "RequestVote is not implemented")
}


