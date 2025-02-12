package main

import (
	"flag"
	"kvstore/internal/raft"
	"kvstore/internal/store"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"
	"Distributed-Key-Value-Store/kvstore/proto"

)

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
    store := store.NewKVStore()
    
    // Start server
    lis, err := net.Listen("tcp", ":"+*port)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    grpcServer := grpc.NewServer()
    kvstore.proto.RegisterKVStoreServer(grpcServer, NewServer(node, store))
    grpcServer.Serve(lis)
}

func NewServer(node *raft.Node, store *store.KVStore) {
	panic("unimplemented")
}