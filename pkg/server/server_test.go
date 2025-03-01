package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type testCluster struct {
	servers   []*server
	clients   []*grpc.ClientConn
	listeners []*bufconn.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	t         *testing.T
}

func setupTestCluster(t *testing.T, nodeCount int) *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	tc := &testCluster{
		servers:   make([]*server, nodeCount),
		clients:   make([]*grpc.ClientConn, nodeCount),
		listeners: make([]*bufconn.Listener, nodeCount),
		ctx:       ctx,
		cancel:    cancel,
		t:         t,
	}

	listenersMap := make(map[string]*bufconn.Listener)

	// Create servers and listeners
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		config := &ServerConfig{
			syncInterval:      5 * time.Minute,
			heartbeatInterval: 1 * time.Second,
			replicationFactor: 3,
			virtualNodes:      10,
		}
		s, err := NewServer(nodeID, config)
		require.NoError(t, err)
		tc.servers[i] = s

		lis := bufconn.Listen(1024 * 1024)
		tc.listeners[i] = lis
		listenersMap[nodeID] = lis

		grpcServer := grpc.NewServer()
		pb.RegisterKVStoreServer(grpcServer, s)
		pb.RegisterNodeInternalServer(grpcServer, s)
		go grpcServer.Serve(lis)
	}

	// Connect nodes as peers
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i == j {
				continue
			}
			peerID := fmt.Sprintf("node-%d", j)
			peerListener := listenersMap[peerID]
			dialer := func(ctx context.Context, addr string) (net.Conn, error) {
				return peerListener.Dial()
			}
			conn, err := grpc.DialContext(ctx, peerID, grpc.WithContextDialer(dialer), grpc.WithInsecure())
			require.NoError(t, err)
			client := pb.NewNodeInternalClient(conn)
			tc.servers[i].clients[peerID] = client
			tc.servers[i].peers[peerID] = peerID
			tc.servers[i].ring.AddNode(fmt.Sprintf("node-%s", peerID))
		}
	}

	// Create test clients
	for i := 0; i < nodeCount; i++ {
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("node-%d", i),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return tc.listeners[i].Dial()
			}), grpc.WithInsecure())
		require.NoError(t, err)
		tc.clients[i] = conn
	}

	return tc
}

func (tc *testCluster) cleanup() {
	tc.cancel()
	for _, conn := range tc.clients {
		conn.Close()
	}
	for _, server := range tc.servers {
		server.Stop()
	}
}

// Test 1.1: Single-node PUT and GET
func TestSingleNodePutGet(t *testing.T) {
	tc := setupTestCluster(t, 1)
	defer tc.cleanup()

	client := pb.NewKVStoreClient(tc.clients[0])
	ctx := context.Background()

	// Test PUT
	putReq := &pb.PutRequest{
		Key:   "test-key",
		Value: "test-value",
	}
	_, err := client.Put(ctx, putReq)
	require.NoError(t, err)

	// Test GET
	getReq := &pb.GetRequest{Key: "test-key"}
	resp, err := client.Get(ctx, getReq)
	require.NoError(t, err)
	assert.True(t, resp.Exists)
	assert.Equal(t, "test-value", resp.Value)
}

// Test 1.2: Multi-node PUT and GET with quorum
func TestMultiNodeQuorum(t *testing.T) {
	tc := setupTestCluster(t, 3)
	defer tc.cleanup()

	client := pb.NewKVStoreClient(tc.clients[0])
	ctx := context.Background()

	// PUT with quorum
	putReq := &pb.PutRequest{
		Key:   "quorum-key",
		Value: "quorum-value",
	}
	_, err := client.Put(ctx, putReq)
	require.NoError(t, err)

	// Wait for replication to complete
	time.Sleep(2 * time.Second) // Adjust delay as needed

	// Verify on all nodes
	for i := 0; i < 3; i++ {
		nodeClient := pb.NewKVStoreClient(tc.clients[i])
		resp, err := nodeClient.Get(ctx, &pb.GetRequest{Key: "quorum-key"})
		require.NoError(t, err)
		assert.True(t, resp.Exists, "Node %d: key should exist", i)
		assert.Equal(t, "quorum-value", resp.Value, "Node %d: value mismatch", i)
	}
}

// Test 2.3: Concurrent writes with conflict resolution
func TestConcurrentWrites(t *testing.T) {
	tc := setupTestCluster(t, 3)
	defer tc.cleanup()

	var wg sync.WaitGroup
	results := make(chan string, 10)

	// Launch concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			client := pb.NewKVStoreClient(tc.clients[0])
			ctx := context.Background()

			putReq := &pb.PutRequest{
				Key:       "concurrent-key",
				Value:     fmt.Sprintf("value-%d", val),
				Timestamp: uint64(time.Now().UnixNano()),
			}
			_, err := client.Put(ctx, putReq)
			if err == nil {
				results <- fmt.Sprintf("value-%d", val)
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Verify consistency across nodes
	finalValue := ""
	for i := 0; i < 3; i++ {
		client := pb.NewKVStoreClient(tc.clients[i])
		resp, err := client.Get(context.Background(), &pb.GetRequest{Key: "concurrent-key"})
		require.NoError(t, err)

		if finalValue == "" {
			finalValue = resp.Value
		} else {
			assert.Equal(t, finalValue, resp.Value, "All nodes should have same value")
		}
	}
}

// Test 4.1: Crash recovery from WAL
func TestWALRecovery(t *testing.T) {
	tc := setupTestCluster(t, 1)
	defer tc.cleanup()

	client := pb.NewKVStoreClient(tc.clients[0])
	ctx := context.Background()

	// Write data
	for i := 0; i < 5; i++ {
		putReq := &pb.PutRequest{
			Key:   fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("value-%d", i),
		}
		_, err := client.Put(ctx, putReq)
		require.NoError(t, err)
	}

	// Simulate crash
	tc.servers[0].Stop()

	// Restart server with proper config
	config := &ServerConfig{
		syncInterval:      5 * time.Minute,
		heartbeatInterval: 1 * time.Second,
		replicationFactor: 3,
		virtualNodes:      10,
	}
	s, err := NewServer("node-0", config)
	require.NoError(t, err)
	tc.servers[0] = s

	// Restart gRPC server
	lis := tc.listeners[0]
	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, s)
	pb.RegisterNodeInternalServer(grpcServer, s)
	go grpcServer.Serve(lis)

	// Verify recovery
	for i := 0; i < 5; i++ {
		getReq := &pb.GetRequest{Key: fmt.Sprintf("key-%d", i)}
		resp, err := client.Get(ctx, getReq)
		require.NoError(t, err)
		assert.True(t, resp.Exists)
		assert.Equal(t, fmt.Sprintf("value-%d", i), resp.Value)
	}
}

// Test 5.1: Anti-entropy sync
func TestAntiEntropy(t *testing.T) {
	// Adjust config for faster sync in tests
	tc := setupTestCluster(t, 3)
	for _, s := range tc.servers {
		s.syncInterval = 1 * time.Second // Faster sync for testing
	}
	defer tc.cleanup()

	client := pb.NewKVStoreClient(tc.clients[0])
	ctx := context.Background()

	// Write initial value
	putReq := &pb.PutRequest{Key: "sync-key", Value: "sync-value"}
	_, err := client.Put(ctx, putReq)
	require.NoError(t, err)

	// Write new value
	putReq.Value = "new-value"
	_, err = client.Put(ctx, putReq)
	require.NoError(t, err)

	// Wait for anti-entropy to sync (slightly more than syncInterval)
	time.Sleep(2 * time.Second)

	// Verify consistency
	for i := 0; i < 3; i++ {
		nodeClient := pb.NewKVStoreClient(tc.clients[i])
		resp, err := nodeClient.Get(ctx, &pb.GetRequest{Key: "sync-key"})
		require.NoError(t, err)
		assert.True(t, resp.Exists)
		assert.Equal(t, "new-value", resp.Value)
	}
}
