package test

import (
	"Distributed-Key-Value-Store/internal/node"
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/client"
	"Distributed-Key-Value-Store/internal/protocol"

	"context"
	"fmt"
	"log"
	"net"
	"os"

	"testing"
	"time"

	"google.golang.org/grpc"
)

const (
	numTestNodes = 3
	basePort     = 50051
	testKey      = "test-key"
	testValue    = "test-value"
)

type testNode struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedNodeInternalServer
	id      uint32
	server  *grpc.Server
	address string
	store   *node.Node
}

func TestMain(m *testing.M) {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Exit(m.Run())
}

func setupClusterWithOptions(t *testing.T, opts ...protocol.NodeOption) ([]*testNode, *client.Client) {
	var nodes []*testNode
	var addresses []string

	// Create and start test nodes
	for i := 0; i < numTestNodes; i++ {
		nodeID := uint32(i + 1)
		port := basePort + i
		addr := fmt.Sprintf("localhost:%d", port)

		n := &testNode{
			id:      nodeID,
			address: addr,
			store:   node.NewNode(nodeID),
		}

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		n.server = grpc.NewServer()
		pb.RegisterKVStoreServer(n.server, n)
		pb.RegisterNodeInternalServer(n.server, n)

		go func(n *testNode) {
			if err := n.server.Serve(lis); err != nil {
				log.Printf("server exited: %v", err)
			}
		}(n)

		nodes = append(nodes, n)
		addresses = append(addresses, addr)
	}

	// Create client with all node addresses
	c, err := client.NewClient(addresses)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Wait for cluster stabilization
	time.Sleep(500 * time.Millisecond)

	return nodes, c
}

func teardownCluster(nodes []*testNode) {
	for _, n := range nodes {
		n.server.GracefulStop()
	}
}

func TestBasicOperations(t *testing.T) {
	nodes, c := setupCluster(t)
	defer teardownCluster(nodes)

	t.Run("PutGet", func(t *testing.T) {
		_, hadOld, err := c.Put(testKey, testValue)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if hadOld {
			t.Error("Unexpected old value for new key")
		}

		val, exists, err := c.Get(testKey)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !exists || val != testValue {
			t.Errorf("Expected %s, got %s (exists: %t)", testValue, val, exists)
		}

		// Verify all replicas have the value
		for _, n := range nodes {
			v, _, exists := n.store.Get(testKey)
			if !exists || string(v) != testValue {
				t.Errorf("Replica %d missing value", n.id)
			}
		}
	})

	t.Run("UpdateValue", func(t *testing.T) {
		newValue := "updated-value"
		oldVal, hadOld, err := c.Put(testKey, newValue)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if !hadOld || oldVal != testValue {
			t.Error("Invalid old value returned")
		}

		val, exists, err := c.Get(testKey)
		if err != nil || !exists || val != newValue {
			t.Error("Failed to retrieve updated value")
		}
	})
}

func TestQuorumBehavior(t *testing.T) {
	nodes, c := setupCluster(t)
	defer teardownCluster(nodes)

	t.Run("WriteQuorum", func(t *testing.T) {
		// Stop one node
		nodes[0].server.GracefulStop()

		_, _, err := c.Put("quorum-key", "quorum-value")
		if err != nil {
			t.Fatalf("Write should succeed with 2/3 nodes: %v", err)
		}

		// Stop second node
		nodes[1].server.GracefulStop()

		_, _, err = c.Put("quorum-key", "quorum-value")
		if err == nil {
			t.Error("Write should fail with 1/3 nodes")
		}
	})

	t.Run("ReadQuorum", func(t *testing.T) {
		// Stop one node
		nodes[0].server.GracefulStop()

		_, _, err := c.Get(testKey)
		if err != nil {
			t.Fatalf("Read should succeed with 2/3 nodes: %v", err)
		}

		// Stop second node
		nodes[1].server.GracefulStop()

		_, _, err = c.Get(testKey)
		if err == nil {
			t.Error("Read should fail with 1/3 nodes")
		}
	})
}

func TestConsistency(t *testing.T) {
	// Use shorter sync interval for tests
	syncInterval := 500 * time.Millisecond
	nodes, c := setupClusterWithOptions(t, protocol.WithSyncInterval(syncInterval))
	defer teardownCluster(nodes)

	t.Run("ReadRepair", func(t *testing.T) {
		// Corrupt one replica
		nodes[0].store.Store(testKey, "stale-value", 1)

		// Read should trigger repair
		val, exists, err := c.Get(testKey)
		if err != nil || !exists || val != testValue {
			t.Fatal("Failed to get correct value")
		}

		// Verify repair
		v, _, _ := nodes[0].store.Get(testKey)
		if string(v) != testValue {
			t.Error("Read repair failed")
		}
	})

	t.Run("AntiEntropy", func(t *testing.T) {
		// Simulate network partition
		nodes[0].server.GracefulStop()
		nodes[1].server.GracefulStop()

		// Write to remaining node
		nodes[2].store.Store("anti-entropy-key", "anti-entropy-value", uint64(time.Now().UnixNano()))

		// Restart nodes
		nodes[0] = restartNode(t, nodes[0])
		nodes[1] = restartNode(t, nodes[1])

		// Wait for two sync cycles to complete
		for i := 0; i < 2; i++ {
			select {
			case <-nodes[0].syncComplete:
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for anti-entropy sync")
			}
		}

		// Verify all nodes have the key
		for _, n := range nodes {
			_, _, exists := n.store.Get("anti-entropy-key")
			if !exists {
				t.Errorf("Node %d missing anti-entropy key", n.id)
			}
		}
	})
}

func TestFailureHandling(t *testing.T) {
	nodes, c := setupCluster(t)
	defer teardownCluster(nodes)

	t.Run("RetryLogic", func(t *testing.T) {
		// Stop first node
		nodes[0].server.GracefulStop()

		start := time.Now()
		_, _, err := c.Put("retry-key", "retry-value")
		if err != nil {
			t.Fatalf("Operation should succeed with retries: %v", err)
		}

		if time.Since(start) < 100*time.Millisecond {
			t.Error("Didn't wait for retries")
		}
	})

	t.Run("NodeFailureDetection", func(t *testing.T) {
		// Stop two nodes
		nodes[0].server.GracefulStop()
		nodes[1].server.GracefulStop()

		time.Sleep(2 * time.Second) // Wait for failure detection

		// Should exclude failed nodes from ring
		_, _, err := c.Get(testKey)
		if err != nil {
			t.Errorf("Should succeed with remaining node: %v", err)
		}
	})
}

func TestEdgeCases(t *testing.T) {
	nodes, c := setupCluster(t)
	defer teardownCluster(nodes)

	t.Run("MaxSizedValues", func(t *testing.T) {
		longKey := string(make([]byte, 128))
		longValue := string(make([]byte, 2048))

		_, _, err := c.Put(longKey, longValue)
		if err != nil {
			t.Errorf("Failed to store max-sized values: %v", err)
		}
	})

	t.Run("InvalidInputs", func(t *testing.T) {
		invalidKey := "invalid[key]"
		_, _, err := c.Put(invalidKey, "value")
		if err == nil {
			t.Error("Should reject invalid key")
		}

		uuValue := "begin 644 file.txt"
		_, _, err = c.Put("valid-key", uuValue)
		if err == nil {
			t.Error("Should reject UU encoded value")
		}
	})
}

func restartNode(t *testing.T, n *testNode) *testNode {
	lis, err := net.Listen("tcp", n.address)
	if err != nil {
		t.Fatalf("failed to restart node: %v", err)
	}

	newNode := &testNode{
		id:      n.id,
		address: n.address,
		store:   node.NewNode(n.id),
		server:  grpc.NewServer(),
	}

	pb.RegisterKVStoreServer(newNode.server, newNode)
	pb.RegisterNodeInternalServer(newNode.server, newNode)

	go func() {
		if err := newNode.server.Serve(lis); err != nil {
			log.Printf("restarted server exited: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	return newNode
}

// Implement required server methods for testNode
func (n *testNode) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, timestamp, exists := n.store.Get(req.Key)
	return &pb.GetResponse{
		Value:     string(value),
		Exists:    exists,
		Timestamp: uint64(timestamp),
	}, nil
}

func (n *testNode) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	oldValue, oldTimestamp, hadOld := n.store.Store(req.Key, req.Value, req.Timestamp)
	return &pb.PutResponse{
		OldValue:     oldValue,
		HadOldValue:  hadOld,
		OldTimestamp: oldTimestamp,
	}, nil
}

// Implement the GetRingState method
func (n *testNode) GetRingState(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
	// Return a mock response for testing
	return &pb.RingStateResponse{
		Version:   1,
		Nodes:     map[string]bool{n.address: true},
		UpdatedAt: time.Now().Unix(),
	}, nil
}
