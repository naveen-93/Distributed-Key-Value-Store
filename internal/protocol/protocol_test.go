package protocol

import (
    "context"
    "fmt"
    "testing"
    "time"

"Distributed-Key-Value-Store/internal/storage"
pb "Distributed-Key-Value-Store/kvstore/proto"

"github.com/stretchr/testify/assert"
"google.golang.org/grpc"

)
type mockNodeInternalClient struct {
    replicateFunc func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error)
}
func (m *mockNodeInternalClient) GetReplica(ctx context.Context, req *pb.GetReplicaRequest, opts ...grpc.CallOption) (*pb.GetReplicaResponse, error) {
    return &pb.GetReplicaResponse{Value: "value2", Timestamp: 2, Exists: true}, nil
}
func (m *mockNodeInternalClient) SyncKeys(ctx context.Context, in *pb.SyncRequest, opts ...grpc.CallOption) (*pb.SyncResponse, error) {
    return nil, nil
}
func (m *mockNodeInternalClient) Heartbeat(ctx context.Context, in *pb.Ping, opts ...grpc.CallOption) (*pb.Pong, error) {
    return nil, nil
}
func (m *mockNodeInternalClient) GetRingState(ctx context.Context, in *pb.RingStateRequest, opts ...grpc.CallOption) (*pb.RingStateResponse, error) {
    return nil, nil
}
func (m *mockNodeInternalClient) GarbageCollect(ctx context.Context, req *pb.GarbageCollectRequest, opts ...grpc.CallOption) (*pb.GarbageCollectResponse, error) {
    return &pb.GarbageCollectResponse{Success: true}, nil
}
func (m *mockNodeInternalClient) Replicate(ctx context.Context, req *pb.ReplicateRequest, opts ...grpc.CallOption) (*pb.ReplicateResponse, error) {
    if m.replicateFunc != nil {
        return m.replicateFunc(ctx, req)
    }
    return &pb.ReplicateResponse{Success: true}, nil // Default response if no function is set
}
func TestQuorumPut(t *testing.T) {
    store := storage.NewStorage(1)
    node := NewNode(1, store, 3)
    defer node.Stop()

    node.clients = map[uint32]pb.NodeInternalClient{
        2: &mockNodeInternalClient{
            replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
                return &pb.ReplicateResponse{Success: true}, nil
            },
        },
        3: &mockNodeInternalClient{
            replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
                return &pb.ReplicateResponse{Success: true}, nil
            },
        },
    }
    node.nodes = map[uint32]string{1: "node-1", 2: "node-2", 3: "node-3"}
    // Use node IDs in the ring instead of addresses
    node.ring.AddNode("1")
    node.ring.AddNode("2")
    node.ring.AddNode("3")

    // Add a timeout to prevent hangs
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    resp, err := node.Put(ctx, &pb.PutRequest{Key: "key", Value: "value"})
    assert.NoError(t, err)
    assert.NotNil(t, resp)

    val, _, exists := store.Get("key")
    assert.True(t, exists)
    assert.Equal(t, "value", val)
}
func TestQuorumGet(t *testing.T) {
    store := storage.NewStorage(1)
    node := NewNode(1, store, 3) // Replication factor 3, quorum 2
    defer node.Stop()

// Mock clients with GetReplica
node.clients = map[uint32]pb.NodeInternalClient{
    2: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
        // Add GetReplica to return a consistent value
    },
    3: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
        // Add GetReplica
    },
}
node.nodes = map[uint32]string{1: "node-1", 2: "node-2", 3: "node-3"}
// Use node IDs in the ring
node.ring.AddNode("1")
node.ring.AddNode("2")
node.ring.AddNode("3")

// Simulate putting values
node.Put(context.Background(), &pb.PutRequest{Key: "key", Value: "value1"})
node.Put(context.Background(), &pb.PutRequest{Key: "key", Value: "value2", Timestamp: 1})

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

resp, err := node.Get(ctx, &pb.GetRequest{Key: "key"})
if err != nil {
    t.Fatalf("Get failed: %v", err)
}
assert.Equal(t, "value2", resp.Value)

}
func TestReplication(t *testing.T) {
    store := storage.NewStorage(1)
    node := NewNode(1, store, 3) // Replication factor 3
    defer node.Stop()

// Mock clients
node.clients = map[uint32]pb.NodeInternalClient{
    2: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
    3: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
}
node.nodes = map[uint32]string{1: "node-1", 2: "node-2", 3: "node-3"}
// Use node IDs in the ring
node.ring.AddNode("1")
node.ring.AddNode("2")
node.ring.AddNode("3")
// Simulate a Put operation
_, err := node.Put(context.Background(), &pb.PutRequest{Key: "key", Value: "value"})
assert.NoError(t, err)

// Verify replication to other nodes
for _, client := range node.clients {
    resp, err := client.GetReplica(context.Background(), &pb.GetReplicaRequest{Key: "key"})
    assert.NoError(t, err)
    assert.Equal(t, "value", resp.Value) // Ensure value is replicated
}

}
func TestAntiEntropy(t *testing.T) {
    store := storage.NewStorage(1)
    node := NewNode(1, store, 3) // Replication factor 3
    defer node.Stop()

// Mock clients
node.clients = map[uint32]pb.NodeInternalClient{
    2: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
    3: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
}
node.nodes = map[uint32]string{1: "node-1", 2: "node-2", 3: "node-3"}
node.ring.AddNode("node-1")
node.ring.AddNode("node-2")
node.ring.AddNode("node-3")

// Simulate divergence
node.Put(context.Background(), &pb.PutRequest{Key: "key", Value: "value1"})
node.clients[2].(*mockNodeInternalClient).replicateFunc = func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
    return &pb.ReplicateResponse{Success: true}, nil
}

// Start anti-entropy
node.StartAntiEntropy()

// Simulate a delay to allow anti-entropy to run
time.Sleep(2 * time.Second)

// Verify synchronization
resp, err := node.Get(context.Background(), &pb.GetRequest{Key: "key"})
assert.NoError(t, err)
assert.Equal(t, "value1", resp.Value) // Ensure value is synchronized

}
func TestHeartbeatAndRebalancing(t *testing.T) {
    store := storage.NewStorage(1)
    node := NewNode(1, store, 3) // Replication factor 3
    defer node.Stop()

// Mock clients
node.clients = map[uint32]pb.NodeInternalClient{
    2: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
    3: &mockNodeInternalClient{
        replicateFunc: func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
            return &pb.ReplicateResponse{Success: true}, nil
        },
    },
}
node.nodes = map[uint32]string{1: "node-1", 2: "node-2", 3: "node-3"}
node.ring.AddNode("node-1")
node.ring.AddNode("node-2")
node.ring.AddNode("node-3")

// Simulate node failure
node.clients[2].(*mockNodeInternalClient).replicateFunc = func(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
    return nil, fmt.Errorf("node down")
}

// Start heartbeat
node.StartHeartbeat()

// Simulate a delay to allow heartbeat to run
time.Sleep(2 * time.Second)

// Verify that the node is marked as down
assert.False(t, node.nodeStatus[2]) // Ensure node 2 is marked down

}





