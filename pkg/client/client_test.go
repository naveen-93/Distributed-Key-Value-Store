package client

import (
	"context"
	"testing"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockKVStoreClient is a mock implementation of the KVStoreClient interface
type MockKVStoreClient struct {
	mock.Mock
}

func (m *MockKVStoreClient) Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.GetResponse), args.Error(1)
}

func (m *MockKVStoreClient) Put(ctx context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.PutResponse), args.Error(1)
}

func (m *MockKVStoreClient) GetRingState(ctx context.Context, in *pb.RingStateRequest, opts ...grpc.CallOption) (*pb.RingStateResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.RingStateResponse), args.Error(1)
}

// TestClient_Get tests the Get method
func TestClient_Get(t *testing.T) {
	mockClient := new(MockKVStoreClient)
	client := &Client{
		clients: map[string]pb.KVStoreClient{
			"node1": mockClient,
			"node2": mockClient,
			"node3": mockClient,
		},
		ring:              consistenthash.NewRing(10),
		readQuorum:        2,
		replicationFactor: 3,
		nodeStatus: map[string]*nodeHealth{
			"node1": {healthy: true},
			"node2": {healthy: true},
			"node3": {healthy: true},
		},
	}

	// Add nodes to ring
	client.ring.AddNode("node1")
	client.ring.AddNode("node2")
	client.ring.AddNode("node3")

	// Setup mock responses
	mockClient.On("Get", mock.Anything, &pb.GetRequest{Key: "test"}).
		Return(&pb.GetResponse{Value: "value1", Timestamp: 1, Exists: true}, nil).Times(2)
	mockClient.On("Get", mock.Anything, &pb.GetRequest{Key: "test"}).
		Return(&pb.GetResponse{Value: "value2", Timestamp: 2, Exists: true}, nil).Once()

	// Test successful read with quorum
	val, err := client.Get("test")
	assert.NoError(t, err)
	assert.Equal(t, "value2", val.Data)
	assert.True(t, val.Exists)

	mockClient.AssertExpectations(t)
}

// TestClient_Put tests the Put method
func TestClient_Put(t *testing.T) {
	mockClient := new(MockKVStoreClient)
	client := &Client{
		clients: map[string]pb.KVStoreClient{
			"node1": mockClient,
			"node2": mockClient,
			"node3": mockClient,
		},
		ring:              consistenthash.NewRing(10),
		writeQuorum:       2,
		replicationFactor: 3,
		nodeStatus: map[string]*nodeHealth{
			"node1": {healthy: true},
			"node2": {healthy: true},
			"node3": {healthy: true},
		},
	}

	// Add nodes to ring
	client.ring.AddNode("node1")
	client.ring.AddNode("node2")
	client.ring.AddNode("node3")

	// Setup mock responses
	mockClient.On("Put", mock.Anything, &pb.PutRequest{Key: "test", Value: "value"}).
		Return(&pb.PutResponse{OldValue: "", HadOldValue: false}, nil).Times(3)

	// Test successful write
	val, err := client.Put("test", "value")
	assert.NoError(t, err)
	assert.Equal(t, "value", val.Data)
	assert.True(t, val.Exists)

	mockClient.AssertExpectations(t)
}

// TestClient_GetReplicaNodes tests replica node selection
func TestClient_GetReplicaNodes(t *testing.T) {
	client := &Client{
		ring:              consistenthash.NewRing(10),
		replicationFactor: 3,
		nodeStatus: map[string]*nodeHealth{
			"node1": {healthy: true},
			"node2": {healthy: true},
			"node3": {healthy: true},
		},
	}

	// Add nodes to the ring
	client.ring.AddNode("node1")
	client.ring.AddNode("node2")
	client.ring.AddNode("node3")

	// Test replica selection
	nodes := client.getReplicaNodes("test")
	assert.Len(t, nodes, 3)
	assert.Contains(t, nodes, "node1")
	assert.Contains(t, nodes, "node2")
	assert.Contains(t, nodes, "node3")

	// Test with unhealthy nodes
	client.nodeStatus["node2"].healthy = false
	nodes = client.getReplicaNodes("test")
	assert.Len(t, nodes, 2)
	assert.NotContains(t, nodes, "node2")
}

// TestClient_ResolveConflicts tests conflict resolution
func TestClient_ResolveConflicts(t *testing.T) {
	client := &Client{}

	responses := []*pb.GetResponse{
		{Value: "value1", Timestamp: 1, Exists: true},
		{Value: "value2", Timestamp: 2, Exists: true},
		{Value: "value3", Timestamp: 1, Exists: true},
	}

	result := client.resolveConflicts(responses)
	assert.Equal(t, "value2", result.Value)
	assert.Equal(t, uint64(2), result.Timestamp)
}

// TestClient_PerformReadRepair tests read repair functionality
func TestClient_PerformReadRepair(t *testing.T) {
	mockClient := new(MockKVStoreClient)
	client := &Client{
		clients: map[string]pb.KVStoreClient{
			"node1": mockClient,
			"node2": mockClient,
			"node3": mockClient,
		},
		nodeStatus: map[string]*nodeHealth{
			"node1": {healthy: true},
			"node2": {healthy: true},
			"node3": {healthy: true},
		},
	}

	key := "test"
	latest := &pb.GetResponse{Value: "new", Timestamp: 2, Exists: true}
	nodes := []string{"node1", "node2", "node3"}
	// Setup mock responses for each node
	for range nodes {
		mockClient.On("Get", mock.Anything, &pb.GetRequest{Key: key}).
			Return(&pb.GetResponse{Value: "old", Timestamp: 1, Exists: true}, nil).Once()
		mockClient.On("Put", mock.Anything, &pb.PutRequest{Key: key, Value: "new"}).
			Return(&pb.PutResponse{}, nil).Once()
	}

	// Perform read repair
	client.performReadRepair(key, latest, nodes)

	// Allow goroutines to complete
	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)
}

// TestClient_UpdateRing tests ring updates
func TestClient_UpdateRing(t *testing.T) {
	mockClient := new(MockKVStoreClient)
	client := &Client{
		clients: map[string]pb.KVStoreClient{
			"node1": mockClient,
			"node2": mockClient,
		},
	}

	// Setup mock responses
	mockClient.On("GetRingState", mock.Anything, &pb.RingStateRequest{}).
		Return(&pb.RingStateResponse{
			Version: 1,
			Nodes: map[string]bool{
				"node1": true,
				"node2": true,
			},
		}, nil).Once()

	// Test ring update
	err := client.updateRing()
	assert.NoError(t, err)
	assert.NotNil(t, client.ring)
	assert.Equal(t, uint64(1), client.ringVersion)

	mockClient.AssertExpectations(t)
}

// TestClient_Close tests the Close method
func TestClient_Close(t *testing.T) {
	mockConn := new(MockConn)
	client := &Client{
		conns: map[string]grpcConnection{
			"node1": mockConn,
			"node2": mockConn,
		},
		closeChan: make(chan struct{}),
	}

	// Setup mock responses
	mockConn.On("Close").Return(nil).Times(2)

	// Test close
	err := client.Close()
	assert.NoError(t, err)

	mockConn.AssertExpectations(t)
}

// MockConn is a mock implementation of grpc.ClientConnInterface
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	return nil
}

func (m *MockConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, nil
}
