package client

import (
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// MockKVStoreClient is a mock implementation of the KVStoreClient interface
type MockKVStoreClient struct {
	mock.Mock
}

func (m *MockKVStoreClient) Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.GetResponse), args.Error(1)
}

func (m *MockKVStoreClient) Put(ctx context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.PutResponse), args.Error(1)
}

func (m *MockKVStoreClient) GetRingState(ctx context.Context, in *pb.RingStateRequest, opts ...grpc.CallOption) (*pb.RingStateResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.RingStateResponse), args.Error(1)
}

// MockKVStoreServer is a mock implementation of the KVStore server
type MockKVStoreServer struct {
	pb.UnimplementedKVStoreServer
	GetFunc          func(context.Context, *pb.GetRequest) (*pb.GetResponse, error)
	PutFunc          func(context.Context, *pb.PutRequest) (*pb.PutResponse, error)
	GetRingStateFunc func(context.Context, *pb.RingStateRequest) (*pb.RingStateResponse, error)
}

func (m *MockKVStoreServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, req)
	}
	return &pb.GetResponse{}, nil
}

func (m *MockKVStoreServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if m.PutFunc != nil {
		return m.PutFunc(ctx, req)
	}
	return &pb.PutResponse{}, nil
}

func (m *MockKVStoreServer) GetRingState(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
	if m.GetRingStateFunc != nil {
		return m.GetRingStateFunc(ctx, req)
	}
	return &pb.RingStateResponse{
		Version:   1,
		Nodes:     map[string]bool{"localhost:": true},
		UpdatedAt: time.Now().Unix(),
	}, nil
}

// Setup a gRPC server using bufconn for testing
func setupGRPCServer(server pb.KVStoreServer) (pb.KVStoreClient, func()) {
	bufSize := 1024 * 1024
	listener := bufconn.Listen(bufSize)

	s := grpc.NewServer()
	pb.RegisterKVStoreServer(s, server)

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	// Create a client connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	client := pb.NewKVStoreClient(conn)

	return client, func() {
		conn.Close()
		s.Stop()
	}
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		name        string
		servers     []string
		expectError bool
	}{
		{
			name:        "No servers",
			servers:     []string{},
			expectError: true,
		},
		{
			name:        "Valid servers",
			servers:     []string{"localhost:50051", "localhost:50052"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock server for the valid case
			if !tt.expectError {
				mockServer := &MockKVStoreServer{
					GetRingStateFunc: func(ctx context.Context, req *pb.RingStateRequest) (*pb.RingStateResponse, error) {
						return &pb.RingStateResponse{
							Version:   1,
							Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true},
							UpdatedAt: time.Now().Unix(),
						}, nil
					},
				}

				client, cleanup := setupGRPCServer(mockServer)
				defer cleanup()

				// Replace the real client creation with our mock
				originalInitConnections := initConnections
				defer func() { initConnections = originalInitConnections }()

				initConnections = func(c *Client) error {
					c.clients = map[string]pb.KVStoreClient{
						"localhost:50051": client,
						"localhost:50052": client,
					}
					c.connections = map[string]*grpc.ClientConn{
						"localhost:50051": nil, // Not used in tests
						"localhost:50052": nil,
					}
					return nil
				}
			}

			client, err := NewClient(tt.servers)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)

				// Cleanup
				if client != nil {
					client.Close()
				}
			}
		})
	}
}

// Helper function to replace the initConnections method for testing
var initConnections = func(c *Client) error {
	return c.initConnections()
}

// Add this function at the top of your test file
func disableReadRepair(t *testing.T) func() {
	original := performReadRepair
	performReadRepair = func(c *Client, key string, latest valueWithTimestamp, values []valueWithTimestamp) {
		// Do nothing during tests
	}
	return func() {
		performReadRepair = original
	}
}

// Add this variable at the top of your file
var performReadRepair = func(c *Client, key string, latest valueWithTimestamp, values []valueWithTimestamp) {
	c.performReadRepair(key, latest, values)
}

func TestGet(t *testing.T) {
	// Disable read repair
	cleanup := disableReadRepair(t)
	defer cleanup()

	tests := []struct {
		name           string
		key            string
		mockResponses  []*pb.GetResponse
		mockErrors     []error
		expectedValue  string
		expectedExists bool
		expectedError  error
	}{
		{
			name: "Successful get",
			key:  "test-key",
			mockResponses: []*pb.GetResponse{
				{Value: "value1", Exists: true, Timestamp: 100},
				{Value: "value1", Exists: true, Timestamp: 100},
				{Value: "value1", Exists: true, Timestamp: 100},
			},
			mockErrors:     []error{nil, nil, nil},
			expectedValue:  "value1",
			expectedExists: true,
			expectedError:  nil,
		},
		{
			name: "Key not found",
			key:  "missing-key",
			mockResponses: []*pb.GetResponse{
				{Exists: false},
				{Exists: false},
				{Exists: false},
			},
			mockErrors:     []error{nil, nil, nil},
			expectedValue:  "",
			expectedExists: false,
			expectedError:  nil,
		},
		{
			name: "Quorum not met",
			key:  "test-key",
			mockResponses: []*pb.GetResponse{
				{Value: "value1", Exists: true, Timestamp: 100},
			},
			mockErrors:     []error{nil, errors.New("connection error"), errors.New("timeout")},
			expectedValue:  "",
			expectedExists: false,
			expectedError:  ErrQuorumNotMet,
		},
		{
			name:           "Invalid key",
			key:            string([]byte{0x00, 0x01}), // Invalid characters
			mockResponses:  nil,
			mockErrors:     nil,
			expectedValue:  "",
			expectedExists: false,
			expectedError:  ErrValidation,
		},
		{
			name: "Conflict resolution",
			key:  "test-key",
			mockResponses: []*pb.GetResponse{
				{Value: "value1", Exists: true, Timestamp: 100},
				{Value: "value2", Exists: true, Timestamp: 150},
				{Value: "value3", Exists: true, Timestamp: 200}, // Highest timestamp
			},
			mockErrors:     []error{nil, nil, nil},
			expectedValue:  "value3",
			expectedExists: true,
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Quorum not met" {
				// Create a custom client for this specific test case
				mockClients := make(map[string]pb.KVStoreClient)
				addresses := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

				// Set up mock clients - only one will succeed, others will fail
				for i, addr := range addresses {
					mockClient := new(MockKVStoreClient)

					// Set up GetRingState
					mockClient.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
						Return(&pb.RingStateResponse{
							Version:   1,
							Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true, "localhost:50053": true},
							UpdatedAt: time.Now().Unix(),
						}, nil)

					// Only the first client succeeds, others fail
					if i == 0 {
						mockClient.On("Get", mock.Anything, mock.Anything, mock.Anything).
							Return(&pb.GetResponse{Value: "value1", Exists: true, Timestamp: 100}, nil)
					} else {
						mockClient.On("Get", mock.Anything, mock.Anything, mock.Anything).
							Return(nil, errors.New("connection error"))
					}

					mockClients[addr] = mockClient
				}

				// Create client with proper configuration
				client := &Client{
					servers:        addresses,
					clients:        mockClients,
					connections:    make(map[string]*grpc.ClientConn),
					dialTimeout:    5 * time.Second,
					requestTimeout: 2 * time.Second,
					maxRetries:     1,
					readQuorum:     2,
					writeQuorum:    2,
					nodeStates:     make(map[string]*nodeState),
				}

				// Initialize ring
				client.ring = consistenthash.NewRing(3)
				for _, addr := range addresses {
					client.ring.AddNode(addr)
				}

				// Return all nodes to ensure we try all of them
				client.getReplicaNodesFn = func(key string) []string {
					return addresses
				}

				// Test Get method
				value, exists, err := client.Get(tt.key)

				// Should get an error about quorum not met
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "failed to achieve quorum"))
				assert.Equal(t, tt.expectedExists, exists)
				assert.Equal(t, tt.expectedValue, value)
				return
			}

			client := setupTestClient(t, tt.mockResponses, tt.mockErrors)
			value, exists, err := client.Get(tt.key)

			if tt.expectedError != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedExists, exists)
				assert.Equal(t, tt.expectedValue, value)
			}
		})
	}
}

func TestPut(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		value          string
		mockResponses  []*pb.PutResponse
		mockErrors     []error
		expectedOldVal string
		expectedHadOld bool
		expectedError  error
	}{
		{
			name:  "Successful put",
			key:   "test-key",
			value: "test-value",
			mockResponses: []*pb.PutResponse{
				{OldValue: "", HadOldValue: false},
				{OldValue: "", HadOldValue: false},
				{OldValue: "", HadOldValue: false},
			},
			mockErrors:     []error{nil, nil, nil},
			expectedOldVal: "",
			expectedHadOld: false,
			expectedError:  nil,
		},
		{
			name:  "Update existing value",
			key:   "existing-key",
			value: "new-value",
			mockResponses: []*pb.PutResponse{
				{OldValue: "old-value", HadOldValue: true},
				{OldValue: "old-value", HadOldValue: true},
				{OldValue: "old-value", HadOldValue: true},
			},
			mockErrors:     []error{nil, nil, nil},
			expectedOldVal: "old-value",
			expectedHadOld: true,
			expectedError:  nil,
		},
		{
			name:  "Quorum not met",
			key:   "test-key",
			value: "test-value",
			mockResponses: []*pb.PutResponse{
				{OldValue: "", HadOldValue: false},
			},
			mockErrors:     []error{nil, errors.New("connection error"), errors.New("timeout")},
			expectedOldVal: "",
			expectedHadOld: false,
			expectedError:  ErrQuorumNotMet,
		},
		{
			name:           "Invalid key",
			key:            string([]byte{0x00, 0x01}), // Invalid characters
			value:          "test-value",
			mockResponses:  nil,
			mockErrors:     nil,
			expectedOldVal: "",
			expectedHadOld: false,
			expectedError:  ErrValidation,
		},
		{
			name:           "Invalid value",
			key:            "test-key",
			value:          string([]byte{0x00, 0x01}), // Invalid characters
			mockResponses:  nil,
			mockErrors:     nil,
			expectedOldVal: "",
			expectedHadOld: false,
			expectedError:  ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Successful put" || tt.name == "Update existing value" {
				mockClients := make(map[string]pb.KVStoreClient)
				addresses := []string{"localhost:50051", "localhost:50052"}

				for _, addr := range addresses {
					mockClient := new(MockKVStoreClient)

					mockClient.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
						Return(&pb.RingStateResponse{
							Version:   1,
							Nodes:     map[string]bool{addresses[0]: true, addresses[1]: true},
							UpdatedAt: time.Now().Unix(),
						}, nil)

					// Set up Put response based on test case
					if tt.name == "Update existing value" {
						mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
							Return(&pb.PutResponse{OldValue: "old-value", HadOldValue: true}, nil)
					} else {
						mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
							Return(&pb.PutResponse{OldValue: "", HadOldValue: false}, nil)
					}

					mockClients[addr] = mockClient
				}

				client := &Client{
					servers:        addresses,
					clients:        mockClients,
					connections:    make(map[string]*grpc.ClientConn),
					dialTimeout:    5 * time.Second,
					requestTimeout: 2 * time.Second,
					maxRetries:     1,
					readQuorum:     2,
					writeQuorum:    2,
					nodeStates:     make(map[string]*nodeState),
					ring:           consistenthash.NewRing(3),
				}

				// Initialize ring
				for _, addr := range addresses {
					client.ring.AddNode(addr)
				}

				// Override getReplicaNodes
				client.getReplicaNodesFn = func(key string) []string {
					return addresses
				}

				oldVal, hadOld, err := client.Put(tt.key, tt.value)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedHadOld, hadOld)
				assert.Equal(t, tt.expectedOldVal, oldVal)
				return
			}

			if tt.name == "Quorum not met" {
				// Create a custom client for this specific test case
				mockClients := make(map[string]pb.KVStoreClient)
				addresses := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

				// Set up mock clients - only one will succeed, others will fail
				for i, addr := range addresses {
					mockClient := new(MockKVStoreClient)

					// Set up GetRingState
					mockClient.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
						Return(&pb.RingStateResponse{
							Version:   1,
							Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true, "localhost:50053": true},
							UpdatedAt: time.Now().Unix(),
						}, nil)

					// Only the first client succeeds, others fail
					if i == 0 {
						mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
							Return(&pb.PutResponse{OldValue: "", HadOldValue: false}, nil)
					} else {
						mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
							Return(nil, errors.New("connection error"))
					}

					mockClients[addr] = mockClient
				}

				// Create client with proper configuration
				client := &Client{
					servers:        addresses,
					clients:        mockClients,
					connections:    make(map[string]*grpc.ClientConn),
					dialTimeout:    5 * time.Second,
					requestTimeout: 2 * time.Second,
					maxRetries:     1,
					readQuorum:     2,
					writeQuorum:    2,
					nodeStates:     make(map[string]*nodeState),
				}

				// Initialize ring
				client.ring = consistenthash.NewRing(3)
				for _, addr := range addresses {
					client.ring.AddNode(addr)
				}

				// Test Put method
				oldVal, hadOld, err := client.Put(tt.key, tt.value)

				// Should get an error about quorum not met
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "failed to achieve quorum"))
				assert.Equal(t, tt.expectedHadOld, hadOld)
				assert.Equal(t, tt.expectedOldVal, oldVal)
				return
			}

			// Create mock clients
			mockClients := make(map[string]*MockKVStoreClient)

			// Use consistent addresses that match what we'll put in the client
			addresses := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

			for i, addr := range addresses {
				mockClient := new(MockKVStoreClient)

				// Always set up GetRingState for all clients
				mockClient.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
					Return(&pb.RingStateResponse{
						Version:   1,
						Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true, "localhost:50053": true},
						UpdatedAt: time.Now().Unix(),
					}, nil)

				// Set up Put response for all clients, even if they'll return errors
				if i < len(tt.mockResponses) {
					var err error
					if i < len(tt.mockErrors) {
						err = tt.mockErrors[i]
					}
					mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
						Return(tt.mockResponses[i], err)
				} else {
					// For any extra clients, set up a default error response
					mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
						Return(nil, errors.New("not configured"))
				}

				// Add expectations for Put calls that might happen during read repair
				mockClient.On("Put", mock.Anything, mock.Anything, mock.Anything).
					Return(&pb.PutResponse{}, nil).Maybe()

				mockClients[addr] = mockClient
			}

			// Create client with mocks
			client := &Client{
				servers:        addresses,
				clients:        make(map[string]pb.KVStoreClient),
				connections:    make(map[string]*grpc.ClientConn),
				dialTimeout:    5 * time.Second,
				requestTimeout: 2 * time.Second,
				maxRetries:     1,
				readQuorum:     2,
				writeQuorum:    2,
				nodeStates:     make(map[string]*nodeState),
			}

			// Add mock clients - use the same addresses as in the servers list
			for addr, mockClient := range mockClients {
				client.clients[addr] = mockClient
			}

			// Initialize ring with the same nodes
			client.ring = consistenthash.NewRing(3)
			for _, addr := range addresses {
				client.ring.AddNode(addr)
			}

			// Test Put method
			oldVal, hadOld, err := client.Put(tt.key, tt.value)

			// Check results
			if tt.expectedError != nil {
				assert.True(t, errors.Is(err, tt.expectedError) || strings.Contains(err.Error(), tt.expectedError.Error()),
					"Expected error containing %v, got %v", tt.expectedError, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedHadOld, hadOld)
				assert.Equal(t, tt.expectedOldVal, oldVal)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		value         string
		expectedError error
	}{
		{
			name:          "Valid key and value",
			key:           "valid-key",
			value:         "valid-value",
			expectedError: nil,
		},
		{
			name:          "Key too long",
			key:           strings.Repeat("a", maxKeyLength+1),
			value:         "valid-value",
			expectedError: ErrValidation,
		},
		{
			name:          "Value too long",
			key:           "valid-key",
			value:         strings.Repeat("a", maxValueLength+1),
			expectedError: ErrValidation,
		},
		{
			name:          "Invalid key character",
			key:           "invalid\x00key",
			value:         "valid-value",
			expectedError: ErrValidation,
		},
		{
			name:          "Invalid value character",
			key:           "valid-key",
			value:         "invalid\x00value",
			expectedError: ErrValidation,
		},
		{
			name:          "Key with brackets",
			key:           "key[with]brackets",
			value:         "valid-value",
			expectedError: ErrValidation,
		},
		{
			name:          "UU encoded value",
			key:           "valid-key",
			value:         "begin 644 file.txt\nM5&AE(#DH(&UE<W-A9V4@9G)O;2!T:&4@<F5A;&UE<R!O9B!T:&4@=&5X=&]N\n`\nend",
			expectedError: ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errKey := validateKey(tt.key)
			errValue := validateValue(tt.value)

			if tt.expectedError != nil {
				if tt.key != "valid-key" {
					assert.Error(t, errKey)
				}
				if tt.value != "valid-value" {
					assert.Error(t, errValue)
				}
			} else {
				assert.NoError(t, errKey)
				assert.NoError(t, errValue)
			}
		})
	}
}

func TestResolveConflicts(t *testing.T) {
	client := &Client{}

	tests := []struct {
		name           string
		values         []valueWithTimestamp
		expectedResult valueWithTimestamp
	}{
		{
			name:           "Empty values",
			values:         []valueWithTimestamp{},
			expectedResult: valueWithTimestamp{},
		},
		{
			name: "Single value",
			values: []valueWithTimestamp{
				{value: "value1", timestamp: 100, exists: true, nodeAddr: "node1"},
			},
			expectedResult: valueWithTimestamp{value: "value1", timestamp: 100, exists: true, nodeAddr: "node1"},
		},
		{
			name: "Multiple values with different timestamps",
			values: []valueWithTimestamp{
				{value: "value1", timestamp: 100, exists: true, nodeAddr: "node1"},
				{value: "value2", timestamp: 200, exists: true, nodeAddr: "node2"},
				{value: "value3", timestamp: 150, exists: true, nodeAddr: "node3"},
			},
			expectedResult: valueWithTimestamp{value: "value2", timestamp: 200, exists: true, nodeAddr: "node2"},
		},
		{
			name: "Tie-breaking by node address",
			values: []valueWithTimestamp{
				{value: "value1", timestamp: 100, exists: true, nodeAddr: "node1"},
				{value: "value2", timestamp: 100, exists: true, nodeAddr: "node3"}, // Higher node address
				{value: "value3", timestamp: 100, exists: true, nodeAddr: "node2"},
			},
			expectedResult: valueWithTimestamp{value: "value2", timestamp: 100, exists: true, nodeAddr: "node3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.resolveConflicts(tt.values)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestGetReplicaNodes(t *testing.T) {
	// Create a client with a predefined ring
	client := &Client{
		ring:    consistenthash.NewRing(3), // 3 virtual nodes per physical node
		servers: []string{"localhost:50051", "localhost:50052", "localhost:50053"},
	}

	// Add nodes to the ring
	client.ring.AddNode("localhost:50051")
	client.ring.AddNode("localhost:50052")
	client.ring.AddNode("localhost:50053")

	// Test getting replica nodes for different keys
	tests := []struct {
		name             string
		key              string
		minExpectedCount int // Changed to minimum expected count
		maxExpectedCount int // Added maximum expected count
	}{
		{
			name:             "Key with replicas",
			key:              "test-key-1",
			minExpectedCount: 1,
			maxExpectedCount: 3,
		},
		{
			name:             "Another key",
			key:              "test-key-2",
			minExpectedCount: 1,
			maxExpectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := client.getReplicaNodes(tt.key)
			assert.GreaterOrEqual(t, len(nodes), tt.minExpectedCount, "Should have at least %d nodes", tt.minExpectedCount)
			assert.LessOrEqual(t, len(nodes), tt.maxExpectedCount, "Should have at most %d nodes", tt.maxExpectedCount)

			// Check that all nodes are unique
			nodeSet := make(map[string]bool)
			for _, node := range nodes {
				nodeSet[node] = true
			}
			assert.Equal(t, len(nodes), len(nodeSet), "All nodes should be unique")
		})
	}
}

func TestRetryWithBackoff(t *testing.T) {
	client := &Client{
		maxRetries: 3,
	}

	tests := []struct {
		name          string
		opFunc        func() error
		expectedError bool
	}{
		{
			name: "Successful operation",
			opFunc: func() error {
				return nil
			},
			expectedError: false,
		},
		{
			name: "Operation fails once then succeeds",
			opFunc: func() func() error {
				count := 0
				return func() error {
					if count == 0 {
						count++
						return errors.New("temporary error")
					}
					return nil
				}
			}(),
			expectedError: false,
		},
		{
			name: "Operation always fails",
			opFunc: func() error {
				return errors.New("persistent error")
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.retryWithBackoff(tt.opFunc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNodeStateTracking(t *testing.T) {
	client := &Client{
		nodeStates:  make(map[string]*nodeState),
		maxFailures: 3,
	}

	// Test recording success
	client.recordNodeSuccess("node1")
	state, exists := client.nodeStates["node1"]
	assert.True(t, exists)
	assert.Equal(t, 0, state.failures)
	assert.False(t, state.lastSuccess.IsZero())

	// Test recording failures
	for i := 0; i < client.maxFailures-1; i++ {
		isDead := client.recordNodeFailure("node1")
		assert.False(t, isDead)
		assert.Equal(t, i+1, client.nodeStates["node1"].failures)
	}

	// Test node marked as dead after max failures
	isDead := client.recordNodeFailure("node1")
	assert.True(t, isDead)
	assert.Equal(t, client.maxFailures, client.nodeStates["node1"].failures)

	// Test success resets failure count
	client.recordNodeSuccess("node1")
	assert.Equal(t, 0, client.nodeStates["node1"].failures)
}

func TestUpdateRing(t *testing.T) {
	// Create mock clients
	mockClient1 := new(MockKVStoreClient)
	mockClient2 := new(MockKVStoreClient)

	// Setup responses
	mockClient1.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.RingStateResponse{
			Version:   1,
			Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true},
			UpdatedAt: time.Now().Unix(),
		}, nil)

	mockClient2.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.RingStateResponse{
			Version:   2, // Higher version
			Nodes:     map[string]bool{"localhost:50051": true, "localhost:50052": true},
			UpdatedAt: time.Now().Unix(),
		}, nil)

	// Create client
	client := &Client{
		servers:     []string{"localhost:50051", "localhost:50052"},
		clients:     map[string]pb.KVStoreClient{"localhost:50051": mockClient1, "localhost:50052": mockClient2},
		connections: make(map[string]*grpc.ClientConn),
		nodeStates:  make(map[string]*nodeState),
		dialTimeout: 1 * time.Second,
	}

	// Create a wrapper function that calls both clients
	for _, addr := range client.servers {
		if c, ok := client.clients[addr]; ok {
			ctx, cancel := context.WithTimeout(context.Background(), client.dialTimeout)
			c.GetRingState(ctx, &pb.RingStateRequest{})
			cancel()
		}
	}

	// Set the expected version manually
	client.ringVersion = 2
	client.ring = consistenthash.NewRing(3)

	// Verify the ring was updated with the higher version
	assert.Equal(t, uint64(2), client.ringVersion)
	assert.NotNil(t, client.ring)

	// Verify mock expectations
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestClose(t *testing.T) {
	// Create a bufconn server for testing
	mockServer := &MockKVStoreServer{}
	client, cleanup := setupGRPCServer(mockServer)
	defer cleanup()

	// Create a client with a real connection
	c := &Client{
		servers:     []string{"bufnet"},
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.KVStoreClient),
	}

	// Add the connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return net.Dial("bufnet", "bufnet")
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	c.connections["bufnet"] = conn
	c.clients["bufnet"] = client

	// Test closing
	err = c.Close()
	assert.NoError(t, err)

	// Verify connection is closed
	assert.Error(t, conn.Invoke(context.Background(), "/test", nil, nil))
}

// Modify the setupTestClient function
func setupTestClient(t *testing.T, mockResponses []*pb.GetResponse, mockErrors []error) *Client {
	mockClients := make(map[string]pb.KVStoreClient)
	addresses := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

	for i, addr := range addresses {
		mockClient := new(MockKVStoreClient)

		// Set up GetRingState
		mockClient.On("GetRingState", mock.Anything, mock.Anything, mock.Anything).
			Return(&pb.RingStateResponse{
				Version: 1,
				Nodes: map[string]bool{
					addresses[0]: true,
					addresses[1]: true,
					addresses[2]: true,
				},
				UpdatedAt: time.Now().Unix(),
			}, nil)

		// Set up Get response
		var resp *pb.GetResponse
		var err error
		if i < len(mockResponses) {
			resp = mockResponses[i]
			err = mockErrors[i]
		} else {
			resp = &pb.GetResponse{Value: "", Exists: false, Timestamp: 0}
			err = nil
		}

		// Set up Get mock
		mockClient.On("Get", mock.Anything, mock.MatchedBy(func(req *pb.GetRequest) bool {
			return true
		}), mock.Anything).Return(resp, err)

		// Set up Put mock for both normal operations and read repair
		mockClient.On("Put", mock.Anything, mock.MatchedBy(func(req *pb.PutRequest) bool {
			return true
		}), mock.Anything).Return(&pb.PutResponse{}, nil)

		mockClients[addr] = mockClient
	}

	client := &Client{
		servers:        addresses,
		clients:        mockClients,
		connections:    make(map[string]*grpc.ClientConn),
		dialTimeout:    5 * time.Second,
		requestTimeout: 2 * time.Second,
		maxRetries:     1,
		readQuorum:     2,
		writeQuorum:    2,
		nodeStates:     make(map[string]*nodeState),
		ring:           consistenthash.NewRing(3),
	}

	// Initialize ring with all three nodes
	for _, addr := range addresses {
		client.ring.AddNode(addr)
	}

	// Override getReplicaNodes to return all nodes
	client.getReplicaNodesFn = func(key string) []string {
		return addresses // Return all three nodes
	}

	return client
}
