package client

import (
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	// Test client creation with valid servers
	servers := []string{"localhost:50051", "localhost:50051"}
	client, err := NewClient(servers)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Verify client initialization
	if len(client.connections) == 0 {
		t.Error("No connections established")
	}
}

func TestClientOperations(t *testing.T) {
	servers := []string{"localhost:50051"}
	client, err := NewClient(servers)
	if err != nil {
		t.Skip("Skipping test as no server available")
	}
	defer client.Close()

	// Test Put operation
	key, value := "test_key", "test_value"
	_, _, err = client.Put(key, value) // Ignore return values for now
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// Test Get operation
	retrievedValue, exists, err := client.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Key should exist")
	}
	if retrievedValue != value {
		t.Errorf("Got %s, want %s", retrievedValue, value)
	}
}

func TestCircuitBreaker(t *testing.T) {
	client := &Client{
		nodeStates: make(map[string]nodeState),
	}

	node := "test_node"
	client.nodeStates[node] = nodeState{
		failures:      0,
		lastFailure:   time.Now(),
		broken:        false,
		failureWindow: time.Minute,
	}

	if client.checkCircuitBreaker(node) {
		t.Error("Circuit breaker should not be triggered initially")
	}
}
