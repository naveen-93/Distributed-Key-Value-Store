package client

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"Distributed-Key-Value-Store/pkg/client"

	"github.com/stretchr/testify/assert"
)

const (
	numServers = 3
	basePort   = 50051
)

var serverProcesses []*exec.Cmd

// TestMain sets up the test environment
func TestMain(m *testing.M) {
	// Start servers
	if err := startServers(); err != nil {
		log.Fatalf("Failed to start servers: %v", err)
	}

	// Wait for servers to initialize
	time.Sleep(3 * time.Second)

	// Run tests
	exitCode := m.Run()

	// Cleanup
	stopServers()

	os.Exit(exitCode)
}

// startServers launches the required number of server instances
func startServers() error {
	for i := 0; i < numServers; i++ {
		port := basePort + i
		nodeID := fmt.Sprintf("%d", i+1) // Simple numeric ID
		addr := fmt.Sprintf(":%d", port) // Just the port

		// Build a list of peer addresses for this server
		var peerArgs []string
		for j := 0; j < numServers; j++ {
			if j != i { // Don't include self as peer
				peerNodeID := fmt.Sprintf("%d", j+1)
				peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
				peerArgs = append(peerArgs, fmt.Sprintf("--peer=%s@%s", peerNodeID, peerAddr))
			}
		}

		// Combine all arguments
		args := []string{"run", "../pkg/server/main.go",
			"--id", nodeID,
			"--addr", addr,
			"--replication-factor", "2", // Set replication factor to 2
			"--quorum", "2"} // Set quorum to 2
		args = append(args, peerArgs...)

		cmd := exec.Command("go", args...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			stopServers() // Clean up any servers that did start
			return fmt.Errorf("failed to start server %d: %v", i+1, err)
		}

		serverProcesses = append(serverProcesses, cmd)
		log.Printf("Started server %d with ID %s on %s with peers %v", i+1, nodeID, addr, peerArgs)
	}

	return nil
}

// stopServers terminates all server processes
func stopServers() {
	for i, cmd := range serverProcesses {
		if cmd.Process != nil {
			log.Printf("Stopping server %d", i+1)
			if err := cmd.Process.Kill(); err != nil {
				log.Printf("Failed to kill server %d: %v", i+1, err)
			}
		}
	}
	serverProcesses = nil
}

// getServerAddresses returns the addresses of all servers
func getServerAddresses() []string {
	addresses := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		addresses[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}
	return addresses
}

// TestBasicOperations verifies basic get/put operations
func TestBasicOperations(t *testing.T) {
	// Create client
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	c, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client")
	defer c.Close()

	// Test Put and Get
	key := "test-key"
	value := "test-value"

	// Put a value
	oldValue, hadOld, err := c.Put(key, value)
	assert.NoError(t, err, "Put operation failed")
	assert.False(t, hadOld, "Key should not exist yet")
	assert.Empty(t, oldValue, "Old value should be empty")

	// Get the value back
	retrievedValue, _, exists, err := c.Get(key)
	assert.NoError(t, err, "Get operation failed")
	assert.True(t, exists, "Key should exist")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match what was put")

	// Update the value
	newValue := "updated-value"
	oldValue, hadOld, err = c.Put(key, newValue)
	assert.NoError(t, err, "Put operation failed")
	assert.True(t, hadOld, "Key should exist")
	assert.Equal(t, value, oldValue, "Old value should match the original value")

	// Get the updated value
	retrievedValue, _, exists, err = c.Get(key)
	assert.NoError(t, err, "Get operation failed")
	assert.True(t, exists, "Key should exist")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should match the updated value")
}

// TestConsistency verifies that all servers return consistent values
func TestConsistency(t *testing.T) {
	// Create multiple clients, each connecting to a different server
	clients := make([]*client.Client, numServers)
	for i := 0; i < numServers; i++ {
		cfg := client.ClientConfig{
			ServerAddresses: []string{fmt.Sprintf("localhost:%d", basePort+i)},
			Timeout:         5 * time.Second,
			RetryAttempts:   3,
			RetryDelay:      500 * time.Millisecond,
		}

		c, err := client.NewClient(cfg)
		assert.NoError(t, err, "Failed to create client %d", i+1)
		clients[i] = c
		defer c.Close()
	}

	// Put a value using the first client
	key := "consistency-test-key"
	value := "consistency-test-value"
	_, _, err := clients[0].Put(key, value)
	assert.NoError(t, err, "Put operation failed")

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Get the value from all clients and verify consistency
	for i, c := range clients {
		retrievedValue, _, exists, err := c.Get(key)
		assert.NoError(t, err, "Get operation failed for client %d", i+1)
		assert.True(t, exists, "Key should exist for client %d", i+1)
		assert.Equal(t, value, retrievedValue, "Retrieved value should be consistent across all servers")
	}

	// Update the value using a different client
	newValue := "consistency-updated-value"
	_, _, err = clients[1].Put(key, newValue)
	assert.NoError(t, err, "Put operation failed")

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Verify all clients see the updated value
	for i, c := range clients {
		retrievedValue, _, exists, err := c.Get(key)
		assert.NoError(t, err, "Get operation failed for client %d", i+1)
		assert.True(t, exists, "Key should exist for client %d", i+1)
		assert.Equal(t, newValue, retrievedValue, "Updated value should be consistent across all servers")
	}
}

// TestConcurrentOperations verifies behavior under concurrent access
func TestConcurrentOperations(t *testing.T) {
	// Create client
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	c, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client")
	defer c.Close()

	// Number of concurrent operations
	numOps := 10 // Reduced for initial testing
	var wg sync.WaitGroup
	wg.Add(numOps)

	// Run concurrent operations
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			key := fmt.Sprintf("concurrent-key-%d", id)
			value := fmt.Sprintf("concurrent-value-%d", id)

			// Put a value
			_, _, err := c.Put(key, value)
			assert.NoError(t, err, "Concurrent Put failed for key %s", key)

			// Get the value back
			retrievedValue, _, exists, err := c.Get(key)
			assert.NoError(t, err, "Concurrent Get failed for key %s", key)
			assert.True(t, exists, "Key %s should exist", key)
			assert.Equal(t, value, retrievedValue, "Retrieved value should match for key %s", key)
		}(i)
	}

	wg.Wait()
}

// TestFaultTolerance verifies the system can handle node failures
func TestFaultTolerance(t *testing.T) {
	// Create client with all servers
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	c, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client")
	defer c.Close()

	// Put some data
	key := "fault-tolerance-key"
	value := "fault-tolerance-value"
	_, _, err = c.Put(key, value)
	assert.NoError(t, err, "Put operation failed")

	// Kill one server (the last one)
	serverToKill := len(serverProcesses) - 1
	log.Printf("Killing server %d", serverToKill+1)
	if err := serverProcesses[serverToKill].Process.Kill(); err != nil {
		t.Fatalf("Failed to kill server: %v", err)
	}

	// Wait for the system to detect the failure
	time.Sleep(3 * time.Second)

	// Verify we can still get the data
	retrievedValue, _, exists, err := c.Get(key)
	assert.NoError(t, err, "Get operation failed after server failure")
	assert.True(t, exists, "Key should still exist after server failure")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match after server failure")

	// Put new data
	newKey := "fault-tolerance-key-2"
	newValue := "fault-tolerance-value-2"
	_, _, err = c.Put(newKey, newValue)
	assert.NoError(t, err, "Put operation failed after server failure")

	// Verify we can get the new data
	retrievedValue, _, exists, err = c.Get(newKey)
	assert.NoError(t, err, "Get operation failed for new key after server failure")
	assert.True(t, exists, "New key should exist after server failure")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should match for new key after server failure")

	// Restart the killed server
	port := basePort + serverToKill
	nodeID := fmt.Sprintf("%d", serverToKill+1)
	addr := fmt.Sprintf("localhost:%d", port)

	// Build peer arguments
	var peerArgs []string
	for j := 0; j < numServers; j++ {
		if j != serverToKill {
			peerNodeID := fmt.Sprintf("%d", j+1)
			peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
			peerArgs = append(peerArgs, fmt.Sprintf("--peer=%s@%s", peerNodeID, peerAddr))
		}
	}

	// Combine all arguments
	args := []string{"run", "../pkg/server/main.go",
		"--id", nodeID,
		"--addr", addr,
		"--replication-factor", "2",
		"--quorum", "2"}
	args = append(args, peerArgs...)

	cmd := exec.Command("go", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to restart server: %v", err)
	}
	serverProcesses[serverToKill] = cmd
	log.Printf("Restarted server %d", serverToKill+1)

	// Wait for the server to initialize and sync
	time.Sleep(5 * time.Second)

	// Verify the restarted server has the data
	cfg = client.ClientConfig{
		ServerAddresses: []string{addr},
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	restartedClient, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client for restarted server")
	defer restartedClient.Close()

	// Check both keys
	retrievedValue, _, exists, err = restartedClient.Get(key)
	assert.NoError(t, err, "Get operation failed on restarted server")
	assert.True(t, exists, "Original key should exist on restarted server")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match on restarted server")

	retrievedValue, _, exists, err = restartedClient.Get(newKey)
	assert.NoError(t, err, "Get operation failed for new key on restarted server")
	assert.True(t, exists, "New key should exist on restarted server")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should match for new key on restarted server")
}

// TestReadRepair verifies that read repair works correctly
func TestReadRepair(t *testing.T) {
	// Create clients for each server
	clients := make([]*client.Client, numServers)
	for i := 0; i < numServers; i++ {
		cfg := client.ClientConfig{
			ServerAddresses: []string{fmt.Sprintf("localhost:%d", basePort+i)},
			Timeout:         5 * time.Second,
			RetryAttempts:   3,
			RetryDelay:      500 * time.Millisecond,
		}

		c, err := client.NewClient(cfg)
		assert.NoError(t, err, "Failed to create client %d", i+1)
		clients[i] = c
		defer c.Close()
	}

	// Put a value using the first client
	key := "read-repair-key"
	value := "read-repair-value"
	_, _, err := clients[0].Put(key, value)
	assert.NoError(t, err, "Put operation failed")

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Kill the second server to prevent it from receiving updates
	serverToKill := 1
	log.Printf("Killing server %d", serverToKill+1)
	if err := serverProcesses[serverToKill].Process.Kill(); err != nil {
		t.Fatalf("Failed to kill server: %v", err)
	}

	// Update the value using the first client
	newValue := "read-repair-updated-value"
	_, _, err = clients[0].Put(key, newValue)
	assert.NoError(t, err, "Put operation failed")

	// Wait for replication (but the killed server won't get it)
	time.Sleep(2 * time.Second)

	// Restart the killed server
	port := basePort + serverToKill
	nodeID := fmt.Sprintf("%d", serverToKill+1)
	addr := fmt.Sprintf("localhost:%d", port)

	// Build peer arguments
	var peerArgs []string
	for j := 0; j < numServers; j++ {
		if j != serverToKill {
			peerNodeID := fmt.Sprintf("%d", j+1)
			peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
			peerArgs = append(peerArgs, fmt.Sprintf("--peer=%s@%s", peerNodeID, peerAddr))
		}
	}

	// Combine all arguments
	args := []string{"run", "../pkg/server/main.go",
		"--id", nodeID,
		"--addr", addr,
		"--replication-factor", "2",
		"--quorum", "2"}
	args = append(args, peerArgs...)

	cmd := exec.Command("go", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to restart server: %v", err)
	}
	serverProcesses[serverToKill] = cmd
	log.Printf("Restarted server %d", serverToKill+1)

	// Wait for the server to initialize
	time.Sleep(3 * time.Second)

	// The restarted server should have the old value
	// Create a new client for the restarted server
	cfg := client.ClientConfig{
		ServerAddresses: []string{addr},
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	restartedClient, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client for restarted server")
	defer restartedClient.Close()

	// Get the value from the restarted server - it should have the old value
	retrievedValue, _, exists, err := restartedClient.Get(key)
	assert.NoError(t, err, "Get operation failed on restarted server")
	assert.True(t, exists, "Key should exist on restarted server")

	// Now get the value from a client that connects to all servers
	// This should trigger read repair
	allServersCfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	allServersClient, err := client.NewClient(allServersCfg)
	assert.NoError(t, err, "Failed to create client for all servers")
	defer allServersClient.Close()

	// This should trigger read repair
	retrievedValue, _, exists, err = allServersClient.Get(key)
	assert.NoError(t, err, "Get operation failed")
	assert.True(t, exists, "Key should exist")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should be the updated value")

	// Wait for read repair to complete
	time.Sleep(2 * time.Second)

	// Now the restarted server should have the new value
	retrievedValue, _, exists, err = restartedClient.Get(key)
	assert.NoError(t, err, "Get operation failed on restarted server after read repair")
	assert.True(t, exists, "Key should exist on restarted server after read repair")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should be the updated value after read repair")
}
