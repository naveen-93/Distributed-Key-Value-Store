package client

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
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
	// if err := startServers(); err != nil {
	// 	log.Fatalf("Failed to start servers: %v", err)
	// }

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
	serverIDs := []string{
		"12345678-1234-5678-1234-567812345678", // Node 1
		"87654321-4321-8765-4321-876543210987", // Node 2
		"11112222-3333-4444-5555-666677778888", // Node 3
	}

	for i := 0; i < numServers; i++ {
		port := basePort + i
		nodeID := serverIDs[i]
		addr := fmt.Sprintf(":%d", port)

		// Build a list of peer addresses for this server
		var peerList []string
		for j := 0; j < numServers; j++ {
			if j != i { // Don't include self as peer
				peerNodeID := serverIDs[j]
				peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
				peerList = append(peerList, fmt.Sprintf("%s@%s", peerNodeID, peerAddr))
			}
		}

		// Combine all arguments
		args := []string{"run", "../pkg/server/main.go",
			"--id", nodeID,
			"--addr", addr,
			"--replication-factor", "2",
			"--peers", strings.Join(peerList, ",")}
		cmd := exec.Command("go", args...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			stopServers() // Clean up any servers that did start
			return fmt.Errorf("failed to start server %d: %v", i+1, err)
		}

		serverProcesses = append(serverProcesses, cmd)
		log.Printf("Started server %d with ID %s on %s with peers %v", i+1, nodeID, addr, peerList)
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
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Test Put and Get
	key := "test-key"
	value := "test-value"

	// Put a value
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oldValue, hadOld, err := c.Put(ctx, key, value)
	assert.NoError(t, err, "Put operation failed")
	assert.False(t, hadOld, "Key should not exist yet")
	assert.Empty(t, oldValue, "Old value should be empty")

	// Wait for eventual consistency
	time.Sleep(2 * time.Second)

	// Get the value back
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retrievedValue, _, exists, err := c.Get(ctx, key)
	assert.NoError(t, err, "Get operation failed")
	assert.True(t, exists, "Key should exist")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match what was put")
}

// TestConsistency verifies that the system is consistent
func TestConsistency(t *testing.T) {
	// Create clients
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	clients := make([]*client.Client, numServers)
	for i := 0; i < numServers; i++ {
		c, err := client.NewClient(cfg)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		defer c.Close()
		clients[i] = c
	}

	// Test Put and Get
	key := "consistency-key"
	value := "consistency-value"

	// Put a value
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oldValue, hadOld, err := clients[0].Put(ctx, key, value)
	assert.NoError(t, err, "Put operation failed")
	assert.False(t, hadOld, "Key should not exist yet")
	assert.Empty(t, oldValue, "Old value should be empty")

	// Wait for eventual consistency
	time.Sleep(5 * time.Second) // Increase this if needed

	// Get the value back from all clients
	for i, c := range clients {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		retrievedValue, _, exists, err := c.Get(ctx, key)
		assert.NoError(t, err, "Get operation failed")
		assert.True(t, exists, "Key should exist")
		assert.Equal(t, value, retrievedValue, "Retrieved value should match what was put on client %d", i+1)
	}
}

// TestFaultTolerance verifies that the system can handle node failures
func TestFaultTolerance(t *testing.T) {
	// Create client
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	c, err := client.NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Put a value
	key := "fault-tolerance-key"
	value := "fault-tolerance-value"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err = c.Put(ctx, key, value)
	assert.NoError(t, err, "Put operation failed")

	// Wait for eventual consistency
	time.Sleep(2 * time.Second)

	// Kill one server (the last one)
	serverToKill := len(serverProcesses) - 1
	if serverToKill < 0 {
		t.Fatalf("No servers available to kill")
	}
	log.Printf("Killing server %d", serverToKill+1)
	if err := serverProcesses[serverToKill].Process.Kill(); err != nil {
		t.Fatalf("Failed to kill server: %v", err)
	}

	// Wait for the system to detect the failure
	time.Sleep(3 * time.Second)

	// Verify we can still get the data
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retrievedValue, _, exists, err := c.Get(ctx, key)
	assert.NoError(t, err, "Get operation failed after server failure")
	assert.True(t, exists, "Key should still exist after server failure")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match what was put after server failure")
}

// TestReadRepair verifies that the system can handle read repair
func TestReadRepair(t *testing.T) {
	// Create clients
	cfg := client.ClientConfig{
		ServerAddresses: getServerAddresses(),
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	clients := make([]*client.Client, numServers)
	for i := 0; i < numServers; i++ {
		c, err := client.NewClient(cfg)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		defer c.Close()
		clients[i] = c
	}

	// Put a value
	key := "read-repair-key"
	value := "read-repair-value"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := clients[0].Put(ctx, key, value)
	assert.NoError(t, err, "Put operation failed")

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Kill one server (the last one)
	serverToKill := len(serverProcesses) - 1
	if serverToKill < 0 {
		t.Fatalf("No servers available to kill")
	}
	log.Printf("Killing server %d", serverToKill+1)
	if err := serverProcesses[serverToKill].Process.Kill(); err != nil {
		t.Fatalf("Failed to kill server: %v", err)
	}

	// Update the value using the first client
	newValue := "read-repair-updated-value"
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err = clients[0].Put(ctx, key, newValue)
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
		"--replication-factor", "2"}
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
	cfg = client.ClientConfig{
		ServerAddresses: []string{addr},
		Timeout:         5 * time.Second,
		RetryAttempts:   3,
		RetryDelay:      500 * time.Millisecond,
	}

	restartedClient, err := client.NewClient(cfg)
	assert.NoError(t, err, "Failed to create client for restarted server")
	defer restartedClient.Close()

	// Get the value from the restarted server - it should have the old value
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retrievedValue, _, exists, err := restartedClient.Get(ctx, key)
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
	retrievedValue, _, exists, err = allServersClient.Get(ctx, key)
	assert.NoError(t, err, "Get operation failed")
	assert.True(t, exists, "Key should exist")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should be the updated value")

	// Wait for read repair to complete
	time.Sleep(2 * time.Second)

	// Now the restarted server should have the new value
	retrievedValue, _, exists, err = restartedClient.Get(ctx, key)
	assert.NoError(t, err, "Get operation failed on restarted server after read repair")
	assert.True(t, exists, "Key should exist on restarted server after read repair")
	assert.Equal(t, newValue, retrievedValue, "Retrieved value should be the updated value after read repair")
}
