package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"Distributed-Key-Value-Store/pkg/client"

	"strings"

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
	time.Sleep(30 * time.Second)

	// Run tests
	exitCode := m.Run()

	// Cleanup
	stopServers()

	os.Exit(exitCode)
}

// startServers launches the required number of server instances
func startServers() error {
	nodeIDs := []string{
		"node1", // Node 1
		"node2", // Node 2
		"node3", // Node 3
	}

	for i := 0; i < numServers; i++ {
		port := basePort + i
		nodeID := nodeIDs[i]
		addr := fmt.Sprintf(":%d", port)

		// Build a list of peer addresses for this server
		var peerArgs string
		if i > 0 {
			var peers []string
			for j := 0; j < i; j++ {
				peerNodeID := nodeIDs[j]
				peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
				peers = append(peers, fmt.Sprintf("%s@%s", peerNodeID, peerAddr))
			}
			// Optionally include self for the third server as shown in the example
			if i == 2 {
				peerNodeID := nodeIDs[i]
				peerAddr := fmt.Sprintf("localhost:%d", port)
				peers = append(peers, fmt.Sprintf("%s@%s", peerNodeID, peerAddr))
			}
			peerArgs = fmt.Sprintf("-peers \"%s\"", strings.Join(peers, ","))
		}

		// Build the command based on the requested format
		cmdString := fmt.Sprintf("go run pkg/server/main.go -addr %s -id %s", addr, nodeID)
		if peerArgs != "" {
			cmdString += " " + peerArgs
		}

		// Add replication factor if needed (keeping from original)
		cmdString += " -replication-factor 2"

		// Split the command string into parts for exec.Command
		args := strings.Fields(cmdString)
		cmd := exec.Command(args[0], args[1:]...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			stopServers() // Clean up any servers that did start
			return fmt.Errorf("failed to start server %d: %v", i+1, err)
		}

		serverProcesses = append(serverProcesses, cmd)
		log.Printf("Started server %d with ID %s on %s with command: %s", i+1, nodeID, addr, cmdString)
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
	nodeID := fmt.Sprintf("node%d", serverToKill+1)
	addr := fmt.Sprintf(":%d", port)

	// Build peer arguments following the format in the example
	var peerArgs string
	var peers []string
	for j := 0; j < numServers; j++ {
		if j != serverToKill {
			peerNodeID := fmt.Sprintf("node%d", j+1)
			peerAddr := fmt.Sprintf("localhost:%d", basePort+j)
			peers = append(peers, fmt.Sprintf("%s@%s", peerNodeID, peerAddr))
		}
	}

	if len(peers) > 0 {
		peerArgs = fmt.Sprintf("-peers \"%s\"", strings.Join(peers, ","))
	}

	// Build the command string
	cmdString := fmt.Sprintf("go run pkg/server/main.go -addr %s -id %s", addr, nodeID)
	if peerArgs != "" {
		cmdString += " " + peerArgs
	}
	cmdString += " -replication-factor 2"

	// Split the command string for exec.Command
	args := strings.Fields(cmdString)
	cmd := exec.Command(args[0], args[1:]...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to restart server: %v", err)
	}
	serverProcesses[serverToKill] = cmd
	log.Printf("Restarted server %d with command: %s", serverToKill+1, cmdString)

	// Wait for the server to initialize
	time.Sleep(3 * time.Second)

	// The restarted server should have the old value
	// Create a new client for the restarted server
	cfg = client.ClientConfig{
		ServerAddresses: []string{fmt.Sprintf("localhost:%d", port)},
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
