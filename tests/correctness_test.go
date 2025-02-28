package kvstore

import (
    "testing"
    "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	
)

// Assume a Client struct and server setup exist
func TestBasicOperations(t *testing.T) {
    servers := []string{"node1", "node2", "node3"}
    client := NewClient(servers, Config{Quorum: 2})
    
    // Test 1.1 & 1.2: PUT and GET
    err := client.Put("key1", "value1")
    assert.NoError(t, err)
    
    val, err := client.Get("key1")
    assert.NoError(t, err)
    assert.Equal(t, "value1", val)
    
    // Test 1.3: DELETE
    err = client.Delete("key1")
    assert.NoError(t, err)
    
    _, err = client.Get("key1")
    assert.Error(t, err) // Key should not exist
}

func TestQuorumWriteFailure(t *testing.T) {
    servers := []string{"node1", "node2", "node3"}
    client := NewClient(servers, Config{Quorum: 2})
    
    // Simulate failure on two nodes (quorum not met)
    simulateFailure("node2")
    simulateFailure("node3")
    
    err := client.Put("key1", "value1")
    assert.Error(t, err) // Write should fail
}

func TestNodeFailureDuringWrite(t *testing.T) {
    servers := []string{"node1", "node2", "node3"}
    client := NewClient(servers, Config{Quorum: 2})
    
    // Kill one node during write
    go simulateFailure("node3")
    err := client.Put("key1", "value1")
    assert.NoError(t, err) // Should succeed with remaining quorum
    
    val, err := client.Get("key1")
    assert.NoError(t, err)
    assert.Equal(t, "value1", val)
}

func TestCrashRecovery(t *testing.T) {
    servers := []string{"node1", "node2", "node3"}
    client := NewClient(servers, Config{Quorum: 2})
    
    // Write data
    client.Put("key1", "value1")
    
    // Simulate crash and recover from WAL
    simulateCrash("node1")
    recoverNode("node1")
    
    val, err := client.Get("key1")
    assert.NoError(t, err)
    assert.Equal(t, "value1", val)
}