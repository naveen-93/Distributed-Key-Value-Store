package consistenthash

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

// TestRingInitialization verifies that a new ring is initialized correctly.
func TestRingInitialization(t *testing.T) {
	ring := NewRing(3) // Assuming 3 virtual nodes per physical node
	if ring.vnodeCount != 3 {
		t.Errorf("Expected vnodeCount to be 3, got %d", ring.vnodeCount)
	}
	if len(ring.hashRing) != 0 {
		t.Errorf("Expected hashRing to be empty, got %d elements", len(ring.hashRing))
	}
	if len(ring.mapping) != 0 {
		t.Errorf("Expected mapping to be empty, got %d elements", len(ring.mapping))
	}
	if len(ring.vnodes) != 0 {
		t.Errorf("Expected vnodes to be empty, got %d elements", len(ring.vnodes))
	}
}

// TestAddNode ensures that adding a node updates the ring correctly.
func TestAddNode(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	
	if len(ring.vnodes["Node1"]) != 3 {
		t.Errorf("Expected 3 virtual nodes for Node1, got %d", len(ring.vnodes["Node1"]))
	}
	if len(ring.hashRing) != 3 {
		t.Errorf("Expected hashRing to have 3 elements, got %d", len(ring.hashRing))
	}
	if len(ring.mapping) != 3 {
		t.Errorf("Expected mapping to have 3 elements, got %d", len(ring.mapping))
	}
	
	// Verify that hashRing is sorted
	if !sort.SliceIsSorted(ring.hashRing, func(i, j int) bool { return ring.hashRing[i] < ring.hashRing[j] }) {
		t.Errorf("hashRing is not sorted: %v", ring.hashRing)
	}
}

// TestRemoveNode ensures that removing a node updates the ring correctly.
func TestRemoveNode(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.RemoveNode("Node1")
	
	if _, exists := ring.vnodes["Node1"]; exists {
		t.Errorf("Expected Node1 to be removed from vnodes")
	}
	if len(ring.hashRing) != 3 {
		t.Errorf("Expected hashRing to have 3 elements after removal, got %d", len(ring.hashRing))
	}
	for _, hash := range ring.hashRing {
		if ring.mapping[hash] == "Node1" {
			t.Errorf("Expected no mappings to Node1 after removal")
		}
	}
}

// TestGetNode verifies that key-to-node mapping is consistent.
func TestGetNode(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	
	key := "testKey"
	node := ring.GetNode(key)
	if node == "" {
		t.Errorf("Expected a node for key %s, got empty string", key)
	}
	
	// Test consistency
	for i := 0; i < 10; i++ {
		node2 := ring.GetNode(key)
		if node != node2 {
			t.Errorf("Expected consistent node for key %s, got %s and %s", key, node, node2)
		}
	}
}

// TestGetReplicas ensures that replicas are returned correctly.
func TestGetReplicas(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	
	key := "testKey"
	replicas := ring.GetReplicas(key, 2)
	if len(replicas) != 2 {
		t.Errorf("Expected 2 replicas for key %s, got %d", key, len(replicas))
	}
	
	// Verify distinct replicas
	seen := make(map[string]struct{})
	for _, replica := range replicas {
		if _, exists := seen[replica]; exists {
			t.Errorf("Duplicate replica %s for key %s", replica, key)
		}
		seen[replica] = struct{}{}
	}
	
	// Test requesting more replicas than nodes
	replicas = ring.GetReplicas(key, 5)
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas when requesting 5, got %d", len(replicas))
	}
}

// TestEdgeCases covers various edge-case scenarios.
func TestEdgeCases(t *testing.T) {
	// Empty ring
	ring := NewRing(3)
	if node := ring.GetNode("key"); node != "" {
		t.Errorf("Expected empty string for GetNode on empty ring, got %s", node)
	}
	if replicas := ring.GetReplicas("key", 2); len(replicas) != 0 {
		t.Errorf("Expected no replicas for GetReplicas on empty ring, got %d", len(replicas))
	}
	
	// Single node
	ring.AddNode("Node1")
	if node := ring.GetNode("key"); node != "Node1" {
		t.Errorf("Expected Node1 for GetNode with single node, got %s", node)
	}
	replicas := ring.GetReplicas("key", 2)
	if len(replicas) != 1 || replicas[0] != "Node1" {
		t.Errorf("Expected [Node1] for GetReplicas with single node, got %v", replicas)
	}
	
	// Multiple nodes with keys at different ring positions
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	keys := []string{"keyBegin", "keyMiddle", "keyEnd"}
	for _, key := range keys {
		node := ring.GetNode(key)
		if node != "Node1" && node != "Node2" && node != "Node3" {
			t.Errorf("Invalid node %s for key %s", node, key)
		}
	}
	
	// Test wrap-around behavior (assuming hash function can produce large values)
	// This assumes the ring handles hashes larger than the max hash by wrapping around
}

// TestConsistencyAfterChanges ensures key mappings adjust correctly after node changes.
func TestConsistencyAfterChanges(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	
	keys := []string{"key1", "key2", "key3"}
	originalNodes := make(map[string]string)
	for _, key := range keys {
		originalNodes[key] = ring.GetNode(key)
	}
	
	ring.RemoveNode("Node2")
	for _, key := range keys {
		newNode := ring.GetNode(key)
		if originalNodes[key] == "Node2" && newNode == "Node2" {
			t.Errorf("Expected key %s to remap after Node2 removal, still mapped to Node2", key)
		}
	}
	
	ring.AddNode("Node4")
	for _, key := range keys {
		consistentNode := ring.GetNode(key)
		for i := 0; i < 5; i++ {
			if ring.GetNode(key) != consistentNode {
				t.Errorf("Inconsistent mapping for key %s after adding Node4", key)
			}
		}
	}
}

// TestConcurrency ensures the ring handles concurrent operations safely.
func TestConcurrency(t *testing.T) {
	ring := NewRing(3)
	var wg sync.WaitGroup
	numGoroutines := 20
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeName := fmt.Sprintf("Node%d", i)
			ring.AddNode(nodeName)
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key%d", j)
				ring.GetNode(key)
				ring.GetReplicas(key, 2)
			}
			ring.RemoveNode(nodeName)
		}(i)
	}
	
	wg.Wait()
	
	// Post-concurrency sanity check
	if len(ring.hashRing) != 0 {
		t.Errorf("Expected hashRing to be empty after all nodes removed, got %d", len(ring.hashRing))
	}
}

// TestHashDistribution verifies that keys are reasonably distributed across nodes.
func TestHashDistribution(t *testing.T) {
	ring := NewRing(3)
	ring.AddNode("Node1")
	ring.AddNode("Node2")
	ring.AddNode("Node3")
	
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	
	nodeCounts := make(map[string]int)
	for _, key := range keys {
		node := ring.GetNode(key)
		nodeCounts[node]++
	}
	
	// Log distribution for inspection
	for node, count := range nodeCounts {
		t.Logf("Node %s: %d keys", node, count)
	}
	
	// Ensure all nodes have some keys (basic distribution check)
	if len(nodeCounts) != 3 {
		t.Errorf("Expected keys to be distributed across 3 nodes, got %d", len(nodeCounts))
	}
	for node, count := range nodeCounts {
		if count == 0 {
			t.Errorf("Node %s received no keys", node)
		}
	}
}