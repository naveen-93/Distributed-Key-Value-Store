package consistenthash

import (
	"fmt"
	"hash"
	
	"testing"
	
)

func TestRingBasicOperations(t *testing.T) {
	t.Run("Empty ring returns empty string", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		if node := ring.GetNode("somekey"); node != "" {
			t.Errorf("Expected empty string for empty ring, got %s", node)
		}
	})

	t.Run("Adding and getting node", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		ring.AddNode("node1")

		// Any key should map to node1 since it's the only node
		if node := ring.GetNode("somekey"); node != "node1" {
			t.Errorf("Expected node1, got %s", node)
		}
	})

	t.Run("Adding multiple nodes", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		ring.AddNode("node1")
		ring.AddNode("node2")
		ring.AddNode("node3")

		// Verify we get some distribution
		nodesFound := make(map[string]bool)
		for i := 0; i < 100; i++ {
			node := ring.GetNode(fmt.Sprintf("key-%d", i))
			nodesFound[node] = true
		}

		if len(nodesFound) < 2 {
			t.Errorf("Expected keys to be distributed across at least 2 nodes, got %d", len(nodesFound))
		}
	})

	t.Run("Removing node", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		ring.AddNode("node1")
		ring.AddNode("node2")

		// Get and save mappings for a few keys
		keys := []string{"key1", "key2", "key3", "key4", "key5"}
		originalMappings := make(map[string]string)
		for _, key := range keys {
			originalMappings[key] = ring.GetNode(key)
		}

		// Count keys mapped to node1
		node1Count := 0
		for _, node := range originalMappings {
			if node == "node1" {
				node1Count++
			}
		}

		// Remove node1
		ring.RemoveNode("node1")

		// All keys should now map to node2
		for _, key := range keys {
			if node := ring.GetNode(key); node != "node2" {
				t.Errorf("Expected node2 after removal, got %s", node)
			}
		}

		// Add node1 back
		ring.AddNode("node1")

		// Count how many keys went back to node1
		revertedCount := 0
		for _, key := range keys {
			if ring.GetNode(key) == "node1" && originalMappings[key] == "node1" {
				revertedCount++
			}
		}

		// Check if at least some keys reverted to original mapping
		if node1Count > 0 && revertedCount == 0 {
			t.Errorf("Expected some keys to revert to node1, but none did")
		}
	})
}

func TestRingConsistency(t *testing.T) {
	t.Run("Consistency with node changes", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		
		// Add initial nodes
		initialNodes := []string{"node1", "node2", "node3"}
		for _, node := range initialNodes {
			ring.AddNode(node)
		}

		// Map 1000 keys
		const keyCount = 1000
		initialMapping := make(map[string]string)
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			initialMapping[key] = ring.GetNode(key)
		}

		// Add a new node
		ring.AddNode("node4")
		
		// Check how many keys were remapped
		remappedCount := 0
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			if ring.GetNode(key) != initialMapping[key] {
				remappedCount++
			}
		}

		// We expect roughly 1/4 of keys to be remapped
		expectedRemapPercentage := 25.0
		actualRemapPercentage := float64(remappedCount) / float64(keyCount) * 100

		t.Logf("Remapped %d/%d keys (%.2f%%) after adding one node", 
			remappedCount, keyCount, actualRemapPercentage)

		// Allow some tolerance, but ensure we're roughly in the expected range
		if actualRemapPercentage < 15.0 || actualRemapPercentage > 35.0 {
			t.Errorf("Expected around %.2f%% keys to be remapped, got %.2f%%", 
				expectedRemapPercentage, actualRemapPercentage)
		}
	})
}

func TestGetReplicas(t *testing.T) {
	t.Run("Replicas with enough nodes", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		nodes := []string{"node1", "node2", "node3", "node4", "node5"}
		for _, node := range nodes {
			ring.AddNode(node)
		}

		replicas := ring.GetReplicas("somekey", 3)
		if len(replicas) != 3 {
			t.Errorf("Expected 3 replicas, got %d", len(replicas))
		}

		// Check that all replicas are unique
		seen := make(map[string]bool)
		for _, replica := range replicas {
			if seen[replica] {
				t.Errorf("Duplicate replica found: %s", replica)
			}
			seen[replica] = true
		}
	})

	t.Run("Replicas with not enough nodes", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		nodes := []string{"node1", "node2"}
		for _, node := range nodes {
			ring.AddNode(node)
		}

		replicas := ring.GetReplicas("somekey", 3)
		if len(replicas) != 2 {
			t.Errorf("Expected 2 replicas (limited by available nodes), got %d", len(replicas))
		}
	})

	t.Run("Replicas with empty ring", func(t *testing.T) {
		ring := NewRing(DefaultVirtualNodes)
		replicas := ring.GetReplicas("somekey", 3)
		if replicas != nil {
			t.Errorf("Expected nil replicas for empty ring, got %v", replicas)
		}
	})
}

func TestHashCollisions(t *testing.T) {
	t.Run("Handling forced collisions", func(t *testing.T) {
		// Create a ring that will have predictable collisions
		ring := NewRing(5) // Use fewer vnodes to make test quicker
		
		// Use our own custom hash function that will create collisions
		originalHashFunc := ring.hashFunc
		
		// This will simulate a hash collision on every other hash
		collisionCounter := 0
		ring.hashFunc = &mockHasher{
			origHasher: originalHashFunc,
			hashFn: func(data []byte) uint32 {
				collisionCounter++
				if collisionCounter%2 == 0 {
					return 12345 // Force collision with the same hash
				}
				return originalHashFunc.Sum32()
			},
		}
		
		// Add a few nodes - since every other hash is the same, we should see collisions
		ring.AddNode("node1")
		ring.AddNode("node2")
		ring.AddNode("node3")
		
		// Verify each node has some virtual nodes
		for _, node := range []string{"node1", "node2", "node3"} {
			if len(ring.vnodes[node]) == 0 {
				t.Errorf("Node %s has no virtual nodes despite collision handling", node)
			}
		}
	})
}

func TestDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping distribution test in short mode")
	}

	t.Run("Key distribution", func(t *testing.T) {
		// Increase virtual nodes for better distribution
		ring := NewRing(160) // Use more virtual nodes for better distribution
		nodeCount := 10
		
		// Add nodes
		for i := 0; i < nodeCount; i++ {
			ring.AddNode(fmt.Sprintf("node%d", i))
		}
		
		// Map a large number of keys
		const keyCount = 100000
		distribution := make(map[string]int)
		
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			node := ring.GetNode(key)
			distribution[node]++
		}
		
		// Calculate statistics
		mean := keyCount / nodeCount
		variance := 0.0
		
		t.Log("Node distribution:")
		for node, count := range distribution {
			t.Logf("  %s: %d keys (%.2f%% of mean)", 
				node, count, float64(count)/float64(mean)*100)
			variance += float64((count - mean) * (count - mean))
		}
		
		variance /= float64(nodeCount)
		stdDev := float64(mean) * 0.20 // Allow 20% standard deviation 
		
		t.Logf("Mean: %d, Variance: %.2f, Std Dev: %.2f", mean, variance, stdDev)
		
		// Check that the distribution is relatively even
		for node, count := range distribution {
			if float64(count) < float64(mean)-stdDev || float64(count) > float64(mean)+stdDev {
				t.Errorf("Node %s has %d keys, which is outside acceptable range [%d, %d]",
					node, count, int(float64(mean)-stdDev), int(float64(mean)+stdDev))
			}
		}
	})
}

func BenchmarkAddNode(b *testing.B) {
	ring := NewRing(DefaultVirtualNodes)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		ring.AddNode(nodeID)
	}
}

func BenchmarkGetNode(b *testing.B) {
	ring := NewRing(DefaultVirtualNodes)
	nodeCount := 100
	
	// Add a reasonable number of nodes
	for i := 0; i < nodeCount; i++ {
		ring.AddNode(fmt.Sprintf("node-%d", i))
	}
	
	// Pre-generate keys to avoid string concatenation in the benchmark
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetNode(keys[i])
	}
}

func BenchmarkGetReplicas(b *testing.B) {
	ring := NewRing(DefaultVirtualNodes)
	nodeCount := 100
	
	// Add a reasonable number of nodes
	for i := 0; i < nodeCount; i++ {
		ring.AddNode(fmt.Sprintf("node-%d", i))
	}
	
	// Pre-generate keys to avoid string concatenation in the benchmark
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetReplicas(keys[i], 3)
	}
}

// Mock hasher for collision testing
type mockHasher struct {
	origHasher hash.Hash32
	hashFn     func([]byte) uint32
}

func (m *mockHasher) Write(p []byte) (n int, err error) {
	return m.origHasher.Write(p)
}

func (m *mockHasher) Sum(b []byte) []byte {
	return m.origHasher.Sum(b)
}

func (m *mockHasher) Reset() {
	m.origHasher.Reset()
}

func (m *mockHasher) Size() int {
	return m.origHasher.Size()
}

func (m *mockHasher) BlockSize() int {
	return m.origHasher.BlockSize()
}

func (m *mockHasher) Sum32() uint32 {
	return m.hashFn(nil)
}