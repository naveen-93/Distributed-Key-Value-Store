package node

import (
	"fmt"
	"math/rand"
	"os"
	
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to clean up test files
func cleanup(nodeID uint32) {
	os.Remove(fmt.Sprintf("wal-%d.log", nodeID))
	os.Remove(fmt.Sprintf("wal-%d.staging", nodeID))
	os.RemoveAll("snapshots")
}

func TestBasicOperations(t *testing.T) {
	nodeID := uint32(1)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)
	
	// Test Store and Get
	t.Run("Store and Get", func(t *testing.T) {
		testCases := []struct {
			key   string
			value string
		}{
			{"key1", "value1"},
			{"key2", "value2"},
			{"key3", "value3"},
		}

		for _, tc := range testCases {
			timestamp := node.generateTimestamp()
			node.Store(tc.key, tc.value, timestamp)
			
			value, ts, exists := node.Get(tc.key)
			assert.True(t, exists)
			assert.Equal(t, tc.value, value)
			assert.Equal(t, timestamp, ts)
		}
	})

	// Test Delete
	t.Run("Delete", func(t *testing.T) {
		key := "delete-test"
		node.Store(key, "value", node.generateTimestamp())
		node.Delete(key)
		
		_, _, exists := node.Get(key)
		assert.False(t, exists)
	})
}

func TestConcurrentOperations(t *testing.T) {
	nodeID := uint32(2)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)
	
	const (
		numGoroutines = 10
		opsPerGoroutine = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	t.Run("Concurrent Stores", func(t *testing.T) {
		for i := 0; i < numGoroutines; i++ {
			go func(routineID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := fmt.Sprintf("key-%d-%d", routineID, j)
					value := fmt.Sprintf("value-%d-%d", routineID, j)
					timestamp := node.generateTimestamp()
					node.Store(key, value, timestamp)
				}
			}(i)
		}
		wg.Wait()

		// Verify all entries
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", i, j)
				value := fmt.Sprintf("value-%d-%d", i, j)
				actualValue, _, exists := node.Get(key)
				assert.True(t, exists)
				assert.Equal(t, value, actualValue)
			}
		}
	})
}

func TestWALRecovery(t *testing.T) {
	nodeID := uint32(3)
	cleanup(nodeID)
	defer cleanup(nodeID)

	t.Run("Recovery After Crash", func(t *testing.T) {
		// Create initial node and store some data
		node1 := NewNode(nodeID)
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			node1.Store(key, value, node1.generateTimestamp())
		}

		// Simulate crash by creating new node instance
		node2 := NewNode(nodeID)

		// Verify recovered data
		for key, expectedValue := range testData {
			value, _, exists := node2.Get(key)
			assert.True(t, exists)
			assert.Equal(t, expectedValue, value)
		}
	})
}

func TestSnapshotting(t *testing.T) {
	nodeID := uint32(4)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)

	t.Run("Snapshot Creation and Recovery", func(t *testing.T) {
		// Store enough data to trigger snapshot
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			node.Store(key, value, node.generateTimestamp())
		}

		// Force compaction
		require.NoError(t, node.compactWAL())

		// Verify snapshot was created
		files, err := os.ReadDir("snapshots")
		require.NoError(t, err)
		assert.True(t, len(files) > 0)

		// Create new node instance to test recovery from snapshot
		node2 := NewNode(nodeID)

		// Verify recovered data
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key-%d", i)
			expectedValue := fmt.Sprintf("value-%d", i)
			value, _, exists := node2.Get(key)
			assert.True(t, exists)
			assert.Equal(t, expectedValue, value)
		}
	})
}

func TestTimestampOrdering(t *testing.T) {
	nodeID := uint32(5)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)

	t.Run("Timestamp Ordering", func(t *testing.T) {
		key := "timestamp-test"
		
		// Store value with older timestamp
		oldTimestamp := node.generateTimestamp()
		node.Store(key, "old-value", oldTimestamp)

		// Store value with newer timestamp
		newTimestamp := node.generateTimestamp()
		node.Store(key, "new-value", newTimestamp)

		// Verify newer value is returned
		value, ts, exists := node.Get(key)
		assert.True(t, exists)
		assert.Equal(t, "new-value", value)
		assert.Equal(t, newTimestamp, ts)

		// Try to store value with older timestamp
		node.Store(key, "outdated-value", oldTimestamp)

		// Verify newer value remains
		value, ts, exists = node.Get(key)
		assert.True(t, exists)
		assert.Equal(t, "new-value", value)
		assert.Equal(t, newTimestamp, ts)
	})
}

func TestTombstones(t *testing.T) {
	nodeID := uint32(6)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)

	t.Run("Tombstone Behavior", func(t *testing.T) {
		key := "tombstone-test"
		
		// Store and delete
		node.Store(key, "value", node.generateTimestamp())
		deleteTime := node.generateTimestamp()
		node.Delete(key)

		// Verify tombstone
		kv := node.store[key]
		assert.True(t, kv.IsTombstone)
		assert.Equal(t, uint64(86400), kv.TTL)
		assert.True(t, kv.Timestamp >= deleteTime)

		// Try to store with older timestamp
		oldTimestamp := deleteTime - 1
		node.Store(key, "old-value", oldTimestamp)

		// Verify tombstone remains
		_, _, exists := node.Get(key)
		assert.False(t, exists)
	})
}

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	nodeID := uint32(7)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)
	
	const (
		numOps        = 10000
		numGoroutines = 5
		keySpace      = 1000
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	t.Run("Mixed Operations Stress Test", func(t *testing.T) {
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				r := rand.New(rand.NewSource(time.Now().UnixNano()))

				for j := 0; j < numOps; j++ {
					key := fmt.Sprintf("key-%d", r.Intn(keySpace))
					op := r.Intn(3)

					switch op {
					case 0: // Store
						value := fmt.Sprintf("value-%d", r.Int())
						node.Store(key, value, node.generateTimestamp())
					case 1: // Get
						node.Get(key)
					case 2: // Delete
						node.Delete(key)
					}
				}
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)
		
		// Calculate operations per second
		totalOps := numOps * numGoroutines
		opsPerSecond := float64(totalOps) / elapsed.Seconds()
		
		t.Logf("Completed %d operations in %v (%.2f ops/sec)", totalOps, elapsed, opsPerSecond)
		
		// Verify node is still functional
		testKey := "final-test"
		testValue := "final-value"
		node.Store(testKey, testValue, node.generateTimestamp())
		value, _, exists := node.Get(testKey)
		assert.True(t, exists)
		assert.Equal(t, testValue, value)
	})
}



func TestMaxSnapshotLimit(t *testing.T) {
	nodeID := uint32(9)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)

	t.Run("Max Snapshots Maintained", func(t *testing.T) {
		// Create multiple snapshots
		for i := 0; i < maxSnapshots+2; i++ {
			// Store some data
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", i, j)
				value := fmt.Sprintf("value-%d-%d", i, j)
				node.Store(key, value, node.generateTimestamp())
			}
			
			// Force snapshot
			require.NoError(t, node.compactWAL())
			time.Sleep(100 * time.Millisecond)
		}

		// Check number of snapshots
		files, err := os.ReadDir("snapshots")
		require.NoError(t, err)
		assert.LessOrEqual(t, len(files), maxSnapshots)
	})
}

func BenchmarkStorageOperations(b *testing.B) {
	nodeID := uint32(10)
	cleanup(nodeID)
	defer cleanup(nodeID)

	node := NewNode(nodeID)

	b.Run("Store Operation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-key-%d", i)
			value := fmt.Sprintf("bench-value-%d", i)
			node.Store(key, value, node.generateTimestamp())
		}
	})

	b.Run("Get Operation", func(b *testing.B) {
		// Pre-populate with test data
		key := "bench-get-key"
		value := "bench-get-value"
		node.Store(key, value, node.generateTimestamp())

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			node.Get(key)
		}
	})

	b.Run("Delete Operation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-delete-key-%d", i)
			node.Delete(key)
		}
	})
}