package node

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupNode(nodeID uint32) *Node {
	n := NewNode(nodeID)
	return n
}

func cleanupNode(nodeID uint32) {
	os.Remove(fmt.Sprintf("wal-%d.log", nodeID))
	os.Remove(fmt.Sprintf("wal-%d.log.new", nodeID))
	os.Remove(fmt.Sprintf("wal-%d.staging", nodeID))
	os.RemoveAll(snapshotPath)
}

func TestNodeInitialization(t *testing.T) {
	nodeID := uint32(1)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	if n.ID != nodeID {
		t.Errorf("Expected node ID %d, got %d", nodeID, n.ID)
	}
	if len(n.store) != 0 {
		t.Errorf("Expected empty store, got %d entries", len(n.store))
	}
	if n.walWriter == nil {
		t.Fatal("Expected WAL writer to be initialized")
	}
}

func TestPutAndGetBasic(t *testing.T) {
	nodeID := uint32(2)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	key := "testKey"
	value := []byte("testValue")
	timestamp := n.generateTimestamp()

	n.Put(key, value, timestamp)
	gotValue, gotTimestamp, exists := n.Get(key)

	if !exists {
		t.Errorf("Expected key %s to exist", key)
	}
	if !bytes.Equal(gotValue, value) {
		t.Errorf("Expected value %s, got %s", value, gotValue)
	}
	if gotTimestamp != timestamp {
		t.Errorf("Expected timestamp %d, got %d", timestamp, gotTimestamp)
	}
}

func TestDeleteAndTombstone(t *testing.T) {
	nodeID := uint32(3)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	key := "testKey"
	value := []byte("testValue")
	timestamp := n.generateTimestamp()

	n.Put(key, value, timestamp)
	n.Delete(key)

	_, _, exists := n.Get(key)
	if exists {
		t.Errorf("Expected key %s to be deleted (tombstone)", key)
	}

	n.storeMu.RLock()
	kv, ok := n.store[key]
	n.storeMu.RUnlock()
	if !ok || !kv.IsTombstone {
		t.Errorf("Expected key %s to have tombstone, got %+v", key, kv)
	}
}

func TestGetKeysWithTombstones(t *testing.T) {
	nodeID := uint32(4)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	n.Put("key1", []byte("value1"), n.generateTimestamp())
	n.Put("key2", []byte("value2"), n.generateTimestamp())
	n.Delete("key1")

	keys := n.GetKeys()
	expected := []string{"key2"}
	if len(keys) != len(expected) || keys[0] != expected[0] {
		t.Errorf("Expected keys %v, got %v", expected, keys)
	}
}

func TestWALPersistence(t *testing.T) {
	nodeID := uint32(5)
	n1 := setupNode(nodeID)
	defer cleanupNode(nodeID)

	n1.Put("key1", []byte("value1"), n1.generateTimestamp())
	n1.Put("key2", []byte("value2"), n1.generateTimestamp())
	n1.Delete("key1")

	n2 := NewNode(nodeID)

	if len(n2.store) != 2 {
		t.Errorf("Expected 2 entries after recovery, got %d", len(n2.store))
	}
	value, _, exists := n2.Get("key2")
	if !exists || !bytes.Equal(value, []byte("value2")) {
		t.Errorf("Expected key2=value2, got exists=%v, value=%s", exists, value)
	}
	_, _, exists = n2.Get("key1")
	if exists {
		t.Errorf("Expected key1 to be deleted (tombstone)")
	}
	n2.storeMu.RLock()
	kv, ok := n2.store["key1"]
	n2.storeMu.RUnlock()
	if !ok || !kv.IsTombstone {
		t.Errorf("Expected key1 to have tombstone, got %+v", kv)
	}
}

func TestSnapshotAndRecovery(t *testing.T) {
	nodeID := uint32(6)
	n1 := setupNode(nodeID)
	defer cleanupNode(nodeID)

	n1.Put("key1", []byte("value1"), n1.generateTimestamp())
	n1.Put("key2", []byte("value2"), n1.generateTimestamp())
	if err := n1.compactWAL(); err != nil {
		t.Fatalf("Failed to compact WAL: %v", err)
	}
	n1.Delete("key1")
	n1.Put("key3", []byte("value3"), n1.generateTimestamp())

	n2 := NewNode(nodeID)

	keys := n2.GetKeys()
	expected := []string{"key2", "key3"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(keys), keys)
	}
	for _, k := range keys {
		if k != "key2" && k != "key3" {
			t.Errorf("Unexpected key %s in %v", k, keys)
		}
	}
	_, _, exists := n2.Get("key1")
	if exists {
		t.Errorf("Expected key1 to be deleted")
	}
}

func TestTTLExpiration(t *testing.T) {
	nodeID := uint32(7)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	key := "ttlKey"
	n.storeMu.Lock()
	n.store[key] = &KeyValue{
		Value:       []byte("ttlValue"),
		Timestamp:   time.Now().Unix() - 100,
		IsTombstone: true,
		TTL:         10,
	}
	n.storeMu.Unlock()

	_, _, exists := n.Get(key)
	if exists {
		t.Errorf("Expected key %s to be expired due to TTL", key)
	}

	keys := n.GetKeys()
	if len(keys) != 0 {
		t.Errorf("Expected no keys due to TTL expiration, got %v", keys)
	}
}

func TestWALCompactionLargeData(t *testing.T) {
	nodeID := uint32(8)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	for i := 0; i < 1000; i++ {
		n.Put(fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)), n.generateTimestamp())
	}
	if err := n.compactWAL(); err != nil {
		t.Fatalf("Failed to compact WAL: %v", err)
	}

	walFile := fmt.Sprintf("wal-%d.log", nodeID)
	stat, err := os.Stat(walFile)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if err == nil && stat.Size() > maxWALSize {
		t.Errorf("Expected WAL size <= %d, got %d", maxWALSize, stat.Size())
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value, _, exists := n.Get(key)
		if !exists || !bytes.Equal(value, []byte(fmt.Sprintf("value%d", i))) {
			t.Errorf("Expected %s=value%d, got exists=%v, value=%s", key, i, exists, value)
		}
	}
}

func TestConcurrentOperationsWithFailures(t *testing.T) {
	nodeID := uint32(9)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	var wg sync.WaitGroup
	numGoroutines := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			value := []byte(fmt.Sprintf("value%d", i))
			timestamp := n.generateTimestamp()
			n.Put(key, value, timestamp)

			// Simulate intermittent failures
			if i%5 == 0 {
				// Force a write error by closing the WAL file
				n.walWriter.mu.Lock()
				n.walWriter.stagingFile.Close()
				n.walWriter.stagingFile, _ = os.OpenFile(fmt.Sprintf("wal-%d.staging", nodeID), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
				n.walWriter.mu.Unlock()
			}

			gotValue, _, exists := n.Get(key)
			if exists && !bytes.Equal(gotValue, value) {
				t.Errorf("Concurrent: Expected %s=%s, got value=%s", key, value, gotValue)
			}

			if i%2 == 0 {
				n.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("key%d", i)
		_, _, exists := n.Get(key)
		if i%2 == 0 && exists {
			t.Errorf("Expected %s to be deleted", key)
		} else if i%2 != 0 && !exists {
			t.Errorf("Expected %s to exist", key)
		}
	}
}

func TestSnapshotCleanupMultiple(t *testing.T) {
	nodeID := uint32(10)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	for i := 0; i < maxSnapshots+2; i++ {
		n.LogicalClock = uint64(i * 1000)
		if err := n.compactWAL(); err != nil {
			t.Fatalf("Failed to compact WAL: %v", err)
		}
	}

	files, err := os.ReadDir(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to read snapshot dir: %v", err)
	}
	snapshotCount := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), fmt.Sprintf("snapshot-%d-", nodeID)) {
			snapshotCount++
		}
	}
	if snapshotCount > maxSnapshots {
		t.Errorf("Expected at most %d snapshots, got %d", maxSnapshots, snapshotCount)
	}
}

func TestMalformedWALRecovery(t *testing.T) {
	nodeID := uint32(11)
	n1 := setupNode(nodeID)
	defer cleanupNode(nodeID)

	n1.Put("key1", []byte("value1"), n1.generateTimestamp())
	n1.Put("key2", []byte("value2"), n1.generateTimestamp())

	// Corrupt WAL by appending invalid entry
	walFile := fmt.Sprintf("wal-%d.log", nodeID)
	f, err := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	_, err = f.WriteString("INVALID ENTRY\n")
	f.Close()

	n2 := NewNode(nodeID)

	value, _, exists := n2.Get("key1")
	if !exists || !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected key1=value1, got exists=%v, value=%s", exists, value)
	}
	value, _, exists = n2.Get("key2")
	if !exists || !bytes.Equal(value, []byte("value2")) {
		t.Errorf("Expected key2=value2, got exists=%v, value=%s", exists, value)
	}
}

func TestEmptyWALAndSnapshot(t *testing.T) {
	nodeID := uint32(12)
	defer cleanupNode(nodeID)

	// Create new node without setupNode
	n2 := NewNode(nodeID)
	if len(n2.store) != 0 {
		t.Errorf("Expected empty store after recovery, got %d entries", len(n2.store))
	}
}

func TestWALOverflow(t *testing.T) {
	nodeID := uint32(13)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	// Write enough data to exceed maxWALSize
	largeValue := make([]byte, maxWALSize/100)
	for i := 0; i < 200; i++ {
		n.Put(fmt.Sprintf("key%d", i), largeValue, n.generateTimestamp())
	}

	// Force compaction
	if err := n.compactWAL(); err != nil {
		t.Fatalf("Failed to compact WAL: %v", err)
	}

	// Verify some data
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%d", i)
		value, _, exists := n.Get(key)
		if !exists || len(value) != len(largeValue) {
			t.Errorf("Expected %s to exist with large value, got exists=%v, len=%d", key, exists, len(value))
		}
	}
}

func TestTimestampCollision(t *testing.T) {
	nodeID := uint32(14)
	n := setupNode(nodeID)
	defer cleanupNode(nodeID)

	key := "collisionKey"
	timestamp := n.generateTimestamp()
	n.Put(key, []byte("value1"), timestamp)
	n.Put(key, []byte("value2"), timestamp) // Same timestamp

	value, gotTimestamp, exists := n.Get(key)
	if !exists || !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected value1 for timestamp collision, got %s", value)
	}
	if gotTimestamp != timestamp {
		t.Errorf("Expected timestamp %d, got %d", timestamp, gotTimestamp)
	}
}

func TestCorruptedSnapshotRecovery(t *testing.T) {
	nodeID := uint32(15)
	n1 := setupNode(nodeID)
	defer cleanupNode(nodeID)

	n1.Put("key1", []byte("value1"), n1.generateTimestamp())
	if err := n1.compactWAL(); err != nil {
		t.Fatalf("Failed to compact WAL: %v", err)
	}

	// Corrupt the snapshot file
	files, _ := os.ReadDir(snapshotPath)
	var snapshotFile string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), fmt.Sprintf("snapshot-%d-", nodeID)) {
			snapshotFile = filepath.Join(snapshotPath, f.Name())
			break
		}
	}
	f, _ := os.OpenFile(snapshotFile, os.O_WRONLY, 0644)
	f.WriteString("corrupted data")
	f.Close()

	// Recovery should fall back to WAL
	n2 := NewNode(nodeID)
	value, _, exists := n2.Get("key1")
	if !exists || !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected key1=value1 after corrupted snapshot recovery, got exists=%v, value=%s", exists, value)
	}
}
