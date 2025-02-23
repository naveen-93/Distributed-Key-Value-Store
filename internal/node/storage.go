package node

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxSnapshots = 3                 // Keep last N snapshots
	maxWALSize   = 100 * 1024 * 1024 // 100MB
	snapshotPath = "snapshots"
)

type Metrics struct {
    TombstoneCount    int64
    ActiveKeyCount    int64
    LastCompactionTS  int64
    WALSize          int64
}

type WALWriter struct {
    nodeID     uint32
    walFile    *os.File  // Open WAL file handle
    mu         sync.Mutex
}

func NewWALWriter(nodeID uint32) (*WALWriter, error) {
    currentWAL := fmt.Sprintf("wal-%d.log", nodeID)
    walFile, err := os.OpenFile(currentWAL, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open WAL file: %v", err)
    }

    return &WALWriter{
        nodeID:  nodeID,
        walFile: walFile,
    }, nil
}

func (w *WALWriter) Write(entry *WALEntry) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    encoded := entry.encode() // Assume encode() converts entry to string
    if _, err := w.walFile.WriteString(encoded); err != nil {
        return fmt.Errorf("failed to write to WAL: %v", err)
    }

    // Sync to ensure durability
    if err := w.walFile.Sync(); err != nil {
        return fmt.Errorf("failed to sync WAL file: %v", err)
    }

    return nil
}

func (w *WALWriter) Close() error {
    return w.walFile.Close() // Close the file when the node shuts down
}

type Node struct {
	ID           uint32
	LogicalClock uint64
	mu           sync.RWMutex
	store        map[string]*KeyValue
	storeMu      sync.RWMutex
	walWriter    *WALWriter
}

type KeyValue struct {
	Value       string
	Timestamp   uint64
	IsTombstone bool
	TTL         uint64
}

// Snapshot represents the state at a point in time
type Snapshot struct {
	Store     map[string]*KeyValue
	Timestamp uint64
}

type WALEntry struct {
	Operation string // PUT or DELETE
	Key       string
	Value     string // Empty for DELETE
	Timestamp uint64
	Checksum  uint32
	TTL       uint64
}

func (e *WALEntry) computeChecksum() uint32 {
	h := fnv.New32a()
	h.Write([]byte(e.Operation))
	h.Write([]byte(e.Key))
	h.Write([]byte(e.Value))
	binary.Write(h, binary.BigEndian, e.Timestamp)
	return h.Sum32()
}

func (e *WALEntry) validate() bool {
	return e.Checksum == e.computeChecksum()
}

func (e *WALEntry) encode() string {
	e.Checksum = e.computeChecksum()
	return fmt.Sprintf("%s %s %s %d %d %d\n",
		e.Operation, e.Key, e.Value, e.Timestamp, e.Checksum, e.TTL)
}

func parseWALEntry(line string) (*WALEntry, error) {
	parts := strings.Fields(line)
	if len(parts) < 6 {
		return nil, fmt.Errorf("malformed WAL entry: %s", line)
	}

	timestamp, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}

	checksum, err := strconv.ParseUint(parts[4], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid checksum: %v", err)
	}

	ttl, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid TTL: %v", err)
	}

	entry := &WALEntry{
		Operation: parts[0],
		Key:       parts[1],
		Value:     parts[2],
		Timestamp: timestamp,
		Checksum:  uint32(checksum),
		TTL:       ttl,
	}

	if !entry.validate() {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return entry, nil
}

func NewNode(id uint32) *Node {
	walWriter, err := NewWALWriter(id)
	if err != nil {
		log.Fatalf("Failed to create WAL writer: %v", err)
	}

	n := &Node{
		ID:        id,
		store:     make(map[string]*KeyValue),
		walWriter: walWriter,
	}
	n.checkWALSize()
	n.recoverFromWAL()
	return n
}

func (n *Node) generateTimestamp() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LogicalClock++
	return (uint64(n.ID) << 32) | n.LogicalClock
}

func (n *Node) Store(key, value string, timestamp uint64) error {
    entry := &WALEntry{
        Operation: "PUT",
        Key:       key,
        Value:     value,
        Timestamp: timestamp,
    }

    // Write to WAL first
    if err := n.walWriter.Write(entry); err != nil {
        log.Printf("WAL write failed: %v", err)
        return err // Do not update store if WAL fails
    }

    // Update store only after successful WAL write
    n.storeMu.Lock()
    defer n.storeMu.Unlock()
    existing, exists := n.store[key]
    if !exists || timestamp > existing.Timestamp {
        n.store[key] = &KeyValue{Value: value, Timestamp: timestamp}
    }
    return nil
}

func (n *Node) Delete(key string) error {
    timestamp := n.generateTimestamp() // Assume this generates a timestamp

    // Write tombstone to WAL first
    err := n.walWriter.Write(&WALEntry{
        Operation: "DELETE",
        Key:       key,
        Timestamp: timestamp,
        TTL:       86400, // Example TTL for tombstone
    })
    if err != nil {
        log.Printf("WAL write failed: %v", err)
        return err // Do not update store if WAL fails
    }

    // Update store only after successful WAL write
    n.storeMu.Lock()
    defer n.storeMu.Unlock()
    n.store[key] = &KeyValue{
        Timestamp:   timestamp,
        IsTombstone: true,
        TTL:         86400,
    }
    return nil
}
func (n *Node) Get(key string) (string, uint64, bool) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	if kv, exists := n.store[key]; exists {
		// Don't return tombstoned entries
		if kv.IsTombstone {
			return "", 0, false
		}
		return kv.Value, kv.Timestamp, true
	}
	return "", 0, false
}

// GetKeys returns all keys in the store
func (n *Node) GetKeys() []string {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	keys := make([]string, 0, len(n.store))
	for k := range n.store {
		keys = append(keys, k)
	}
	return keys
}

func (n *Node) cleanupSnapshots() error {
	files, err := os.ReadDir(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read snapshot directory: %v", err)
	}

	type snapshotInfo struct {
		name      string
		timestamp uint64
	}

	// Collect snapshots for this node
	snapshots := make([]snapshotInfo, 0)
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), fmt.Sprintf("snapshot-%d-", n.ID)) {
			continue
		}

		parts := strings.Split(file.Name(), "-")
		timestamp, err := strconv.ParseUint(strings.TrimSuffix(parts[2], ".dat"), 10, 64)
		if err != nil {
			log.Printf("Warning: invalid snapshot filename: %s", file.Name())
			continue
		}

		snapshots = append(snapshots, snapshotInfo{
			name:      file.Name(),
			timestamp: timestamp,
		})
	}

	// Sort snapshots by timestamp (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].timestamp > snapshots[j].timestamp
	})

	// Remove old snapshots, keeping maxSnapshots most recent
	for i := maxSnapshots; i < len(snapshots); i++ {
		path := filepath.Join(snapshotPath, snapshots[i].name)
		if err := os.Remove(path); err != nil {
			log.Printf("Warning: failed to remove old snapshot %s: %v", path, err)
		} else {
			log.Printf("Removed old snapshot: %s", path)
		}
	}

	return nil
}


func (n *Node) compactWAL() error {
    // Take a read lock for creating snapshot
    n.storeMu.RLock()
    snapshot := Snapshot{
        Store:     make(map[string]*KeyValue, len(n.store)),
        Timestamp: n.LogicalClock,
    }
    
    // Copy only non-tombstoned entries
    for k, v := range n.store {
        if !v.IsTombstone {
            snapshot.Store[k] = &KeyValue{
                Value:     v.Value,
                Timestamp: v.Timestamp,
                TTL:      v.TTL,
            }
        }
    }
    n.storeMu.RUnlock()  // Release read lock after snapshot creation

    // Create snapshots directory if it doesn't exist
    if err := os.MkdirAll(snapshotPath, 0755); err != nil {
        return fmt.Errorf("failed to create snapshot directory: %v", err)
    }

    // Handle WAL compaction with proper locking
    if err := n.handleWALCompaction(&snapshot); err != nil {
        return fmt.Errorf("WAL compaction failed: %v", err)
    }
	var err error
    maxRetries := 3
    for retry := 0; retry < maxRetries; retry++ {
        if err = n.handleWALCompaction(&snapshot); err == nil {
            break // Success
        }
        log.Printf("Compaction attempt %d failed: %v", retry+1, err)
        time.Sleep(time.Duration(retry+1) * time.Second)
    }
    if err != nil {
        return fmt.Errorf("WAL compaction failed after %d attempts: %v", maxRetries, err)
    }
    return nil
}

// New helper method for WAL compaction
func (n *Node) handleWALCompaction(snapshot *Snapshot) error {
	// Create snapshots directory if it doesn't exist
    if err := os.MkdirAll(snapshotPath, 0755); err != nil {
        return fmt.Errorf("failed to create snapshot directory: %v", err)
    }

    // Verify write permissions
    testFile := filepath.Join(snapshotPath, "write_test.tmp")
    if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
        os.Remove(testFile)
        return fmt.Errorf("snapshot directory %s is not writable: %v", snapshotPath, err)
    }
    os.Remove(testFile)
    tempFile := fmt.Sprintf("%s/snapshot-%d-%d.tmp", snapshotPath, n.ID, snapshot.Timestamp)
    finalFile := fmt.Sprintf("%s/snapshot-%d-%d.dat", snapshotPath, n.ID, snapshot.Timestamp)

    // Write snapshot to temporary file
    if err := n.writeSnapshotToFile(tempFile, snapshot); err != nil {
        return err
    }

    // Atomically rename temporary file to final name
    if err := os.Rename(tempFile, finalFile); err != nil {
        os.Remove(tempFile)
        return fmt.Errorf("failed to finalize snapshot: %v", err)
    }

    // Take write lock only for WAL truncation
    n.storeMu.Lock()
    err := n.truncateWAL(snapshot.Timestamp)
    n.storeMu.Unlock()
    if err != nil {
        return fmt.Errorf("failed to truncate WAL: %v", err)
    }

    // Clean up old snapshots
    if err := n.cleanupSnapshots(); err != nil {
        log.Printf("Warning: failed to cleanup old snapshots: %v", err)
    }

    return nil
}

// Helper method for writing snapshot to file
func (n *Node) writeSnapshotToFile(filename string, snapshot *Snapshot) error {
    f, err := os.Create(filename)
    if err != nil {
        return fmt.Errorf("failed to create snapshot file: %v", err)
    }
    defer f.Close()

    if err := json.NewEncoder(f).Encode(snapshot); err != nil {
        os.Remove(filename)
        return fmt.Errorf("failed to write snapshot: %v", err)
    }

    if err := f.Sync(); err != nil {
        os.Remove(filename)
        return fmt.Errorf("failed to sync snapshot file: %v", err)
    }

    return nil
}
func (n *Node) truncateWAL(timestamp uint64) error {
	// Create new WAL file
	newWAL, err := os.Create(fmt.Sprintf("wal-%d.log.new", n.ID))
	if err != nil {
		return err
	}
	defer newWAL.Close()

	// Read existing WAL and copy entries after snapshot timestamp
	oldWAL, err := os.Open(fmt.Sprintf("wal-%d.log", n.ID))
	if err != nil {
		return err
	}
	defer oldWAL.Close()

	scanner := bufio.NewScanner(oldWAL)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 4 {
			continue
		}
		entryTimestamp, err := strconv.ParseUint(parts[3], 10, 64)
		if err != nil {
			continue
		}
		if entryTimestamp > timestamp {
			newWAL.WriteString(scanner.Text() + "\n")
		}
	}

	// Rename new WAL to replace old WAL
	if err := os.Rename(
		fmt.Sprintf("wal-%d.log.new", n.ID),
		fmt.Sprintf("wal-%d.log", n.ID),
	); err != nil {
		return err
	}

	return nil
}

func (n *Node) checkWALSize() {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			stat, err := os.Stat(fmt.Sprintf("wal-%d.log", n.ID))
			if err != nil {
				log.Printf("Failed to check WAL size: %v", err)
				continue
			}

			if stat.Size() > maxWALSize {
				if err := n.compactWAL(); err != nil {
					log.Printf("Failed to compact WAL: %v", err)
				}
			}
		}
	}()
}

func (n *Node) recoverFromWAL() {
	var snapshotTimestamp uint64
	if err := n.recoverFromSnapshot(); err != nil {
		log.Printf("Failed to recover from snapshot: %v", err)
	} else {
		snapshotTimestamp = n.LogicalClock
	}

	file, err := os.Open(fmt.Sprintf("wal-%d.log", n.ID))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	const batchSizeLimit = 1000
    batch := make(map[string]*WALEntry)
	var maxTimestamp uint64
	applyBatch := func(batch map[string]*WALEntry) {
        converted := make(map[string]*KeyValue)
        for key, entry := range batch {
            switch entry.Operation {
            case "PUT":
                converted[key] = &KeyValue{
                    Value:     entry.Value,
                    Timestamp: entry.Timestamp,
                }
            case "DELETE":
                converted[key] = &KeyValue{
                    Timestamp:   entry.Timestamp,
                    IsTombstone: true,
                    TTL:         entry.TTL,
                }
            }
        }
        n.applyWALBatch(converted)
    }
	for scanner.Scan() {
		entry, err := parseWALEntry(scanner.Text())
		if err != nil {
			log.Printf("Skipping invalid WAL entry: %v", err)
			continue
		}
		if uint64(entry.Timestamp) <= snapshotTimestamp {
			continue
		}
		if uint64(entry.Timestamp) > maxTimestamp {
			maxTimestamp = uint64(entry.Timestamp)
		}

		batch[entry.Key] = entry
        if len(batch) >= batchSizeLimit {
            applyBatch(batch)
            batch = make(map[string]*WALEntry)
        }
	}
	if len(batch) > 0 {
        applyBatch(batch)
    }
	n.mu.Lock()
	n.LogicalClock = maxTimestamp & 0xFFFFFFFF
	n.mu.Unlock()
}
func (n *Node) applyWALBatch(batch map[string]*KeyValue) {
    n.storeMu.Lock() // Lock the store
    defer n.storeMu.Unlock()

    for key, kv := range batch {
        if kv == nil || kv.IsTombstone {
            n.store[key] = &KeyValue{Timestamp: kv.Timestamp, IsTombstone: true}
        } else {
            n.store[key] = kv
        }
    }
}

func (n *Node) recoverFromSnapshot() error {
	// Find latest snapshot
	files, err := os.ReadDir(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No snapshots yet
		}
		return err
	}

	var latestSnapshot string
	var latestTimestamp uint64
	for _, file := range files {
		if strings.HasPrefix(file.Name(), fmt.Sprintf("snapshot-%d-", n.ID)) {
			parts := strings.Split(file.Name(), "-")
			timestamp, err := strconv.ParseUint(strings.TrimSuffix(parts[2], ".dat"), 10, 64)
			if err != nil {
				continue
			}
			if timestamp > latestTimestamp {
				latestTimestamp = timestamp
				latestSnapshot = file.Name()
			}
		}
	}

	if latestSnapshot == "" {
		return nil // No snapshots found
	}

	// Read snapshot
	f, err := os.Open(filepath.Join(snapshotPath, latestSnapshot))
	if err != nil {
		return err
	}
	defer f.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(f).Decode(&snapshot); err != nil {
		return err
	}

	// Apply snapshot
	n.storeMu.Lock()
	n.store = snapshot.Store
	n.LogicalClock = snapshot.Timestamp
	n.storeMu.Unlock()

	return nil
}

func (n *Node) startTTLCleanup() {
    ticker := time.NewTicker(1 * time.Hour)
    go func() {
        for range ticker.C {
            n.cleanupExpiredEntries()
        }
    }()
}

func (n *Node) cleanupExpiredEntries() {
    n.storeMu.Lock()
    defer n.storeMu.Unlock()
    
    now := time.Now().Unix()
    for key, kv := range n.store {
        if kv.TTL > 0 && kv.IsTombstone && 
           now > int64(kv.Timestamp)+int64(kv.TTL) {
            delete(n.store, key)
        }
    }
}
func (n *Node) Shutdown() error {
    if err := n.walWriter.Close(); err != nil {
        return fmt.Errorf("error closing WAL writer: %v", err)
    }
    return nil
}