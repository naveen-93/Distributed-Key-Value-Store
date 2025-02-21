package node

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

// WALWriter handles atomic WAL writes
type WALWriter struct {
	nodeID      uint32
	stagingFile *os.File
	currentWAL  string
	mu          sync.Mutex
}

func NewWALWriter(nodeID uint32) (*WALWriter, error) {
	currentWAL := fmt.Sprintf("wal-%d.log", nodeID)
	stagingPath := fmt.Sprintf("wal-%d.staging", nodeID)

	// Create or truncate staging file
	stagingFile, err := os.OpenFile(stagingPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create staging file: %v", err)
	}

	return &WALWriter{
		nodeID:      nodeID,
		stagingFile: stagingFile,
		currentWAL:  currentWAL,
	}, nil
}

func (w *WALWriter) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset staging file
	if err := w.stagingFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate staging file: %v", err)
	}
	if _, err := w.stagingFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek staging file: %v", err)
	}

	// Write entry to staging file
	encoded := entry.encode()
	if _, err := w.stagingFile.WriteString(encoded); err != nil {
		return fmt.Errorf("failed to write to staging: %v", err)
	}

	// Ensure data is written to disk
	if err := w.stagingFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync staging file: %v", err)
	}

	// Append staging content to WAL
	if err := w.appendToWAL(); err != nil {
		return fmt.Errorf("failed to append to WAL: %v", err)
	}

	return nil
}

func (w *WALWriter) appendToWAL() error {
	// Open WAL in append mode
	wal, err := os.OpenFile(w.currentWAL, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer wal.Close()

	// Reset staging file read position
	if _, err := w.stagingFile.Seek(0, 0); err != nil {
		return err
	}

	// Copy staging content to WAL
	if _, err := io.Copy(wal, w.stagingFile); err != nil {
		return err
	}

	// Ensure WAL is synced to disk
	return wal.Sync()
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

func (n *Node) Store(key, value string, timestamp uint64) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	entry := &WALEntry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	// Write atomically to WAL
	if err := n.walWriter.Write(entry); err != nil {
		log.Printf("WAL write failed: %v", err)
		return
	}

	// Update in-memory store
	existing, exists := n.store[key]
	if !exists || timestamp > existing.Timestamp {
		n.store[key] = &KeyValue{Value: value, Timestamp: timestamp}
	}
}

func (n *Node) Get(key string) (string, uint64, bool) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	if kv, exists := n.store[key]; exists {
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

// Modified compactWAL to use atomic snapshot creation
func (n *Node) compactWAL() error {
	// Take a read lock during snapshot creation to allow concurrent reads
	n.storeMu.RLock()
	defer n.storeMu.Unlock()
	snapshot := Snapshot{
		Store:     make(map[string]*KeyValue, len(n.store)),
		Timestamp: n.LogicalClock,
	}
	for k, v := range n.store {
		snapshot.Store[k] = &KeyValue{
			Value:     v.Value,
			Timestamp: v.Timestamp,
		}
	}
	n.storeMu.RUnlock()

	// Create snapshots directory if it doesn't exist
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	// Create temporary snapshot file
	tempFile := fmt.Sprintf("%s/snapshot-%d-%d.tmp", snapshotPath, n.ID, snapshot.Timestamp)
	finalFile := fmt.Sprintf("%s/snapshot-%d-%d.dat", snapshotPath, n.ID, snapshot.Timestamp)

	f, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary snapshot file: %v", err)
	}

	// Write snapshot to temporary file
	if err := json.NewEncoder(f).Encode(snapshot); err != nil {
		f.Close()
		os.Remove(tempFile)
		return fmt.Errorf("failed to write snapshot: %v", err)
	}

	// Ensure all data is written to disk
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempFile)
		return fmt.Errorf("failed to sync snapshot file: %v", err)
	}
	f.Close()

	// Atomically rename temporary file to final name
	if err := os.Rename(tempFile, finalFile); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	// Take write lock during WAL truncation
	n.storeMu.Lock()
	err = n.truncateWAL(snapshot.Timestamp)
	n.storeMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %v", err)
	}

	// Clean up old snapshots after successful creation
	if err := n.cleanupSnapshots(); err != nil {
		log.Printf("Warning: failed to cleanup old snapshots: %v", err)
	}

	// During snapshot creation
	for key, kv := range n.store {
		if kv.IsTombstone && time.Now().Unix() > int64(kv.Timestamp)+int64(kv.TTL) {
			delete(n.store, key)
		}
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

// Modified recoverFromWAL to properly handle logical clock
func (n *Node) recoverFromWAL() {
	// First try to recover from snapshot
	var snapshotTimestamp uint64
	if err := n.recoverFromSnapshot(); err != nil {
		log.Printf("Failed to recover from snapshot: %v", err)
	} else {
		snapshotTimestamp = n.LogicalClock
	}

	// Then apply any WAL entries after the snapshot
	file, err := os.Open(fmt.Sprintf("wal-%d.log", n.ID))
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No WAL file found for node %d, starting fresh", n.ID)
			return
		}
		log.Fatalf("Failed to open WAL file for node %d: %v", n.ID, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lineNum int64
	batchSize := 1000
	batch := make(map[string]*KeyValue, batchSize)
	var maxTimestamp uint64

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Parse and validate WAL entry
		entry, err := parseWALEntry(line)
		if err != nil {
			log.Printf("Warning: invalid WAL entry at line %d: %v", lineNum, err)
			continue
		}

		// Skip entries that are already in the snapshot
		if entry.Timestamp <= snapshotTimestamp {
			continue
		}

		// Track highest timestamp seen
		if entry.Timestamp > maxTimestamp {
			maxTimestamp = entry.Timestamp
		}

		switch entry.Operation {
		case "PUT":
			if existing, exists := batch[entry.Key]; !exists || entry.Timestamp > existing.Timestamp {
				batch[entry.Key] = &KeyValue{
					Value:     entry.Value,
					Timestamp: entry.Timestamp,
				}
			}

		case "DELETE":
			if existing, exists := batch[entry.Key]; !exists || entry.Timestamp > existing.Timestamp {
				batch[entry.Key] = nil
			}
		}

		// Apply batch when it reaches the size limit
		if len(batch) >= batchSize {
			n.applyWALBatch(batch)
			batch = make(map[string]*KeyValue, batchSize)
		}
	}

	// Apply final batch
	if len(batch) > 0 {
		n.applyWALBatch(batch)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading WAL: %v", err)
	}

	// Update logical clock to highest timestamp seen
	// Extract node-specific portion (lower 32 bits)
	n.mu.Lock()
	n.LogicalClock = maxTimestamp & 0xFFFFFFFF
	n.mu.Unlock()

	log.Printf("Node %d: Recovered %d entries from WAL, logical clock set to %d",
		n.ID, lineNum, n.LogicalClock)
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

func (n *Node) applyWALBatch(batch map[string]*KeyValue) {
	for key, kv := range batch {
		if kv.IsTombstone {
			n.store[key] = &KeyValue{Timestamp: kv.Timestamp, IsTombstone: true}
		} else {
			n.store[key] = kv
		}
	}
}

func (n *Node) Delete(key string) {
	timestamp := n.generateTimestamp()

	// Write tombstone to WAL
	n.walWriter.Write(&WALEntry{
		Operation: "DELETE",
		Key:       key,
		Timestamp: timestamp,
		TTL:       86400, // 24-hour tombstone
	})

	n.storeMu.Lock()
	// Store tombstone marker
	n.store[key] = &KeyValue{
		Timestamp:   timestamp,
		IsTombstone: true,
		TTL:         86400,
	}
	n.storeMu.Unlock()
}
