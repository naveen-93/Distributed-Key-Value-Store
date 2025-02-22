package node

import (
	"bufio"
	"encoding/base64"
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

type WALWriter struct {
	nodeID      uint32
	stagingFile *os.File
	currentWAL  string
	mu          sync.Mutex
}

func NewWALWriter(nodeID uint32) (*WALWriter, error) {
	currentWAL := fmt.Sprintf("wal-%d.log", nodeID)
	stagingPath := fmt.Sprintf("wal-%d.staging", nodeID)

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

	if err := w.stagingFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate staging file: %v", err)
	}
	if _, err := w.stagingFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek staging file: %v", err)
	}

	encoded := entry.encode()
	if _, err := w.stagingFile.WriteString(encoded); err != nil {
		return fmt.Errorf("failed to write to staging: %v", err)
	}
	if err := w.stagingFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync staging file: %v", err)
	}
	if err := w.appendToWAL(); err != nil {
		return fmt.Errorf("failed to append to WAL: %v", err)
	}
	return nil
}

func (w *WALWriter) appendToWAL() error {
	wal, err := os.OpenFile(w.currentWAL, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer wal.Close()

	if _, err := w.stagingFile.Seek(0, 0); err != nil {
		return err
	}
	if _, err := io.Copy(wal, w.stagingFile); err != nil {
		return err
	}
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
	Value       []byte
	Timestamp   int64
	IsTombstone bool
	TTL         int64 // TTL in seconds
}

type Snapshot struct {
	Store     map[string]*KeyValue
	Timestamp uint64
}

type WALEntry struct {
	Operation string
	Key       string
	Value     []byte
	Timestamp int64
	Checksum  uint32
	TTL       int64
}

func (e *WALEntry) computeChecksum() uint32 {
	h := fnv.New32a()
	h.Write([]byte(e.Operation))
	h.Write([]byte(e.Key))
	h.Write(e.Value)
	binary.Write(h, binary.BigEndian, e.Timestamp)
	return h.Sum32()
}

func (e *WALEntry) validate() bool {
	return e.Checksum == e.computeChecksum()
}

func (e *WALEntry) encode() string {
	e.Checksum = e.computeChecksum()
	valueStr := base64.StdEncoding.EncodeToString(e.Value)
	if len(e.Value) == 0 {
		valueStr = "-" // Placeholder for empty value
	}
	return fmt.Sprintf("%s %s %s %d %d %d\n",
		e.Operation, e.Key, valueStr, e.Timestamp, e.Checksum, e.TTL)
}

func parseWALEntry(line string) (*WALEntry, error) {
	parts := strings.Fields(line)
	if len(parts) != 6 {
		return nil, fmt.Errorf("malformed WAL entry: %s (expected 6 parts, got %d)", line, len(parts))
	}

	timestamp, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}

	checksum, err := strconv.ParseUint(parts[4], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid checksum: %v", err)
	}

	ttl, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid TTL: %v", err)
	}

	var value []byte
	if parts[2] == "-" {
		value = []byte{}
	} else {
		value, err = base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid value encoding: %v", err)
		}
	}

	entry := &WALEntry{
		Operation: parts[0],
		Key:       parts[1],
		Value:     value,
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

func (n *Node) generateTimestamp() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LogicalClock++
	return int64((uint64(n.ID) << 32) | n.LogicalClock)
}

func (n *Node) Put(key string, value []byte, timestamp int64) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	entry := &WALEntry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	if err := n.walWriter.Write(entry); err != nil {
		log.Printf("WAL write failed: %v", err)
		return
	}

	existing, exists := n.store[key]
	if !exists || timestamp > existing.Timestamp {
		n.store[key] = &KeyValue{Value: value, Timestamp: timestamp}
	}
}

func (n *Node) Get(key string) ([]byte, int64, bool) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	if kv, exists := n.store[key]; exists {
		if kv.IsTombstone && time.Now().Unix() > kv.Timestamp+kv.TTL {
			return nil, 0, false
		}
		if kv.IsTombstone {
			return nil, kv.Timestamp, false
		}
		return kv.Value, kv.Timestamp, true
	}
	return nil, 0, false
}

func (n *Node) GetKeys() []string {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	keys := make([]string, 0, len(n.store))
	for k, kv := range n.store {
		if !kv.IsTombstone {
			keys = append(keys, k)
		}
	}
	return keys
}

func (n *Node) Delete(key string) {
	timestamp := n.generateTimestamp()

	entry := &WALEntry{
		Operation: "DELETE",
		Key:       key,
		Value:     []byte{}, // Explicitly empty for DELETE
		Timestamp: timestamp,
		TTL:       86400, // 24-hour tombstone
	}

	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	if err := n.walWriter.Write(entry); err != nil {
		log.Printf("WAL write failed: %v", err)
		return
	}

	n.store[key] = &KeyValue{
		Timestamp:   timestamp,
		IsTombstone: true,
		TTL:         86400,
	}
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

	snapshots := make([]snapshotInfo, 0)
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), fmt.Sprintf("snapshot-%d-", n.ID)) {
			continue
		}
		parts := strings.Split(file.Name(), "-")
		timestamp, err := strconv.ParseUint(strings.TrimSuffix(parts[2], ".dat"), 10, 64)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, snapshotInfo{name: file.Name(), timestamp: timestamp})
	}

	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].timestamp > snapshots[j].timestamp })
	for i := maxSnapshots; i < len(snapshots); i++ {
		path := filepath.Join(snapshotPath, snapshots[i].name)
		if err := os.Remove(path); err != nil {
			log.Printf("Warning: failed to remove old snapshot %s: %v", path, err)
		}
	}
	return nil
}

func (n *Node) compactWAL() error {
	n.storeMu.RLock()
	snapshot := Snapshot{
		Store:     make(map[string]*KeyValue, len(n.store)),
		Timestamp: n.LogicalClock,
	}
	for k, v := range n.store {
		snapshot.Store[k] = &KeyValue{Value: v.Value, Timestamp: v.Timestamp, IsTombstone: v.IsTombstone, TTL: v.TTL}
	}
	n.storeMu.RUnlock()

	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	tempFile := fmt.Sprintf("%s/snapshot-%d-%d.tmp", snapshotPath, n.ID, snapshot.Timestamp)
	finalFile := fmt.Sprintf("%s/snapshot-%d-%d.dat", snapshotPath, n.ID, snapshot.Timestamp)

	f, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary snapshot file: %v", err)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(snapshot); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if err := f.Sync(); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to sync snapshot file: %v", err)
	}

	if err := os.Rename(tempFile, finalFile); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	n.storeMu.Lock()
	err = n.truncateWAL(snapshot.Timestamp)
	n.storeMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %v", err)
	}

	return n.cleanupSnapshots()
}

func (n *Node) truncateWAL(timestamp uint64) error {
	newWAL, err := os.Create(fmt.Sprintf("wal-%d.log.new", n.ID))
	if err != nil {
		return err
	}
	defer newWAL.Close()

	oldWAL, err := os.Open(fmt.Sprintf("wal-%d.log", n.ID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer oldWAL.Close()

	scanner := bufio.NewScanner(oldWAL)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 4 {
			continue
		}
		entryTimestamp, _ := strconv.ParseUint(parts[3], 10, 64)
		if entryTimestamp > timestamp {
			newWAL.WriteString(scanner.Text() + "\n")
		}
	}

	return os.Rename(fmt.Sprintf("wal-%d.log.new", n.ID), fmt.Sprintf("wal-%d.log", n.ID))
}

func (n *Node) checkWALSize() {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			stat, err := os.Stat(fmt.Sprintf("wal-%d.log", n.ID))
			if err != nil && !os.IsNotExist(err) {
				log.Printf("Failed to check WAL size: %v", err)
				continue
			}
			if err == nil && stat.Size() > maxWALSize {
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
	batch := make(map[string]*WALEntry, 1000) // Store latest entry per key
	var maxTimestamp uint64

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

		// Store the latest entry for each key
		if existing, exists := batch[entry.Key]; !exists || entry.Timestamp > existing.Timestamp {
			batch[entry.Key] = entry
		}
	}

	// Apply the batch
	n.storeMu.Lock()
	for key, entry := range batch {
		switch entry.Operation {
		case "PUT":
			n.store[key] = &KeyValue{
				Value:     entry.Value,
				Timestamp: entry.Timestamp,
			}
		case "DELETE":
			n.store[key] = &KeyValue{
				Timestamp:   entry.Timestamp,
				IsTombstone: true,
				TTL:         entry.TTL,
			}
		}
	}
	n.storeMu.Unlock()

	n.mu.Lock()
	n.LogicalClock = maxTimestamp & 0xFFFFFFFF
	n.mu.Unlock()
}

func (n *Node) recoverFromSnapshot() error {
	files, err := os.ReadDir(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
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
		return nil
	}

	f, err := os.Open(filepath.Join(snapshotPath, latestSnapshot))
	if err != nil {
		return err
	}
	defer f.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(f).Decode(&snapshot); err != nil {
		return err
	}

	n.storeMu.Lock()
	n.store = snapshot.Store
	n.LogicalClock = snapshot.Timestamp
	n.storeMu.Unlock()
	return nil
}