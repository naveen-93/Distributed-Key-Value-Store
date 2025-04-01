package storage

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

// Add at package level
var timeNow = time.Now

type Metrics struct {
	TombstoneCount   int64
	ActiveKeyCount   int64
	LastCompactionTS int64
	WALSize          int64
}

type WALWriter struct {
	nodeID     uint32
	walFile    *os.File
	mu         sync.Mutex
	buffer     []string  // Buffer for batching writes
	bufferSize int       // Current size of buffer
	maxBuffer  int       // Max buffer size before flush
	syncChan   chan bool // Channel for sync requests
	stopChan   chan bool // Channel for shutdown
	doneChan   chan struct{}
}

func NewWALWriter(nodeID uint32) (*WALWriter, error) {
	currentWAL := fmt.Sprintf("wal-%d.log", nodeID)
	walFile, err := os.OpenFile(currentWAL, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	w := &WALWriter{
		nodeID:    nodeID,
		walFile:   walFile,
		buffer:    make([]string, 0, 5000), // Increased from 1000 to 5000
		maxBuffer: 5000,                    // Increased from 1000 to 5000
		syncChan:  make(chan bool),
		stopChan:  make(chan bool),
	}
	w.doneChan = make(chan struct{})

	go w.backgroundSync()
	return w, nil
}

func (w *WALWriter) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	encoded := entry.encode()
	w.buffer = append(w.buffer, encoded)
	w.bufferSize++

	if w.bufferSize >= w.maxBuffer {
		return w.flush()
	}

	return nil
}

func (w *WALWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	// Write all buffered entries
	data := strings.Join(w.buffer, "")
	if _, err := w.walFile.WriteString(data); err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Signal sync is needed
	select {
	case w.syncChan <- true:
	default:
		// Channel is full, sync will happen soon anyway
	}

	// Clear buffer
	w.buffer = w.buffer[:0]
	w.bufferSize = 0
	return nil
}

func (w *WALWriter) backgroundSync() {
	defer close(w.doneChan)
	ticker := time.NewTicker(200 * time.Millisecond) // Increased from 100ms to 200ms
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			// Only flush if there's something to flush
			if w.bufferSize > 0 {
				w.flush()
			}
			w.mu.Unlock()
		case <-w.syncChan:
			w.walFile.Sync()
		case <-w.stopChan:
			w.mu.Lock()
			w.flush()
			w.walFile.Sync()
			w.mu.Unlock()
			return
		}
	}
}

func (w *WALWriter) Close() error {
	select {
	case w.stopChan <- true:
		// Wait for background sync to finish
		<-w.doneChan
	case <-w.doneChan:
		// Already closed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Final flush
	if err := w.flush(); err != nil {
		return err
	}

	// Final sync
	if err := w.walFile.Sync(); err != nil {
		return err
	}

	return w.walFile.Close()
}

type Storage struct {
	ID                uint32
	LogicalClock      uint32 // Increased to 32-bit
	mu                sync.RWMutex
	store             map[string]*KeyValue
	storeMu           sync.RWMutex
	walWriter         *WALWriter
	stopChan          chan struct{}
	ReplicationFactor int
	lastPhysical      int64 // Track last physical time in nanoseconds
}

type KeyValue struct {
	Value       string
	Timestamp   uint64
	CreatedAt   int64
	IsTombstone bool
	TTL         uint64
}

// Snapshot represents the state at a point in time
type Snapshot struct {
	Store     map[string]*KeyValue
	Timestamp uint64
	Checksum  uint32
}

type WALEntry struct {
	Operation string // PUT or DELETE
	Key       string
	Value     string // Empty for DELETE
	Timestamp uint64
	CreatedAt int64
	Checksum  uint32
	TTL       uint64
}

func (e *WALEntry) computeChecksum() uint32 {
	h := fnv.New32a()
	h.Write([]byte(e.Operation))
	h.Write([]byte(e.Key))
	h.Write([]byte(e.Value))
	binary.Write(h, binary.BigEndian, e.Timestamp)
	binary.Write(h, binary.BigEndian, e.CreatedAt)
	return h.Sum32()
}

func (e *WALEntry) validate() bool {
	return e.Checksum == e.computeChecksum()
}

func (e *WALEntry) encode() string {
	e.Checksum = e.computeChecksum()
	return fmt.Sprintf("%s %s %s %d %d %d %d\n",
		e.Operation, e.Key, e.Value, e.Timestamp, e.CreatedAt, e.Checksum, e.TTL)
}

func parseWALEntry(line string) (*WALEntry, error) {
	parts := strings.Fields(line)
	if len(parts) < 7 { // Now expecting 7 parts
		return nil, fmt.Errorf("malformed WAL entry: %s", line)
	}

	timestamp, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}

	createdAt, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid createdAt: %v", err)
	}

	checksum, err := strconv.ParseUint(parts[5], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid checksum: %v", err)
	}

	ttl, err := strconv.ParseUint(parts[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid TTL: %v", err)
	}

	entry := &WALEntry{
		Operation: parts[0],
		Key:       parts[1],
		Value:     parts[2],
		Timestamp: timestamp,
		CreatedAt: createdAt,
		Checksum:  uint32(checksum),
		TTL:       ttl,
	}

	if !entry.validate() {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return entry, nil
}

func NewStorage(id uint32, replicationFactor ...int) *Storage {
	rf := 3 // Default replication factor
	if len(replicationFactor) > 0 && replicationFactor[0] > 0 {
		rf = replicationFactor[0]
	}

	walWriter, err := NewWALWriter(id)
	if err != nil {
		log.Fatalf("Failed to create WAL writer: %v", err)
	}

	n := &Storage{
		ID:                id,
		LogicalClock:      0,
		store:             make(map[string]*KeyValue),
		walWriter:         walWriter,
		stopChan:          make(chan struct{}),
		ReplicationFactor: rf,
	}

	// Initialize background tasks
	n.CheckWALSize()
	n.StartTTLCleanup() // Start TTL cleanup process

	// First recover from the latest snapshot, then apply any WAL entries after the snapshot
	if err := n.recoverFromSnapshot(); err != nil {
		log.Printf("Error recovering from snapshot: %v", err)
	}
	n.recoverFromWAL()

	return n
}

func (n *Storage) generateTimestamp() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Get current physical time in nanoseconds
	physical := timeNow().UnixNano()

	// If physical time hasn't advanced, increment logical clock
	if physical <= n.lastPhysical {
		n.LogicalClock++
		// Handle counter overflow
		if n.LogicalClock == 0 {
			// Force time advancement if counter overflows
			physical = n.lastPhysical + 1
		}
	} else {
		// Reset logical clock if physical time advanced
		n.LogicalClock = 0
	}

	n.lastPhysical = physical

	// Combine node ID (16 bits), physical time (32 bits), and logical counter (16 bits)
	return (uint64(n.ID) << 48) | (uint64(physical&0xFFFFFFFF) << 16) | uint64(n.LogicalClock&0xFFFF)
}

func (n *Storage) Store(key, value string, timestamp uint64, ttl ...uint64) error {
	defaultTTL := uint64(0)
	if len(ttl) > 0 {
		defaultTTL = ttl[0]
	}

	// If this is an update, write a DELETE entry for the old value first
	// if exists && !existingKV.IsTombstone {
	// 	deleteEntry := &WALEntry{
	// 		Operation: "DELETE",
	// 		Key:       key,
	// 		Timestamp: timestamp - 1, // Use slightly earlier timestamp for the delete
	// 		CreatedAt: timeNow().Unix(),
	// 		TTL:       0,
	// 	}

	// 	// Write the delete entry to WAL
	// 	if err := n.walWriter.Write(deleteEntry); err != nil {
	// 		log.Printf("WAL delete write failed: %v", err)
	// 		return err
	// 	}
	// }

	// Now write the new PUT entry
	entry := &WALEntry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		CreatedAt: timeNow().Unix(),
		TTL:       defaultTTL,
	}

	// Write to WAL
	if err := n.walWriter.Write(entry); err != nil {
		log.Printf("WAL write failed: %v", err)
		return err
	}

	// Update store only after successful WAL write
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	// Double-check if we should update (in case another thread updated while we were writing to WAL)
	existing, exists := n.store[key]
	if !exists || timestamp > existing.Timestamp {
		n.store[key] = &KeyValue{
			Value:     value,
			Timestamp: timestamp,
			CreatedAt: timeNow().Unix(),
			TTL:       defaultTTL,
		}
	}
	return nil
}

func (n *Storage) Delete(key string) error {
	timestamp := n.generateTimestamp()

	// Write tombstone to WAL first
	entry := &WALEntry{
		Operation: "DELETE",
		Key:       key,
		Timestamp: timestamp,
		CreatedAt: timeNow().Unix(),
		TTL:       86400, // Example TTL for tombstone
	}
	if err := n.walWriter.Write(entry); err != nil {
		log.Printf("WAL write failed: %v", err)
		return err
	}

	// Update store only after successful WAL write
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	n.store[key] = &KeyValue{
		Timestamp:   timestamp,
		CreatedAt:   timeNow().Unix(),
		IsTombstone: true,
		TTL:         86400,
	}
	return nil
}

func (n *Storage) Get(key string) (string, uint64, bool) {
	// Use a read lock for better concurrency
	n.storeMu.RLock()
	kv, exists := n.store[key]

	// If the key doesn't exist, we can return early
	if !exists {
		n.storeMu.RUnlock()
		return "", 0, false
	}

	// If the key exists but is a tombstone, return as not found
	if kv.IsTombstone {
		n.storeMu.RUnlock()
		return "", 0, false
	}

	// Copy values while under lock
	value := kv.Value
	timestamp := kv.Timestamp
	n.storeMu.RUnlock()

	return value, timestamp, true
}

// GetKeys returns all keys in the store
func (n *Storage) GetKeys() []string {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()

	keys := make([]string, 0, len(n.store))
	for k := range n.store {
		keys = append(keys, k)
	}
	return keys
}

func (n *Storage) cleanupSnapshots() error {
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

func (n *Storage) compactWAL() error {
	// Take a read lock for creating snapshot
	n.storeMu.RLock()
	snapshot := Snapshot{
		Store:     make(map[string]*KeyValue, len(n.store)),
		Timestamp: uint64(n.LogicalClock),
	}

	// Copy only non-tombstoned entries
	for k, v := range n.store {
		if !v.IsTombstone {
			snapshot.Store[k] = &KeyValue{
				Value:     v.Value,
				Timestamp: v.Timestamp,
				TTL:       v.TTL,
			}
		}
	}
	n.storeMu.RUnlock() // Release read lock after snapshot creation

	// Create snapshots directory if it doesn't exist
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
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
func (n *Storage) handleWALCompaction(snapshot *Snapshot) error {

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
func (n *Storage) writeSnapshotToFile(filename string, snapshot *Snapshot) error {
	// Compute checksum before writing
	snapshot.Checksum = snapshot.computeChecksum()

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

func (n *Storage) truncateWAL(timestamp uint64) error {
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

func (n *Storage) CheckWALSize() {
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

func (n *Storage) recoverFromWAL() error {
	// We already recovered from snapshot in NewStorage, so just use the current logical clock
	// as the snapshot timestamp
	snapshotTimestamp := uint64(n.LogicalClock)

	// Open WAL file
	walFile := fmt.Sprintf("wal-%d.log", n.ID)
	file, err := os.Open(walFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file is not an error
		}
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	// Use a more robust scanning approach
	scanner := bufio.NewScanner(file)
	entries := make([]*WALEntry, 0)

	// First pass: read all valid entries from WAL
	for scanner.Scan() {
		line := scanner.Text()
		entry, err := parseWALEntry(line)
		if err != nil {
			// Log error but continue processing other entries
			log.Printf("Skipping invalid WAL entry: %v", err)
			continue
		}

		// Skip entries covered by snapshot
		if entry.Timestamp <= snapshotTimestamp {
			continue
		}

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading WAL file: %v", err)
		// Continue with what we have - don't abort recovery
	}

	// Second pass: apply entries in timestamp order
	// Sort entries by timestamp (newer entries override older ones)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp < entries[j].Timestamp
	})

	// Group entries by key to find the latest for each key
	latestEntries := make(map[string]*WALEntry)
	for _, entry := range entries {
		existing, exists := latestEntries[entry.Key]
		if !exists || entry.Timestamp > existing.Timestamp {
			latestEntries[entry.Key] = entry
		}
	}

	// Apply the final batch
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	for _, entry := range latestEntries {
		switch entry.Operation {
		case "PUT":
			n.store[entry.Key] = &KeyValue{
				Value:     entry.Value,
				Timestamp: entry.Timestamp,
				CreatedAt: entry.CreatedAt,
				TTL:       entry.TTL,
			}
		case "DELETE":
			n.store[entry.Key] = &KeyValue{
				Timestamp:   entry.Timestamp,
				CreatedAt:   entry.CreatedAt,
				IsTombstone: true,
				TTL:         entry.TTL,
			}
		}
	}

	return nil
}

func (n *Storage) recoverFromSnapshot() error {
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
		return fmt.Errorf("failed to decode snapshot: %v", err)
	}

	// Verify checksum
	expectedChecksum := snapshot.Checksum
	snapshot.Checksum = 0 // Reset checksum for computation
	actualChecksum := snapshot.computeChecksum()

	if expectedChecksum != actualChecksum {
		return fmt.Errorf("snapshot checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}

	// Apply snapshot
	n.storeMu.Lock()
	n.store = snapshot.Store
	n.LogicalClock = uint32(snapshot.Timestamp & 0xFFFFFFFF)
	n.storeMu.Unlock()

	return nil
}

func (n *Storage) StartTTLCleanup() {
	ticker := time.NewTicker(1 * time.Hour)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.cleanupExpiredEntries()
			case <-n.stopChan:
				return
			}
		}
	}()
}

func (n *Storage) cleanupExpiredEntries() {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	now := timeNow().Unix()
	for key, kv := range n.store {
		// Check if TTL is set and if entry has expired
		if kv.TTL > 0 {
			// TTL is in seconds, so add it directly to the creation time
			expirationTime := kv.CreatedAt + int64(kv.TTL)
			if now > expirationTime {
				delete(n.store, key)
			}
		}
	}
}

func (s *Storage) Shutdown() error {
	log.Println("Shutting down storage...")

	// First close the WAL writer to ensure all entries are flushed
	if s.walWriter != nil {
		if err := s.walWriter.Close(); err != nil {
			log.Printf("Error closing WAL writer: %v", err)
			// Continue with shutdown even if there's an error
		}
	}

	// Then close the stop channel
	select {
	case <-s.stopChan:
		log.Println("Stop channel already closed")
	default:
		close(s.stopChan)
	}

	return nil
}

// Add method to compute snapshot checksum
func (s *Snapshot) computeChecksum() uint32 {
	h := fnv.New32a()
	// Hash timestamp
	binary.Write(h, binary.BigEndian, s.Timestamp)

	// Hash store entries in deterministic order
	keys := make([]string, 0, len(s.Store))
	for k := range s.Store {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := s.Store[k]
		h.Write([]byte(k))
		h.Write([]byte(v.Value))
		binary.Write(h, binary.BigEndian, v.Timestamp)
		binary.Write(h, binary.BigEndian, v.TTL)
		binary.Write(h, binary.BigEndian, bool2int(v.IsTombstone))
	}
	return h.Sum32()
}

// Helper function for checksum computation
func bool2int(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// Add this method to the Storage struct
func (n *Storage) GarbageCollect(key string) error {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	if _, exists := n.store[key]; exists {
		delete(n.store, key) // Remove the key from the store
	}
	return nil
}
