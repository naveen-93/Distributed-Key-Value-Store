package node

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Node struct {
	ID           uint32
	LogicalClock uint64
	mu           sync.RWMutex
	store        map[string]*KeyValue
	storeMu      sync.RWMutex
	wal          *os.File
}

type KeyValue struct {
	Value     string
	Timestamp uint64
}

func NewNode(id uint32) *Node {
	f, err := os.OpenFile(fmt.Sprintf("wal-%d.log", id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	n := &Node{
		ID:    id,
		store: make(map[string]*KeyValue),
		wal:   f,
	}
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

	// Log to WAL
	_, err := n.wal.WriteString(fmt.Sprintf("PUT %s %s %d\n", key, value, timestamp))
	if err != nil {
		log.Printf("WAL write failed: %v", err)
		return
	}
	n.wal.Sync()

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
func (n *Node) recoverFromWAL() {
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

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Parse WAL entry
		parts := strings.Fields(line)
		if len(parts) < 4 {
			log.Printf("Warning: malformed WAL entry at line %d: %s", lineNum, line)
			continue
		}

		op, key := parts[0], parts[1]
		switch op {
		case "PUT":
			if len(parts) < 4 {
				log.Printf("Warning: invalid PUT entry at line %d: %s", lineNum, line)
				continue
			}
			value := parts[2]
			timestamp, err := strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				log.Printf("Warning: invalid timestamp at line %d: %v", lineNum, err)
				continue
			}

			// Update batch with latest value
			if existing, exists := batch[key]; !exists || timestamp > existing.Timestamp {
				batch[key] = &KeyValue{
					Value:     value,
					Timestamp: timestamp,
				}
			}

		case "DELETE":
			// Handle DELETE operations if supported
			timestamp, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				log.Printf("Warning: invalid timestamp at line %d: %v", lineNum, err)
				continue
			}
			if existing, exists := batch[key]; !exists || timestamp > existing.Timestamp {
				batch[key] = nil // Mark for deletion
			}

		default:
			log.Printf("Warning: unknown operation at line %d: %s", lineNum, op)
			continue
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

	log.Printf("Node %d: Recovered %d entries from WAL", n.ID, lineNum)
}

func (n *Node) applyWALBatch(batch map[string]*KeyValue) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	for key, kv := range batch {
		if kv == nil {
			// Handle deletion
			delete(n.store, key)
		} else {
			// Apply PUT operation
			if existing, exists := n.store[key]; !exists || kv.Timestamp > existing.Timestamp {
				n.store[key] = kv
			}
		}
	}
}
