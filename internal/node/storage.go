package node

import (
	"sync"
)

type Node struct {
	ID           uint32
	LogicalClock uint64
	mu           sync.RWMutex
	store        map[string]*KeyValue
	storeMu      sync.RWMutex
}

type KeyValue struct {
	Value     string
	Timestamp uint64
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

	existing, exists := n.store[key]
	if !exists || timestamp > existing.Timestamp {
		n.store[key] = &KeyValue{
			Value:     value,
			Timestamp: timestamp,
		}
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
