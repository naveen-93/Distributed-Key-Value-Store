package consistenthash

import (
	"fmt"
	"hash"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

const DefaultVirtualNodes = 10

// Ring represents a consistent hash ring
type Ring struct {
	sync.RWMutex
	hashRing   []uint32            // Sorted list of hashes
	mapping    map[uint32]string   // Hash to node mapping
	vnodes     map[string][]uint32 // Node to virtual nodes mapping
	vnodeCount int                 // Number of virtual nodes per node
	hashFunc   hash.Hash32         // Hash function
}

// NewRing creates a new consistent hashing ring
func NewRing(vnodeCount int) *Ring {
	return &Ring{
		hashRing:   make([]uint32, 0),
		mapping:    make(map[uint32]string),
		vnodes:     make(map[string][]uint32),
		vnodeCount: vnodeCount,
		hashFunc:   murmur3.New32(),
	}
}

// AddNode adds a node and its virtual nodes to the ring
func (r *Ring) AddNode(node string) {
	r.Lock()
	defer r.Unlock()

	if _, exists := r.vnodes[node]; exists {
		return // Node already exists
	}

	hashes := make([]uint32, 0, r.vnodeCount)
	for i := 0; i < r.vnodeCount; i++ {
		vnode := fmt.Sprintf("%s-%d", node, i)
		hash := r.HashKey(vnode)

		// Check if the hash already exists in the mapping
		if _, exists := r.mapping[hash]; !exists {
			hashes = append(hashes, hash)
			r.mapping[hash] = node
		}
	}

	r.vnodes[node] = hashes
	r.hashRing = append(r.hashRing, hashes...)
	sort.Slice(r.hashRing, func(i, j int) bool {
		return r.hashRing[i] < r.hashRing[j]
	})
}

// RemoveNode removes a node and its virtual nodes from the ring
func (r *Ring) RemoveNode(node string) {
	r.Lock()
	defer r.Unlock()

	if vnodes, exists := r.vnodes[node]; exists {
		hashSet := make(map[uint32]struct{}, len(vnodes))
		for _, h := range vnodes {
			delete(r.mapping, h)
			hashSet[h] = struct{}{}
		}

		// Efficiently remove virtual nodes from the sorted ring
		newRing := r.hashRing[:0]
		for _, h := range r.hashRing {
			if _, found := hashSet[h]; !found {
				newRing = append(newRing, h)
			}
		}
		r.hashRing = newRing
		delete(r.vnodes, node)
	}
}

// GetNode finds the node responsible for a given key
func (r *Ring) GetNode(key string) string {
	r.RLock()
	defer r.RUnlock()

	if len(r.hashRing) == 0 {
		return ""
	}

	hash := r.HashKey(key)
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})

	if idx == len(r.hashRing) {
		idx = 0
	}

	return r.mapping[r.hashRing[idx]]
}

// GetNextNode returns the next node in the ring
func (r *Ring) GetNextNode(hash uint32) string {
	r.RLock()
	defer r.RUnlock()

	if len(r.hashRing) == 0 {
		return ""
	}

	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] > hash
	})

	if idx == len(r.hashRing) {
		idx = 0
	}

	return r.mapping[r.hashRing[idx]]
}

// HashKey generates a hash for a string using MurmurHash3
func (r *Ring) HashKey(s string) uint32 {
	r.hashFunc.Reset()
	r.hashFunc.Write([]byte(s))
	return r.hashFunc.Sum32()
}

// DisplayRing prints the hash ring for debugging
func (r *Ring) DisplayRing() {
	r.RLock()
	defer r.RUnlock()

	fmt.Println("Hash Ring:")
	for _, hash := range r.hashRing {
		fmt.Printf("Hash: %d -> Node: %s\n", hash, r.mapping[hash])
	}
}

// GetReplicas returns up to N unique responsible nodes for a key
func (r *Ring) GetReplicas(key string, count int) []string {
	r.RLock()
	defer r.RUnlock()

	if len(r.hashRing) == 0 || count < 1 {
		return nil
	}

	hash := r.HashKey(key)
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})

	if idx == len(r.hashRing) {
		idx = 0
	}

	replicas := make([]string, 0, count)
	seen := make(map[string]struct{})

	steps := 0
	for len(replicas) < count && steps < len(r.hashRing) {
		node := r.mapping[r.hashRing[idx]]
		if _, exists := seen[node]; !exists {
			replicas = append(replicas, node)
			seen[node] = struct{}{}
		}

		idx = (idx + 1) % len(r.hashRing)
		steps++
	}

	return replicas
}
