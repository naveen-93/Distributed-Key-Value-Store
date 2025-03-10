package consistenthash

import (
	"fmt"
	"hash"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

const (
	DefaultVirtualNodes   = 10
	MaxAttemptsMultiplier = 100 // Increased multiplier for more collision attempts
)

// Ring represents a consistent hash ring
type Ring struct {
	sync.RWMutex
	hashRing   []uint32            // Sorted list of hashes
	mapping    map[uint32]string   // Hash to node mapping
	vnodes     map[string][]uint32 // Node to virtual nodes mapping
	vnodeCount int                 // Number of virtual nodes per node
	hashFunc   hash.Hash32         // Hash function
	randSource *rand.Rand          // Random source for unique suffixes
}

// NewRing creates a new consistent hashing ring
func NewRing(vnodeCount int) *Ring {
	if vnodeCount <= 0 {
		vnodeCount = DefaultVirtualNodes
	}
	return &Ring{
		hashRing:   make([]uint32, 0),
		mapping:    make(map[uint32]string),
		vnodes:     make(map[string][]uint32),
		vnodeCount: vnodeCount,
		hashFunc:   murmur3.New32(),
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
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
	attempts := 0
	collisions := 0
	maxAttempts := r.vnodeCount * MaxAttemptsMultiplier

	for i := 0; i < r.vnodeCount && attempts < maxAttempts; {
		// Generate unique suffix using node, index, and random component
		uniqueSuffix := r.randSource.Intn(1000000)
		vnodeKey := fmt.Sprintf("%s-vnode-%d-%d", node, i, uniqueSuffix)
		hash := r.HashKey(vnodeKey)

		if _, exists := r.mapping[hash]; exists {
			collisions++
			attempts++
			continue
		}

		hashes = append(hashes, hash)
		r.mapping[hash] = node
		i++
		attempts++
	}

	if collisions > 0 {
		fmt.Printf("Warning: Node %s experienced %d hash collisions while adding virtual nodes\n", node, collisions)
	}

	if len(hashes) < r.vnodeCount {
		fmt.Printf("Warning: Node %s could only add %d/%d virtual nodes due to excessive collisions\n",
			node, len(hashes), r.vnodeCount)
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

		newRing := make([]uint32, 0, len(r.hashRing)-len(vnodes))
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

// HashKey hashes a key to a position on the ring
func (r *Ring) HashKey(key string) uint32 {
	// Handle empty keys to prevent nil pointer dereference
	if key == "" {
		return 0
	}

	h := murmur3.New32()
	h.Write([]byte(key))
	return h.Sum32()
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

// GetReplicas returns the n replicas for a given key
func (r *Ring) GetReplicas(key string, n int) []string {
	r.RLock()
	defer r.RUnlock()

	if len(r.hashRing) == 0 {
		return nil
	}

	// Get unique nodes if available
	uniqueNodeCount := len(r.vnodes)
	if uniqueNodeCount == 0 {
		// Count unique nodes manually if vnodes map isn't maintained
		uniqueSet := make(map[string]bool)
		for _, nodeID := range r.mapping {
			uniqueSet[nodeID] = true
		}
		uniqueNodeCount = len(uniqueSet)
	}

	// Can't have more replicas than unique nodes
	if n > uniqueNodeCount {
		n = uniqueNodeCount
	}

	// If we only need one replica, we can optimize by just returning the primary node
	if n == 1 {
		hash := r.HashKey(key)
		idx := r.search(hash)
		if idx >= len(r.hashRing) {
			idx = 0
		}
		return []string{r.mapping[r.hashRing[idx]]}
	}

	// For small n, preallocate the exact size
	replicas := make([]string, 0, n)
	seenNodes := make(map[string]bool, n)

	hash := r.HashKey(key)

	// Find the starting point in the ring
	startIdx := r.search(hash)
	if startIdx >= len(r.hashRing) {
		startIdx = 0
	}

	// Collect n unique nodes, starting from the position after hash
	idx := startIdx
	for len(replicas) < n {
		if idx >= len(r.hashRing) {
			idx = 0 // Wrap around
		}

		nodeHash := r.hashRing[idx]
		nodeID := r.mapping[nodeHash]

		if !seenNodes[nodeID] {
			replicas = append(replicas, nodeID)
			seenNodes[nodeID] = true
		}

		idx++

		// Break if we've checked all nodes in the ring
		if idx == startIdx {
			break
		}
	}

	return replicas
}

// Add this helper method after the existing GetReplicas method
func (r *Ring) search(hash uint32) int {
	return sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})
}

func HashString(s string) uint32 {
	h := murmur3.New32()
	h.Write([]byte(s))
	return h.Sum32()
}

// GetNodes returns n nodes for a given key
func (r *Ring) GetNodes(key string, n int) []string {
	r.RLock()
	defer r.RUnlock()

	if len(r.hashRing) == 0 {
		return nil
	}

	hash := r.HashKey(key)
	idx := r.search(hash)

	nodes := make([]string, 0, n)
	seen := make(map[string]bool)

	for i := 0; len(nodes) < n && i < len(r.hashRing); i++ {
		nodeIdx := (idx + i) % len(r.hashRing)
		nodeID := r.mapping[r.hashRing[nodeIdx]]

		if !seen[nodeID] {
			nodes = append(nodes, nodeID)
			seen[nodeID] = true
		}
	}

	return nodes
}
