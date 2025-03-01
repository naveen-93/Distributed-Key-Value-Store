package consistenthash

import (
	"fmt"
	"hash"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
	"strings"
	"strconv"
)

const (
	DefaultVirtualNodes   = 10
	MaxAttemptsMultiplier = 100 // Increased multiplier for more collision attempts
)
// NodeID represents the format of a node identifier
const NodeIDPrefix = "node-"

// GetNodeID extracts the node ID from the full identifier
func GetNodeID(node string) (uint32, error) {
    if !strings.HasPrefix(node, NodeIDPrefix) {
        return 0, fmt.Errorf("invalid node identifier format: %s", node)
    }
    idStr := strings.TrimPrefix(node, NodeIDPrefix)
    id, err := strconv.ParseUint(idStr, 10, 32)
    if err != nil {
        return 0, fmt.Errorf("failed to parse node ID: %v", err)
    }
    return uint32(id), nil
}
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
        // Consider increasing the hash space or using a different strategy here
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

	uniqueNodes := len(r.vnodes)
	if count > uniqueNodes {
		count = uniqueNodes
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

	for len(replicas) < count {
		node := r.mapping[r.hashRing[idx]]
		if _, exists := seen[node]; !exists {
			replicas = append(replicas, node)
			seen[node] = struct{}{}
		}

		idx = (idx + 1) % len(r.hashRing)

		// Break early if all nodes are exhausted
		if len(seen) == uniqueNodes {
			break
		}
	}

	return replicas
}

func HashString(s string) uint32 {
	h := murmur3.New32()
	h.Write([]byte(s))
	return h.Sum32()
}
