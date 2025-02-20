package consistenthash

import (
	"fmt"
	"hash/fnv"
	"sort"
)

const (
	DefaultVirtualNodes = 10
)

// Ring implements consistent hashing ring
type Ring struct {
	hashRing   []uint32            // Sorted hash values
	mapping    map[uint32]string   // Hash to node mapping
	vnodes     map[string][]uint32 // Node to virtual nodes mapping
	vnodeCount int                 // Number of virtual nodes per physical node
}

// NewRing creates a new consistent hashing ring
func NewRing(vnodeCount int) *Ring {
	return &Ring{
		hashRing:   make([]uint32, 0),
		mapping:    make(map[uint32]string),
		vnodes:     make(map[string][]uint32),
		vnodeCount: vnodeCount,
	}
}

// AddNode adds a node and its virtual nodes to the ring
func (r *Ring) AddNode(node string) {
	r.vnodes[node] = make([]uint32, 0, r.vnodeCount)

	for i := 0; i < r.vnodeCount; i++ {
		vnode := fmt.Sprintf("%s-%d", node, i)
		hash := HashString(vnode)

		// Insert hash into sorted ring
		idx := sort.Search(len(r.hashRing), func(i int) bool {
			return r.hashRing[i] >= hash
		})
		r.hashRing = append(r.hashRing, 0)
		copy(r.hashRing[idx+1:], r.hashRing[idx:])
		r.hashRing[idx] = hash

		// Update mappings
		r.mapping[hash] = node
		r.vnodes[node] = append(r.vnodes[node], hash)
	}
}

// RemoveNode removes a node and its virtual nodes from the ring
func (r *Ring) RemoveNode(node string) {
	if vnodes, exists := r.vnodes[node]; exists {
		for _, hash := range vnodes {
			delete(r.mapping, hash)
			for i, h := range r.hashRing {
				if h == hash {
					r.hashRing = append(r.hashRing[:i], r.hashRing[i+1:]...)
					break
				}
			}
		}
		delete(r.vnodes, node)
	}
}

// GetNode finds the node responsible for a key
func (r *Ring) GetNode(key string) string {
	if len(r.hashRing) == 0 {
		return ""
	}

	hash := HashString(key)
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})

	if idx == len(r.hashRing) {
		idx = 0
	}

	return r.mapping[r.hashRing[idx]]
}

// GetNextNode returns the next node in the ring after the given hash
func (r *Ring) GetNextNode(hash uint32) string {
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] > hash
	})
	if idx == len(r.hashRing) {
		idx = 0
	}
	return r.mapping[r.hashRing[idx]]
}

// GetNodeHash returns the hash value for a key
func (r *Ring) GetNodeHash(key string) uint32 {
	return HashString(key)
}

// HashString generates a hash for a string using FNV-1a
func HashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
