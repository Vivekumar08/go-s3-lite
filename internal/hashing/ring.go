package hashing

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"sync"
)

// Node represents a node in the ring.
type Node struct {
	ID      string
	Address string
}

// Ring implements consistent hashing
type Ring struct {
	nodes       map[uint32]Node
	sortedKeys  []uint32
	mu          sync.RWMutex
	replication int
}

// NewRing initializes a new ring
func NewRing(replication int) *Ring {
	return &Ring{
		nodes:       make(map[uint32]Node),
		sortedKeys:  []uint32{},
		replication: replication,
	}
}

func (r *Ring) hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	bs := h.Sum(nil)
	return (uint32(bs[0])<<24 | uint32(bs[1])<<16 | uint32(bs[2])<<8 | uint32(bs[3]))
}

// AddNode adds a new node to the ring
func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash := r.hashKey(node.ID)
	r.nodes[hash] = node
	r.sortedKeys = append(r.sortedKeys, hash)
	sort.Slice(r.sortedKeys, func(i, j int) bool {
		return r.sortedKeys[i] < r.sortedKeys[j]
	})
	fmt.Printf("Node added: %s (%s)\n", node.ID, node.Address)
}

// GetNode returns the node responsible for the given key
func (r *Ring) GetNode(key string) Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return Node{}
	}

	hash := r.hashKey(key)
	idx := sort.Search(len(r.sortedKeys), func(i int) bool {
		return r.sortedKeys[i] >= hash
	})

	if idx == len(r.sortedKeys) {
		idx = 0
	}

	return r.nodes[r.sortedKeys[idx]]
}
