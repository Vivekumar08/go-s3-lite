package hashing

import (
	"crypto/sha1"
	"sort"
	"sync"
)

type Node struct {
	ID      string
	Address string
}

type Ring struct {
	mu          sync.RWMutex
	nodes       map[uint32]Node
	sortedKeys  []uint32
	replication int
}

func NewRing(replication int) *Ring {
	if replication <= 0 {
		replication = 100
	}
	return &Ring{
		nodes:       make(map[uint32]Node),
		sortedKeys:  []uint32{},
		replication: replication,
	}
}

func (r *Ring) hashKey(key string) uint32 {
	h := sha1.Sum([]byte(key))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hash := r.hashKey(node.ID)
	if _, ok := r.nodes[hash]; !ok {
		r.nodes[hash] = node
		r.sortedKeys = append(r.sortedKeys, hash)
		sort.Slice(r.sortedKeys, func(i, j int) bool {
			return r.sortedKeys[i] < r.sortedKeys[j]
		})
	}
}

// GetNode returns single node for key
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

// GetReplicas returns up to count unique nodes clockwise on the ring
func (r *Ring) GetReplicas(key string, count int) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []Node{}
	if len(r.sortedKeys) == 0 || count <= 0 {
		return out
	}
	hash := r.hashKey(key)
	idx := sort.Search(len(r.sortedKeys), func(i int) bool {
		return r.sortedKeys[i] >= hash
	})
	seen := make(map[string]bool)
	for i := 0; len(out) < count && i < len(r.sortedKeys); i++ {
		pos := (idx + i) % len(r.sortedKeys)
		n := r.nodes[r.sortedKeys[pos]]
		if !seen[n.ID] {
			out = append(out, n)
			seen[n.ID] = true
		}
	}
	return out
}

// Nodes returns snapshot of nodes currently in ring
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.sortedKeys))
	for _, k := range r.sortedKeys {
		out = append(out, r.nodes[k])
	}
	return out
}
