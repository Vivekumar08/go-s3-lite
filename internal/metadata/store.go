package metadata

import (
	"errors"
	"sync"

	"github.com/vivekumar08/go-s3-lite/internal/hashing"
)

// ErrNodeExists returned when trying to add a node that already exists.
var ErrNodeExists = errors.New("node already exists")

// NodeStore is a threadsafe in-memory registry of active nodes.
type NodeStore struct {
	mu    sync.RWMutex
	nodes map[string]hashing.Node // key: node ID
	ring  *hashing.Ring
}

// NewNodeStore creates a new NodeStore with a hashing ring.
// replication parameter is forwarded to the ring constructor.
func NewNodeStore(replication int) *NodeStore {
	return &NodeStore{
		nodes: make(map[string]hashing.Node),
		ring:  hashing.NewRing(replication),
	}
}

// AddNode adds node to the store and ring
func (s *NodeStore) AddNode(n hashing.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[n.ID]; ok {
		return ErrNodeExists
	}
	s.nodes[n.ID] = n
	s.ring.AddNode(n)
	return nil
}

// GetNode returns a node by ID
func (s *NodeStore) GetNode(id string) (hashing.Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	return n, ok
}

// GetNodes returns a snapshot of all nodes
func (s *NodeStore) GetNodes() []hashing.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]hashing.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	return out
}

// ResponsibleNode returns the node responsible for the given key (via ring)
func (s *NodeStore) ResponsibleNode(key string) hashing.Node {
	// ring.GetNode uses its own internal lock
	return s.ring.GetNode(key)
}
