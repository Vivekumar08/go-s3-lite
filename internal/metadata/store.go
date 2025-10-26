package metadata

import (
	"sync"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/hashing"
	"gorm.io/gorm"
)

type NodeStore struct {
	mu   sync.RWMutex
	db   *gorm.DB
	ring *hashing.Ring
}

func NewNodeStoreWithDB(db *gorm.DB, replication int) *NodeStore {
	ns := &NodeStore{db: db, ring: hashing.NewRing(replication)}
	// load active nodes from DB to ring
	var models []NodeModel
	_ = db.Find(&models).Error
	for _, m := range models {
		ns.ring.AddNode(hashing.Node{ID: m.ID, Address: m.Address})
	}
	return ns
}

func (s *NodeStore) AddNode(n hashing.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// upsert node model
	now := time.Now()
	model := NodeModel{ID: n.ID, Address: n.Address, LastSeen: now}
	// GORM upsert via Save (works since primary key provided)
	if err := s.db.Save(&model).Error; err != nil {
		return err
	}
	s.ring.AddNode(n)
	return nil
}

func (s *NodeStore) AllNodes() []hashing.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var models []NodeModel
	_ = s.db.Find(&models).Error
	out := make([]hashing.Node, 0, len(models))
	for _, m := range models {
		out = append(out, hashing.Node{ID: m.ID, Address: m.Address})
	}
	return out
}

func (s *NodeStore) ResponsibleNodes(key string, count int) []hashing.Node {
	return s.ring.GetReplicas(key, count)
}

func (s *NodeStore) UpdateLastSeen(nodeID string) error {
	return s.db.Model(&NodeModel{}).Where("id = ?", nodeID).Update("last_seen", time.Now()).Error
}

func (s *NodeStore) RemoveNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// remove from DB (soft delete)
	if err := s.db.Delete(&NodeModel{ID: nodeID}).Error; err != nil {
		return err
	}
	// rebuild ring from DB to drop node reliably
	var models []NodeModel
	_ = s.db.Find(&models).Error
	s.ring = hashing.NewRing(100)
	for _, m := range models {
		s.ring.AddNode(hashing.Node{ID: m.ID, Address: m.Address})
	}
	return nil
}
