package metadata

import (
	"context"
	"log"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/hashing"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	utils "github.com/vivekumar08/go-s3-lite/internal/utils"
	"gorm.io/gorm"
)

type Service struct {
	pb.UnimplementedMetadataServiceServer
	db             *gorm.DB
	store          *NodeStore
	defaultReplica int
}

func NewService(db *gorm.DB, store *NodeStore, defaultReplica int) *Service {
	return &Service{db: db, store: store, defaultReplica: defaultReplica}
}

func (s *Service) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if req == nil || req.Node == nil {
		return &pb.RegisterNodeResponse{Success: false, Message: "missing node"}, nil
	}
	n := hashing.Node{ID: req.Node.Id, Address: req.Node.Address}
	if err := s.store.AddNode(n); err != nil {
		log.Printf("RegisterNode: add error: %v", err)
		return &pb.RegisterNodeResponse{Success: false, Message: err.Error()}, nil
	}
	// also ensure last seen updated
	_ = s.store.UpdateLastSeen(n.ID)
	return &pb.RegisterNodeResponse{Success: true, Message: "registered"}, nil
}

func (s *Service) GetReplicasForFile(ctx context.Context, req *pb.GetReplicasRequest) (*pb.GetReplicasResponse, error) {
	if req == nil || req.FileKey == "" {
		return &pb.GetReplicasResponse{}, nil
	}
	rep := int(req.Replicas)
	if rep <= 0 {
		rep = s.defaultReplica
	}
	nodes := s.store.ResponsibleNodes(req.FileKey, rep)
	// persist mapping
	nodeIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		nodeIDs = append(nodeIDs, n.ID)
	}
	m := FileMapping{Key: req.FileKey, Replicas: utils.JoinCSV(nodeIDs)}
	// save mapping (upsert)
	if err := s.db.Save(&m).Error; err != nil {
		log.Printf("warning: failed to persist mapping: %v", err)
	}
	resp := &pb.GetReplicasResponse{}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &pb.NodeInfo{Id: n.ID, Address: n.Address})
	}
	return resp, nil
}

func (s *Service) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodes := s.store.AllNodes()
	resp := &pb.ListNodesResponse{}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &pb.NodeInfo{Id: n.ID, Address: n.Address})
	}
	return resp, nil
}

func (s *Service) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req == nil || req.NodeId == "" {
		return &pb.HeartbeatResponse{Ok: false}, nil
	}
	if err := s.store.UpdateLastSeen(req.NodeId); err != nil {
		log.Printf("heartbeat update failed: %v", err)
		return &pb.HeartbeatResponse{Ok: false}, nil
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// Background worker: remove dead nodes and trigger simple re-replication
func (s *Service) RunBackgroundWorkers(stopCh <-chan struct{}, checkInterval time.Duration, ttl time.Duration) {
	ticker := time.NewTicker(checkInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.checkAndRepair(ttl)
			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *Service) checkAndRepair(ttl time.Duration) {
	// find nodes with last_seen older than ttl
	var dead []NodeModel
	cutoff := time.Now().Add(-ttl)
	if err := s.db.Where("last_seen < ?", cutoff).Find(&dead).Error; err != nil {
		return
	}
	for _, dm := range dead {
		log.Printf("Detected dead node: %s, removing from ring", dm.ID)
		_ = s.store.RemoveNode(dm.ID)
		// re-replicate files referencing this node
		s.replicateFilesFrom(dm.ID)
	}
}

func (s *Service) replicateFilesFrom(deadNodeID string) {
	// find file mappings that include deadNodeID
	var mappings []FileMapping
	if err := s.db.Find(&mappings).Error; err != nil {
		return
	}
	for _, m := range mappings {
		repIDs := utils.SplitCSV(m.Replicas)
		contains := false
		for _, id := range repIDs {
			if id == deadNodeID {
				contains = true
				break
			}
		}
		if !contains {
			continue
		}
		// compute replacement replicas
		targets := s.store.ResponsibleNodes(m.Key, s.defaultReplica)
		newIDs := []string{}
		for _, t := range targets {
			newIDs = append(newIDs, t.ID)
		}
		m.Replicas = utils.JoinCSV(newIDs)
		_ = s.db.Save(&m) // update mapping; actual data copying between nodes left as TODO or implemented by separate job
		log.Printf("Updated replicas for %s => %s", m.Key, m.Replicas)
	}
}
