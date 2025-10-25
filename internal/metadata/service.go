package metadata

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/vivekumar08/go-s3-lite/internal/hashing"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
)

// Service implements the gRPC MetadataService defined in internal/pb/metadata.proto
type Service struct {
	pb.UnimplementedMetadataServiceServer
	store *NodeStore

	// optional: simple health/metrics counters
	mu            sync.Mutex
	registerCalls int
	getCalls      int
}

// NewService returns a Metadata service using given store
func NewService(store *NodeStore) *Service {
	return &Service{store: store}
}

// RegisterNode registers a new node and adds it to the ring
func (s *Service) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	s.mu.Lock()
	s.registerCalls++
	s.mu.Unlock()

	if req == nil || req.Node == nil {
		return &pb.RegisterNodeResponse{
			Success: false,
			Message: "missing node info",
		}, nil
	}

	nodeID := req.Node.Id
	addr := req.Node.Address

	// create hashing.Node and add it
	n := hashing.Node{
		ID:      nodeID,
		Address: addr,
	}

	err := s.store.AddNode(n)
	if err != nil {
		// If node exists, we can still accept (idempotent) — or return error depending on policy
		if err == ErrNodeExists {
			// idempotent success
			log.Printf("RegisterNode: node already exists: %s", nodeID)
			return &pb.RegisterNodeResponse{
				Success: true,
				Message: fmt.Sprintf("node %s already registered", nodeID),
			}, nil
		}
		return &pb.RegisterNodeResponse{
			Success: false,
			Message: fmt.Sprintf("failed to register node: %v", err),
		}, nil
	}

	log.Printf("RegisterNode: added node %s @ %s", nodeID, addr)
	return &pb.RegisterNodeResponse{
		Success: true,
		Message: "registered",
	}, nil
}

// GetNodeForFile returns the node responsible for storing a file key.
func (s *Service) GetNodeForFile(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	s.mu.Lock()
	s.getCalls++
	s.mu.Unlock()

	if req == nil || req.FileKey == "" {
		return &pb.GetNodeResponse{}, nil
	}

	// Use consistent hashing ring to pick node
	node := s.store.ResponsibleNode(req.FileKey)
	if node.ID == "" {
		return &pb.GetNodeResponse{}, nil
	}

	return &pb.GetNodeResponse{
		Node: &pb.NodeInfo{
			Id:      node.ID,
			Address: node.Address,
		},
	}, nil
}
