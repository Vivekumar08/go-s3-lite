package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
)

// NodeService implements NodeService gRPC
type NodeService struct {
	pb.UnimplementedNodeServiceServer
	dataDir string
	mu      sync.Mutex
}

// NewNodeService creates a new NodeService
func NewNodeService(dataDir string) *NodeService {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.MkdirAll(dataDir, 0755)
	}
	return &NodeService{
		dataDir: dataDir,
	}
}

// UploadFile stores the file on disk
func (s *NodeService) UploadFileStream(ctx context.Context, req *pb.FileChunk) (*pb.UploadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Filename == "" || len(req.Data) == 0 {
		return &pb.UploadResponse{Success: false, Message: "invalid file"}, nil
	}

	filePath := fmt.Sprintf("%s/%s", s.dataDir, req.Filename)
	if err := ioutil.WriteFile(filePath, req.Data, 0644); err != nil {
		log.Printf("UploadFile error: %v", err)
		return &pb.UploadResponse{Success: false, Message: err.Error()}, nil
	}

	log.Printf("Uploaded file: %s", filePath)
	return &pb.UploadResponse{Success: true, Message: "uploaded successfully"}, nil
}

// DownloadFile returns the file content
func (s *NodeService) DownloadFileStream(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Filename == "" {
		return nil, fmt.Errorf("filename required")
	}

	filePath := fmt.Sprintf("%s/%s", s.dataDir, req.Filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}

	return &pb.DownloadResponse{Data: data}, nil
}
