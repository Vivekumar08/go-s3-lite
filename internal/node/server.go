package node

import (
	"context"
	"fmt"

	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

type NodeServer struct {
	pb.UnimplementedNodeServiceServer
	storage *FileStorage
}

func NewNodeServer(basePath string) *NodeServer {
	return &NodeServer{
		storage: NewFileStorage(basePath),
	}
}

func (s *NodeServer) UploadFile(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	if req.FileKey == "" || len(req.Data) == 0 {
		return &pb.UploadResponse{
			Success: false,
			Message: "invalid file key or data",
		}, nil
	}

	err := s.storage.SaveFile(req.FileKey, req.Data)
	if err != nil {
		return &pb.UploadResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save file: %v", err),
		}, nil
	}

	return &pb.UploadResponse{
		Success: true,
		Message: "File uploaded successfully",
	}, nil
}

// DownloadFile streams file data to client
func (s *NodeServer) DownloadFile(req *pb.DownloadRequest, stream grpc.ServerStreamingServer[pb.DownloadResponse]) error {
	data, err := s.storage.ReadFile(req.Filename)
	if err != nil {
		return err
	}

	chunk := pb.DownloadResponse{Data: data}
	if err := stream.Send(&chunk); err != nil {
		return err
	}
	return nil
}
