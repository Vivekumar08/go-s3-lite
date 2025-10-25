package node

import (
	"fmt"
	"io"
	"log"

	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
)

type NodeServer struct {
	pb.UnimplementedNodeServiceServer
	storage *FileStorage
}

// NewNodeServer creates a new NodeServer
func NewNodeServer(storage *FileStorage) *NodeServer {
	return &NodeServer{storage: storage}
}

// UploadFile receives a stream of file chunks
func (s *NodeServer) UploadFile(stream pb.NodeService_UploadFileServer) error {
	var filename string
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// finished receiving
			log.Printf("Upload finished: %s", filename)
			return stream.SendAndClose(&pb.UploadResponse{
				Success: true,
				Message: "uploaded",
			})
		}
		if err != nil {
			return err
		}

		filename = chunk.Filename
		if err := s.storage.SaveFile(chunk.Filename, chunk.Data); err != nil {
			return fmt.Errorf("failed to save file: %v", err)
		}
	}
}

// DownloadFile streams file data to client
func (s *NodeServer) DownloadFile(req *pb.DownloadRequest, stream pb.NodeService_DownloadFileServer) error {
	data, err := s.storage.ReadFile(req.Filename)
	if err != nil {
		return err
	}

	chunk := &pb.DownloadResponse{Data: data}
	if err := stream.Send(chunk); err != nil {
		return err
	}
	return nil
}
