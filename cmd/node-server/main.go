package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/node"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

func main() {
	// Node configuration (in real system, read from env/config)
	nodeID := "node-1"
	nodeAddress := "127.0.0.1:6000"
	metadataAddr := "127.0.0.1:50051"
	storagePath := "./data/node-1"

	// Connect to metadata server
	conn, err := grpc.Dial(metadataAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect to metadata server: %v", err)
	}
	defer conn.Close()

	metaClient := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	regResp, err := metaClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		Node: &pb.NodeInfo{
			Id:      nodeID,
			Address: nodeAddress,
		},
	})

	if err != nil || !regResp.Success {
		log.Fatalf("node registration failed: %v", err)
	}
	fmt.Printf("✅ Node registered successfully: %s\n", nodeID)

	// --- Start Node gRPC server ---
	fs, err := node.NewFileStorage(storagePath)
	if err != nil {
		log.Fatalf("failed to init storage: %v", err)
	}

	ns := node.NewNodeServer(fs)
	lis, err := net.Listen("tcp", nodeAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, ns)
	fmt.Printf("Node server listening on %s\n", nodeAddress)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc serve failed: %v", err)
	}
}
