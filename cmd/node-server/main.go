package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

func main() {
	// Node configuration (in real system, read from env/config)
	nodeID := "node-1"
	nodeAddress := "127.0.0.1:6000"
	metadataAddr := "127.0.0.1:50051"

	// Connect to metadata server
	conn, err := grpc.Dial(metadataAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect to metadata server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)

	// Prepare registration request
	req := &pb.RegisterNodeRequest{
		Node: &pb.NodeInfo{
			Id:      nodeID,
			Address: nodeAddress,
		},
	}

	// Send registration
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.RegisterNode(ctx, req)
	if err != nil {
		log.Fatalf("node registration failed: %v", err)
	}

	if res.Success {
		fmt.Printf("✅ Node registered successfully with metadata server at %s\n", metadataAddr)
	} else {
		fmt.Println("❌ Registration failed, retrying...")
		// You could implement retry logic here
	}
}
