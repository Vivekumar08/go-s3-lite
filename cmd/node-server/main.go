package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/node"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

func main() {
	nodeID := flag.String("id", "node-1", "node id")
	addr := flag.String("addr", "127.0.0.1:6000", "node listen address")
	metadataAddr := flag.String("metadata", "127.0.0.1:50051", "metadata addr")
	dataDir := flag.String("data", "./data", "data directory")
	flag.Parse()

	// register with metadata
	metaConn, err := grpc.Dial(*metadataAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial metadata: %v", err)
	}
	metaClient := pb.NewMetadataServiceClient(metaConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = metaClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		Node: &pb.NodeInfo{Id: *nodeID, Address: *addr},
	})
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	fmt.Printf("registered node %s\n", *nodeID)

	// start heartbeat goroutine
	go func() {
		t := time.NewTicker(5 * time.Second)
		for range t.C {
			_, _ = metaClient.Heartbeat(context.Background(), &pb.HeartbeatRequest{NodeId: *nodeID})
		}
	}()

	// start node gRPC server
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, node.NewNodeServer(*dataDir))

	fmt.Printf("node server listening on %s\n", *addr)
	fmt.Printf("data directory: %s\n", *dataDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
