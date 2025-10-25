package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/vivekumar08/go-s3-lite/internal/metadata"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

func main() {
	var (
		port        = flag.Int("port", 50051, "metadata gRPC server port")
		replication = flag.Int("replication", 3, "virtual nodes / replication factor for hashing ring")
	)
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	store := metadata.NewNodeStore(*replication)
	svc := metadata.NewService(store)

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, svc)

	log.Printf("Metadata server listening on %s (replication=%d)", addr, *replication)

	// graceful shutdown handling
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC serve failed: %v", err)
		}
	}()

	// wait for signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	log.Printf("shutdown requested, stopping gRPC server...")
	grpcServer.GracefulStop()
}
