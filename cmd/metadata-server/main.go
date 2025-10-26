package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vivekumar08/go-s3-lite/internal/metadata"
	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := flag.Int("port", 50051, "metadata port")
	dbPath := flag.String("db", "./meta.db", "sqlite db path")
	rep := flag.Int("replication", 3, "default replication factor")
	flag.Parse()

	db, err := metadata.OpenDB(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	store := metadata.NewNodeStoreWithDB(db, 100)
	svc := metadata.NewService(db, store, *rep)

	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	stopCh := make(chan struct{})
	svc.RunBackgroundWorkers(stopCh, 10*time.Second, 30*time.Second)

	go func() {
		log.Printf("metadata server listening %s", addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	// graceful shutdown
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	close(stopCh)
	grpcServer.GracefulStop()
}
