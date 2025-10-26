package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var metadataAddr = flag.String("metadata", "127.0.0.1:50051", "metadata server")

func main() {
	flag.Parse()
	if len(os.Args) < 3 {
		fmt.Println("Usage: client-cli <upload|download> <file>")
		return
	}
	cmd := os.Args[1]
	filename := os.Args[2]

	conn, err := grpc.Dial(*metadataAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial metadata: %v", err)
	}
	defer conn.Close()
	meta := pb.NewMetadataServiceClient(conn)

	switch cmd {
	case "upload":
		upload(meta, filename)
	case "download":
		download(meta, filename)
	default:
		fmt.Println("unknown command")
	}
}

func upload(meta pb.MetadataServiceClient, filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("read file: %v", err)
	}

	// ask metadata for replicas
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := meta.GetReplicasForFile(ctx, &pb.GetReplicasRequest{FileKey: filename, Replicas: 0})
	if err != nil {
		log.Fatalf("get replicas: %v", err)
	}
	if len(resp.Nodes) == 0 {
		log.Fatalf("no replicas returned")
	}

	// upload in parallel
	g := new(errgroup.Group)
	for _, n := range resp.Nodes {
		n := n
		g.Go(func() error {
			conn, err := grpc.Dial(n.Address, grpc.WithInsecure())
			if err != nil {
				return err
			}
			defer conn.Close()

			client := pb.NewNodeServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			res, err := client.UploadFile(ctx, &pb.UploadRequest{
				FileKey: filename,
				Data:    data,
			})
			if err != nil {
				return err
			}
			if !res.Success {
				return fmt.Errorf("upload failed: %s", res.Message)
			}
			fmt.Printf("uploaded to %s\n", n.Address)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatalf("upload failed: %v", err)
	}

	fmt.Println("upload complete")
}

func download(meta pb.MetadataServiceClient, filename string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := meta.GetReplicasForFile(ctx, &pb.GetReplicasRequest{FileKey: filename, Replicas: 0})
	if err != nil {
		log.Fatalf("get replicas: %v", err)
	}
	if len(resp.Nodes) == 0 {
		log.Fatalf("no replicas")
	}

	// try each replica until successful
	for _, n := range resp.Nodes {
		conn, err := grpc.Dial(n.Address, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := pb.NewNodeServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := client.DownloadFile(ctx, &pb.DownloadRequest{Filename: filename})
		if err != nil {
			cancel()
			conn.Close()
			continue
		}

		var data []byte
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				cancel()
				break
			}
			if err != nil {
				cancel()
				conn.Close()
				break
			}
			data = append(data, chunk.Data...)
		}
		conn.Close()

		if len(data) > 0 {
			if err := os.WriteFile("downloaded_"+filename, data, 0644); err != nil {
				log.Fatalf("save: %v", err)
			}
			fmt.Printf("downloaded from %s\n", n.Address)
			return
		}
	}
	log.Fatalf("download failed from all replicas")
}
