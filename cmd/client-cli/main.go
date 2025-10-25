package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	pb "github.com/vivekumar08/go-s3-lite/internal/pb"
	"google.golang.org/grpc"
)

var (
	metadataAddr = flag.String("metadata", "127.0.0.1:50051", "Metadata server address")
)

func main() {
	flag.Parse()

	if len(os.Args) < 3 {
		fmt.Println("Usage: client-cli <upload|download> <file>")
		return
	}

	cmd := os.Args[1]
	file := os.Args[2]

	conn, err := grpc.Dial(*metadataAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to metadata: %v", err)
	}
	defer conn.Close()
	metaClient := pb.NewMetadataServiceClient(conn)

	switch cmd {
	case "upload":
		uploadFile(metaClient, file)
	case "download":
		downloadFile(metaClient, file)
	default:
		fmt.Println("Unknown command:", cmd)
	}
}

// --- Upload ---
func uploadFile(metaClient pb.MetadataServiceClient, filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ask metadata which node to use
	resp, err := metaClient.GetNodeForFile(ctx, &pb.GetNodeRequest{
		FileKey: filename,
	})
	if err != nil {
		log.Fatalf("Failed to get node for file: %v", err)
	}

	if resp.Node == nil {
		log.Fatalf("No node available to store file")
	}

	nodeAddr := resp.Node.Address
	fmt.Printf("Uploading '%s' to node %s...\n", filename, nodeAddr)

	// Connect to node
	nodeConn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	defer nodeConn.Close()

	nodeClient := pb.NewNodeServiceClient(nodeConn)

	// Upload file
	stream, err := nodeClient.UploadFile(context.Background())
	if err != nil {
		log.Fatalf("Failed to open upload stream: %v", err)
	}

	chunk := &pb.FileChunk{
		Filename: filename,
		Data:     data,
	}

	if err := stream.Send(chunk); err != nil {
		log.Fatalf("Failed to send file chunk: %v", err)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to receive upload response: %v", err)
	}

	fmt.Printf("Upload success: %v, message: %s\n", res.Success, res.Message)
}

// --- Download ---
func downloadFile(metaClient pb.MetadataServiceClient, filename string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ask metadata which node to use
	resp, err := metaClient.GetNodeForFile(ctx, &pb.GetNodeRequest{
		FileKey: filename,
	})
	if err != nil {
		log.Fatalf("Failed to get node for file: %v", err)
	}

	if resp.Node == nil {
		log.Fatalf("No node found for file: %s", filename)
	}

	nodeAddr := resp.Node.Address
	fmt.Printf("Downloading '%s' from node %s...\n", filename, nodeAddr)

	nodeConn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	defer nodeConn.Close()

	nodeClient := pb.NewNodeServiceClient(nodeConn)

	stream, err := nodeClient.DownloadFile(context.Background(), &pb.DownloadRequest{
		Filename: filename,
	})
	if err != nil {
		log.Fatalf("Failed to start download stream: %v", err)
	}

	var fileData []byte
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		fileData = append(fileData, chunk.Data...)
	}

	// Save file locally
	outFile := "downloaded_" + filename
	if err := ioutil.WriteFile(outFile, fileData, 0644); err != nil {
		log.Fatalf("Failed to save file: %v", err)
	}

	fmt.Printf("Downloaded file saved as '%s'\n", outFile)
}
