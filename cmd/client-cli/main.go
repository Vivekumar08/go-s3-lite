package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/serialx/hashring"
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
	// Fetch all nodes
	nodes := getAllNodes(metaClient)
	if len(nodes) == 0 {
		log.Fatalf("No nodes available")
	}

	// Create hashring for node selection
	nodeMap := make(map[string]string)
	nodeIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		nodeIDs = append(nodeIDs, n.Id)
		nodeMap[n.Id] = n.Address
	}
	ring := hashring.New(nodeIDs)

	switch cmd {
	case "upload":
		uploadFile(file, nodeMap, ring)
	case "download":
		downloadFile(file, nodeMap, ring)
	default:
		fmt.Println("Unknown command:", cmd)
	}
}

// --- Get all nodes from metadata ---
func getAllNodes(metaClient pb.MetadataServiceClient) []*pb.NodeInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := metaClient.ListNodes(ctx, &pb.ListNodesRequest{})
	if err != nil {
		log.Fatalf("Failed to list nodes: %v", err)
	}
	return resp.Nodes
}

// --- Upload with retry ---
func uploadFile(filename string, nodeMap map[string]string, ring *hashring.HashRing) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Select node using hashring
	_, ok := ring.GetNode(filename)
	if !ok {
		log.Fatalf("Failed to select node from hashring")
	}

	tryNodes := make([]string, len(nodeMap))
	i := 0
	for k := range nodeMap {
		tryNodes[i] = k
		i++
	}

	// Try nodes in order until success
	for _, nid := range tryNodes {
		addr := nodeMap[nid]
		fmt.Printf("Uploading '%s' to node %s (%s)...\n", filename, nid, addr)
		if doUpload(addr, filename, data) {
			fmt.Println("Upload successful!")
			return
		}
		fmt.Printf("Failed on node %s, trying next...\n", nid)
	}

	log.Fatalf("Upload failed on all nodes")
}

func doUpload(addr, filename string, data []byte) bool {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.UploadFile(ctx)
	if err != nil {
		return false
	}

	err = stream.Send(&pb.FileChunk{
		Filename: filename,
		Data:     data,
	})
	if err != nil {
		return false
	}

	res, err := stream.CloseAndRecv()
	if err != nil || !res.Success {
		return false
	}
	return true
}

// --- Download with retry ---
func downloadFile(filename string, nodeMap map[string]string, ring *hashring.HashRing) {
	_, ok := ring.GetNode(filename)
	if !ok {
		log.Fatalf("Failed to select node from hashring")
	}

	tryNodes := make([]string, len(nodeMap))
	i := 0
	for k := range nodeMap {
		tryNodes[i] = k
		i++
	}

	var fileData []byte
	for _, nid := range tryNodes {
		addr := nodeMap[nid]
		fmt.Printf("Downloading '%s' from node %s (%s)...\n", filename, nid, addr)
		data, ok := doDownload(addr, filename)
		if ok {
			fileData = data
			break
		}
		fmt.Printf("Failed on node %s, trying next...\n", nid)
	}

	if fileData == nil {
		log.Fatalf("Download failed on all nodes")
	}

	outFile := "downloaded_" + filename
	if err := ioutil.WriteFile(outFile, fileData, 0644); err != nil {
		log.Fatalf("Failed to save file: %v", err)
	}
	fmt.Printf("Downloaded file saved as '%s'\n", outFile)
}

func doDownload(addr, filename string) ([]byte, bool) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, false
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.DownloadFile(ctx, &pb.DownloadRequest{Filename: filename})
	if err != nil {
		return nil, false
	}

	var fileData []byte
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		fileData = append(fileData, chunk.Data...)
	}

	return fileData, true
}
