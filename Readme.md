# Go-S3-Lite: Distributed Storage System

## Overview

Go-S3-Lite is a lightweight, distributed object storage system inspired by Amazon S3. It implements a decentralized architecture with metadata management, consistent hashing for sharding, automatic replication, and node health monitoring.

## Architecture

### Components

The system consists of three main components:

1. **Metadata Server** - Central coordinator managing node registry, file-to-node mappings, and health monitoring
2. **Node Servers** - Storage nodes that actually store files locally
3. **Client CLI** - Command-line interface for uploading and downloading files

### System Flow

```
┌─────────────────┐
│  Client CLI     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│ Metadata Server │──────► Node Server 1  │
└─────────────────┘      │ (file storage) │
         │                └─────────────────┘
         │                ┌─────────────────┐
         └───────────────►│ Node Server 2  │
                          │ (replica)       │
                          └─────────────────┘
```

## Key Features

### 1. Consistent Hashing (Ring Topology)

- Uses SHA-1 hashing to create a virtual ring
- Each node and file gets a position on the ring based on hash
- Files are assigned to nodes following the ring clockwise
- Provides automatic sharding and load balancing

**Location**: `internal/hashing/ring.go`

**Key Functions**:

- `GetNode(key)` - Returns single responsible node for a key
- `GetReplicas(key, count)` - Returns multiple replica nodes clockwise on ring
- Supports configurable replication factor

### 2. Node Registry & Health Monitoring

**Metadata Server Responsibilities**:

- Track all active nodes in the cluster
- Store node-to-file mappings in SQLite database
- Heartbeat monitoring (30-second TTL by default)
- Automatic dead node detection and removal
- File re-replication when nodes fail

**Database Schema**:

```sql
NodeModel {
  ID        string    -- Primary key
  Address   string    -- gRPC address (e.g., 127.0.0.1:6000)
  LastSeen  time      -- Last heartbeat timestamp
  CreatedAt time
  UpdatedAt time
  DeletedAt time      -- Soft delete
}

FileMapping {
  Key      string    -- File key (Primary key)
  Replicas string    -- CSV of node IDs storing this file
  CreatedAt time
  UpdatedAt time
}
```

### 3. Replication

- Configurable replication factor (default: 3)
- Files are replicated to multiple nodes on the ring
- Replicas are placed clockwise around the hash position
- Ensures data durability and availability

**Location**: `internal/metadata/service.go` - `GetReplicasForFile()`

### 4. Fault Tolerance

**Background Worker** (runs every 10 seconds):

1. Scans for nodes with `last_seen` older than 30 seconds
2. Removes dead nodes from hash ring
3. Updates file mappings to point to healthy nodes
4. Triggers re-replication logic

**Location**: `internal/metadata/service.go` - `RunBackgroundWorkers()`

## Directory Structure

```
go-s3-lite/
├── cmd/
│   ├── metadata-server/    # Metadata coordination server
│   ├── node-server/         # Storage node server
│   └── client-cli/          # Client CLI tool
├── internal/
│   ├── hashing/
│   │   └── ring.go          # Consistent hashing ring implementation
│   ├── metadata/
│   │   ├── db.go            # Database connection
│   │   ├── models.go        # GORM models
│   │   ├── service.go       # Metadata gRPC service
│   │   └── store.go         # Node store operations
│   ├── node/
│   │   ├── server.go        # Node gRPC server
│   │   ├── service.go       # Legacy node service
│   │   └── storage.go       # File storage operations
│   ├── pb/                  # Protocol buffers
│   │   ├── metadata.proto   # Metadata service definitions
│   │   ├── node.proto       # Node service definitions
│   │   └── *.pb.go          # Generated gRPC code
│   └── utils/
│       └── utils.go         # CSV join/split utilities
├── config/
│   └── metadata.yml         # Metadata server config
└── data/                     # Node storage directory
```

## gRPC Services

### Metadata Service

**Endpoints**:

1. **RegisterNode** - Register a new storage node

   - Input: `NodeInfo{id, address}`
   - Output: Success/error status
2. **GetReplicasForFile** - Get nodes responsible for storing a file

   - Input: `FileKey`, optional `Replicas` count
   - Output: List of node addresses
   - Persists file-to-node mapping to database
3. **ListNodes** - List all active nodes in cluster

   - Returns all registered nodes
4. **Heartbeat** - Update node's last seen timestamp

   - Input: `NodeId`
   - Output: Success status

### Node Service

**Endpoints**:

1. **UploadFile** - Store a file on the node

   - Input: `UploadRequest{FileKey, Data}`
   - Output: `UploadResponse{Success, Message}`
   - Saves file to local storage directory
2. **DownloadFile** - Retrieve a file from the node

   - Input: `DownloadRequest{Filename}`
   - Output: Stream of `DownloadResponse{Data}`
   - Streams file data back to client

## Usage

### Starting the Metadata Server

```bash
go build -o bin/metadata-server cmd/metadata-server/main.go
./bin/metadata-server \
  -port 50051 \
  -db ./meta.db \
  -replication 3
```

### Starting Node Servers

```bash
# Node 1
go build -o bin/node-server cmd/node-server/main.go
./bin/node-server \
  -id node-1 \
  -addr 127.0.0.1:6000 \
  -metadata 127.0.0.1:50051 \
  -data ./data/node-1

# Node 2
./bin/node-server \
  -id node-2 \
  -addr 127.0.0.1:6001 \
  -metadata 127.0.0.1:50051 \
  -data ./data/node-2

# Node 3
./bin/node-server \
  -id node-3 \
  -addr 127.0.0.1:6002 \
  -metadata 127.0.0.1:50051 \
  -data ./data/node-3
```

### Using the Client CLI

**Upload a file**:

```bash
go build -o bin/client-cli cmd/client-cli/main.go
./bin/client-cli upload test.txt
```

**Download a file**:

```bash
./bin/client-cli download test.txt
```

The downloaded file will be saved as `downloaded_test.txt` in the current directory.

## Client Flow

### Upload Flow

1. Client reads file from disk
2. Client asks metadata server for responsible nodes
3. Metadata server:
   - Hashes file key
   - Finds appropriate nodes on hash ring
   - Returns node addresses
   - Persists mapping to database
4. Client uploads file to all replica nodes in parallel using goroutines
5. Each node saves file to its local storage

### Download Flow

1. Client asks metadata server for file location
2. Metadata server returns list of nodes storing the file
3. Client tries each replica until successful:
   - Connects to node
   - Streams file data
   - Accumulates data chunks
   - Saves to disk

## Implementation Details

### Consistent Hashing Algorithm

```go
hashKey(key string) uint32 {
    h := sha1.Sum([]byte(key))
    return uint32(h[0])<<24 | uint32(h[1])<<16 | 
           uint32(h[2])<<8 | uint32(h[3])
}
```

Properties:

- Deterministic: Same key always hashes to same position
- Distributed: Keys are evenly distributed across nodes
- Minimal remapping: Adding/removing nodes only affects adjacent entries

### Concurrent Operations

- **Parallel uploads**: Uses `golang.org/x/sync/errgroup` for concurrent file uploads to replica nodes
- **Thread-safe**: Hash ring uses `sync.RWMutex` for concurrent read/write operations
- **Database**: GORM with connection pooling for metadata persistence

### Error Handling

- Graceful degradation when replica nodes fail
- Retry logic: Download tries next replica on failure
- Dead node detection and automatic removal
- Background re-replication of data from failed nodes

## Configuration

### Metadata Server

- **Port**: Default 50051
- **Database**: SQLite file (default: `./meta.db`)
- **Replication Factor**: Default 3 replicas per file
- **Heartbeat Interval**: 10 seconds
- **Dead Node TTL**: 30 seconds

### Node Server

- **Node ID**: Unique identifier for the node
- **Address**: IP and port to listen on (default: `127.0.0.1:6000`)
- **Metadata Server**: Address of metadata coordinator
- **Data Directory**: Local path for storing files

## Technologies Used

- **Go 1.24+**: Language
- **gRPC**: Inter-service communication
- **Protocol Buffers**: Data serialization and service definitions
- **SQLite/GORM**: Metadata persistence
- **Consistent Hashing**: SHA-1 based ring topology
- **goroutines**: Concurrent operations

## Design Decisions

### Why gRPC?

- Type-safe service definitions
- Efficient binary protocol
- Native streaming support for large files
- Strong tooling ecosystem

### Why Consistent Hashing?

- Automatic load balancing
- Natural sharding across nodes
- Efficient node addition/removal
- Predictable data placement

### Why SQLite?

- Zero configuration database
- Suitable for moderate scale metadata
- ACID transactions
- Embedded deployment

## Limitations & Future Improvements

### Current Limitations

1. **No actual data replication**: Metadata is updated but actual file copying between nodes is not implemented
2. **Single metadata server**: Central bottleneck (could be federated)
3. **No authentication/authorization**: Open access
4. **No encryption**: Data stored in plaintext
5. **Limited to single machine**: All nodes on same network

### Potential Enhancements

1. **Multi-master metadata**: Remove single point of failure
2. **Actual data replication**: Implement background worker to copy data
3. **Erasure coding**: More efficient than replication
4. **Object versioning**: Track file versions
5. **Quotas and billing**: Per-user storage limits
6. **RESTful API**: HTTP/HTTPS interface in addition to gRPC
7. **Consistency levels**: Eventual vs strong consistency options
8. **Monitoring and metrics**: Prometheus, Grafana integration

## Testing

Basic workflow test:

1. Start metadata server
2. Start 3 node servers
3. Upload a test file
4. Verify it appears in node data directories
5. Download the file
6. Verify downloaded file matches original
7. Stop a node
8. Download should still work from another replica

## Contact

For questions or contributions, please refer to the project repository.
