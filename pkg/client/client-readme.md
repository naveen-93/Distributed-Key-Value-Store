# Distributed Key-Value Store Client

## Overview

This client package implements a fault-tolerant, distributed key-value store client that communicates with a cluster of storage servers using gRPC. The client provides a reliable interface for storing and retrieving data in the distributed system with configurable consistency guarantees.

## Key Features

- **Consistent Hashing**: Uses consistent hashing to determine data placement
- **Configurable Consistency**: Supports tunable read and write quorums
- **Data Replication**: Replicates data across multiple nodes for fault tolerance
- **Read Repair**: Implements read repair to maintain consistency
- **Health Monitoring**: Monitors node and cluster health
- **Automatic Retries**: Includes exponential backoff retry mechanisms
- **Conflict Resolution**: Resolves value conflicts using timestamps

## Architecture

The client connects to multiple key-value store servers and manages these connections throughout its lifecycle. It uses a consistent hash ring to determine which servers should store each key, allowing for efficient routing and load balancing.

## Core Components

### Client Struct

The `Client` struct manages server connections and contains configuration parameters:

```go
type Client struct {
    // Server management
    servers     []string                       // List of server addresses
    ring        *consistenthash.Ring           // Consistent hash ring
    ringVersion uint64                         // Version of current ring
    connections map[string]*grpc.ClientConn    // gRPC connections
    clients     map[string]pb.KVStoreClient    // gRPC clients
    
    // Configuration
    dialTimeout    time.Duration    // Connection timeout
    requestTimeout time.Duration    // Request timeout
    maxRetries     int              // Maximum retries for operations
    readQuorum     int              // Minimum reads for consistency
    writeQuorum    int              // Minimum writes for consistency
    numReplicas    int              // Number of replicas per key
    
    // Ring management
    ringUpdateInterval time.Duration
    lastRingUpdate     time.Time
    
    // Request tracking
    clientID       uint64
    requestCounter uint64
    
    // Health tracking
    nodeStates  map[string]*nodeState
    maxFailures int
    ringHealth  ringHealth
    
    // Various mutex locks
    mu         sync.RWMutex
    nodeStateMu sync.RWMutex
}
```

### Initialization

The client connects to the specified servers during initialization:

```go
func NewClient(servers []string, config *ClientConfig) (*Client, error) {
    // Initialize client with defaults or provided configuration
    // Establish connections to servers
    // Update ring information
    // Start ring health monitoring
}
```

## Key Operations

### Get Operation (Two-Phase Read)

The `Get` method implements a two-phase read protocol to ensure consistent reads:

1. **Phase 1: Collect responses from all replicas**
   - Identify replicas for the key using consistent hashing
   - Send concurrent Get requests to all replicas
   - Collect responses and identify the candidate (latest value)

2. **Phase 2: Confirm candidate with quorum**
   - Verify that enough replicas (matching read quorum) agree on the candidate value

3. **Read Repair (Asynchronous)**
   - If some replicas have stale data, asynchronously update them with the latest value

```go
func (c *Client) Get(key string) (string, bool, error) {
    // Validate key
    // Phase 1: Collect read responses
    // Phase 2: Confirm value with quorum
    // Perform read repair asynchronously
    // Return value, existence flag, and any error
}
```

### Put Operation

The `Put` method ensures consistent writes across the cluster:

1. **Identify target replicas** using consistent hashing
2. **Send concurrent Put requests** to all replicas
3. **Verify write quorum** is achieved
4. **Return previous value** if it existed

```go
func (c *Client) Put(key, value string) (string, bool, error) {
    // Validate key and value
    // Get replica nodes for the key
    // Send concurrent Put requests
    // Verify write quorum is achieved
    // Return old value if it existed
}
```

## Fault Tolerance Mechanisms

### Consistent Hashing

The client uses a consistent hash ring to determine data placement:

```go
func (c *Client) getReplicaNodes(key string) []string {
    // Get primary node
    // Get additional replicas by walking the ring
    // Filter out unhealthy nodes
    // Return list of replica nodes
}
```

### Health Monitoring

The client tracks node health and ring health:

```go
func (c *Client) isNodeHealthy(node string) bool {
    // Check node failure count and last success time
}

func (c *Client) CheckRingHealth() (bool, error) {
    // Check ring update recency and failure count
}
```

### Ring Management

The client periodically updates its view of the cluster:

```go
func (c *Client) updateRing() error {
    // Query servers for latest ring state
    // Update if newer version is available
    // Record success or failure
}
```

### Conflict Resolution

Value conflicts are resolved based on timestamps:

```go
func (c *Client) resolveConflicts(values []valueWithTimestamp) valueWithTimestamp {
    // Sort by timestamp (descending)
    // Use node address for tie-breaking
    // Return the highest timestamp value
}
```

## Error Handling

The client defines several error types:

```go
var (
    ErrValidation     = errors.New("validation error")
    ErrQuorumNotMet   = errors.New("failed to achieve quorum")
    ErrNoNodes        = errors.New("no available nodes")
    ErrConnectionFail = errors.New("failed to connect to servers")
)
```

And implements retry mechanisms:

```go
func (c *Client) retryWithBackoff(op func() error) error {
    // Retry operation with exponential backoff
}
```

## Data Validation

The client validates keys and values:

```go
func validateKey(key string) error {
    // Check key length and character validity
}

func validateValue(value string) error {
    // Check value length and character validity
    // Reject UU-encoded values
}
```

## Usage Example

```go
// Create client
client, err := client.NewClient([]string{"server1:50051", "server2:50051"}, &client.ClientConfig{
    ReadQuorum:  2,
    WriteQuorum: 2,
    NumReplicas: 3,
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Store value
oldValue, existed, err := client.Put("user:1234", "{\"name\":\"John\",\"email\":\"john@example.com\"}")
if err != nil {
    log.Fatal(err)
}

// Retrieve value
value, exists, err := client.Get("user:1234")
if err != nil {
    log.Fatal(err)
}
```

## Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| ReadQuorum | Minimum reads for consistency | 2 |
| WriteQuorum | Minimum writes for consistency | 2 |
| NumReplicas | Number of replicas per key | 3 |
| DialTimeout | Connection timeout | 5 seconds |
| RequestTimeout | Request timeout | 2 seconds |
| MaxRetries | Maximum retries for operations | 3 |
