package client

import (
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
	"Distributed-Key-Value-Store/pkg/consistenthash"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultReadQuorum   = 2
	defaultWriteQuorum  = 2
	numReplicas         = 3
)

// Client represents a KVStore client
type Client struct {
	mu sync.RWMutex

	// Server management
	servers     []string
	ring        *consistenthash.Ring
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.KVStoreClient

	// Configuration
	dialTimeout    time.Duration
	requestTimeout time.Duration
	maxRetries     int
	readQuorum     int
	writeQuorum    int

	// Request tracking
	clientID       uint64
	requestCounter uint64
}


// Value with timestamp for conflict resolution
type valueWithTimestamp struct {
	value     string
	timestamp uint64
	exists    bool
	nodeAddr  string
}

// NewClient creates a new KVStore client
func NewClient(servers []string) (*Client, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("at least one server address is required")
	}

	client := &Client{
		servers:        servers,
		connections:    make(map[string]*grpc.ClientConn),
		clients:        make(map[string]pb.KVStoreClient),
		dialTimeout:    5 * time.Second,
		requestTimeout: 2 * time.Second,
		maxRetries:     3,
		readQuorum:     defaultReadQuorum,
		writeQuorum:    defaultWriteQuorum,
	}

	// Initialize consistent hashing ring
	client.ring = consistenthash.NewRing(consistenthash.DefaultVirtualNodes)
	for _, server := range servers {
		client.ring.AddNode(server)
	}

	// Initialize connections to all servers
	if err := client.initConnections(); err != nil {
		return nil, fmt.Errorf("failed to initialize connections: %v", err)
	}

	return client, nil
}
// initConnections establishes connections to all servers
func (c *Client) initConnections() error {
	for _, server := range c.servers {
		ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
		defer cancel()

		conn, err := grpc.DialContext(ctx, server,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			log.Printf("Warning: failed to connect to %s: %v", server, err)
			continue
		}

		c.connections[server] = conn
		c.clients[server] = pb.NewKVStoreClient(conn)
	}

	if len(c.connections) == 0 {
		return fmt.Errorf("failed to connect to any server")
	}
	return nil
}

func (c *Client) Get(key string) (string, bool, error) {
	// Get replica nodes for the key
	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, fmt.Errorf("no available nodes for key %s", key)
	}

	// Prepare request
	req := &pb.GetRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}

	// Collect responses from replicas
	responses := make(chan valueWithTimestamp, len(nodes))
	errors := make(chan error, len(nodes))

	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	// Query all replicas concurrently
	for _, node := range nodes {
		go func(addr string) {
			client := c.clients[addr]
			resp, err := client.Get(ctx, req)
			if err != nil {
				errors <- err
				return
			}
			responses <- valueWithTimestamp{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				nodeAddr:  addr,
			}
		}(node)
	}

	// Wait for quorum responses
	values := make([]valueWithTimestamp, 0, len(nodes))
	for i := 0; i < len(nodes); i++ {
		select {
		case resp := <-responses:
			values = append(values, resp)
			if len(values) >= c.readQuorum {
				// We have quorum
				latest := c.resolveConflicts(values)
				go c.performReadRepair(key, latest, values)
				return latest.value, latest.exists, nil
			}
		case err := <-errors:
			log.Printf("Error reading from replica: %v", err)
		}
	}

	return "", false, fmt.Errorf("failed to achieve read quorum")
}

// Put sets a value for the given key
func (c *Client) Put(key, value string) (string, bool, error) {
	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, fmt.Errorf("no available nodes for key %s", key)
	}

	timestamp := c.generateTimestamp()
	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
		Timestamp: timestamp,
	}

	// Track successful writes and old values
	successes := 0
	oldValues := make([]valueWithTimestamp, 0, len(nodes))
	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	// Send PUT to all replicas concurrently
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			resp, err := c.sendPutToReplica(ctx, addr, req)
			if err != nil {
				log.Printf("Error writing to %s: %v", addr, err)
				return
			}

			mu.Lock()
			successes++
			if resp != nil && resp.HadOldValue {
				oldValues = append(oldValues, valueWithTimestamp{
					value:     resp.OldValue,
					timestamp: resp.OldTimestamp,
					exists:    true,
					nodeAddr:  addr,
				})
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	if successes < c.writeQuorum {
		return "", false, fmt.Errorf("failed to achieve write quorum")
	}

	// If we have old values, return the most recent one
	if len(oldValues) > 0 {
		latest := c.resolveConflicts(oldValues)
		return latest.value, true, nil
	}

	return "", false, nil
}

// Close closes all client connections
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for server, conn := range c.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection to %s: %v", server, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}

	return nil
}

// Move validation functions to main.go since they're used by both C and Go APIs
func validateKey(key string) error {
	if len(key) > 128 {
		return fmt.Errorf("key too long (max 128 bytes)")
	}
	for _, r := range key {
		if r < 32 || r > 126 || r == '[' || r == ']' {
			return fmt.Errorf("invalid character in key")
		}
	}
	return nil
}

func validateValue(value string) error {
	if len(value) > 2048 {
		return fmt.Errorf("value too long (max 2048 bytes)")
	}
	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value")
		}
	}
	if isUUEncoded(value) {
		return fmt.Errorf("UU encoded values not allowed")
	}
	return nil
}

func (c *Client) nextRequestID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestCounter++
	return c.requestCounter
}

func (c *Client) retryWithBackoff(op func() error) error {
	backoff := 100 * time.Millisecond
	for retry := 0; retry < c.maxRetries; retry++ {
		err := op()
		if err == nil {
			return nil
		}

		// Exponential backoff
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("operation failed after %d retries", c.maxRetries)
}

// isUUEncoded returns true if the provided string appears to be UU encoded.
func isUUEncoded(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), "begin ")
}

func (c *Client) getReplicaNodes(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	primaryNode := c.ring.GetNode(key)
	if primaryNode == "" {
		return nil
	}

	nodes := make([]string, 0, numReplicas)
	nodes = append(nodes, primaryNode)

	current := c.ring.GetNodeHash(key)
	for i := 1; i < numReplicas && len(nodes) < len(c.servers); i++ {
		next := c.ring.GetNextNode(current)
		if next != "" && !contains(nodes, next) {
			nodes = append(nodes, next)
		}
		current = c.ring.GetNodeHash(next)
	}

	return nodes
}
func (c *Client) resolveConflicts(values []valueWithTimestamp) valueWithTimestamp {
	if len(values) == 0 {
		return valueWithTimestamp{}
	}

	// Sort by timestamp (descending) and node address for tie-breaking
	sort.Slice(values, func(i, j int) bool {
		if values[i].timestamp == values[j].timestamp {
			return values[i].nodeAddr > values[j].nodeAddr
		}
		return values[i].timestamp > values[j].timestamp
	})

	return values[0]
}

func (c *Client) performReadRepair(key string, latest valueWithTimestamp, values []valueWithTimestamp) {
	for _, val := range values {
		if val.timestamp < latest.timestamp {
			// Update stale replica
			req := &pb.PutRequest{
				Key:       key,
				Value:     latest.value,
				ClientId:  c.clientID,
				RequestId: c.nextRequestID(),
				Timestamp: latest.timestamp,
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
			defer cancel()

			client := c.clients[val.nodeAddr]
			_, err := client.Put(ctx, req)
			if err != nil {
				log.Printf("Read repair failed for %s on %s: %v", key, val.nodeAddr, err)
			}
		}
	}
}


func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// // generateTimestamp generates a unique timestamp for requests
func (c *Client) generateTimestamp() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestCounter++
	return c.requestCounter // Server should combine with node ID
}



func (c *Client) sendPutToReplica(ctx context.Context, node string, req *pb.PutRequest) (*pb.PutResponse, error) {
	for retry := 0; retry < c.maxRetries; retry++ {
		client := c.clients[node]
		resp, err := client.Put(ctx, req)
		if err == nil {
			return resp, nil
		}
		time.Sleep(time.Duration(retry+1) * 50 * time.Millisecond)
	}
	return nil, fmt.Errorf("failed after %d retries", c.maxRetries)
}
