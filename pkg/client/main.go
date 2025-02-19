package client

import (
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a KVStore client
type Client struct {
	mu sync.RWMutex // Single mutex for all client state

	// List of all server addresses
	servers []string

	// Current primary server connection
	currentPrimary string
	primaryConn    *grpc.ClientConn
	primaryClient  pb.KVStoreClient

	// Connection pool for all servers
	connections map[string]*grpc.ClientConn

	// Configuration
	dialTimeout    time.Duration
	requestTimeout time.Duration
	maxRetries     int

	// Request tracking
	clientID       uint64
	requestCounter uint64
}

// NewClient creates a new KVStore client
func NewClient(servers []string) (*Client, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("at least one server address is required")
	}

	client := &Client{
		servers:        servers,
		connections:    make(map[string]*grpc.ClientConn),
		dialTimeout:    5 * time.Second,
		requestTimeout: 2 * time.Second,
		maxRetries:     3,
	}

	// Initialize connections to all servers
	if err := client.initConnections(); err != nil {
		return nil, fmt.Errorf("failed to initialize connections: %v", err)
	}

	// Find the primary server
	if err := client.findPrimary(); err != nil {
		return nil, fmt.Errorf("failed to find primary: %v", err)
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
	}

	if len(c.connections) == 0 {
		return fmt.Errorf("failed to connect to any server")
	}

	return nil
}

// findPrimary attempts to identify the current primary server
func (c *Client) findPrimary() error {
	// Try each server until we find the primary
	for server, conn := range c.connections {
		client := pb.NewKVStoreClient(conn)

		// Try a simple Put operation to test if this is the primary
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		defer cancel()

		_, err := client.Put(ctx, &pb.PutRequest{
			Key:   "__test_primary__",
			Value: "test",
		})

		if err == nil {
			c.mu.Lock()
			c.currentPrimary = server
			c.primaryConn = conn
			c.primaryClient = client
			c.mu.Unlock()
			return nil
		}
	}

	return fmt.Errorf("no primary server found")
}

// Get retrieves a value for the given key
func (c *Client) Get(key string) (string, bool, error) {
	req := &pb.GetRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}
	for retry := 0; retry <= c.maxRetries; retry++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		defer cancel()

		c.mu.RLock()
		client := c.primaryClient
		c.mu.RUnlock()

		resp, err := client.Get(ctx, req)
		if err != nil {
			if retry == c.maxRetries {
				return "", false, fmt.Errorf("get failed after %d retries: %v", retry, err)
			}

			// Try to find new primary if current one failed
			if err := c.findPrimary(); err != nil {
				log.Printf("Failed to find new primary: %v", err)
			}
			continue
		}

		// Handle leader hint
		c.handleLeaderHint(resp.LeaderHint)

		if resp.Error != "" {
			return "", false, fmt.Errorf(resp.Error)
		}

		return resp.Value, resp.Exists, nil
	}

	return "", false, fmt.Errorf("get failed after %d retries", c.maxRetries)
}

// Put sets a value for the given key
func (c *Client) Put(key, value string) (string, bool, error) {
	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}
	for retry := 0; retry <= c.maxRetries; retry++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		defer cancel()

		c.mu.RLock()
		client := c.primaryClient
		c.mu.RUnlock()

		resp, err := client.Put(ctx, req)
		if err != nil {
			if retry == c.maxRetries {
				return "", false, fmt.Errorf("put failed after %d retries: %v", retry, err)
			}

			// Try to find new primary if current one failed
			if err := c.findPrimary(); err != nil {
				log.Printf("Failed to find new primary: %v", err)
			}
			continue
		}

		// Handle leader hint
		c.handleLeaderHint(resp.LeaderHint)

		if resp.Error != "" {
			return "", false, fmt.Errorf(resp.Error)
		}

		return resp.OldValue, resp.HadOldValue, nil
	}

	return "", false, fmt.Errorf("put failed after %d retries", c.maxRetries)
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

func (c *Client) handleLeaderHint(hint string) {
	if hint != "" {
		c.mu.Lock()
		c.currentPrimary = hint
		// Update connection if needed
		if conn, exists := c.connections[hint]; exists {
			c.primaryConn = conn
			c.primaryClient = pb.NewKVStoreClient(conn)
		}
		c.mu.Unlock()
	}
}

// isUUEncoded returns true if the provided string appears to be UU encoded.
func isUUEncoded(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), "begin ")
}
