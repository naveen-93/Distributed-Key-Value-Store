package kvclient

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for key and value validation
const (
	maxKeyLength   = 128
	maxValueLength = 2048
)

// ClientConfig holds configuration options for the KV client
type ClientConfig struct {
	ServerAddresses []string
	Timeout         time.Duration
	RetryAttempts   int
	RetryDelay      time.Duration
}

// Client handles communication with KVStore servers
type Client struct {
	config         ClientConfig
	connections    map[string]*grpc.ClientConn
	clients        map[string]pb.KVStoreClient
	mu             sync.RWMutex
	current        int    // Current server index
	requestCounter uint64 // For round-robin load balancing

	// Cluster awareness fields
	ring              *consistenthash.Ring
	nodeAddresses     map[string]string // Maps nodeID to address
	replicationFactor int
	ringVersion       uint64
	ringMu            sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewClient creates a new KVStore client
func NewClient(config ClientConfig) (*Client, error) {
	if len(config.ServerAddresses) == 0 {
		return nil, fmt.Errorf("no server addresses provided")
	}

	// Set default values if not specified
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 500 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		config:            config,
		current:           0,
		connections:       make(map[string]*grpc.ClientConn),
		clients:           make(map[string]pb.KVStoreClient),
		requestCounter:    0,
		ring:              consistenthash.NewRing(10),
		nodeAddresses:     make(map[string]string),
		replicationFactor: 3,
		ctx:               ctx,
		cancel:            cancel,
	}

	// Connect to all provided servers
	for _, addr := range config.ServerAddresses {
		if err := client.connectToServer(addr); err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
		}
	}

	if len(client.clients) == 0 {
		return nil, fmt.Errorf("failed to connect to any server")
	}

	// Try to get the initial ring state with retries
	if err := client.tryInitialRingStateUpdate(); err != nil {
		log.Printf("Warning: %v. Operations may be inconsistent.", err)
	}

	// Start background ring state updater
	go client.ringStateUpdater()
	return client, nil
}

// tryInitialRingStateUpdate attempts to get the initial ring state with multiple retries
func (c *Client) tryInitialRingStateUpdate() error {
	const maxAttempts = 5
	const retryDelay = 1 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.updateRingState(); err != nil {
			log.Printf("Attempt %d: Failed to fetch initial ring state: %v", attempt+1, err)
			time.Sleep(retryDelay)
		} else {
			return nil
		}
	}

	return fmt.Errorf("failed to fetch initial ring state after %d attempts", maxAttempts)
}

// connectToServer establishes a connection to a KVStore server
func (c *Client) connectToServer(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we're already connected
	if _, exists := c.connections[addr]; exists {
		return nil
	}

	// Try to connect
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	c.connections[addr] = conn
	c.clients[addr] = pb.NewKVStoreClient(conn)
	log.Printf("Connected to server at %s", addr)
	return nil
}

// Close closes all connections and stops background goroutines
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cancel() // Stop the background goroutine

	for addr, conn := range c.connections {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", addr, err)
		}
	}

	c.connections = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]pb.KVStoreClient)
}

// GetServerAddresses returns a copy of the current list of server addresses
func (c *Client) GetServerAddresses() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := make([]string, 0, len(c.clients))
	for addr := range c.clients {
		addresses = append(addresses, addr)
	}
	return addresses
}

// GetCurrentServerIndex returns the index of the current server
func (c *Client) GetCurrentServerIndex() int {
	return c.current
}

// SetCurrentServerIndex sets the current server index
func (c *Client) SetCurrentServerIndex(idx int) {
	c.current = idx
}

// AddServerAddress adds a new server address to the client configuration
func (c *Client) AddServerAddress(addr string) {
	c.config.ServerAddresses = append(c.config.ServerAddresses, addr)
}
