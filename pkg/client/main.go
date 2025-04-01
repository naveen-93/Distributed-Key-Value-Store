package client

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	maxKeyLength   = 128
	maxValueLength = 2048
)

func validateKey(key string) error {
	if len(key) > maxKeyLength {
		return fmt.Errorf("key exceeds %d bytes", maxKeyLength)
	}
	for _, r := range key {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in key: %q", r)
		}
	}
	return nil
}

func validateValue(value string) error {
	if len(value) > maxValueLength {
		return fmt.Errorf("value exceeds %d bytes", maxValueLength)
	}
	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value: %q", r)
		}
	}
	return nil
}

type ClientConfig struct {
	ServerAddresses []string
	Timeout         time.Duration
	RetryAttempts   int
	RetryDelay      time.Duration
}

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
	ctx               context.Context    // Add context
	cancel            context.CancelFunc // Add cancel function
}

func NewClient(config ClientConfig) (*Client, error) {
	if len(config.ServerAddresses) == 0 {
		return nil, fmt.Errorf("no server addresses provided")
	}

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

	for _, addr := range config.ServerAddresses {
		if err := client.connectToServer(addr); err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
		}
	}

	if len(client.clients) == 0 {
		return nil, fmt.Errorf("failed to connect to any server")
	}

	if err := client.updateRingState(); err != nil {
		log.Printf("Warning: Failed to fetch initial ring state: %v", err)
	}

	go client.ringStateUpdater()
	return client, nil
}

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

// Get retrieves a value with load balancing and automatic failover
func (c *Client) Get(ctx context.Context, key string) (string, uint64, bool, error) {
	if err := validateKey(key); err != nil {
		return "", 0, false, err
	}

	// Try to use ring-aware routing first
	value, timestamp, exists, err := c.ringAwareGet(ctx, key)
	if err == nil {
		return value, timestamp, exists, nil
	}

	// Fall back to querying all servers if ring-aware routing fails
	log.Printf("Ring-aware routing failed: %v, falling back to querying all servers", err)
	return c.fallbackGet(ctx, key)
}

// Add ring-aware Get implementation
func (c *Client) ringAwareGet(ctx context.Context, key string) (string, uint64, bool, error) {
	c.ringMu.RLock()
	if c.ring == nil || len(c.nodeAddresses) == 0 {
		c.ringMu.RUnlock()
		return "", 0, false, fmt.Errorf("ring state not available")
	}

	// Get replicas for this key
	replicas := c.ring.GetReplicas(key, c.replicationFactor)
	c.ringMu.RUnlock()

	if len(replicas) == 0 {
		return "", 0, false, fmt.Errorf("no replicas found for key")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// Optimize for the common case - try the primary replica first
	// This avoids creating goroutines and channels when not needed
	primaryNodeID := replicas[0]
	c.ringMu.RLock()
	primaryAddr, ok := c.nodeAddresses[primaryNodeID]
	c.ringMu.RUnlock()

	if ok {
		c.mu.RLock()
		client, ok := c.clients[primaryAddr]
		c.mu.RUnlock()

		if ok {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err == nil && resp.Exists {
				return resp.Value, resp.Timestamp, true, nil
			}
			// If primary fails, fall through to parallel query
		}
	}

	// Query replicas in parallel
	type result struct {
		value     string
		timestamp uint64
		exists    bool
		err       error
		server    string
	}

	resultChan := make(chan result, len(replicas))
	queriedAddresses := make(map[string]bool)

	// Launch parallel requests to replica nodes
	for _, nodeID := range replicas {
		c.ringMu.RLock()
		addr, ok := c.nodeAddresses[nodeID]
		c.ringMu.RUnlock()

		if !ok {
			log.Printf("No address found for node %s", nodeID)
			continue
		}

		// Skip if we've already queried this address
		if queriedAddresses[addr] {
			continue
		}
		queriedAddresses[addr] = true

		go func(serverAddr string) {
			c.mu.RLock()
			client, ok := c.clients[serverAddr]
			c.mu.RUnlock()

			if !ok {
				resultChan <- result{err: fmt.Errorf("no client for server %s", serverAddr), server: serverAddr}
				return
			}

			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				resultChan <- result{err: err, server: serverAddr}
				return
			}

			resultChan <- result{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				err:       nil,
				server:    serverAddr,
			}
		}(addr)
	}

	// Wait for results
	var bestResult *result
	var successCount int
	timeout := time.After(c.config.Timeout)

	for i := 0; i < len(queriedAddresses); i++ {
		select {
		case res := <-resultChan:
			if res.err == nil {
				successCount++
				if res.exists {
					if bestResult == nil || res.timestamp > bestResult.timestamp {
						bestResult = &res
					}
				}
			}
		case <-timeout:
			// Timeout waiting for responses
			if bestResult != nil {
				// We have at least one successful result, return it
				return bestResult.value, bestResult.timestamp, bestResult.exists, nil
			}
			return "", 0, false, fmt.Errorf("timeout waiting for responses")
		case <-ctx.Done():
			// Context cancelled
			return "", 0, false, ctx.Err()
		}
	}

	if bestResult != nil {
		return bestResult.value, bestResult.timestamp, bestResult.exists, nil
	}

	if successCount > 0 {
		// We got responses but none had the key
		return "", 0, false, nil
	}

	// No successful responses
	return "", 0, false, fmt.Errorf("failed to get key from any replica")
}

// Add fallback Get implementation (existing implementation)
func (c *Client) fallbackGet(ctx context.Context, key string) (string, uint64, bool, error) {
	c.mu.RLock()
	addresses := make([]string, 0, len(c.clients))
	for addr := range c.clients {
		addresses = append(addresses, addr)
	}
	c.mu.RUnlock()

	if len(addresses) == 0 {
		return "", 0, false, fmt.Errorf("no available servers")
	}

	// Query all servers in parallel
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	type result struct {
		value     string
		timestamp uint64
		exists    bool
		err       error
		server    string
	}

	resultChan := make(chan result, len(addresses))

	// Launch parallel requests to all servers
	for _, addr := range addresses {
		go func(serverAddr string) {
			c.mu.RLock()
			client, ok := c.clients[serverAddr]
			c.mu.RUnlock()

			if !ok {
				resultChan <- result{err: fmt.Errorf("no client for server %s", serverAddr), server: serverAddr}
				return
			}

			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				resultChan <- result{err: err, server: serverAddr}
				return
			}

			resultChan <- result{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				server:    serverAddr,
			}
		}(addr)
	}

	// Process results
	var mostRecentValue string
	var mostRecentTimestamp uint64
	var mostRecentExists bool
	var mostRecentServer string
	var successCount int
	var lastErr error

	// Wait for all responses or timeout
	for i := 0; i < len(addresses); i++ {
		select {
		case res := <-resultChan:
			if res.err != nil {
				lastErr = res.err
				log.Printf("Error from server %s: %v", res.server, res.err)

				// Try to reconnect in background if it's a connection error
				s, ok := status.FromError(res.err)
				if ok && (s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded) {
					go func(address string) {
						if err := c.connectToServer(address); err != nil {
							log.Printf("Failed to reconnect to %s: %v", address, err)
						}
					}(res.server)
				}
				continue
			}

			successCount++

			// If this server has the key and it's newer than what we've seen so far
			if res.exists && (mostRecentTimestamp == 0 || res.timestamp > mostRecentTimestamp) {
				mostRecentValue = res.value
				mostRecentTimestamp = res.timestamp
				mostRecentExists = true
				mostRecentServer = res.server
			}

		case <-ctx.Done():
			// Timeout occurred
			if successCount == 0 {
				return "", 0, false, fmt.Errorf("all servers timed out")
			}
			// Break out of the loop if we've got at least one response
			i = len(addresses)
		}
	}

	// If we found the key on at least one server
	if mostRecentExists {
		// Update current server index for UI display
		for i, addr := range addresses {
			if addr == mostRecentServer {
				c.current = i
				break
			}
		}
		return mostRecentValue, mostRecentTimestamp, true, nil
	}

	// If we got responses but no server had the key
	if successCount > 0 {
		return "", 0, false, nil
	}

	// All servers failed
	return "", 0, false, fmt.Errorf("all servers failed: %v", lastErr)
}

// Put stores a value with load balancing and automatic failover
func (c *Client) Put(ctx context.Context, key, value string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, err
	}
	if err := validateValue(value); err != nil {
		return "", false, err
	}

	// Try to use ring-aware routing first
	oldValue, hadOldValue, err := c.ringAwarePut(ctx, key, value)
	if err == nil {
		return oldValue, hadOldValue, nil
	}

	// Fall back to round-robin if ring-aware routing fails
	log.Printf("Ring-aware routing failed: %v, falling back to round-robin", err)
	return c.fallbackPut(ctx, key, value)
}

// Add ring-aware Put implementation
func (c *Client) ringAwarePut(ctx context.Context, key, value string) (string, bool, error) {
	c.ringMu.RLock()
	if c.ring == nil || len(c.nodeAddresses) == 0 {
		c.ringMu.RUnlock()
		return "", false, fmt.Errorf("ring state not available")
	}

	// Get primary node for this key
	replicas := c.ring.GetReplicas(key, c.replicationFactor)
	c.ringMu.RUnlock()

	if len(replicas) == 0 {
		return "", false, fmt.Errorf("no replicas found for key")
	}

	// Try each replica in order until success
	var lastErr error
	for _, nodeID := range replicas {
		c.ringMu.RLock()
		addr, ok := c.nodeAddresses[nodeID]
		c.ringMu.RUnlock()

		if !ok {
			log.Printf("No address found for node %s", nodeID)
			continue
		}

		c.mu.RLock()
		client, ok := c.clients[addr]
		c.mu.RUnlock()

		if !ok {
			log.Printf("No client for server %s", addr)
			continue
		}

		ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
		resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		cancel()

		if err == nil {
			// Success - update current server index for UI display
			c.mu.RLock()
			addresses := make([]string, 0, len(c.clients))
			for a := range c.clients {
				addresses = append(addresses, a)
			}
			c.mu.RUnlock()

			for i, a := range addresses {
				if a == addr {
					c.current = i
					break
				}
			}
			return resp.OldValue, resp.HadOldValue, nil
		}

		lastErr = err
		log.Printf("Error from server %s: %v", addr, err)

		// If this is a connection error, try to reconnect
		s, ok := status.FromError(err)
		if ok && (s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded) {
			go func(address string) {
				if err := c.connectToServer(address); err != nil {
					log.Printf("Failed to reconnect to %s: %v", address, err)
				}
			}(addr)
		}
	}

	return "", false, fmt.Errorf("all replicas failed: %v", lastErr)
}

// Add fallback Put implementation (existing implementation)
func (c *Client) fallbackPut(ctx context.Context, key, value string) (string, bool, error) {
	c.mu.RLock()
	addresses := make([]string, 0, len(c.clients))
	for addr := range c.clients {
		addresses = append(addresses, addr)
	}
	c.mu.RUnlock()

	if len(addresses) == 0 {
		return "", false, fmt.Errorf("no available servers")
	}

	// Choose initial server using round-robin
	startIndex := int(atomic.AddUint64(&c.requestCounter, 1) % uint64(len(addresses)))

	// Try each server until success or all fail
	var lastErr error
	for attempt := 0; attempt < c.config.RetryAttempts; attempt++ {
		// Start with the selected server, then try others
		for i := 0; i < len(addresses); i++ {
			serverIndex := (startIndex + i) % len(addresses)
			addr := addresses[serverIndex]

			c.mu.RLock()
			client, ok := c.clients[addr]
			c.mu.RUnlock()

			if !ok {
				continue
			}

			reqCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
			resp, err := client.Put(reqCtx, &pb.PutRequest{Key: key, Value: value})
			cancel()

			if err == nil {
				// Success - update current server index for UI display
				c.current = serverIndex
				return resp.OldValue, resp.HadOldValue, nil
			}

			lastErr = err
			log.Printf("Error from server %s: %v", addr, err)

			// If this is a connection error, try to reconnect
			s, ok := status.FromError(err)
			if ok && (s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded) {
				// Try to reconnect in background
				go func(address string) {
					if err := c.connectToServer(address); err != nil {
						log.Printf("Failed to reconnect to %s: %v", address, err)
					}
				}(addr)
			}
		}

		// All servers failed this attempt, wait before retry
		if attempt < c.config.RetryAttempts-1 {
			time.Sleep(c.config.RetryDelay)
		}
	}

	return "", false, fmt.Errorf("all servers failed: %v", lastErr)
}

// Add method to update ring state
func (c *Client) updateRingState() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try to get ring state from any available server
	var lastErr error
	for addr, client := range c.clients {
		ctx, cancel := context.WithTimeout(c.ctx, c.config.Timeout)
		resp, err := client.GetRingState(ctx, &pb.RingStateRequest{})
		cancel()

		if err != nil {
			lastErr = err
			log.Printf("Failed to get ring state from %s: %v", addr, err)
			continue
		}

		// Successfully got ring state
		c.ringMu.Lock()

		// Only update if this is a newer version
		if resp.Version > c.ringVersion {
			// Reset the ring
			c.ring = consistenthash.NewRing(10)

			// Add all nodes to the ring
			for nodeID := range resp.Nodes {
				c.ring.AddNode(nodeID)

				// If we don't have this node's address yet, use a default one
				// This will be updated when we connect to the node
				if _, exists := c.nodeAddresses[nodeID]; !exists {
					// Try to extract address from our connections
					found := false
					for serverAddr := range c.clients {
						// This is a simplistic approach - in a real system you'd have
						// a more robust way to map node IDs to addresses
						if strings.Contains(serverAddr, strings.TrimPrefix(nodeID, "node-")) {
							c.nodeAddresses[nodeID] = serverAddr
							found = true
							break
						}
					}

					if !found {
						// Use any server as fallback
						for serverAddr := range c.clients {
							c.nodeAddresses[nodeID] = serverAddr
							break
						}
					}
				}
			}

			c.ringVersion = resp.Version

			log.Printf("Updated ring state: version=%d, nodes=%d",
				c.ringVersion, len(resp.Nodes))
		}

		c.ringMu.Unlock()
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to get ring state from any server: %v", lastErr)
	}
	return fmt.Errorf("no servers available to get ring state")
}

// Add background updater for ring state
func (c *Client) ringStateUpdater() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.updateRingState(); err != nil {
				log.Printf("Failed to update ring state: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}
