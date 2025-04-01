package kvclient

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validateKey checks if a key is valid
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

// validateValue checks if a value is valid
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

// Get retrieves a value with load balancing and automatic failover
func (c *Client) Get(key string) (string, uint64, bool, error) {
	if err := validateKey(key); err != nil {
		return "", 0, false, err
	}

	// Try to use ring-aware routing first
	value, timestamp, exists, err := c.ringAwareGet(key)
	if err == nil {
		return value, timestamp, exists, nil
	}

	// Fall back to querying all servers if ring-aware routing fails
	log.Printf("Ring-aware routing failed: %v, falling back to querying all servers", err)
	return c.fallbackGet(key)
}

// Result type for Get operations
type getResult struct {
	value     string
	timestamp uint64
	exists    bool
	err       error
	server    string
}

// ringAwareGet attempts to get a value using the consistent hash ring
func (c *Client) ringAwareGet(key string) (string, uint64, bool, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()

	resultChan := make(chan getResult, len(replicas))
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
				resultChan <- getResult{err: fmt.Errorf("no client for server %s", serverAddr), server: serverAddr}
				return
			}

			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				resultChan <- getResult{err: err, server: serverAddr}
				return
			}

			resultChan <- getResult{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				server:    serverAddr,
			}
		}(addr)
	}

	return c.processGetResults(resultChan, len(queriedAddresses), ctx)
}

// processGetResults processes the results from multiple Get operations
func (c *Client) processGetResults(resultChan chan getResult, numQueries int, ctx context.Context) (string, uint64, bool, error) {
	var mostRecentValue string
	var mostRecentTimestamp uint64
	var mostRecentExists bool
	var mostRecentServer string
	var successCount int
	var lastErr error

	// Wait for all responses or timeout
	for i := 0; i < numQueries; i++ {
		select {
		case res := <-resultChan:
			if res.err != nil {
				lastErr = res.err
				log.Printf("Error from server %s: %v", res.server, res.err)

				// Try to reconnect in background if it's a connection error
				s, ok := status.FromError(res.err)
				if ok && (s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded) {
					go c.attemptReconnect(res.server)
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
				return "", 0, false, fmt.Errorf("all replica servers timed out")
			}
			// Break out of the loop if we've got at least one response
			i = numQueries
		}
	}

	// If we found the key on at least one server
	if mostRecentExists {
		// Update current server index for UI display
		c.updateCurrentServerIndex(mostRecentServer)
		return mostRecentValue, mostRecentTimestamp, true, nil
	}

	// If we got responses but no server had the key
	if successCount > 0 {
		return "", 0, false, nil
	}

	// All servers failed
	return "", 0, false, fmt.Errorf("all replica servers failed: %v", lastErr)
}

// updateCurrentServerIndex updates the current server index based on the server address
func (c *Client) updateCurrentServerIndex(serverAddr string) {
	addresses := c.GetServerAddresses()
	for i, addr := range addresses {
		if addr == serverAddr {
			c.current = i
			break
		}
	}
}

// attemptReconnect tries to reestablish a connection to a server
func (c *Client) attemptReconnect(serverAddr string) {
	if err := c.connectToServer(serverAddr); err != nil {
		log.Printf("Failed to reconnect to %s: %v", serverAddr, err)
	}
}

// fallbackGet attempts to get a value by querying all servers
func (c *Client) fallbackGet(key string) (string, uint64, bool, error) {
	addresses := c.GetServerAddresses()
	if len(addresses) == 0 {
		return "", 0, false, fmt.Errorf("no available servers")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()

	resultChan := make(chan getResult, len(addresses))

	// Launch parallel requests to all servers
	for _, addr := range addresses {
		go func(serverAddr string) {
			c.mu.RLock()
			client, ok := c.clients[serverAddr]
			c.mu.RUnlock()

			if !ok {
				resultChan <- getResult{err: fmt.Errorf("no client for server %s", serverAddr), server: serverAddr}
				return
			}

			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				resultChan <- getResult{err: err, server: serverAddr}
				return
			}

			resultChan <- getResult{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				server:    serverAddr,
			}
		}(addr)
	}

	return c.processGetResults(resultChan, len(addresses), ctx)
}

// Put stores a value with load balancing and automatic failover
func (c *Client) Put(key, value string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, err
	}
	if err := validateValue(value); err != nil {
		return "", false, err
	}

	// Try to use ring-aware routing first
	oldValue, hadOldValue, err := c.ringAwarePut(key, value)
	if err == nil {
		return oldValue, hadOldValue, nil
	}

	// Fall back to round-robin if ring-aware routing fails
	log.Printf("Ring-aware routing failed: %v, falling back to round-robin", err)
	return c.fallbackPut(key, value)
}

// ringAwarePut attempts to put a value using the consistent hash ring
func (c *Client) ringAwarePut(key, value string) (string, bool, error) {
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

		ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
		resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		cancel()

		if err == nil {
			// Success - update current server index for UI display
			c.updateCurrentServerIndex(addr)
			return resp.OldValue, resp.HadOldValue, nil
		}

		lastErr = err
		log.Printf("Error from server %s: %v", addr, err)

		// If this is a connection error, try to reconnect
		s, ok := status.FromError(err)
		if ok && (s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded) {
			go c.attemptReconnect(addr)
		}
	}

	return "", false, fmt.Errorf("all replicas failed: %v", lastErr)
}

// fallbackPut attempts to put a value using round-robin load balancing
func (c *Client) fallbackPut(key, value string) (string, bool, error) {
	addresses := c.GetServerAddresses()
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

			ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
			resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
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
				go c.attemptReconnect(addr)
			}
		}

		// All servers failed this attempt, wait before retry
		if attempt < c.config.RetryAttempts-1 {
			time.Sleep(c.config.RetryDelay)
		}
	}

	return "", false, fmt.Errorf("all servers failed: %v", lastErr)
}
