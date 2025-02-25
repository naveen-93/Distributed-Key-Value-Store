package client

import (
	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrValidation       = errors.New("validation error")
	ErrQuorumNotMet     = errors.New("failed to achieve quorum")
	ErrNoNodes          = errors.New("no available nodes")
	ErrConnectionFail   = errors.New("failed to connect to servers")
	ErrTimeout          = errors.New("operation timed out")
	ErrRingUpdate       = errors.New("ring update failed")
	ErrNoNodesAvailable = errors.New("no nodes available")
)

const (
	defaultReadQuorum  = 2
	defaultWriteQuorum = 2
	numReplicas        = 3
	maxKeyLength       = 128
	maxValueLength     = 2048
)

// Add this to the top of main.go
type getReplicaNodesFunc func(string) []string

// Client represents a KVStore client
type Client struct {
	mu sync.RWMutex

	// Server management
	servers     []string
	ring        *consistenthash.Ring
	ringVersion uint64
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.KVStoreClient

	// Configuration
	dialTimeout    time.Duration
	requestTimeout time.Duration
	maxRetries     int
	readQuorum     int
	writeQuorum    int

	// Ring update configuration
	ringUpdateInterval time.Duration
	lastRingUpdate     time.Time

	// Request tracking
	clientID       uint64
	requestCounter uint64

	nodeStates  map[string]*nodeState
	nodeStateMu sync.RWMutex
	maxFailures int

	getReplicaNodesFn getReplicaNodesFunc
}

// Value with timestamp for conflict resolution
type valueWithTimestamp struct {
	value     string
	timestamp uint64
	exists    bool
	nodeAddr  string
}

type nodeState struct {
	lastSuccess time.Time
	failures    int
}

func NewClient(servers []string) (*Client, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("no servers provided")
	}

	client := &Client{
		servers:        servers,
		clients:        make(map[string]pb.KVStoreClient),
		connections:    make(map[string]*grpc.ClientConn),
		dialTimeout:    5 * time.Second,
		requestTimeout: 2 * time.Second,
		maxRetries:     3,
		readQuorum:     2,
		writeQuorum:    2,
		nodeStates:     make(map[string]*nodeState),
		ring:           consistenthash.NewRing(10),
	}

	if err := client.initConnections(); err != nil {
		return nil, fmt.Errorf("failed to initialize connections: %w", err)
	}

	if err := client.updateRing(); err != nil {
		return nil, fmt.Errorf("failed to initialize ring: %w", err)
	}

	return client, nil
}

// Get retrieves a value with improved validation and error handling
func (c *Client) Get(key string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	var (
		wg            sync.WaitGroup
		mu            sync.Mutex
		values        []valueWithTimestamp
		errors        []error
		successCount  int
		notFoundCount int
	)

	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, ErrNoNodesAvailable
	}

	// Track responses from each node
	responses := make(map[string]valueWithTimestamp)

	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			value, exists, timestamp, err := c.getFromNode(node, key)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				c.recordNodeFailure(node)
				errors = append(errors, fmt.Errorf("node %s: %w", node, err))
				return
			}

			c.recordNodeSuccess(node)
			successCount++

			if exists {
				val := valueWithTimestamp{
					value:     value,
					timestamp: timestamp,
					nodeAddr:  node,
					exists:    exists,
				}
				values = append(values, val)
				responses[node] = val
			} else {
				notFoundCount++
			}
		}(node)
	}

	wg.Wait()

	if successCount < c.readQuorum {
		return "", false, fmt.Errorf("%w: got %d successes, need %d. Errors: %v",
			ErrQuorumNotMet, successCount, c.readQuorum, errors)
	}

	if notFoundCount >= c.readQuorum {
		return "", false, nil
	}

	if len(values) == 0 {
		return "", false, nil
	}

	latest := c.resolveConflicts(values)

	// Only perform read repair if we have inconsistent values
	if len(responses) > 1 {
		inconsistent := false
		latestValue := latest.value
		latestTimestamp := latest.timestamp

		for _, resp := range responses {
			if resp.value != latestValue || resp.timestamp != latestTimestamp {
				inconsistent = true
				break
			}
		}

		if inconsistent {
			go c.performReadRepair(key, latest, values)
		}
	}

	return latest.value, latest.exists, nil
}

// GetWithContext is like Get but accepts a context for cancellation
func (c *Client) GetWithContext(ctx context.Context, key string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, ErrNoNodes
	}

	req := &pb.GetRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}

	responses := make(chan valueWithTimestamp, len(nodes))
	errs := make(chan error, len(nodes))
	var wg sync.WaitGroup

	// Query all replicas concurrently with improved error handling
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if err := c.retryWithBackoff(func() error {
				client := c.clients[addr]
				resp, err := client.Get(ctx, req)
				if err != nil {
					return err
				}
				responses <- valueWithTimestamp{
					value:     resp.Value,
					timestamp: resp.Timestamp,
					exists:    resp.Exists,
					nodeAddr:  addr,
				}
				return nil
			}); err != nil {
				errs <- fmt.Errorf("failed to read from %s: %w", addr, err)
			}
		}(node)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(responses)
		close(errs)
	}()

	// Collect responses and errors
	var values []valueWithTimestamp
	var errors []error
	for resp := range responses {
		values = append(values, resp)
	}
	for err := range errs {
		errors = append(errors, err)
	}

	if len(values) < c.readQuorum {
		return "", false, fmt.Errorf("%w: got %d responses, need %d", ErrQuorumNotMet, len(values), c.readQuorum)
	}

	latest := c.resolveConflicts(values)
	go c.performReadRepair(key, latest, values)
	return latest.value, latest.exists, nil
}

// Put sets a value with improved validation and error handling
func (c *Client) Put(key string, value string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := validateValue(value); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, ErrNoNodesAvailable
	}

	timestamp := time.Now().UnixNano()

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		oldValues    []string
		errors       []error
		successCount int
	)

	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			oldVal, hadOld, err := c.putToNode(node, key, value, timestamp)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				c.recordNodeFailure(node)
				errors = append(errors, fmt.Errorf("node %s: %w", node, err))
				return
			}

			c.recordNodeSuccess(node)
			if hadOld {
				oldValues = append(oldValues, oldVal)
			}
			successCount++
		}(node)
	}

	wg.Wait()

	if successCount < c.writeQuorum {
		return "", false, fmt.Errorf("%w: got %d successes, need %d. Errors: %v",
			ErrQuorumNotMet, successCount, c.writeQuorum, errors)
	}

	if len(oldValues) > 0 {
		return oldValues[0], true, nil
	}
	return "", false, nil
}

// Validation functions
func validateKey(key string) error {
	if len(key) > maxKeyLength {
		return fmt.Errorf("key exceeds maximum length of %d bytes", maxKeyLength)
	}
	for _, r := range key {
		if r < 32 || r > 126 || r == '[' || r == ']' {
			return fmt.Errorf("invalid character in key: %q", r)
		}
	}
	return nil
}

func validateValue(value string) error {
	if len(value) > maxValueLength {
		return fmt.Errorf("value exceeds maximum length of %d bytes", maxValueLength)
	}
	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value: %q", r)
		}
	}
	if isUUEncoded(value) {
		return fmt.Errorf("UU encoded values are not allowed")
	}
	return nil
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
	if c.getReplicaNodesFn != nil {
		return c.getReplicaNodesFn(key)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	primaryNode := c.ring.GetNode(key)
	if primaryNode == "" {
		return nil
	}

	nodes := make([]string, 0, numReplicas)
	nodes = append(nodes, primaryNode)

	current := c.ring.HashKey(key)
	for i := 1; i < numReplicas && len(nodes) < len(c.servers); i++ {
		next := c.ring.GetNextNode(current)
		if next != "" && !contains(nodes, next) {
			nodes = append(nodes, next)
		}
		current = c.ring.HashKey(next)
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
	// Only repair if we have inconsistencies
	for _, val := range values {
		if val.value != latest.value || val.timestamp != latest.timestamp {
			node := val.nodeAddr
			err := c.retryWithBackoff(func() error {
				_, _, err := c.putToNode(node, key, latest.value, int64(latest.timestamp))
				return err
			})
			if err != nil {
				log.Printf("Read repair failed for node %s: %v", node, err)
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

func (c *Client) sendPutToReplica(ctx context.Context, node string, req *pb.PutRequest) (*pb.PutResponse, error) {
	var lastErr error

	for retry := 0; retry < c.maxRetries; retry++ {
		if retry > 0 {
			// Exponential backoff
			backoff := time.Duration(1<<uint(retry-1)) * 50 * time.Millisecond
			time.Sleep(backoff)
		}

		client, ok := c.clients[node]
		if !ok {
			return nil, fmt.Errorf("no connection to node %s", node)
		}

		resp, err := client.Put(ctx, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		log.Printf("Retry %d failed for node %s: %v", retry+1, node, err)
	}

	return nil, fmt.Errorf("failed after %d retries: %v", c.maxRetries, lastErr)
}

func (c *Client) recordNodeSuccess(addr string) {
	c.nodeStateMu.Lock()
	defer c.nodeStateMu.Unlock()

	state, exists := c.nodeStates[addr]
	if !exists {
		state = &nodeState{}
		c.nodeStates[addr] = state
	}
	state.lastSuccess = time.Now()
	state.failures = 0
}

func (c *Client) recordNodeFailure(addr string) bool {
	c.nodeStateMu.Lock()
	defer c.nodeStateMu.Unlock()

	state, exists := c.nodeStates[addr]
	if !exists {
		state = &nodeState{}
		c.nodeStates[addr] = state
	}
	state.failures++
	return state.failures >= c.maxFailures
}

func (c *Client) updateRing() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var updateErrors []error
	updatedFromAny := false

	for _, server := range c.servers {
		if client, ok := c.clients[server]; ok {
			ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
			resp, err := client.GetRingState(ctx, &pb.RingStateRequest{})
			cancel()

			if err != nil {
				updateErrors = append(updateErrors, fmt.Errorf("failed to update from %s: %v", server, err))
				if c.recordNodeFailure(server) {
					log.Printf("Node %s marked as dead after %d failures", server, c.maxFailures)
				}
				continue
			}

			c.recordNodeSuccess(server)

			// Only update if version is newer
			if resp.Version > c.ringVersion {
				newRing := consistenthash.NewRing(consistenthash.DefaultVirtualNodes)
				for node, isActive := range resp.Nodes {
					if isActive {
						newRing.AddNode(node)
					}
				}
				c.ring = newRing
				c.ringVersion = resp.Version
				c.lastRingUpdate = time.Unix(resp.UpdatedAt, 0)
				updatedFromAny = true
				break
			}
		}
	}

	// Only return error if we couldn't update from any server AND we don't have a valid ring
	if !updatedFromAny && c.ring == nil {
		return fmt.Errorf("failed to initialize ring: %v", updateErrors)
	}

	// Log errors but don't fail if we have a working ring
	if len(updateErrors) > 0 {
		log.Printf("Some ring updates failed: %v", updateErrors)
	}

	return nil
}

func (c *Client) startRingUpdates() {
	go func() {
		ticker := time.NewTicker(c.ringUpdateInterval)
		defer ticker.Stop()

		for range ticker.C {
			if err := c.updateRing(); err != nil {
				log.Printf("Ring update failed: %v", err)
			}
		}
	}()
}

// StartHealthChecks periodically checks node health
func (c *Client) StartHealthChecks(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			c.checkNodeHealth()
		}
	}()
}

func (c *Client) checkNodeHealth() {
	for _, addr := range c.servers {
		if client, ok := c.clients[addr]; ok {
			ctx, cancel := context.WithTimeout(context.Background(), c.dialTimeout)
			_, err := client.GetRingState(ctx, &pb.RingStateRequest{})
			cancel()

			if err != nil {
				if c.recordNodeFailure(addr) {
					log.Printf("Node %s marked as dead after %d failures", addr, c.maxFailures)
				}
			} else {
				c.recordNodeSuccess(addr)
			}
		}
	}
}

// Add this method to the Client struct
func (c *Client) getFromNode(node string, key string) (string, bool, uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	client, ok := c.clients[node]
	if !ok {
		return "", false, 0, fmt.Errorf("no connection to node %s", node)
	}

	req := &pb.GetRequest{
		Key:       key,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}

	resp, err := client.Get(ctx, req)
	if err != nil {
		return "", false, 0, err
	}

	return resp.Value, resp.Exists, resp.Timestamp, nil
}

func (c *Client) putToNode(node string, key string, value string, timestamp int64) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	client, ok := c.clients[node]
	if !ok {
		return "", false, fmt.Errorf("no connection to node %s", node)
	}

	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
		Timestamp: uint64(timestamp),
	}

	resp, err := client.Put(ctx, req)
	if err != nil {
		return "", false, err
	}

	return resp.OldValue, resp.HadOldValue, nil
}
