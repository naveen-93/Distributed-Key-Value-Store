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
	ErrValidation     = errors.New("validation error")
	ErrQuorumNotMet   = errors.New("failed to achieve quorum")
	ErrNoNodes        = errors.New("no available nodes")
	ErrConnectionFail = errors.New("failed to connect to servers")
)

const (
	maxKeyLength   = 128
	maxValueLength = 2048
)

// Enhanced ring health tracking
type ringHealth struct {
	lastSuccessfulUpdate time.Time
	consecutiveFailures  int
	lastError            error
	mu                   sync.RWMutex
}

// Client struct update
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
	numReplicas    int

	// Ring update configuration
	ringUpdateInterval time.Duration
	lastRingUpdate     time.Time

	// Request tracking
	clientID       uint64
	requestCounter uint64

	nodeStates  map[string]*nodeState
	nodeStateMu sync.RWMutex
	maxFailures int

	ringHealth         ringHealth
	healthyThreshold   time.Duration
	unhealthyThreshold int
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

type ClientConfig struct {
	ReadQuorum     int
	WriteQuorum    int
	NumReplicas    int
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	MaxRetries     int
}

// Add new types for two-phase read
type readResponse struct {
	value     string
	timestamp uint64
	exists    bool
	nodeAddr  string
	err       error
}

func NewClient(servers []string, config *ClientConfig) (*Client, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("%w: at least one server required", ErrValidation)
	}

	// Use provided config or defaults
	if config == nil {
		config = &ClientConfig{
			ReadQuorum:     2,
			WriteQuorum:    2,
			NumReplicas:    3,
			DialTimeout:    5 * time.Second,
			RequestTimeout: 2 * time.Second,
			MaxRetries:     3,
		}
	}

	client := &Client{
		servers:            servers,
		connections:        make(map[string]*grpc.ClientConn),
		clients:            make(map[string]pb.KVStoreClient),
		dialTimeout:        config.DialTimeout,
		requestTimeout:     config.RequestTimeout,
		maxRetries:         config.MaxRetries,
		readQuorum:         config.ReadQuorum,
		writeQuorum:        config.WriteQuorum,
		numReplicas:        config.NumReplicas,
		ringUpdateInterval: 30 * time.Second,
		nodeStates:         make(map[string]*nodeState),
		maxFailures:        3,
		healthyThreshold:   5 * time.Minute,
		unhealthyThreshold: 3,
	}

	if err := client.initConnections(); err != nil {
		return nil, fmt.Errorf("connection initialization failed: %w", err)
	}

	if err := client.updateRing(); err != nil {
		return nil, fmt.Errorf("initial ring update failed: %w", err)
	}

	// Start ring health monitoring
	client.startRingHealthMonitoring()
	return client, nil
}

// Get retrieves a value with improved validation and error handling
func (c *Client) Get(key string) (string, bool, error) {
	if healthy, err := c.CheckRingHealth(); !healthy {
		log.Printf("Warning: Ring health check failed: %v", err)
		// Continue with operation but log the warning
	}

	if err := validateKey(key); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	// Phase 1: Collect all responses
	candidate, responses, err := c.collectReadResponses(key)
	if err != nil {
		return "", false, err
	}

	// Phase 2: Confirm candidate value
	confirmed, err := c.confirmValue(key, candidate, responses)
	if err != nil {
		return "", false, err
	}

	if !confirmed {
		return "", false, fmt.Errorf("failed to confirm consistent value across quorum")
	}

	// Perform read repair only after confirming the true latest value
	go c.performReadRepair(key, candidate, responses)

	return candidate.value, candidate.exists, nil
}

// Phase 1: Collect responses from all replicas
func (c *Client) collectReadResponses(key string) (valueWithTimestamp, []readResponse, error) {
	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return valueWithTimestamp{}, nil, ErrNoNodes
	}

	responses := make(chan readResponse, len(nodes))
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	// Query all replicas concurrently
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			client := c.clients[addr]
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})

			if err != nil {
				responses <- readResponse{err: err, nodeAddr: addr}
				return
			}

			responses <- readResponse{
				value:     resp.Value,
				timestamp: resp.Timestamp,
				exists:    resp.Exists,
				nodeAddr:  addr,
			}
		}(node)
	}

	// Wait for all responses or timeout
	go func() {
		wg.Wait()
		close(responses)
	}()

	// Collect responses
	var allResponses []readResponse
	var validResponses []valueWithTimestamp

	for resp := range responses {
		if resp.err == nil {
			validResponses = append(validResponses, valueWithTimestamp{
				value:     resp.value,
				timestamp: resp.timestamp,
				exists:    resp.exists,
				nodeAddr:  resp.nodeAddr,
			})
		}
		allResponses = append(allResponses, resp)
	}

	if len(validResponses) < c.readQuorum {
		return valueWithTimestamp{}, allResponses, ErrQuorumNotMet
	}

	// Identify candidate (latest value)
	candidate := c.resolveConflicts(validResponses)
	return candidate, allResponses, nil
}

// Phase 2: Confirm candidate value with quorum
func (c *Client) confirmValue(key string, candidate valueWithTimestamp, responses []readResponse) (bool, error) {
	matchCount := 0

	// Count nodes that have the candidate value
	for _, resp := range responses {
		if resp.err == nil &&
			resp.timestamp == candidate.timestamp &&
			resp.value == candidate.value {
			matchCount++
		}
	}

	// Check if we have quorum agreement
	return matchCount >= c.readQuorum, nil
}

// Updated read repair to ensure consistent propagation
func (c *Client) performReadRepair(key string, confirmed valueWithTimestamp, responses []readResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, resp := range responses {
		// Skip nodes that already have the correct value
		if resp.timestamp >= confirmed.timestamp {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			req := &pb.PutRequest{
				Key:       key,
				Value:     confirmed.value,
				ClientId:  c.clientID,
				RequestId: c.nextRequestID(),
				Timestamp: confirmed.timestamp,
			}

			client := c.clients[addr]
			_, err := client.Put(ctx, req)
			if err != nil {
				log.Printf("Read repair failed for %s on %s: %v", key, addr, err)
			}
		}(resp.nodeAddr)
	}

	wg.Wait()
}

// Put sets a value with improved validation and error handling
func (c *Client) Put(key, value string) (string, bool, error) {
	if healthy, err := c.CheckRingHealth(); !healthy {
		log.Printf("Warning: Ring health check failed: %v", err)
		// Continue with operation but log the warning
	}

	if err := validateKey(key); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := validateValue(value); err != nil {
		return "", false, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	nodes := c.getReplicaNodes(key)
	if len(nodes) == 0 {
		return "", false, ErrNoNodes
	}

	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		ClientId:  c.clientID,
		RequestId: c.nextRequestID(),
	}

	type putResult struct {
		oldValue    string
		hadOldValue bool
		err         error
	}

	results := make(chan putResult, len(nodes))
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
				results <- putResult{err: err}
				return
			}
			results <- putResult{
				oldValue:    resp.OldValue,
				hadOldValue: resp.HadOldValue,
			}
		}(node)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	var successes int
	var oldValues []valueWithTimestamp
	var errors []error

	for result := range results {
		if result.err != nil {
			errors = append(errors, result.err)
			continue
		}
		successes++
		if result.hadOldValue {
			oldValues = append(oldValues, valueWithTimestamp{
				value:  result.oldValue,
				exists: true,
			})
		}
	}

	if successes < c.writeQuorum {
		return "", false, fmt.Errorf("%w: got %d successes, need %d. Errors: %v",
			ErrQuorumNotMet, successes, c.writeQuorum, errors)
	}

	if len(oldValues) > 0 {
		latest := c.resolveConflicts(oldValues)
		return latest.value, true, nil
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	primaryNode := c.ring.GetNode(key)
	if primaryNode == "" {
		return nil
	}

	nodes := make([]string, 0, c.numReplicas)
	nodes = append(nodes, primaryNode)

	current := c.ring.HashKey(key)
	for i := 1; i < c.numReplicas && len(nodes) < len(c.servers); i++ {
		next := c.ring.GetNextNode(current)
		if next != "" && !contains(nodes, next) && c.isNodeHealthy(next) {
			nodes = append(nodes, next)
		}
		current = c.ring.HashKey(next)
	}
	return nodes
}

func (c *Client) isNodeHealthy(node string) bool {
	c.nodeStateMu.RLock()
	defer c.nodeStateMu.RUnlock()

	state, exists := c.nodeStates[node]
	if !exists {
		return true // New nodes are assumed healthy
	}
	return state.failures < c.maxFailures && time.Since(state.lastSuccess) < c.healthyThreshold
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

	if state, exists := c.nodeStates[addr]; exists {
		state.lastSuccess = time.Now()
		state.failures = 0
	}
}

func (c *Client) recordNodeFailure(addr string) bool {
	c.nodeStateMu.Lock()
	defer c.nodeStateMu.Unlock()

	state := c.nodeStates[addr]
	if state == nil {
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
				c.recordRingUpdateFailure(err)
				continue
			}

			// Only update if version is newer
			if resp.Version > c.ringVersion {
				if err := c.applyRingUpdate(resp); err != nil {
					updateErrors = append(updateErrors, err)
					c.recordRingUpdateFailure(err)
					continue
				}
				updatedFromAny = true
				c.recordRingUpdateSuccess()
				break
			}
		}
	}

	if !updatedFromAny && c.ring == nil {
		return fmt.Errorf("failed to initialize ring: %v", updateErrors)
	}

	return nil
}

func (c *Client) recordRingUpdateSuccess() {
	c.ringHealth.mu.Lock()
	defer c.ringHealth.mu.Unlock()

	c.ringHealth.lastSuccessfulUpdate = time.Now()
	c.ringHealth.consecutiveFailures = 0
	c.ringHealth.lastError = nil
}

func (c *Client) recordRingUpdateFailure(err error) {
	c.ringHealth.mu.Lock()
	defer c.ringHealth.mu.Unlock()

	c.ringHealth.consecutiveFailures++
	c.ringHealth.lastError = err
}

func (c *Client) startRingHealthMonitoring() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			healthy, err := c.CheckRingHealth()
			if !healthy {
				log.Printf("Ring health check failed: %v", err)
				// Trigger immediate ring update attempt
				if err := c.updateRing(); err != nil {
					log.Printf("Failed to update ring after health check failure: %v", err)
				}
			}
		}
	}()
}

// Helper method to apply ring updates
func (c *Client) applyRingUpdate(resp *pb.RingStateResponse) error {
	newRing := consistenthash.NewRing(consistenthash.DefaultVirtualNodes)
	for node, isActive := range resp.Nodes {
		if isActive {
			newRing.AddNode(node)
		}
	}
	c.ring = newRing
	c.ringVersion = resp.Version
	c.lastRingUpdate = time.Unix(resp.UpdatedAt, 0)
	return nil
}

// Enhanced ring health check
func (c *Client) CheckRingHealth() (bool, error) {
	c.ringHealth.mu.RLock()
	defer c.ringHealth.mu.RUnlock()

	if time.Since(c.ringHealth.lastSuccessfulUpdate) > c.healthyThreshold {
		return false, fmt.Errorf("ring update too old: last success was %v ago",
			time.Since(c.ringHealth.lastSuccessfulUpdate))
	}

	if c.ringHealth.consecutiveFailures >= c.unhealthyThreshold {
		return false, fmt.Errorf("ring unstable: %d consecutive failures, last error: %v",
			c.ringHealth.consecutiveFailures, c.ringHealth.lastError)
	}

	return true, nil
}