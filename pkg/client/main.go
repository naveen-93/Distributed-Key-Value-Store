package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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
}

func NewClient(config ClientConfig) (*Client, error) {
	if len(config.ServerAddresses) == 0 {
		return nil, fmt.Errorf("no server addresses provided")
	}

	// Set defaults if not specified
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 500 * time.Millisecond
	}

	client := &Client{
		config:         config,
		current:        0,
		connections:    make(map[string]*grpc.ClientConn),
		clients:        make(map[string]pb.KVStoreClient),
		requestCounter: 0, // Initialize the counter
	}

	// Connect to all servers
	for _, addr := range config.ServerAddresses {
		if err := client.connectToServer(addr); err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
			// Continue trying other servers
		}
	}

	if len(client.clients) == 0 {
		return nil, fmt.Errorf("failed to connect to any server")
	}

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

	for addr, conn := range c.connections {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", addr, err)
		}
	}

	c.connections = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]pb.KVStoreClient)
}

// Get retrieves a value with load balancing and automatic failover
func (c *Client) Get(key string) (string, uint64, bool, error) {
	c.mu.RLock()
	addresses := make([]string, 0, len(c.clients))
	for addr := range c.clients {
		addresses = append(addresses, addr)
	}
	c.mu.RUnlock()

	if len(addresses) == 0 {
		return "", 0, false, fmt.Errorf("no available servers")
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
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			cancel()

			if err == nil {
				// Success - update current server index for UI display
				c.current = serverIndex
				return resp.Value, resp.Timestamp, resp.Exists, nil
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

	return "", 0, false, fmt.Errorf("all servers failed: %v", lastErr)
}

// Put stores a value with load balancing and automatic failover
func (c *Client) Put(key, value string) (string, bool, error) {
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

// Helper function to print the formatted timestamp
func formatTimestamp(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format(time.RFC3339)
}

func main() {
	// Configuration
	config := ClientConfig{
		ServerAddresses: []string{
			"localhost:50051",
			"localhost:50052",
			"localhost:50053",
		},
		Timeout:       5 * time.Second,
		RetryAttempts: 3,
		RetryDelay:    500 * time.Millisecond,
	}

	// Initialize client
	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	defer client.Close()

	fmt.Println("=== Distributed Key-Value Store Client ===")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1. Get value")
		fmt.Println("2. Put value")
		fmt.Println("3. Batch operations")
		fmt.Println("4. Import data")
		fmt.Println("5. Export data")
		fmt.Println("6. Switch server")
		fmt.Println("7. Exit")
		fmt.Print("Enter choice (1-7): ")

		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			handleGet(client, scanner)
		case "2":
			handlePut(client, scanner)
		case "3":
			handleBatch(client, scanner)
		case "4":
			handleImport(client, scanner)
		case "5":
			handleExport(client, scanner)
		case "6":
			handleSwitchServer(client, scanner)
		case "7":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid choice, please try again")
		}
	}
}

func handleGet(client *Client, scanner *bufio.Scanner) {
	fmt.Print("Enter key: ")
	scanner.Scan()
	key := strings.TrimSpace(scanner.Text())

	if key == "" {
		fmt.Println("Key cannot be empty")
		return
	}

	value, timestamp, exists, err := client.Get(key)
	if err != nil {
		fmt.Printf("Error getting value: %v\n", err)
		return
	}

	if exists {
		fmt.Println("=== Key Found ===")
		fmt.Printf("Key:         %s\n", key)
		fmt.Printf("Value:       %s\n", value)
		fmt.Printf("Last update: %s\n", formatTimestamp(timestamp))
	} else {
		fmt.Printf("Key '%s' not found\n", key)
	}
}

func handlePut(client *Client, scanner *bufio.Scanner) {
	fmt.Print("Enter key: ")
	scanner.Scan()
	key := strings.TrimSpace(scanner.Text())

	if key == "" {
		fmt.Println("Key cannot be empty")
		return
	}

	fmt.Print("Enter value: ")
	scanner.Scan()
	value := strings.TrimSpace(scanner.Text())

	oldValue, hadOldValue, err := client.Put(key, value)
	if err != nil {
		fmt.Printf("Error putting value: %v\n", err)
		return
	}

	if hadOldValue {
		fmt.Printf("Updated key '%s' successfully\n", key)
		fmt.Printf("Previous value: %s\n", oldValue)
	} else {
		fmt.Printf("Created new key '%s' successfully\n", key)
	}
}

func handleBatch(client *Client, scanner *bufio.Scanner) {
	fmt.Println("\n=== Batch Operations ===")
	fmt.Println("Enter key-value pairs (one per line)")
	fmt.Println("Format: PUT key value or GET key")
	fmt.Println("Enter 'done' when finished")

	operations := make([]string, 0)
	for {
		fmt.Print("> ")
		scanner.Scan()
		line := strings.TrimSpace(scanner.Text())

		if line == "done" {
			break
		}

		operations = append(operations, line)
	}

	if len(operations) == 0 {
		fmt.Println("No operations to perform")
		return
	}

	// Process operations
	fmt.Println("\n=== Results ===")
	for i, op := range operations {
		parts := strings.Fields(op)
		if len(parts) < 2 {
			fmt.Printf("[%d] Invalid format: %s\n", i+1, op)
			continue
		}

		cmd := strings.ToUpper(parts[0])
		key := parts[1]

		switch cmd {
		case "GET":
			value, timestamp, exists, err := client.Get(key)
			if err != nil {
				fmt.Printf("[%d] GET %s: Error: %v\n", i+1, key, err)
			} else if exists {
				fmt.Printf("[%d] GET %s: %s (Updated: %s)\n",
					i+1, key, value, formatTimestamp(timestamp))
			} else {
				fmt.Printf("[%d] GET %s: Not found\n", i+1, key)
			}

		case "PUT":
			if len(parts) < 3 {
				fmt.Printf("[%d] PUT requires a value\n", i+1)
				continue
			}

			value := strings.Join(parts[2:], " ")
			_, hadOldValue, err := client.Put(key, value)
			if err != nil {
				fmt.Printf("[%d] PUT %s: Error: %v\n", i+1, key, err)
			} else if hadOldValue {
				fmt.Printf("[%d] PUT %s: Updated successfully\n", i+1, key)
			} else {
				fmt.Printf("[%d] PUT %s: Created successfully\n", i+1, key)
			}

		default:
			fmt.Printf("[%d] Unknown command: %s\n", i+1, cmd)
		}
	}
}

func handleImport(client *Client, scanner *bufio.Scanner) {
	fmt.Print("Enter file path: ")
	scanner.Scan()
	path := strings.TrimSpace(scanner.Text())

	if path == "" {
		fmt.Println("File path cannot be empty")
		return
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	var data map[string]string
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		fmt.Printf("Failed to parse JSON: %v\n", err)
		return
	}

	fmt.Printf("Importing %d key-value pairs...\n", len(data))

	success := 0
	for key, value := range data {
		_, _, err := client.Put(key, value)
		if err != nil {
			fmt.Printf("Failed to import key '%s': %v\n", key, err)
		} else {
			success++
		}
	}

	fmt.Printf("Import complete: %d/%d successful\n", success, len(data))
}

func handleExport(client *Client, scanner *bufio.Scanner) {
	fmt.Println("Note: Export functionality is limited as the API doesn't support listing all keys.")
	fmt.Print("Enter comma-separated keys to export: ")
	scanner.Scan()
	keysInput := strings.TrimSpace(scanner.Text())

	if keysInput == "" {
		fmt.Println("No keys specified")
		return
	}

	keys := strings.Split(keysInput, ",")
	data := make(map[string]string)

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		value, _, exists, err := client.Get(key)
		if err != nil {
			fmt.Printf("Failed to get key '%s': %v\n", key, err)
		} else if exists {
			data[key] = value
		} else {
			fmt.Printf("Key '%s' not found\n", key)
		}
	}

	if len(data) == 0 {
		fmt.Println("No data to export")
		return
	}

	fmt.Print("Enter output file path: ")
	scanner.Scan()
	path := strings.TrimSpace(scanner.Text())

	if path == "" {
		fmt.Println("File path cannot be empty")
		return
	}

	file, err := os.Create(path)
	if err != nil {
		fmt.Printf("Failed to create file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		fmt.Printf("Failed to write JSON: %v\n", err)
		return
	}

	fmt.Printf("Successfully exported %d key-value pairs to %s\n", len(data), path)
}

func handleSwitchServer(client *Client, scanner *bufio.Scanner) {
	fmt.Println("Current server addresses:")
	for i, addr := range client.config.ServerAddresses {
		marker := " "
		if i == client.current {
			marker = "*"
		}
		fmt.Printf("%s %d: %s\n", marker, i+1, addr)
	}

	fmt.Print("Enter new server index (or 'add' to add a new server): ")
	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())

	if input == "add" {
		fmt.Print("Enter new server address: ")
		scanner.Scan()
		addr := strings.TrimSpace(scanner.Text())

		if addr == "" {
			fmt.Println("Server address cannot be empty")
			return
		}

		client.config.ServerAddresses = append(client.config.ServerAddresses, addr)
		fmt.Printf("Added server %s\n", addr)
		return
	}

	var idx int
	if _, err := fmt.Sscanf(input, "%d", &idx); err != nil {
		fmt.Println("Invalid index")
		return
	}

	idx-- // Convert to 0-based
	if idx < 0 || idx >= len(client.config.ServerAddresses) {
		fmt.Println("Index out of range")
		return
	}

	client.current = idx
	if err := client.connectToServer(client.config.ServerAddresses[idx]); err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		return
	}

	fmt.Printf("Switched to server at %s\n", client.config.ServerAddresses[idx])
}
