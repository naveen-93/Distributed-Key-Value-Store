package kvclient

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Helper function to print the formatted timestamp
func formatTimestamp(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format(time.RFC3339)
}

// HandleGet processes a Get operation from user input
func HandleGet(client *Client, scanner *bufio.Scanner) {
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

// HandlePut processes a Put operation from user input
func HandlePut(client *Client, scanner *bufio.Scanner) {
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

// HandleBatch processes batch operations from user input
func HandleBatch(client *Client, scanner *bufio.Scanner) {
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
		processOperation(client, op, i)
	}
}

// processOperation handles a single operation from the batch
func processOperation(client *Client, op string, index int) {
	parts := strings.Fields(op)
	if len(parts) < 2 {
		fmt.Printf("[%d] Invalid format: %s\n", index+1, op)
		return
	}

	cmd := strings.ToUpper(parts[0])
	key := parts[1]

	switch cmd {
	case "GET":
		value, timestamp, exists, err := client.Get(key)
		if err != nil {
			fmt.Printf("[%d] GET %s: Error: %v\n", index+1, key, err)
		} else if exists {
			fmt.Printf("[%d] GET %s: %s (Updated: %s)\n",
				index+1, key, value, formatTimestamp(timestamp))
		} else {
			fmt.Printf("[%d] GET %s: Not found\n", index+1, key)
		}

	case "PUT":
		if len(parts) < 3 {
			fmt.Printf("[%d] PUT requires a value\n", index+1)
			return
		}

		value := strings.Join(parts[2:], " ")
		_, hadOldValue, err := client.Put(key, value)
		if err != nil {
			fmt.Printf("[%d] PUT %s: Error: %v\n", index+1, key, err)
		} else if hadOldValue {
			fmt.Printf("[%d] PUT %s: Updated successfully\n", index+1, key)
		} else {
			fmt.Printf("[%d] PUT %s: Created successfully\n", index+1, key)
		}

	default:
		fmt.Printf("[%d] Unknown command: %s\n", index+1, cmd)
	}
}

// HandleImport imports key-value pairs from a JSON file
func HandleImport(client *Client, scanner *bufio.Scanner) {
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

// HandleExport exports key-value pairs to a JSON file
func HandleExport(client *Client, scanner *bufio.Scanner) {
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

// HandleSwitchServer allows the user to switch the current server
func HandleSwitchServer(client *Client, scanner *bufio.Scanner) {
	fmt.Println("Current server addresses:")
	addresses := client.GetServerAddresses()
	currentIdx := client.GetCurrentServerIndex()

	for i, addr := range addresses {
		marker := " "
		if i == currentIdx {
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

		client.AddServerAddress(addr)
		fmt.Printf("Added server %s\n", addr)
		return
	}

	var idx int
	if _, err := fmt.Sscanf(input, "%d", &idx); err != nil {
		fmt.Println("Invalid index")
		return
	}

	idx-- // Convert to 0-based
	if idx < 0 || idx >= len(addresses) {
		fmt.Println("Index out of range")
		return
	}

	client.SetCurrentServerIndex(idx)
	fmt.Printf("Switched to server %s\n", addresses[idx])
}
