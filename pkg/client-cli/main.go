package main

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"Distributed-Key-Value-Store/pkg/client-cli/kvclient"
)

func main() {
	// Configuration
	config := kvclient.ClientConfig{
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
	client, err := kvclient.NewClient(config)
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
			kvclient.HandleGet(client, scanner)
		case "2":
			kvclient.HandlePut(client, scanner)
		case "3":
			kvclient.HandleBatch(client, scanner)
		case "4":
			kvclient.HandleImport(client, scanner)
		case "5":
			kvclient.HandleExport(client, scanner)
		case "6":
			kvclient.HandleSwitchServer(client, scanner)
		case "7":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid choice, please try again")
		}
	}
}
