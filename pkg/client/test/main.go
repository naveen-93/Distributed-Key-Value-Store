package main

import (
	"fmt"
	"log"
	"sync"

	"Distributed-Key-Value-Store/pkg/client"
)

func main() {
	fmt.Println("=== Key-Value Store Interoperability Tests ===")

	// Run sequential tests
	fmt.Println("\nRunning Sequential Tests...")
	runSequentialTests()

	// Run concurrent tests
	fmt.Println("\nRunning Concurrent Tests...")
	runConcurrentTests()

	// Run failure recovery tests
	// fmt.Println("\nRunning Failure Recovery Tests...")
	// runFailureTests()
}

func runSequentialTests() {
	c, err := client.NewClient([]string{"localhost:50051"})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Test basic operations
	testBasicOperations(c)

	// Test invalid inputs
	testInvalidInputs(c)

	// Test boundary conditions
	testBoundaryConditions(c)
}

func runConcurrentTests() {
	fmt.Println("  Testing concurrent operations...")
	c, err := client.NewClient([]string{"localhost:50051"})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent_key_%d", id)
			value := fmt.Sprintf("concurrent_value_%d", id)

			// Test concurrent puts
			_, _, err := c.Put(key, value)
			if err != nil {
				log.Printf("  Concurrent put failed for %d: %v", id, err)
				return
			}

			// Test concurrent gets
			val, exists, err := c.Get(key)
			if err != nil {
				log.Printf("  Concurrent get failed for %d: %v", id, err)
				return
			}
			if !exists || val != value {
				log.Printf("  Concurrent consistency check failed for %d", id)
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	fmt.Printf("  Concurrent tests completed: %d/10 operations successful\n", successCount)
}

func testBasicOperations(c *client.Client) {
	fmt.Println("  Testing basic operations...")

	// Test Put
	key, value := "test_key", "test_value"
	_, _, err := c.Put(key, value)
	if err != nil {
		log.Printf("Put failed: %v", err)
	} else {
		fmt.Printf("  Put successful: key=%s, value=%s\n", key, value)
	}

	// Test Get
	retrievedValue, exists, err := c.Get(key)
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else if !exists {
		log.Printf("Key not found when it should exist")
	} else if retrievedValue != value {
		log.Printf("Value mismatch: expected %s, got %s", value, retrievedValue)
	} else {
		fmt.Printf("  Get successful: key=%s, value=%s\n", key, retrievedValue)
	}
}

func testInvalidInputs(c *client.Client) {
	fmt.Println("  Testing invalid inputs...")

	// Test empty key
	_, _, err := c.Put("", "empty_key_test")
	if err != nil {
		fmt.Printf("  Empty key test passed: %v\n", err)
	}

	// Test key too long (>128 bytes)
	longKey := string(make([]byte, 129))
	_, _, err = c.Put(longKey, "long_key_test")
	if err != nil {
		fmt.Printf("  Long key test passed: %v\n", err)
	}

	// Test value too long (>2048 bytes)
	longValue := string(make([]byte, 2049))
	_, _, err = c.Put("key", longValue)
	if err != nil {
		fmt.Printf("  Long value test passed: %v\n", err)
	}
}

func testBoundaryConditions(c *client.Client) {
	fmt.Println("  Testing boundary conditions...")

	// Test maximum allowed key size
	maxKey := string(make([]byte, 128))
	_, _, err := c.Put(maxKey, "max_key_test")
	if err == nil {
		fmt.Println("  Max key size test passed")
	}

	// Test maximum allowed value size
	maxValue := string(make([]byte, 2048))
	_, _, err = c.Put("max_value_key", maxValue)
	if err == nil {
		fmt.Println("  Max value size test passed")
	}
}

// func runFailureTests() {
// 	c, err := client.NewClient([]string{
// 		"localhost:50051",
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create client: %v", err)
// 	}
// 	defer c.Close()

// 	// Test operations with some servers down
// 	key := "failure_test_key"
// 	value := "failure_test_value"

// 	_, _, err = c.Put(key, value)
// 	if err != nil {
// 		fmt.Printf("  Failure test - Put failed: %v\n", err)
// 	} else {
// 		fmt.Println("  Failure test - Put succeeded")
// 	}

// 	// Wait briefly to ensure replication
// 	time.Sleep(time.Second)

// 	// Test get after failure
// 	retrievedValue, exists, err := c.Get(key)
// 	if err != nil {
// 		fmt.Printf("  Failure test - Get failed: %v\n", err)
// 	} else if !exists {
// 		fmt.Println("  Failure test - Key not found")
// 	} else if retrievedValue != value {
// 		fmt.Printf("  Failure test - Value mismatch: expected %s, got %s\n",
// 			value, retrievedValue)
// 	} else {
// 		fmt.Println("  Failure test - Get succeeded")
// 	}
// }
