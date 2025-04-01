package test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Test configuration
	keyCount       = 100
	keyPrefix      = "test-key-"
	valueSize      = 100
	benchmarkTime  = 10 * time.Second
	clientCount    = 10
	readPercentage = 80 // For mixed workload

	// Zipfian distribution parameters
	zipfS    = 1.1          // Higher values make distribution more skewed
	zipfV    = 1.0          // First element rank
	zipfImax = keyCount - 1 // Maximum element rank
)

// Client represents a connection to the key-value store
type Client struct {
	conn   *grpc.ClientConn
	client pb.KVStoreClient
}

// WorkloadType defines the type of workload to run
type WorkloadType int

const (
	ReadHeavy WorkloadType = iota
	WriteHeavy
	Mixed
)

// DistributionType defines how keys are selected
type DistributionType int

const (
	Uniform DistributionType = iota
	HotCold
)

// setupClient creates a client connected to multiple servers
func setupClient(t testing.TB) []*Client {
	serverAddrs := []string{":50051", ":50052", ":50053"}
	clients := make([]*Client, len(serverAddrs))

	for i, addr := range serverAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("Failed to connect to server at %s: %v", addr, err)
		}

		clients[i] = &Client{
			conn:   conn,
			client: pb.NewKVStoreClient(conn),
		}
	}

	return clients
}

// closeClients closes all client connections
func closeClients(clients []*Client) {
	for _, client := range clients {
		client.conn.Close()
	}
}

// generateKeys creates a slice of unique keys
func generateKeys() []string {
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("%s%d", keyPrefix, i)
	}
	return keys
}

// generateValue creates a random string of specified size
func generateValue(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// populateStore pre-populates the store with keys for Get tests
func populateStore(b *testing.B, client pb.KVStoreClient, keys []string) {
	ctx := context.Background()
	value := generateValue(valueSize)

	b.ResetTimer()
	for _, key := range keys {
		_, err := client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: value,
		})
		if err != nil {
			b.Fatalf("Failed to populate store with key %s: %v", key, err)
		}
	}
	b.StopTimer()
}

// selectKey chooses a key based on the distribution type
func selectKey(keys []string, dist DistributionType, zipfGen *rand.Zipf) string {
	switch dist {
	case Uniform:
		return keys[rand.Intn(len(keys))]
	case HotCold:
		// Use Zipfian distribution where a small percentage of keys get most requests
		idx := zipfGen.Uint64()
		return keys[idx]
	default:
		panic("Unknown distribution type")
	}
}

// runWorkload executes the specified workload and returns metrics
func runWorkload(
	b *testing.B,
	client pb.KVStoreClient,
	keys []string,
	workloadType WorkloadType,
	dist DistributionType,
	duration time.Duration,
) (int64, time.Duration) {
	ctx := context.Background()
	value := generateValue(valueSize)

	// Create Zipfian generator if needed
	var zipfGen *rand.Zipf
	if dist == HotCold {
		zipfGen = rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), zipfS, zipfV, uint64(zipfImax))
	}

	startTime := time.Now()
	endTime := startTime.Add(duration)

	var completedOps int64
	var totalLatency time.Duration

	for time.Now().Before(endTime) {
		var err error
		var start time.Time
		var latency time.Duration

		key := selectKey(keys, dist, zipfGen)

		// Determine operation type based on workload
		doRead := false
		switch workloadType {
		case ReadHeavy:
			doRead = true
		case WriteHeavy:
			doRead = false
		case Mixed:
			doRead = rand.Intn(100) < readPercentage
		}

		// Execute operation
		if doRead {
			start = time.Now()
			_, err = client.Get(ctx, &pb.GetRequest{Key: key})
			latency = time.Since(start)
		} else {
			start = time.Now()
			_, err = client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
			latency = time.Since(start)
		}

		if err != nil {
			b.Logf("Operation failed: %v", err)
			continue
		}

		completedOps++
		totalLatency += latency
	}

	return completedOps, totalLatency
}

// BenchmarkUniform tests performance with uniform key distribution
func BenchmarkUniform(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	keys := generateKeys()

	// Pre-populate store for read operations
	populateStore(b, clients[0].client, keys)

	workloads := []struct {
		name  string
		wType WorkloadType
	}{
		{"ReadHeavy", ReadHeavy},
		{"WriteHeavy", WriteHeavy},
		{"Mixed", Mixed},
	}

	for _, wl := range workloads {
		b.Run(wl.name, func(b *testing.B) {
			completedOps, totalLatency := runWorkload(
				b,
				clients[0].client,
				keys,
				wl.wType,
				Uniform,
				benchmarkTime,
			)

			avgLatency := totalLatency.Seconds() * 1000 / float64(completedOps) // in ms
			throughput := float64(completedOps) / benchmarkTime.Seconds()       // ops/sec

			b.ReportMetric(avgLatency, "ms/op")
			b.ReportMetric(throughput, "ops/sec")
		})
	}
}

// BenchmarkHotCold tests performance with hot/cold key distribution (Zipfian)
func BenchmarkHotCold(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	keys := generateKeys()

	// Pre-populate store for read operations
	populateStore(b, clients[0].client, keys)

	workloads := []struct {
		name  string
		wType WorkloadType
	}{
		{"ReadHeavy", ReadHeavy},
		{"WriteHeavy", WriteHeavy},
		{"Mixed", Mixed},
	}

	for _, wl := range workloads {
		b.Run(wl.name, func(b *testing.B) {
			completedOps, totalLatency := runWorkload(
				b,
				clients[0].client,
				keys,
				wl.wType,
				HotCold,
				benchmarkTime,
			)

			avgLatency := totalLatency.Seconds() * 1000 / float64(completedOps) // in ms
			throughput := float64(completedOps) / benchmarkTime.Seconds()       // ops/sec

			b.ReportMetric(avgLatency, "ms/op")
			b.ReportMetric(throughput, "ops/sec")
		})
	}
}

// BenchmarkMultiClient tests performance with multiple concurrent clients
func BenchmarkMultiClient(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	keys := generateKeys()

	// Pre-populate store for read operations
	populateStore(b, clients[0].client, keys)

	workloads := []struct {
		name  string
		wType WorkloadType
	}{
		{"ReadHeavy", ReadHeavy},
		{"WriteHeavy", WriteHeavy},
		{"Mixed", Mixed},
	}

	for _, wl := range workloads {
		b.Run(wl.name, func(b *testing.B) {
			var wg sync.WaitGroup
			var totalOps int64
			var totalLatency time.Duration
			var mu sync.Mutex

			// Create multiple client goroutines
			for i := 0; i < clientCount; i++ {
				wg.Add(1)
				go func(clientIdx int) {
					defer wg.Done()

					// Use round-robin to distribute clients across servers
					client := clients[clientIdx%len(clients)].client

					ops, latency := runWorkload(
						b,
						client,
						keys,
						wl.wType,
						Uniform,
						benchmarkTime,
					)

					mu.Lock()
					totalOps += ops
					totalLatency += latency
					mu.Unlock()
				}(i)
			}

			wg.Wait()

			avgLatency := totalLatency.Seconds() * 1000 / float64(totalOps) // in ms
			throughput := float64(totalOps) / benchmarkTime.Seconds()       // ops/sec

			b.ReportMetric(avgLatency, "ms/op")
			b.ReportMetric(throughput, "ops/sec")
		})
	}
}

// BenchmarkReplicationLatency measures the latency impact of different replication factors
func BenchmarkReplicationLatency(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	value := generateValue(valueSize)
	ctx := context.Background()

	// Test write latency with different key patterns
	patterns := []struct {
		name  string
		keyFn func(i int) string
	}{
		{"Sequential", func(i int) string { return fmt.Sprintf("seq-key-%d", i) }},
		{"Random", func(i int) string { return fmt.Sprintf("rnd-key-%d", rand.Intn(10000)) }},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			b.ResetTimer()

			var totalLatency time.Duration
			for i := 0; i < b.N; i++ {
				key := pattern.keyFn(i)

				start := time.Now()
				_, err := clients[0].client.Put(ctx, &pb.PutRequest{
					Key:   key,
					Value: value,
				})
				latency := time.Since(start)

				if err != nil {
					b.Fatalf("Put operation failed: %v", err)
				}

				totalLatency += latency
			}

			avgLatency := totalLatency.Seconds() * 1000 / float64(b.N) // in ms
			b.ReportMetric(avgLatency, "ms/op")
		})
	}
}

// BenchmarkConsistencyLevel tests performance with different consistency levels
// Note: This assumes your system supports configurable consistency levels
func BenchmarkConsistencyLevel(b *testing.B) {
	// This is a placeholder - implementation would depend on whether your
	// system supports configurable consistency levels through the API
	b.Skip("Consistency level benchmarking not implemented")
}

// BenchmarkNodeFailure tests performance during node failure scenarios
func BenchmarkNodeFailure(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	keys := generateKeys()

	// Pre-populate store
	populateStore(b, clients[0].client, keys)

	b.Run("DuringFailure", func(b *testing.B) {
		// Note: This test assumes you have a way to simulate node failure
		// You would need to implement a mechanism to take down one of your nodes
		// during the test and observe the impact on performance

		log.Println("This test requires manually stopping one of the servers during execution")
		log.Println("Please stop the server on port 50053 when prompted")
		log.Println("Press Enter to continue...")
		fmt.Scanln() // Wait for user input

		b.ResetTimer()

		completedOps, totalLatency := runWorkload(
			b,
			clients[0].client,
			keys,
			Mixed,
			Uniform,
			benchmarkTime,
		)

		avgLatency := totalLatency.Seconds() * 1000 / float64(completedOps) // in ms
		throughput := float64(completedOps) / benchmarkTime.Seconds()       // ops/sec

		b.ReportMetric(avgLatency, "ms/op")
		b.ReportMetric(throughput, "ops/sec")
	})
}

// BenchmarkLargeValues tests performance with different value sizes
func BenchmarkLargeValues(b *testing.B) {
	clients := setupClient(b)
	defer closeClients(clients)

	keys := generateKeys()[0:100] // Use fewer keys for large value test
	ctx := context.Background()

	valueSizes := []int{1024, 10 * 1024} // 1KB, 10KB, 100KB, 1MB

	for _, size := range valueSizes {
		b.Run(fmt.Sprintf("ValueSize_%dKB", size/1024), func(b *testing.B) {
			value := generateValue(size)

			b.ResetTimer()

			var totalLatency time.Duration
			for i := 0; i < b.N; i++ {
				key := keys[i%len(keys)]

				start := time.Now()
				_, err := clients[0].client.Put(ctx, &pb.PutRequest{
					Key:   key,
					Value: value,
				})
				latency := time.Since(start)

				if err != nil {
					b.Fatalf("Put operation failed: %v", err)
				}

				totalLatency += latency
			}

			avgLatency := totalLatency.Seconds() * 1000 / float64(b.N) // in ms
			b.ReportMetric(avgLatency, "ms/op")
		})
	}
}
