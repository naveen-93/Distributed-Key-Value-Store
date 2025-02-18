package raft

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestConfig contains test configuration
type TestConfig struct {
	NodeCount     int
	Commands      int
	ConcurrentOps int
	TestDuration  time.Duration
}

// TestCluster represents a test Raft cluster
type TestCluster struct {
	nodes    []*Raft
	configs  []Config
	t        testing.TB
	cleanups []func()
}

// Setup helper functions

func getFreePorts(n int) ([]string, error) {
	ports := make([]string, n)
	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}
		defer listener.Close()
		ports[i] = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

func setupCluster(t *testing.T, n int) []*Raft {
	ports, err := getFreePorts(n)
	require.NoError(t, err)

	nodes := make([]*Raft, n)
	peers := make(map[string]string)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i)
		peers[id] = fmt.Sprintf("localhost:%s", ports[i])
	}

	for i := 0; i < n; i++ {
		config := Config{
			ID:               fmt.Sprintf("node%d", i),
			Peers:            peers,
			DataDir:          fmt.Sprintf("testdata/node%d", i),
			RPCPort:          ports[i],
			HeartbeatTimeout: 50 * time.Millisecond,
			ElectionTimeout:  150 * time.Millisecond,
		}

		node, err := NewRaft(config)
		require.NoError(t, err)
		nodes[i] = node
	}

	return nodes
}

func setupTestCluster(t testing.TB, nodeCount int) *TestCluster {
	cluster := &TestCluster{t: t}
	testDir := t.TempDir()

	// Get free ports first
	ports, err := getFreePorts(nodeCount)
	require.NoError(t, err)

	// Create configs
	for i := 0; i < nodeCount; i++ {
		config := Config{
			ID:               fmt.Sprintf("node%d", i),
			DataDir:          filepath.Join(testDir, fmt.Sprintf("node%d", i)),
			RPCPort:          ports[i],
			HeartbeatTimeout: 50 * time.Millisecond,
			ElectionTimeout:  150 * time.Millisecond,
		}

		// Setup peer map
		config.Peers = make(map[string]string)
		for j := 0; j < nodeCount; j++ {
			config.Peers[fmt.Sprintf("node%d", j)] = fmt.Sprintf("localhost:%s", ports[j])
		}

		cluster.configs = append(cluster.configs, config)
	}

	// Create and start nodes
	for _, config := range cluster.configs {
		node, err := NewRaft(config)
		require.NoError(t, err)
		cluster.nodes = append(cluster.nodes, node)
		cluster.cleanups = append(cluster.cleanups, func() {
			node.Stop()
			os.RemoveAll(config.DataDir)
		})
	}

	return cluster
}

func (tc *TestCluster) cleanup() {
	for _, cleanup := range tc.cleanups {
		cleanup()
	}
}

func (tc *TestCluster) waitForLeader(timeout time.Duration) (*Raft, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range tc.nodes {
			if node.getState() == Leader {
				// Wait a bit to ensure stability
				time.Sleep(50 * time.Millisecond)
				if node.getState() == Leader {
					return node, nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader elected within timeout")
}

// Correctness Tests

func TestBasicElection(t *testing.T) {
	cluster := setupTestCluster(t, 3)
	defer cluster.cleanup()

	// Wait for leader election
	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, Leader, leader.getState())

	// Verify only one leader
	leaderCount := 0
	for _, node := range cluster.nodes {
		if node.getState() == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount)
}

func TestLogReplication(t *testing.T) {
	cluster := setupTestCluster(t, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Submit commands
	commands := []string{"cmd1", "cmd2", "cmd3"}
	for _, cmd := range commands {
		_, err := leader.Submit(cmd)
		require.NoError(t, err)
	}

	// Wait for replication with timeout
	success := false
	for i := 0; i < 10; i++ { // Try for up to 1 second
		time.Sleep(100 * time.Millisecond)

		allReplicated := true
		for _, node := range cluster.nodes {
			node.mu.RLock()
			if len(node.log) < len(commands) {
				allReplicated = false
			}
			node.mu.RUnlock()
		}

		if allReplicated {
			success = true
			break
		}
	}
	require.True(t, success, "Failed to replicate logs to all nodes")

	// Verify log replication
	for _, node := range cluster.nodes {
		node.mu.RLock()
		realLog := node.log[1:]
		assert.Equal(t, len(commands), len(realLog), "Log length mismatch")
		for i, cmd := range commands {
			assert.Equal(t, cmd, string(realLog[i].Command), "Command mismatch")
		}
		node.mu.RUnlock()
	}
}

func TestLeaderFailure(t *testing.T) {
	cluster := setupTestCluster(t, 5)
	defer cluster.cleanup()

	// Get initial leader
	leader1, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Stop leader
	leader1.Stop()

	// Wait for new leader
	leader2, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)
	assert.NotEqual(t, leader1.config.ID, leader2.config.ID)

	// Verify cluster still functional
	_, err = leader2.Submit("test-after-failure")
	require.NoError(t, err)
}

func TestNetworkPartition(t *testing.T) {
	cluster := setupTestCluster(t, 5)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Simulate network partition
	partitionedNodes := cluster.nodes[3:]
	for _, node := range partitionedNodes {
		for _, client := range node.peerClients {
			if conn, ok := client.(interface{ GetClientConn() *grpc.ClientConn }); ok {
				conn.GetClientConn().Close()
			}
		}
	}

	// Verify leader can still operate with majority
	_, err = leader.Submit("test-during-partition")
	require.NoError(t, err)
}

// Performance Tests

func BenchmarkThroughput(b *testing.B) {
	cluster := setupTestCluster(b, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := leader.Submit(fmt.Sprintf("cmd-%d", i))
		require.NoError(b, err)
	}
}

func BenchmarkConcurrentWrites(b *testing.B) {
	cluster := setupTestCluster(b, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(b, err)

	var wg sync.WaitGroup
	concurrency := 10

	b.ResetTimer()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < b.N/concurrency; j++ {
				_, err := leader.Submit(fmt.Sprintf("cmd-%d-%d", id, j))
				require.NoError(b, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestZipfianWorkload(t *testing.T) {
	cluster := setupTestCluster(t, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Create Zipfian distribution
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.5, 1.0, 1000)

	// Run workload
	start := time.Now()
	operations := 1000
	latencies := make([]time.Duration, operations)

	for i := 0; i < operations; i++ {
		key := zipf.Uint64()
		cmdStart := time.Now()
		_, err := leader.Submit(fmt.Sprintf("set-key-%d", key))
		require.NoError(t, err)
		latencies[i] = time.Since(cmdStart)
	}

	// Calculate statistics
	var totalLatency time.Duration
	for _, lat := range latencies {
		totalLatency += lat
	}
	avgLatency := totalLatency / time.Duration(operations)
	throughput := float64(operations) / time.Since(start).Seconds()

	t.Logf("Zipfian Workload Results:")
	t.Logf("Average Latency: %v", avgLatency)
	t.Logf("Throughput: %.2f ops/sec", throughput)
}

// Recovery Tests

func TestLogRecoveryAfterRestart(t *testing.T) {
	cluster := setupTestCluster(t, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Submit some commands
	commands := []string{"cmd1", "cmd2", "cmd3"}
	for _, cmd := range commands {
		_, err := leader.Submit(cmd)
		require.NoError(t, err)
	}

	// Restart all nodes
	for i, node := range cluster.nodes {
		node.Stop()
		newNode, err := NewRaft(cluster.configs[i])
		require.NoError(t, err)
		cluster.nodes[i] = newNode
	}

	// Wait for new leader
	newLeader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Verify log recovery
	for _, node := range cluster.nodes {
		assert.Equal(t, len(commands), len(node.log))
		for i, cmd := range commands {
			assert.Equal(t, cmd, string(node.log[i].Command))
		}
	}

	// Verify cluster is still functional
	_, err = newLeader.Submit("test-after-restart")
	require.NoError(t, err)
}

func TestConsistencyUnderLoad(t *testing.T) {
	cluster := setupTestCluster(t, 3)
	defer cluster.cleanup()

	leader, err := cluster.waitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Run concurrent operations with random failures
	var wg sync.WaitGroup
	ops := 100
	concurrent := 5

	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				// Randomly stop and restart nodes
				if rand.Float32() < 0.1 {
					nodeIdx := rand.Intn(len(cluster.nodes))
					cluster.nodes[nodeIdx].Stop()
					time.Sleep(100 * time.Millisecond)
					newNode, err := NewRaft(cluster.configs[nodeIdx])
					require.NoError(t, err)
					cluster.nodes[nodeIdx] = newNode
				}

				// Submit command
				cmd := fmt.Sprintf("cmd-%d-%d", id, j)
				_, err := leader.Submit(cmd)
				if err != nil {
					// Leader might have changed, find new leader
					newLeader, err := cluster.waitForLeader(5 * time.Second)
					require.NoError(t, err)
					leader = newLeader
					_, err = leader.Submit(cmd)
					require.NoError(t, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify consistency
	for i := 1; i < len(cluster.nodes); i++ {
		assert.Equal(t, len(cluster.nodes[0].log), len(cluster.nodes[i].log))
		for j := range cluster.nodes[0].log {
			assert.Equal(t, cluster.nodes[0].log[j].Command, cluster.nodes[i].log[j].Command)
		}
	}
}
