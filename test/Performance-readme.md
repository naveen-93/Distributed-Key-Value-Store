# Distributed Key-Value Store: Performance Testing

This repository contains a comprehensive performance testing suite for a distributed key-value store implemented in Go. The testing framework allows you to evaluate various performance aspects of the system under different workloads, distribution patterns, and failure scenarios.

## Overview

The performance test suite evaluates the following aspects of the distributed key-value store:
- Performance with uniform key distribution
- Performance with hot/cold key distribution (Zipfian)
- Performance with multiple concurrent clients
- Impact of replication latency
- Performance with different value sizes
- Behavior during node failure scenarios

## Prerequisites

- Go 1.16 or higher
- A running distributed key-value store with 3 nodes on ports 50051, 50052, and 50053
- Protocol buffer compiler (protoc)

## Running the Tests

To run all the performance tests, use the following command from the main folder:

```bash
go test -bench=. -benchtime=10s ./test/performance_test.go
```

This will run each benchmark for 10 seconds (customizable) and report the average latency and throughput for each test scenario.

## Available Benchmarks

### BenchmarkUniform

Tests performance with uniform key distribution across all keys. This benchmark evaluates:
- Read-heavy workload
- Write-heavy workload
- Mixed workload (80% reads, 20% writes by default)

### BenchmarkHotCold

Tests performance with a Zipfian distribution, where a small subset of keys receive most of the requests. This simulates real-world access patterns where some data is "hot" and accessed frequently. The benchmark evaluates:
- Read-heavy workload with hot spots
- Write-heavy workload with hot spots
- Mixed workload with hot spots

### BenchmarkMultiClient

Tests the system's performance under load from multiple concurrent clients. By default, it uses 10 concurrent clients distributed across the available server nodes. This benchmark evaluates:
- Read-heavy workload with multiple clients
- Write-heavy workload with multiple clients
- Mixed workload with multiple clients

### BenchmarkReplicationLatency

Measures the latency impact of replication by testing with different key patterns:
- Sequential keys
- Random keys

### BenchmarkLargeValues

Tests the system's performance with increasingly large value sizes:
- 1KB values
- 10KB values
- 100KB values
- 1MB values

### BenchmarkNodeFailure

Tests the system's performance during a simulated node failure. This test requires manual intervention to stop one of the servers during execution.

## Configuration

The test suite can be configured by modifying the constants at the top of the file:

```go
const (
    keyCount       = 1000        // Number of unique keys to use
    keyPrefix      = "test-key-"  // Prefix for generated keys
    valueSize      = 100         // Default size of values in bytes
    benchmarkTime  = 10 * time.Second  // Duration of each benchmark
    clientCount    = 10          // Number of concurrent clients
    readPercentage = 80          // For mixed workload (80% reads, 20% writes)

    // Zipfian distribution parameters
    zipfS    = 1.1          // Higher values make distribution more skewed
    zipfV    = 1.0          // First element rank
    zipfImax = keyCount - 1 // Maximum element rank
)
```

## Test Architecture

- **Client**: Represents a connection to one of the key-value store servers
- **WorkloadType**: Defines the type of workload (ReadHeavy, WriteHeavy, Mixed)
- **DistributionType**: Defines how keys are selected (Uniform, HotCold)

## Key Functions

- `setupClient`: Creates clients connected to multiple servers
- `generateKeys`: Creates a slice of unique keys
- `generateValue`: Creates a random string of specified size
- `populateStore`: Pre-populates the store with keys for tests
- `selectKey`: Chooses a key based on the distribution type
- `runWorkload`: Executes the specified workload and returns metrics

## Metrics Reported

For each benchmark, the following metrics are reported:
- **Average Latency** (ms/op): The average time taken to complete a single operation
- **Throughput** (ops/sec): The number of operations completed per second

## Extending the Tests

To add new benchmarks or modify existing ones:

1. Create a new function with the `Benchmark` prefix
2. Use the existing helper functions to set up clients and run workloads
3. Report metrics using `b.ReportMetric()`

## Notes

- The `BenchmarkNodeFailure` test requires manual intervention to stop one of the servers during execution
- The `BenchmarkConsistencyLevel` test is a placeholder and is skipped by default
- For production testing, consider running these benchmarks in an environment that closely mirrors your production setup

