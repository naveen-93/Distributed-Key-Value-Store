#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include "../libkv.h"

#define NUM_THREADS 10

// Structure to pass data to threads
typedef struct {
    int id;
    int success;
} thread_data_t;

// Mutex for thread-safe printing
pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

// Function that each thread will run
void* concurrent_test(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    char key[32], value[32], retrieved[2049] = {0}, old_value[2049] = {0};

    // Create unique key-value pair for this thread
    snprintf(key, sizeof(key), "concurrent_key_%d", data->id);
    snprintf(value, sizeof(value), "concurrent_value_%d", data->id);

    // Test put
    int result = kv_put(key, value, old_value);
    if (result < 0) {
        pthread_mutex_lock(&print_mutex);
        printf("  Thread %d: Put failed\n", data->id);
        pthread_mutex_unlock(&print_mutex);
        return NULL;
    }

    // Test get
    result = kv_get(key, retrieved);
    if (result != 0 || strcmp(retrieved, value) != 0) {
        pthread_mutex_lock(&print_mutex);
        printf("  Thread %d: Get failed or value mismatch\n", data->id);
        pthread_mutex_unlock(&print_mutex);
        return NULL;
    }

    data->success = 1;
    return NULL;
}

void test_concurrent_operations() {
    printf("\nTesting concurrent operations...\n");

    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];
    int success_count = 0;

    // Create threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].id = i;
        thread_data[i].success = 0;
        if (pthread_create(&threads[i], NULL, concurrent_test, &thread_data[i]) != 0) {
            printf("  Failed to create thread %d\n", i);
            return;
        }
    }

    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (thread_data[i].success) {
            success_count++;
        }
    }

    printf("  Concurrent tests completed: %d/%d operations successful\n",
           success_count, NUM_THREADS);
}

void test_basic_operations() {
    printf("Testing basic operations...\n");

    // Test Put and Get
    char *key = "test_key";
    char *value = "test_value";
    char old_value[2049] = {0};
    char retrieved_value[2049] = {0};

    int result = kv_put(key, value, old_value);
    if (result < 0) {
        printf("  Put failed\n");
        return;
    }
    printf("  Put successful: key=%s, value=%s\n", key, value);

    result = kv_get(key, retrieved_value);
    if (result == 0) {
        printf("  Get successful: key=%s, value=%s\n", key, retrieved_value);
    } else {
        printf("  Get failed with code: %d\n", result);
    }
}

void test_invalid_inputs() {
    printf("\nTesting invalid inputs...\n");

    char old_value[2049] = {0};

    // Test empty key
    int result = kv_put("", "empty_key_test", old_value);
    if (result < 0) {
        printf("  Empty key test passed\n");
    }

    // Test key too long (>128 bytes)
    char long_key[130];
    memset(long_key, 'a', 129);
    long_key[129] = '\0';
    result = kv_put(long_key, "long_key_test", old_value);
    if (result < 0) {
        printf("  Long key test passed\n");
    }

    // Test value too long (>2048 bytes)
    char long_value[2050];
    memset(long_value, 'v', 2049);
    long_value[2049] = '\0';
    result = kv_put("key", long_value, old_value);
    if (result < 0) {
        printf("  Long value test passed\n");
    }
}

void test_boundary_conditions() {
    printf("\nTesting boundary conditions...\n");

    // Test maximum allowed key size (128 bytes)
    char max_key[129];
    memset(max_key, 'k', 128);
    max_key[128] = '\0';

    char old_value[2049] = {0};
    char retrieved_value[2049] = {0};

    int result = kv_put(max_key, "max_key_test", old_value);
    if (result >= 0) {
        result = kv_get(max_key, retrieved_value);
        if (result == 0 && strcmp(retrieved_value, "max_key_test") == 0) {
            printf("  Max key size test passed\n");
        }
    }

    // Test maximum allowed value size (2048 bytes)
    char max_value[2049];
    memset(max_value, 'v', 2048);
    max_value[2048] = '\0';

    result = kv_put("max_value_key", max_value, old_value);
    if (result >= 0) {
        result = kv_get("max_value_key", retrieved_value);
        if (result == 0 && strcmp(retrieved_value, max_value) == 0) {
            printf("  Max value size test passed\n");
        }
    }
}

int main() {
    printf("=== Key-Value Store Library Tests ===\n\n");

    // Initialize client
    char *servers[] = {"localhost:50051", NULL};
    if (kv_init(servers) != 0) {
        printf("Failed to initialize client\n");
        return 1;
    }
    printf("Client initialized successfully\n\n");

    // Run tests
    test_basic_operations();
    test_invalid_inputs();
    test_boundary_conditions();
    test_concurrent_operations();

    // Shutdown client
    if (kv_shutdown() != 0) {
        printf("\nShutdown failed\n");
        return 1;
    }
    printf("\nShutdown successful\n");

    return 0;
} 