#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Include the generated header file
#include "../libkv.h"

// Helper function to check results
void check_result(int result, const char* operation) {
    if (result == -1) {
        printf("ERROR: %s operation failed\n", operation);
    } else if (result == 0) {
        printf("SUCCESS: %s operation completed successfully\n", operation);
    } else if (result == 1) {
        printf("INFO: %s - key not found or new key created\n", operation);
    }
}

// Test basic operations
void test_basic_operations() {
    printf("\n=== Testing Basic Operations ===\n");
    
    // Buffer for values
    char value_buffer[2048] = {0};
    char old_value_buffer[2048] = {0};
    char *test_key = "test-key-1";
    char *test_value = "test-value-1";
    char *updated_value = "updated-value-1";
    
    // Test PUT operation for a new key
    int result = kv_put(test_key, test_value, old_value_buffer);
    check_result(result, "PUT (new key)");
    
    // Test GET operation for existing key
    memset(value_buffer, 0, sizeof(value_buffer));
    result = kv_get(test_key, value_buffer);
    check_result(result, "GET");
    printf("Retrieved value: %s\n", value_buffer);
    
    // Test PUT operation to update existing key
    memset(old_value_buffer, 0, sizeof(old_value_buffer));
    result = kv_put(test_key, updated_value, old_value_buffer);
    check_result(result, "PUT (update)");
    printf("Old value: %s\n", old_value_buffer);
    
    // Test GET operation for updated key
    memset(value_buffer, 0, sizeof(value_buffer));
    result = kv_get(test_key, value_buffer);
    check_result(result, "GET (after update)");
    printf("Retrieved updated value: %s\n", value_buffer);
    
    // Test GET for non-existent key
    memset(value_buffer, 0, sizeof(value_buffer));
    result = kv_get("non-existent-key", value_buffer);
    check_result(result, "GET (non-existent)");
}

// Test multiple operations in sequence
void test_multiple_operations() {
    printf("\n=== Testing Multiple Operations ===\n");
    
    char value_buffer[2048] = {0};
    char old_value_buffer[2048] = {0};
    int result;
    
    // Create multiple keys
    char *keys[] = {"multi-key-1", "multi-key-2", "multi-key-3"};
    char *values[] = {"multi-value-1", "multi-value-2", "multi-value-3"};
    
    for (int i = 0; i < 3; i++) {
        memset(old_value_buffer, 0, sizeof(old_value_buffer));
        result = kv_put(keys[i], values[i], old_value_buffer);
        check_result(result, "PUT (multiple)");
        printf("Put %s = %s\n", keys[i], values[i]);
    }
    
    // Retrieve all keys
    for (int i = 0; i < 3; i++) {
        memset(value_buffer, 0, sizeof(value_buffer));
        result = kv_get(keys[i], value_buffer);
        check_result(result, "GET (multiple)");
        printf("Get %s = %s\n", keys[i], value_buffer);
    }
}

// Test edge cases
void test_edge_cases() {
    printf("\n=== Testing Edge Cases ===\n");
    
    char value_buffer[2048] = {0};
    char old_value_buffer[2048] = {0};
    int result;
    
    // Test with empty key (should be valid)
    result = kv_put("", "empty-key-value", old_value_buffer);
    check_result(result, "PUT (empty key)");
    
    // Test with empty value (should be valid)
    result = kv_put("empty-value-key", "", old_value_buffer);
    check_result(result, "PUT (empty value)");
    
    // Verify empty value
    memset(value_buffer, 0, sizeof(value_buffer));
    result = kv_get("empty-value-key", value_buffer);
    check_result(result, "GET (empty value)");
    printf("Empty value length: %lu\n", strlen(value_buffer));
    
    // Test with long key (close to max length)
    char long_key[128] = {0};
    memset(long_key, 'a', 127);
    result = kv_put(long_key, "long-key-value", old_value_buffer);
    check_result(result, "PUT (long key)");
    
    // Test with long value (close to max length)
    char long_value[2048] = {0};
    memset(long_value, 'b', 2047);
    result = kv_put("long-value-key", long_value, old_value_buffer);
    check_result(result, "PUT (long value)");
    
    // Verify long value
    memset(value_buffer, 0, sizeof(value_buffer));
    result = kv_get("long-value-key", value_buffer);
    check_result(result, "GET (long value)");
    printf("Long value length: %lu\n", strlen(value_buffer));
}

int main() {
    printf("=== Key-Value Store Client Test ===\n");
    
    // Initialize the client with server addresses
    char *servers[] = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        NULL  // Null-terminated array
    };
    
    int init_result = kv_init(servers);
    if (init_result != 0) {
        printf("Failed to initialize client. Make sure servers are running.\n");
        return 1;
    }
    
    printf("Client initialized successfully.\n");
    
    // Run tests
    test_basic_operations();
    test_multiple_operations();
    test_edge_cases();
    
    // Shutdown the client
    kv_shutdown();
    printf("\nClient shut down successfully.\n");
    
    return 0;
}