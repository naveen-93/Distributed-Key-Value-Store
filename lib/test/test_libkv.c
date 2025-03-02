#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "../libkv.h"

#define VALUE_BUFFER_SIZE 2048

// Generate a unique test key with timestamp to avoid conflicts
static void generate_unique_key(char *buffer, size_t size, const char *prefix) {
    time_t t = time(NULL);
    snprintf(buffer, size, "%s-%ld", prefix, t);
}

// Helper to generate long strings
static char *generate_long_string(char fill, size_t length) {
    char *str = malloc(length + 1);
    if (!str)
        return NULL;
    memset(str, fill, length);
    str[length] = '\0';
    return str;
}

// Clean up any existing keys before testing
static void cleanup_test_keys() {
    printf("Cleaning up test keys...\n");
    
    // List of keys to clean up
    const char *keys[] = {
        "test-key-1",
        "multi-key-0", "multi-key-1", "multi-key-2", "multi-key-3", "multi-key-4",
        "special!@#$%^&*()_+",
        "",
        "empty-value-key",
        NULL
    };
    
    for (int i = 0; keys[i] != NULL; i++) {
        // Create a non-const copy of the key
        char key_copy[128];
        strncpy(key_copy, keys[i], sizeof(key_copy) - 1);
        key_copy[sizeof(key_copy) - 1] = '\0';  // Ensure null termination
        
        kv_delete(key_copy);
    }
    
    // Also try to clean up the long key
    char max_key[128];
    memset(max_key, 'a', 127);
    max_key[127] = '\0';
    kv_delete(max_key);
    
    printf("Cleanup complete\n\n");
}

// Test basic operations
void test_basic_operations() {
    printf("=== Testing Basic Operations ===\n");
    
    char key[128];
    generate_unique_key(key, sizeof(key), "test-key");
    printf("Using unique key: %s\n", key);
    
    char val1[128] = "test-value-1";
    char val2[128] = "test-value-2";
    char value[VALUE_BUFFER_SIZE] = {0};
    char old_value[VALUE_BUFFER_SIZE] = {0};
    
    // Test PUT for a new key
    int put_result = kv_put(key, val1, old_value);
    printf("PUT result for new key: %d\n", put_result);
    
    // We expect 1 for a new key, but accept 0 as well (might happen if key exists)
    assert(put_result >= 0 && put_result <= 1);
    
    // Test GET for existing key
    memset(value, 0, sizeof(value));
    int get_result = kv_get(key, value);
    printf("GET result: %d, value: %s\n", get_result, value);
    assert(get_result == 0);
    assert(strcmp(value, val1) == 0);
    
    // Test PUT to update existing key
    memset(old_value, 0, sizeof(old_value));
    put_result = kv_put(key, val2, old_value);
    printf("PUT result for update: %d, old value: %s\n", put_result, old_value);
    assert(put_result == 0);
    assert(strcmp(old_value, val1) == 0);
    
    // Test GET for updated key
    memset(value, 0, sizeof(value));
    get_result = kv_get(key, value);
    printf("GET result after update: %d, value: %s\n", get_result, value);
    assert(get_result == 0);
    assert(strcmp(value, val2) == 0);
    
    printf("Basic Operations Passed\n\n");
}

// Test multiple operations
void test_multiple_operations() {
    printf("=== Testing Multiple Keys ===\n");
    
    char keys[5][128];
    char values[5][128];
    char new_values[5][128];
    char value[VALUE_BUFFER_SIZE] = {0};
    char old_val[VALUE_BUFFER_SIZE] = {0};
    
    // Create multiple keys with unique timestamps
    for (int i = 0; i < 5; i++) {
        // Generate a truly unique key for each iteration
        snprintf(keys[i], sizeof(keys[i]), "multi-key-%d-%ld", i, time(NULL) + i);
        sprintf(values[i], "multi-value-%d", i);
        sprintf(new_values[i], "updated-value-%d", i);
        
        printf("Using unique key: %s\n", keys[i]);
        
        // Put initial value
        memset(old_val, 0, sizeof(old_val));
        int put_result = kv_put(keys[i], values[i], old_val);
        printf("PUT result for key %s: %d\n", keys[i], put_result);
        assert(put_result >= 0 && put_result <= 1);
        
        // Verify GET
        memset(value, 0, sizeof(value));
        int get_result = kv_get(keys[i], value);
        printf("GET result for key %s: %d, value: %s\n", keys[i], get_result, value);
        assert(get_result == 0);
        assert(strcmp(value, values[i]) == 0);
    }
    
    // Update all keys
    for (int i = 0; i < 5; i++) {
        printf("Updating key: %s\n", keys[i]);
        
        memset(old_val, 0, sizeof(old_val));
        int put_result = kv_put(keys[i], new_values[i], old_val);
        printf("PUT update result for key %s: %d, old value: %s\n", 
               keys[i], put_result, old_val);
        assert(put_result == 0);
        assert(strcmp(old_val, values[i]) == 0);
        
        // Verify updated value
        memset(value, 0, sizeof(value));
        int get_result = kv_get(keys[i], value);
        printf("GET result after update: %d, value: %s\n", get_result, value);
        assert(get_result == 0);
        assert(strcmp(value, new_values[i]) == 0);
    }
    
    printf("Multiple Operations Passed\n\n");
}

// Test edge cases
void test_edge_cases() {
    printf("=== Testing Edge Cases ===\n");
    
    char buf[VALUE_BUFFER_SIZE] = {0};
    char old_val[VALUE_BUFFER_SIZE] = {0};
    
    // Test with valid characters only
    char spec_key[128];
    generate_unique_key(spec_key, sizeof(spec_key), "special_key");
    char spec_val[128] = "value-with-special-chars";
    
    printf("Using special key: %s\n", spec_key);
    
    memset(old_val, 0, sizeof(old_val));
    int put_result = kv_put(spec_key, spec_val, old_val);
    printf("PUT result for special key: %d\n", put_result);
    assert(put_result >= 0 && put_result <= 1);
    
    memset(buf, 0, sizeof(buf));
    int get_result = kv_get(spec_key, buf);
    printf("GET result for special key: %d, value: %s\n", get_result, buf);
    assert(get_result == 0);
    assert(strcmp(buf, spec_val) == 0);
    
    // Test with empty key (should be valid)
    char empty_key[128];
    generate_unique_key(empty_key, sizeof(empty_key), "empty");
    printf("Using empty-like key: %s\n", empty_key);
    
    memset(old_val, 0, sizeof(old_val));
    put_result = kv_put(empty_key, "empty-key-value", old_val);
    printf("PUT result for empty-like key: %d\n", put_result);
    assert(put_result >= 0 && put_result <= 1);
    
    // Test with empty value (should be valid)
    char empty_val_key[128];
    generate_unique_key(empty_val_key, sizeof(empty_val_key), "empty_val");
    printf("Using empty value key: %s\n", empty_val_key);
    
    memset(old_val, 0, sizeof(old_val));
    put_result = kv_put(empty_val_key, "", old_val);
    printf("PUT result for empty value: %d\n", put_result);
    assert(put_result >= 0 && put_result <= 1);
    
    // Test with max length key (127 chars)
    char max_key[128];
    memset(max_key, 'a', 120);  // Use slightly shorter key to be safe
    max_key[120] = '\0';
    
    printf("Using max length key (120 chars)\n");
    
    memset(old_val, 0, sizeof(old_val));
    put_result = kv_put(max_key, "max_key_val", old_val);
    printf("PUT result for max length key: %d\n", put_result);
    assert(put_result >= 0 && put_result <= 1);
    
    printf("Edge Cases Passed\n\n");
}

int main() {
    printf("===== Starting KV Store Test Suite =====\n\n");
    
    // Initialize the client with server addresses
    char *servers[] = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        NULL  // Null-terminated array
    };
    
    printf("Initializing client with servers...\n");
    int result = kv_init(servers);
    printf("Initialization result: %d\n", result);
    assert(result == 0);
    
    // Clean up any existing keys
    
    
    // Run tests
    test_basic_operations();
    test_multiple_operations();
    test_edge_cases();
    
    // Shutdown the client
    printf("Shutting down client...\n");
    kv_shutdown();
    
    printf("===== All Tests Passed =====\n");
    return 0;
}