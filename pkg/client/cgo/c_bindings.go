package client

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"strings"
	"sync"
	"unsafe"

	
	"fmt"
	
)

var (
	clientsMu sync.Mutex
	clients   = make(map[unsafe.Pointer]*client.Client)
)

//export kv_init
func kv_init(serverList **C.char) C.int {
	// Convert C string array to Go string slice
	var servers []string
	for ptr := serverList; *ptr != nil; ptr = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + unsafe.Sizeof(uintptr(0)))) {
		servers = append(servers, C.GoString(*ptr))
	}

	// Create new client
	client, err := NewClient(servers)
	if err != nil {
		return -1
	}

	// Store client in global map
	clientsMu.Lock()
	clients[unsafe.Pointer(serverList)] = client
	clientsMu.Unlock()

	return 0
}

//export kv_shutdown
func kv_shutdown() C.int {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for key, client := range clients {
		if err := client.Close(); err != nil {
			return -1
		}
		delete(clients, key)
	}

	return 0
}

//export kv_get
func kv_get(key *C.char, value *C.char) C.int {
	// Add bounds checking
	if value == nil {
		return -1
	}

	goKey := C.GoString(key)
	if len(goKey) > 128 {
		return -1
	}

	// Get client (using first client for simplicity)
	clientsMu.Lock()
	var client *Client
	for _, c := range clients {
		client = c
		break
	}
	clientsMu.Unlock()

	if client == nil {
		return -1
	}

	// Perform get operation
	val, exists, err := client.Get(goKey)
	if err != nil {
		return -1
	}

	if !exists {
		return 1
	}

	// Use safer string copy
	cValue := C.CString(val)
	defer C.free(unsafe.Pointer(cValue))
	C.strncpy(value, cValue, 2048)

	return 0
}

//export kv_put
func kv_put(key *C.char, value *C.char, oldValue *C.char) C.int {
	// Validate key and value
	goKey := C.GoString(key)
	goValue := C.GoString(value)

	if err := validateKey(goKey); err != nil {
		return -1
	}
	if err := validateValue(goValue); err != nil {
		return -1
	}

	// Get client
	clientsMu.Lock()
	var client *Client
	for _, c := range clients {
		client = c
		break
	}
	clientsMu.Unlock()

	if client == nil {
		return -1
	}

	// Perform put operation
	old, hadOld, err := client.Put(goKey, goValue)
	if err != nil {
		return -1
	}

	// Copy old value if it existed
	if hadOld {
		if len(old) > 2048 {
			return -1
		}
		C.strncpy(oldValue, C.CString(old), 2048)
		return 0
	}

	return 1
}

// // Helper function to validate keys
func validateKey(key string) error {
	if len(key) > 128 {
		return fmt.Errorf("key too long (max 128 bytes)")
	}

	for _, r := range key {
		if r < 32 || r > 126 || r == '[' || r == ']' {
			return fmt.Errorf("invalid character in key")
		}
	}
	return nil
}

// Helper function to validate values
func validateValue(value string) error {
	if len(value) > 2048 {
		return fmt.Errorf("value too long (max 2048 bytes)")
	}

	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value")
		}
	}

	// Check for UU encoding
	if isUUEncoded(value) {
		return fmt.Errorf("UU encoded values not allowed")
	}

	return nil
}

// Helper function to check for UU encoding
func isUUEncoded(s string) bool {
	// Simple heuristic: UU encoded data typically starts with 'begin'
	// and contains mostly base64-like characters
	return strings.HasPrefix(strings.ToLower(s), "begin ")
}
