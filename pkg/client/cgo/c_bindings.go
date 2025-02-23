package client

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"strings"
	"sync"
	"unsafe"

	"fmt"

	"Distributed-Key-Value-Store/pkg/client"
)

var (
	globalClient *client.Client
	globalMu     sync.Mutex
)

//export kv_init
func kv_init(serverList **C.char) C.int {
	globalMu.Lock()
	defer globalMu.Unlock()

	// Check if already initialized
	if globalClient != nil {
		return C.int(-1)
	}

	// Convert C string array to Go string slice
	var servers []string
	for ptr := serverList; *ptr != nil; ptr = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + unsafe.Sizeof(uintptr(0)))) {
		servers = append(servers, C.GoString(*ptr))
	}

	// Create new client
	var err error
	globalClient, err = client.NewClient(servers)
	if err != nil {
		return C.int(-1)
	}

	return C.int(0)
}

//export kv_shutdown
func kv_shutdown() C.int {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalClient == nil {
		return C.int(-1)
	}

	if err := globalClient.Close(); err != nil {
		return C.int(-1)
	}

	globalClient = nil
	return C.int(0)
}


//export kv_get
func kv_get(key *C.char, value *C.char) C.int {
	if globalClient == nil {
		return C.int(-1)
	}

	goKey := C.GoString(key)
	if err := validateKey(goKey); err != nil {
		return C.int(-1)
	}

	val, exists, err := globalClient.Get(goKey)
	if err != nil {
		return C.int(-1)
	}

	if !exists {
		return C.int(1)
	}

	// Safely copy value to C string buffer
	if len(val) >= 2048 {
		return C.int(-1)
	}

	// Copy with null termination
	valBytes := []byte(val)
	dest := (*[2048]byte)(unsafe.Pointer(value))
	copy(dest[:], valBytes)
	dest[len(valBytes)] = 0

	return C.int(0)
}
//export kv_put
func kv_put(key *C.char, value *C.char, oldValue *C.char) C.int {
	if globalClient == nil {
		return C.int(-1)
	}

	goKey := C.GoString(key)
	goValue := C.GoString(value)

	if err := validateKey(goKey); err != nil {
		return C.int(-1)
	}
	if err := validateValue(goValue); err != nil {
		return C.int(-1)
	}

	old, hadOld, err := globalClient.Put(goKey, goValue)
	if err != nil {
		return C.int(-1)
	}

	if hadOld {
		if len(old) >= 2048 {
			return C.int(-1)
		}
		// Copy old value with null termination
		oldBytes := []byte(old)
		dest := (*[2048]byte)(unsafe.Pointer(oldValue))
		copy(dest[:], oldBytes)
		dest[len(oldBytes)] = 0
		return C.int(0)
	}

	return C.int(1)
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
