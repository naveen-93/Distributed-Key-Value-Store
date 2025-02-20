package main

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"Distributed-Key-Value-Store/pkg/client"
)

var (
	globalClient *client.Client
	globalMu     sync.Mutex
)

//export kv_init
func kv_init(serverList **C.char) C.int {
<<<<<<< Updated upstream:lib/libkv.go
=======
	globalMu.Lock()
	defer globalMu.Unlock()

	// Check if already initialized
	if globalClient != nil {
		return C.int(-1)
	}

	// Convert C string array to Go string slice
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	var servers []string
	for ptr := serverList; *ptr != nil; ptr = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + unsafe.Sizeof(uintptr(0)))) {
		servers = append(servers, C.GoString(*ptr))
	}

<<<<<<< Updated upstream:lib/libkv.go
	client, err := client.NewClient(servers)
=======
	// Create new client
	var err error
	globalClient, err = client.NewClient(servers)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	if err != nil {
		return C.int(-1)
	}

<<<<<<< Updated upstream:lib/libkv.go
	clientsMu.Lock()
	clients[unsafe.Pointer(serverList)] = client
	clientsMu.Unlock()

	return 0
=======
	return C.int(0)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
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
<<<<<<< Updated upstream:lib/libkv.go
	if value == nil {
		return -1
=======
	if globalClient == nil {
		return C.int(-1)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	}

	goKey := C.GoString(key)
	if err := validateKey(goKey); err != nil {
<<<<<<< Updated upstream:lib/libkv.go
		return -1
	}

	clientsMu.Lock()
	var client *client.Client
	for _, c := range clients {
		client = c
		break
	}
	clientsMu.Unlock()

	if client == nil {
		return -1
	}

	val, exists, err := client.Get(goKey)
=======
		return C.int(-1)
	}

	val, exists, err := globalClient.Get(goKey)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	if err != nil {
		return C.int(-1)
	}

	if !exists {
		return C.int(1)
	}

<<<<<<< Updated upstream:lib/libkv.go
	cValue := C.CString(val)
	defer C.free(unsafe.Pointer(cValue))
	C.strncpy(value, cValue, 2048)
=======
	// Safely copy value to C string buffer
	if len(val) >= 2048 {
		return C.int(-1)
	}
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go

	// Copy with null termination
	valBytes := []byte(val)
	dest := (*[2048]byte)(unsafe.Pointer(value))
	copy(dest[:], valBytes)
	dest[len(valBytes)] = 0

	return C.int(0)
}
//export kv_put
func kv_put(key *C.char, value *C.char, oldValue *C.char) C.int {
<<<<<<< Updated upstream:lib/libkv.go
=======
	if globalClient == nil {
		return C.int(-1)
	}

>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	goKey := C.GoString(key)
	goValue := C.GoString(value)

	if err := validateKey(goKey); err != nil {
		return C.int(-1)
	}
	if err := validateValue(goValue); err != nil {
		return C.int(-1)
	}

<<<<<<< Updated upstream:lib/libkv.go
	clientsMu.Lock()
	var client *client.Client
	for _, c := range clients {
		client = c
		break
	}
	clientsMu.Unlock()

	if client == nil {
		return -1
	}

	old, hadOld, err := client.Put(goKey, goValue)
=======
	old, hadOld, err := globalClient.Put(goKey, goValue)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	if err != nil {
		return C.int(-1)
	}

	if hadOld {
		if len(old) >= 2048 {
			return C.int(-1)
		}
<<<<<<< Updated upstream:lib/libkv.go
		cOld := C.CString(old)
		defer C.free(unsafe.Pointer(cOld))
		C.strncpy(oldValue, cOld, 2048)
		return 0
=======
		// Copy old value with null termination
		oldBytes := []byte(old)
		dest := (*[2048]byte)(unsafe.Pointer(oldValue))
		copy(dest[:], oldBytes)
		dest[len(oldBytes)] = 0
		return C.int(0)
>>>>>>> Stashed changes:pkg/client/cgo/c_bindings.go
	}

	return C.int(1)
}

func main() {} // Required for building shared library

// Helper functions remain the same as in your original code
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

func validateValue(value string) error {
	if len(value) > 2048 {
		return fmt.Errorf("value too long (max 2048 bytes)")
	}

	for _, r := range value {
		if r < 32 || r > 126 {
			return fmt.Errorf("invalid character in value")
		}
	}

	if isUUEncoded(value) {
		return fmt.Errorf("UU encoded values not allowed")
	}
	return nil
}

func isUUEncoded(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), "begin ")
}
