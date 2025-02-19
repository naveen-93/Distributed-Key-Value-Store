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
	clientsMu sync.Mutex
	clients   = make(map[unsafe.Pointer]*client.Client)
)

//export kv_init
func kv_init(serverList **C.char) C.int {
	var servers []string
	for ptr := serverList; *ptr != nil; ptr = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(ptr)) + unsafe.Sizeof(uintptr(0)))) {
		servers = append(servers, C.GoString(*ptr))
	}

	client, err := client.NewClient(servers)
	if err != nil {
		return -1
	}

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
	if value == nil {
		return -1
	}

	goKey := C.GoString(key)
	if err := validateKey(goKey); err != nil {
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
	if err != nil {
		return -1
	}

	if !exists {
		return 1
	}

	cValue := C.CString(val)
	defer C.free(unsafe.Pointer(cValue))
	C.strncpy(value, cValue, 2048)

	return 0
}

//export kv_put
func kv_put(key *C.char, value *C.char, oldValue *C.char) C.int {
	goKey := C.GoString(key)
	goValue := C.GoString(value)

	if err := validateKey(goKey); err != nil {
		return -1
	}
	if err := validateValue(goValue); err != nil {
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

	old, hadOld, err := client.Put(goKey, goValue)
	if err != nil {
		return -1
	}

	if hadOld {
		if len(old) > 2048 {
			return -1
		}
		cOld := C.CString(old)
		defer C.free(unsafe.Pointer(cOld))
		C.strncpy(oldValue, cOld, 2048)
		return 0
	}

	return 1
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
