package main

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"context"
	"sync"
	"unsafe"

	"Distributed-Key-Value-Store/pkg/client"
)

var (
	globalClient *client.Client
	clientMu     sync.Mutex
)

//export kv_init
func kv_init(serverList **C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	// If client exists, close it to allow reinitialization
	if globalClient != nil {
		globalClient.Close()
	}

	// Convert C char** to Go []string
	var servers []string
	ptr := uintptr(unsafe.Pointer(serverList))
	for {
		p := (**C.char)(unsafe.Pointer(ptr))
		if *p == nil {
			break
		}
		servers = append(servers, C.GoString(*p))
		ptr += unsafe.Sizeof(uintptr(0))
	}

	if len(servers) == 0 {
		return -1
	}

	config := client.ClientConfig{
		ServerAddresses: servers,
		Timeout:         5 * 1000000000, // 5 seconds in nanoseconds
		RetryAttempts:   3,
		RetryDelay:      500 * 1000000, // 500ms in nanoseconds
	}

	var err error
	globalClient, err = client.NewClient(config)
	if err != nil {
		return -1
	}
	return 0
}

//export kv_shutdown
func kv_shutdown() C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		return 0 // Already shut down
	}

	globalClient.Close()
	globalClient = nil
	return 0
}

//export kv_get
func kv_get(cKey *C.char, cValue *C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		return -1 // Not initialized
	}

	key := C.GoString(cKey)
	value, _, exists, err := globalClient.Get(context.Background(), key)
	if err != nil {
		return -1
	}
	if !exists {
		return 1
	}

	// Copy value to C buffer (assumes buffer is pre-allocated)
	cstr := C.CString(value)
	defer C.free(unsafe.Pointer(cstr))
	C.strcpy(cValue, cstr)
	return 0
}

//export kv_put
func kv_put(cKey *C.char, cValue *C.char, cOldValue *C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		return -1 // Not initialized
	}

	key := C.GoString(cKey)
	value := C.GoString(cValue)
	oldValue, hadOldValue, err := globalClient.Put(context.Background(), key, value)
	if err != nil {
		return -1
	}

	if hadOldValue {
		cstr := C.CString(oldValue)
		defer C.free(unsafe.Pointer(cstr))
		C.strcpy(cOldValue, cstr)
		return 0
	} else {
		cstr := C.CString("")
		defer C.free(unsafe.Pointer(cstr))
		C.strcpy(cOldValue, cstr)
		return 1
	}
}

func main() {
	// This is required for buildmode=c-shared
}
