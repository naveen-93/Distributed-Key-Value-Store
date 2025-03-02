package main

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"context"
	"log"
	"sync"
	"time"
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
		log.Println("Closing existing client connection")
		globalClient.Close()
		globalClient = nil
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
		log.Println("Error: No servers provided")
		return -1
	}

	log.Printf("Initializing client with servers: %v", servers)

	config := client.ClientConfig{
		ServerAddresses: servers,
		Timeout:         10 * time.Second, // Increased timeout
		RetryAttempts:   5,                // Increased retries
		RetryDelay:      500 * time.Millisecond,
	}

	var err error
	globalClient, err = client.NewClient(config)
	if err != nil {
		log.Printf("Failed to create client: %v", err)
		return -1
	}

	// Wait a moment for client to establish connections
	time.Sleep(500 * time.Millisecond)

	log.Println("Client initialized successfully")
	return 0
}

//export kv_shutdown
func kv_shutdown() C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		log.Println("Client already shut down")
		return 0
	}

	log.Println("Shutting down client")
	globalClient.Close()
	globalClient = nil
	return 0
}

//export kv_get
func kv_get(cKey *C.char, cValue *C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		log.Println("Error: Client not initialized")
		return -1
	}

	key := C.GoString(cKey)
	log.Printf("GET request: key=%s", key)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	value, _, exists, err := globalClient.Get(ctx, key)
	if err != nil {
		log.Printf("GET error for key %s: %v", key, err)
		return -1
	}

	// Copy value to C buffer (assumes buffer is pre-allocated)
	if exists {
		cstr := C.CString(value)
		defer C.free(unsafe.Pointer(cstr))
		C.strcpy(cValue, cstr)
		log.Printf("GET success: key=%s, value=%s", key, value)
		return 0
	} else {
		// Key not found, set empty string
		C.strcpy(cValue, C.CString(""))
		log.Printf("GET key not found: key=%s", key)
		return 1
	}
}

//export kv_put
func kv_put(cKey *C.char, cValue *C.char, cOldValue *C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		log.Println("Error: Client not initialized")
		return -1
	}

	key := C.GoString(cKey)
	value := C.GoString(cValue)
	log.Printf("PUT request: key=%s, value=%s", key, value)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oldValue, hadOldValue, err := globalClient.Put(ctx, key, value)
	if err != nil {
		log.Printf("PUT error for key %s: %v", key, err)
		return -1
	}

	// Always initialize the old value buffer
	if hadOldValue {
		cstr := C.CString(oldValue)
		defer C.free(unsafe.Pointer(cstr))
		C.strcpy(cOldValue, cstr)
		log.Printf("PUT success (update): key=%s, oldValue=%s", key, oldValue)
		return 0
	} else {
		cstr := C.CString("")
		defer C.free(unsafe.Pointer(cstr))
		C.strcpy(cOldValue, cstr)
		log.Printf("PUT success (new key): key=%s", key)
		return 1
	}
}

//export kv_delete
func kv_delete(cKey *C.char) C.int {
	clientMu.Lock()
	defer clientMu.Unlock()

	if globalClient == nil {
		log.Println("Error: Client not initialized")
		return -1
	}

	key := C.GoString(cKey)
	log.Printf("DELETE request: key=%s", key)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If your client has a Delete method, use it
	// Otherwise, you can implement deletion by setting an empty value
	// or a special tombstone value

	// For now, we'll simulate deletion by setting an empty value
	_, exists, err := globalClient.Put(ctx, key, "")
	if err != nil {
		log.Printf("DELETE error for key %s: %v", key, err)
		return -1
	}

	if exists {
		log.Printf("DELETE success: key=%s", key)
		return 0
	} else {
		log.Printf("DELETE key not found: key=%s", key)
		return 1
	}
}

func main() {
	// This is required for buildmode=c-shared
	log.Println("KV client library loaded")
}
