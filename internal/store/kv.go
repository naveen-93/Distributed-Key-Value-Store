package store

import (
    "sync"
    
)

type KVStore struct {
    mu   sync.RWMutex
    data map[string]string
}

func NewKVStore() *KVStore {
    return &KVStore{
        data: make(map[string]string),
    }
}

func (s *KVStore) Get(key string) (string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    val, exists := s.data[key]
    return val, exists
}

func (s *KVStore) Put(key, value string) (string, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()
    oldVal, exists := s.data[key]
    s.data[key] = value
    return oldVal, exists
}