package db

import (
	"context"
)

var _ StorageWriter = &memoryStorage{}

type memoryStorage struct {
	store map[string][]byte
}

func NewMemoryStorage() *memoryStorage {
	return &memoryStorage{
		store: map[string][]byte{},
	}
}

func (s *memoryStorage) Put(ctx context.Context, path string, content []byte) error {
	s.store[path] = content
	return nil
}
