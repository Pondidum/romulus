package db

import (
	"context"
)

type memoryStorage struct {
	store map[string][]byte
}

func NewMemoryStorage() StorageWriter {
	return &memoryStorage{}
}

func (s *memoryStorage) Put(ctx context.Context, path string, content []byte) error {
	s.store[path] = content
	return nil
}
