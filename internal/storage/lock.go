package storage

import (
	"fmt"
	"sync"
)

func (s *Storage) blockTables(tables []string) error {
	for _, tableName := range tables {
		mu, ok := s.tableBlockingMutex.Get(tableName).(*sync.Mutex)
		if !ok {
			return fmt.Errorf("table %s not found", tableName)
		}
		mu.Lock()
	}
	return nil
}

func (s *Storage) unBlockTables(tables []string) error {
	for _, tableName := range tables {
		mu, ok := s.tableBlockingMutex.Get(tableName).(*sync.Mutex)
		if !ok {
			return fmt.Errorf("table %s not found", tableName)
		}
		mu.Unlock()
	}
	return nil
}
