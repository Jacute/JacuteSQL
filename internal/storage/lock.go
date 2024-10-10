package storage

import (
	"JacuteSQL/internal/lib/utils"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"

	"github.com/jacute/prettylogger"
)

func (s *Storage) block(tableName string) error {
	mu, ok := s.tableBlockingMutex.Get(tableName).(*sync.Mutex)
	if !ok {
		return fmt.Errorf("mutex not found %s", tableName)
	}
	mu.Lock()
	defer mu.Unlock()

	if s.isBlock(tableName) {
		return fmt.Errorf("table %s is already blocked", tableName)
	}

	tablePath, ok := s.TablePathes.Get(tableName).(string)
	if !ok || tablePath == "" {
		return fmt.Errorf("table not found %s", tableName)
	}

	blockPath := path.Join(tablePath, tableName+"_lock")
	if utils.FileExists(blockPath) {
		return utils.WriteFile(blockPath, "1")
	}
	return fmt.Errorf("lock file not found %s", blockPath)
}

func (s *Storage) blockTables(tables []string) error {
	for _, tableName := range tables {
		err := s.block(tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) unBlock(tableName string) error {
	mu, ok := s.tableBlockingMutex.Get(tableName).(*sync.Mutex)
	if !ok {
		return fmt.Errorf("mutex not found %s", tableName)
	}
	mu.Lock()
	defer mu.Unlock()

	tablePath, ok := s.TablePathes.Get(tableName).(string)
	if !ok || tablePath == "" {
		return fmt.Errorf("table not found %s", tableName)
	}

	blockPath := path.Join(tablePath, tableName+"_lock")
	return utils.WriteFile(blockPath, "0")
}

func (s *Storage) unBlockTables(tables []string) {
	for _, tableName := range tables {
		s.unBlock(tableName)
	}
}

func (s *Storage) isBlock(tableName string) bool {
	tablePath, ok := s.TablePathes.Get(tableName).(string)
	if !ok || tablePath == "" {
		return false
	}

	blockPath := path.Join(tablePath, tableName+"_lock")
	data, _ := os.ReadFile(blockPath)
	return string(data) == "1"
}

func (s *Storage) isBlockTables(tables []string) (string, bool) {
	for _, tableName := range tables {
		if s.isBlock(tableName) {
			return tableName, true
		}
	}
	return "", false
}

func (s *Storage) UnBlockAllTables() {
	const op = "storage.unblockAllTables"

	tables := s.Schema.Tables.Keys()
	for i := 0; i < tables.Len(); i++ {
		err := s.unBlock(tables.Get(i))
		if err != nil {
			s.log.Error(
				"error unblocking table",
				slog.String("op", op),
				slog.String("table", tables.Get(i)),
				prettylogger.Err(err),
			)
		}
	}
}
