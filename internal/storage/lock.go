package storage

import (
	"JacuteSQL/internal/lib/utils"
	"fmt"
	"os"
	"path"
)

func (s *Storage) block(tableName string) error {
	tablePath, ok := s.TablePathes.Get(tableName).(string)
	if !ok || tablePath == "" {
		return fmt.Errorf("table not found %s", tableName)
	}

	blockPath := path.Join(tablePath, tableName+"_lock")
	return utils.WriteFile(blockPath, "1")
}

func (s *Storage) blockTables(tables []string) {
	for _, tableName := range tables {
		s.block(tableName)
	}
}

func (s *Storage) unBlock(tableName string) error {
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
