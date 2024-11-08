package storage

import (
	"JacuteSQL/internal/lib/utils"
	"errors"
	"log/slog"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/jacute/prettylogger"
)

// Create creates a new storage
func (s *Storage) Create() {
	// const op = "storage.MakeStorage"

	if _, err := os.Stat(s.StoragePath); err != nil {
		err = os.Mkdir(s.StoragePath, 0755)
		if err != nil {
			panic("Can't create storage directory: " + err.Error())
		}
	}

	schemaPath := path.Join(s.StoragePath, s.Schema.Name)
	if _, err := os.Stat(schemaPath); err != nil {
		err = os.Mkdir(schemaPath, 0755)
		if err != nil {
			panic("Can't create schema directory: " + err.Error())
		}
	}

	keys := s.Schema.Tables.Keys()
	for i := 0; i < keys.Len(); i++ {
		tableName := keys.Get(i)
		tablePath := path.Join(schemaPath, tableName)

		cols := s.Schema.Tables.Get(tableName).([]string)
		s.Schema.Tables.Add(tableName, slices.Insert(cols, 0, tableName+"_pk"))
		cols = s.Schema.Tables.Get(tableName).([]string)
		s.CreateTable(tableName, tablePath, cols)
	}
}

// CreateTable adds a new table to the storage
func (s *Storage) CreateTable(tableName string, tablePath string, columns []string) {
	const op = "storage.CreateTable"
	log := s.log.With(
		slog.String("op", op),
		slog.String("tablename", tableName),
	)

	log.Debug(
		"Creating table",
		slog.Any("columns", columns),
	)
	if _, err := os.Stat(tablePath); err == nil {
		log.Info("Table already exists", slog.String("tablepath", tablePath))
	} else {
		err := os.Mkdir(tablePath, 0755)
		if err != nil {
			panic("Can't create table: " + err.Error())
		}
	}
	s.TablePathes.Add(tableName, tablePath)

	header := strings.Join(columns, ",") + "\n"
	firstSheetPath := path.Join(tablePath, "1.csv")
	if utils.FileExists(firstSheetPath) {
		log.Info("Sheet already exists", slog.String("sheetpath", firstSheetPath))
	} else {
		err := utils.WriteFile(firstSheetPath, header)
		if err != nil {
			if errors.Is(err, utils.ErrWriteFile) {
				log.Warn(
					"Can't write columns",
					slog.String("sheetpath", firstSheetPath),
					slog.Any("columns", columns),
				)
			} else {
				log.Error(
					"Can't create sheet",
					prettylogger.Err(err),
					slog.String("sheetpath", firstSheetPath),
				)
			}
		}
	}

	pkPath := path.Join(tablePath, tableName+"_pk_sequence")
	if utils.FileExists(pkPath) {
		log.Info("pk file already exists", slog.String("pkpath", pkPath))
	} else {
		err := utils.WriteFile(pkPath, "1")
		if err != nil {
			if errors.Is(err, utils.ErrWriteFile) {
				log.Warn(
					"Can't write to pk file",
					slog.String("pkpath", pkPath),
					slog.Any("columns", columns),
				)
			} else {
				log.Error(
					"Can't create pk file",
					prettylogger.Err(err),
					slog.String("pkpath", pkPath),
				)
			}
		}
	}
}

func (s *Storage) Destroy() {
	if _, err := os.Stat(s.StoragePath); err == nil {
		err = os.RemoveAll(s.StoragePath)
		if err != nil {
			panic("Can't remove storage directory " + s.StoragePath + ": " + err.Error())
		}
	}
}
