package storage_suite

import (
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"log/slog"
	"os"
	"testing"

	"github.com/jacute/prettylogger"
)

type StorageSuite struct {
	*testing.T
	Cfg     *config.Config
	Storage *storage.Storage
}

func New(t *testing.T) *StorageSuite {
	// t.Helper()
	// t.Parallel()

	v := os.Getenv("CONFIG_PATH")
	if v == "" {
		v = "test_config.yaml"
	}

	cfg := config.MustLoadByPath(v)
	discardLogger := &logger.Logger{
		Log: slog.New(prettylogger.NewDiscardHandler()),
	}
	st := storage.New(cfg.StoragePath, cfg.LoadedSchema, discardLogger.Log)
	st.Destroy()
	st.Create()
	return &StorageSuite{
		T:       t,
		Cfg:     cfg,
		Storage: st,
	}
}
