package suite

import (
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"log/slog"
	"os"
	"testing"

	"github.com/jacute/prettylogger"
)

type Suite struct {
	*testing.T
	Cfg     *config.Config
	Storage *storage.Storage
}

func New(t *testing.T) *Suite {
	// t.Helper()
	// t.Parallel()

	v := os.Getenv("CONFIG_PATH")
	if v == "" {
		v = "test_config.yaml"
	}

	cfg := config.MustLoadByPath(v)
	discardLogger := &logger.Logger{
		Log:    slog.New(prettylogger.NewDiscardHandler()),
		Writer: nil,
	}
	st := storage.New(cfg.StoragePath, cfg.LoadedSchema, discardLogger)
	st.Destroy()
	st.Create()
	return &Suite{
		T:       t,
		Cfg:     cfg,
		Storage: st,
	}
}
