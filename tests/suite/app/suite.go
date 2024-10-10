package app_suite

import (
	"JacuteSQL/internal/app"
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"log/slog"
	"os"
	"testing"

	"github.com/jacute/prettylogger"
)

type ApplicationSuite struct {
	*testing.T
	Cfg     *config.Config
	Storage *storage.Storage
	App     *app.App
}

func New(t *testing.T) *ApplicationSuite {
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
	application := app.New(discardLogger.Log, st, cfg.ConnTL, cfg.Port)
	return &ApplicationSuite{
		T:       t,
		Cfg:     cfg,
		Storage: st,
		App:     application,
	}
}
