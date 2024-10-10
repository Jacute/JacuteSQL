package main

import (
	"JacuteSQL/internal/app"
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // for use all processor cores

	cfg := config.MustLoad()
	log := logger.SetupPrettyLogger(cfg)
	st := storage.New(cfg.StoragePath, cfg.LoadedSchema, log.Log)

	log.Log.Info(
		"Making storage",
		slog.String("storage_path", cfg.StoragePath),
		slog.String("database_name", cfg.LoadedSchema.Name),
		slog.Any("tables", cfg.LoadedSchema.Tables),
	)
	st.Create()

	log.Log.Info(
		"Starting app",
		slog.Int("port", cfg.Port),
		slog.String("env", cfg.Env),
	)
	application := app.New(log.Log, st, cfg.ConnTL, cfg.Port)
	go application.MustRun()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	sign := <-stop
	application.Stop()
	log.Log.Info("App stopped", slog.String("signal", sign.String()))
}
