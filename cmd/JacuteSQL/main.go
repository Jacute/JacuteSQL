package main

import (
	"JacuteSQL/internal/app"
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.MustLoad()
	log := logger.SetupPrettyLogger(cfg)
	st := storage.New(cfg.StoragePath, cfg.LoadedSchema, log)

	log.Log.Info(
		"Making storage",
		slog.String("storage_path", cfg.StoragePath),
		slog.String("database_name", cfg.LoadedSchema.Name),
		slog.Any("tables", cfg.LoadedSchema.Tables),
	)
	st.Create()

	log.Log.Info("Starting app")
	application := app.New(log, st)
	go application.MustRun()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	sign := <-stop
	application.Stop()
	log.Log.Info("App stopped", slog.String("signal", sign.String()))
}
