package logger

import (
	"JacuteSQL/internal/config"
	"log/slog"
	"os"

	"github.com/jacute/prettylogger"
)

type Logger struct {
	Log    *slog.Logger
	Writer *os.File
}

func SetupPrettyLogger(cfg *config.Config) *Logger {
	log := &Logger{}

	file, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic("Can't open log file: " + err.Error())
	}
	log.Log = slog.New(
		prettylogger.NewJsonHandler(file, &slog.HandlerOptions{Level: slog.LevelInfo}),
	)

	return log
}
