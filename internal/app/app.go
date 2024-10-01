package app

import (
	"JacuteSQL/internal/logger"
	"JacuteSQL/internal/storage"
	"bufio"
	"fmt"
	"os"
)

type App struct {
	log     *logger.Logger
	storage *storage.Storage
}

func New(
	log *logger.Logger,
	storage *storage.Storage,
) *App {
	return &App{
		log:     log,
		storage: storage,
	}
}

func (a *App) MustRun() {
	if err := a.Run(); err != nil {
		panic(err)
	}
}

func (a *App) Run() error {
	const op = "app.Run"

	input := bufio.NewReader(os.Stdin)
	output := bufio.NewWriter(os.Stdout)
	for {
		fmt.Fprint(output, ">> ")
		output.Flush()

		str, err := input.ReadString('\n')
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}

		cmdOutput, err := a.storage.Exec(str)
		if err != nil {
			fmt.Fprintln(output, err.Error())
			continue
		}
		fmt.Fprintln(output, "command executed successfully")
		if cmdOutput != "" {
			fmt.Fprintln(output, "output:\n"+cmdOutput)
		}
	}
}

func (a *App) Stop() {
	// TODO: Create graceful shutdown for db
}
