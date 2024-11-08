package app

import (
	"JacuteSQL/internal/storage"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jacute/prettylogger"
)

type App struct {
	log     *slog.Logger
	storage *storage.Storage
	port    int
	connTL  time.Duration
	ln      net.Listener
}

func New(
	log *slog.Logger,
	storage *storage.Storage,
	connTL time.Duration,
	port int,
) *App {
	return &App{
		log:     log,
		storage: storage,
		connTL:  connTL,
		port:    port,
	}
}

func (a *App) MustRun() {
	if err := a.Run(); err != nil {
		panic(err)
	}
}

func (a *App) Run() error {
	const op = "app.Run"

	var err error
	a.ln, err = net.Listen("tcp", fmt.Sprintf(":%d", a.port))
	if err != nil {
		return err
	}
	defer a.ln.Close()

	fmt.Println("Listening on port", a.port)

	var wg sync.WaitGroup

	for {
		conn, err := a.ln.Accept()
		if err != nil {
			a.log.Info(
				"error accepting connection",
				slog.String("op", op),
				prettylogger.Err(err),
			)
			continue
		}

		wg.Add(1)
		go a.handleConnection(conn, &wg)
	}
}

func (a *App) Stop() { // Graceful shutdown
	err := a.ln.Close()
	if err != nil {
		a.log.Error(
			"error closing listener",
			slog.String("op", "app.Stop"),
			prettylogger.Err(err),
		)
	}
}

func (a *App) handleConnection(conn net.Conn, wg *sync.WaitGroup) error {
	defer wg.Done()
	defer conn.Close()
	if a.connTL > 0 {
		conn.SetReadDeadline(time.Now().Add(a.connTL))
	}

	const op = "app.handleConnection"

	inputBuffer := make([]byte, 1024)
	for {
		conn.Write([]byte(">> "))

		n, err := conn.Read(inputBuffer)
		if err != nil {
			a.log.Info(
				"connection close",
				slog.String("op", op),
				prettylogger.Err(err),
				slog.String("addr", conn.RemoteAddr().String()),
			)
			return fmt.Errorf("%s: %w", op, err)
		}

		if a.connTL > 0 {
			conn.SetReadDeadline(time.Now().Add(a.connTL))
		}

		received := string(inputBuffer[:n])

		if received == "exit\n" {
			break
		}

		cmdOutput, err := a.storage.Exec(received)
		if err != nil {
			conn.Write([]byte(err.Error() + "\n"))
			continue
		}
		conn.Write([]byte("command executed successfully\n"))
		if cmdOutput != "" {
			conn.Write([]byte("output:\n" + cmdOutput))
		}
	}

	return nil
}
