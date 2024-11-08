package tests

import (
	suite "JacuteSQL/tests/suite/app"
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	fakeit "github.com/brianvoe/gofakeit"
	"github.com/stretchr/testify/require"
)

const serverAddr = "127.0.0.1:7432"

func randomString(n int) string {
	result := ""

	for i := 0; i < n; i++ {
		letter := rune(65 + rand.Intn(26))
		result += string(letter)
	}

	return result
}

func handleRequest(t *testing.T, id int, query string) string {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Errorf("goroutine %d: could not connect to server: %v", id, err)
		return ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)

	_, err = reader.ReadString(' ') // read greetings
	if err != nil {
		t.Errorf("goroutine %d: could not read greeting from server: %v", id, err)
		return ""
	}

	_, err = conn.Write([]byte(query))
	if err != nil {
		t.Errorf("goroutine %d: could not send query: %v", id, err)
		return ""
	}

	output, err := reader.ReadString('>')
	output = output[:len(output)-2]
	if err != nil {
		t.Errorf("goroutine %d: could not read response: %v", id, err)
		return ""
	}

	fmt.Printf("goroutine %d: response lines: %v\n", id, output)
	return strings.TrimSpace(output)
}

func TestBlocks(t *testing.T) {
	maxGoroutines := runtime.NumCPU()
	runtime.GOMAXPROCS(maxGoroutines)

	st := suite.New(t)
	FillTableBeer(t, st.Storage, 1100)
	FillTableCars(t, st.Storage, 1100)
	go st.App.MustRun()
	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(maxGoroutines)

	resultLines := make([]string, 0)
	for i := 0; i < maxGoroutines/2; i++ {
		go func(id int) {
			defer wg.Done()

			beerResult := handleRequest(t, id, "SELECT beer.name FROM beer")
			resultLines = append(resultLines, beerResult)
		}(i)
		go func(id int) {
			defer wg.Done()

			carsResult := handleRequest(t, id, "SELECT cars.model FROM cars")
			resultLines = append(resultLines, carsResult)
		}(maxGoroutines/2 + i)
	}
	wg.Wait()

	successCount := 0
	for _, result := range resultLines {
		if result == "command executed successfully" {
			successCount++
		}
	}

	require.Equal(t, maxGoroutines, successCount)

	st.App.Stop()
}

func TestBlocks2(t *testing.T) {
	maxGoroutines := runtime.NumCPU()
	runtime.GOMAXPROCS(maxGoroutines)

	st := suite.New(t)
	FillTableBeer(t, st.Storage, 1100)
	FillTableCars(t, st.Storage, 1100)
	go st.App.MustRun()
	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(maxGoroutines)

	rStr1 := randomString(32)
	rStr2 := randomString(32)
	for i := 0; i < maxGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			handleRequest(t, id, fmt.Sprintf("INSERT INTO beer VALUES ('%s', '%s', '%s', '%s', '%s')", rStr1, rStr2, fakeit.BeerAlcohol(), fakeit.BeerIbu(), fakeit.BeerBlg()))
		}(i)
	}
	wg.Wait()

	result := handleRequest(t, 0, fmt.Sprintf("SELECT beer.beer_pk FROM beer WHERE beer.name = '%s' AND beer.style = '%s'", rStr1, rStr2))
	require.Equal(t, maxGoroutines, strings.Count(result, "\n")-2)

	st.App.Stop()
}
