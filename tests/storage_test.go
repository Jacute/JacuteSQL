package tests

import (
	"JacuteSQL/internal/data_structures/mymap"
	"JacuteSQL/internal/lib/csv"
	"JacuteSQL/internal/lib/utils"
	"JacuteSQL/internal/storage"
	suite "JacuteSQL/tests/suite/storage"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"

	fakeit "github.com/brianvoe/gofakeit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FillTableBeer(t *testing.T, storage *storage.Storage, rowCount int) {
	var wg sync.WaitGroup
	wg.Add(rowCount)
	for i := 0; i < rowCount; i++ {
		go func() {
			beer := strings.ReplaceAll(fakeit.BeerName(), ",", "")
			style := strings.ReplaceAll(fakeit.BeerStyle(), ",", "")

			_, err := storage.Exec(fmt.Sprintf("INSERT INTO beer VALUES ('%s', '%s', '%s', '%s', '%s')", beer, style, fakeit.BeerAlcohol(), fakeit.BeerIbu(), fakeit.BeerBlg()))
			require.Nil(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

func FillTableCars(t *testing.T, storage *storage.Storage, rowCount int) {
	var wg sync.WaitGroup
	wg.Add(rowCount)
	for i := 0; i < rowCount; i++ {
		go func() {
			_, err := storage.Exec(fmt.Sprintf("INSERT INTO cars VALUES ('%s', '%s', '%s', '%s')", fakeit.CarModel(), fakeit.CarMaker(), "random_type"+fakeit.HexColor(), "random_fueltype"+fakeit.HexColor()))
			require.Nil(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestInsertHappyPath(t *testing.T) {
	st := suite.New(t)

	cases := []struct {
		tableName string
		rowCount  int
	}{
		{
			tableName: "beer",
			rowCount:  1000,
		},
		{
			tableName: "cars",
			rowCount:  1000,
		},
	}
	for _, c := range cases {
		t.Run(c.tableName, func(tt *testing.T) {
			tt.Parallel()
			tablepath := st.Storage.TablePathes.Get(c.tableName).(string)

			if c.tableName == "beer" {
				FillTableBeer(t, st.Storage, c.rowCount)
			} else if c.tableName == "cars" {
				FillTableCars(t, st.Storage, c.rowCount)
			} else {
				t.Errorf("incorrect table name")
			}

			// Check pk_sequence file
			pkPath := path.Join(tablepath, c.tableName+"_pk_sequence")
			idBytes, _ := os.ReadFile(pkPath)
			idInt, _ := strconv.Atoi(string(idBytes))
			assert.Equal(t, c.rowCount+1, idInt)

			// Check count of sheets
			sheets, err := utils.GetSheetsFromFiles(tablepath)
			assert.Nil(t, err)
			expectedSheetsCount := c.rowCount / st.Cfg.LoadedSchema.TuplesLimit
			if c.rowCount%st.Cfg.LoadedSchema.TuplesLimit != 0 {
				expectedSheetsCount++
			}

			assert.Equal(t, expectedSheetsCount, len(sheets))
		})
	}
}

func TestCondition(t *testing.T) {
	st := suite.New(t)
	head := st.Storage.GetConditionTree("table.id = '1' AND table.name = 'aboba'")
	row := mymap.New()
	row.Add("table.id", "1")
	row.Add("table.name", "ded")
	test1 := st.Storage.IsValidRow(head, row, []string{"table"}, "table")
	row2 := mymap.New()
	row2.Add("table.id", "1")
	row2.Add("table.name", "aboba")
	test2 := st.Storage.IsValidRow(head, row2, []string{"table"}, "table")
	assert.False(t, test1)
	assert.True(t, test2)
}

func TestDeleteWhereHappyPath(t *testing.T) {
	st := suite.New(t)
	table := "beer"
	tablepath := st.Storage.TablePathes.Get(table).(string)

	_, err := st.Storage.Insert(table, []string{fakeit.BeerName(), fakeit.BeerStyle(), fakeit.BeerAlcohol(), fakeit.BeerIbu(), fakeit.BeerBlg()})
	require.Nil(t, err)

	// error column name
	err, _ = st.Storage.DeleteWhere(table, "id = 1")
	require.Nil(t, err)

	_, rowsCount, err := csv.ReadCSV(path.Join(tablepath, "1.csv"), table)
	assert.Nil(t, err)
	assert.Equal(t, 1, rowsCount)

	err, _ = st.Storage.DeleteWhere(table, "beer.beer_pk = 1")
	require.Nil(t, err)

	_, rowsCount, err = csv.ReadCSV(path.Join(tablepath, "1.csv"), table)
	assert.Nil(t, err)
	assert.Equal(t, 0, rowsCount)
}

func TestBlock(t *testing.T) {
	st := suite.New(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			_, err := st.Storage.Exec(
				fmt.Sprintf("INSERT INTO beer VALUES (%s, %s, %s, %s, %s)",
					fakeit.BeerName(),
					fakeit.BeerStyle(),
					fakeit.BeerAlcohol(),
					fakeit.BeerIbu(),
					fakeit.BeerBlg()),
			)
			fmt.Println(err)
			assert.Error(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}
