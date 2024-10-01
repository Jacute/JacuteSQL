package csv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testFilename = "test.csv"

func TestReadCSV(t *testing.T) {
	data, rowCount, err := ReadCSV(testFilename, "test")
	assert.Nil(t, err)
	assert.Equal(t, 2, rowCount)
	for i := 0; i < data.Len(); i++ {
		fmt.Print(data.Get(i))
	}
}
