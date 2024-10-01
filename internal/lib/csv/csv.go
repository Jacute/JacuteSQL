package csv

import (
	"JacuteSQL/internal/data_structures/mymap"
	"JacuteSQL/internal/data_structures/mysl"
	"encoding/csv"
	"errors"
	"io"
	"os"
)

var (
	ErrOpenFile                 = errors.New("error opening file")
	ErrWriteFile                = errors.New("error writing file")
	ErrIncorrectNumberOfColumns = errors.New("incorrect number of columns")
)

func ReadCSV(filename string, table string) (*mysl.MySl[*mymap.CustomMap], int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return &mysl.MySl[*mymap.CustomMap]{}, 0, ErrOpenFile
	}
	defer file.Close()

	result := mysl.New[*mymap.CustomMap]()
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return &mysl.MySl[*mymap.CustomMap]{}, 0, err
	}
	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		rowCount++
		if err != nil {
			return &mysl.MySl[*mymap.CustomMap]{}, 0, err
		}
		newElement := mymap.New()
		for i, col := range record {
			newElement.Add(table+"."+headers[i], col)
		}
		result.Append(newElement)
	}

	return result, rowCount, nil
}

func AddRow(filename string, cols []string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return ErrOpenFile
	}

	writer := csv.NewWriter(file)
	err = writer.Write(cols)
	if err != nil {
		return ErrWriteFile
	}
	writer.Flush()
	return nil
}

func WriteFile(filename string, tableName string, rows *mysl.MySl[*mymap.CustomMap], header []string) error {
	file, err := os.OpenFile(filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return ErrOpenFile
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.Write(header)
	if err != nil {
		return ErrWriteFile
	}

	for i := 0; i < rows.Len(); i++ {
		forWrite := mysl.New[string]()
		for _, col := range header {
			forWrite.Append(rows.Get(i).Get(tableName + "." + col).(string))
		}
		err = writer.Write(forWrite.GetData())
		if err != nil {
			return ErrWriteFile
		}
	}

	writer.Flush()
	return nil
}
