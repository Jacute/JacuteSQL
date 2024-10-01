package utils

import (
	"errors"
	"os"
	"regexp"
)

var (
	ErrFileExists = errors.New("file already exists")
	ErrWriteFile  = errors.New("can't write file")
)

var (
	sheetnameRegexp = regexp.MustCompile(`^\d+.csv$`)
)

func WriteFile(filePath string, header string) error {
	if _, err := os.Stat(filePath); err == nil {
		return ErrFileExists
	}

	err := os.WriteFile(filePath, []byte(header), 0644)
	if err != nil {
		return ErrWriteFile
	}

	return nil
}

func GetSheetsFromFiles(tablePath string) ([]string, error) {
	sheets := make([]string, 0)
	files, err := os.ReadDir(tablePath)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !file.IsDir() && sheetnameRegexp.Match([]byte(file.Name())) {
			sheets = append(sheets, file.Name())
		}
	}

	return sheets, nil
}
