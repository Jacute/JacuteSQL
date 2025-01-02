package storage

import (
	"JacuteSQL/internal/config"
	"JacuteSQL/internal/data_structures/mymap"
	"JacuteSQL/internal/data_structures/mysl"
	"JacuteSQL/internal/lib/csv"
	"JacuteSQL/internal/lib/utils"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/jacute/prettylogger"
)

var (
	selectRegexp      = regexp.MustCompile(`(?i)^SELECT\s+([\w\d\.,\s]+)\s+FROM\s+([\w\d,\s]+)$`)
	selectWhereRegexp = regexp.MustCompile(`(?i)^SELECT\s+([\w\d\.,\s]+)\s+FROM\s+([\w\d,\s]+)\s+\s*WHERE\s+(.+?)?\s*;?$`)
	insertRegexp      = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+(\w+)\s+VALUES\s+\((.+)\)\s*;?$`)
	deleteRegexp      = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+([\w\d,\s]+)\s*;?$`)
	deleteWhereRegexp = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+([\w\d,\s]+)\s*WHERE\s+(.+?)?\s*;?$`)
)

var (
	ErrNoSheets                 = errors.New("no sheets for writing a new row")
	ErrIncorrectNumberOfColumns = errors.New("invalid number of columns")
	ErrIncorectTable            = errors.New("incorrect table")
	ErrParse                    = errors.New("parse error")
)

type Storage struct {
	StoragePath        string
	Schema             *config.Schema
	TablePathes        *mymap.CustomMap
	tableBlockingMutex *mymap.CustomMap
	log                *slog.Logger
}

// New creates a new Storage
func New(storagePath string, schema *config.Schema, log *slog.Logger) *Storage {
	tableBlockingMutex := mymap.New()
	tableNames := schema.Tables.Keys()
	for i := 0; i < tableNames.Len(); i++ {
		tableBlockingMutex.Add(tableNames.Get(i), &sync.Mutex{})
	}

	return &Storage{
		StoragePath:        storagePath,
		Schema:             schema,
		log:                log,
		TablePathes:        mymap.New(),
		tableBlockingMutex: tableBlockingMutex,
	}
}

// Exec parse the command and execute select/insert/delete
func (s *Storage) Exec(str string) (string, error) {
	str = strings.TrimSpace(str)
	if selectRegexp.Match([]byte(str)) {
		matches := selectRegexp.FindStringSubmatch(str)
		fields := matches[1]
		tables := matches[2]

		fieldsSplitted := strings.Split(fields, ",")
		for i := range fieldsSplitted {
			fieldsSplitted[i] = strings.TrimSpace(fieldsSplitted[i])
		}
		tablesSplitted := strings.Split(tables, ",")
		for i := range tablesSplitted {
			tablesSplitted[i] = strings.TrimSpace(tablesSplitted[i])
		}

		if err := s.blockTables(tablesSplitted); err != nil {
			return "", fmt.Errorf("error: " + err.Error())
		}
		rows, err := s.Select(fieldsSplitted, tablesSplitted, "")
		s.unBlockTables(tablesSplitted)
		if err != nil {
			return "", fmt.Errorf("error: " + err.Error())
		}

		output := strings.Join(fieldsSplitted, ",") + "\n"
		for i := 0; i < rows.Len(); i++ {
			row := rows.Get(i)
			if row.Len() > 0 {
				output += strings.Join(row.GetData(), ",") + "\n"
			}
		}
		return output, nil
	} else if selectWhereRegexp.Match([]byte(str)) {
		matches := selectWhereRegexp.FindStringSubmatch(str)
		fields := matches[1]
		tables := matches[2]
		condition := matches[3]

		fieldsSplitted := strings.Split(fields, ",")
		for i := range fieldsSplitted {
			fieldsSplitted[i] = strings.TrimSpace(fieldsSplitted[i])
		}
		tablesSplitted := strings.Split(tables, ",")
		for i := range tablesSplitted {
			tablesSplitted[i] = strings.TrimSpace(tablesSplitted[i])
		}

		if err := s.blockTables(tablesSplitted); err != nil {
			return "", fmt.Errorf("error: " + err.Error())
		}
		rows, err := s.Select(fieldsSplitted, tablesSplitted, condition)
		if err != nil {
			return "", fmt.Errorf("error: " + err.Error())
		}
		s.unBlockTables(tablesSplitted)

		output := strings.Join(fieldsSplitted, ",") + "\n"
		for i := 0; i < rows.Len(); i++ {
			row := rows.Get(i)
			if row.Len() > 0 {
				output += strings.Join(row.GetData(), ",") + "\n"
			}
		}

		return output, nil
	} else if insertRegexp.Match([]byte(str)) {
		matches := insertRegexp.FindStringSubmatch(str)
		tableName := matches[1]
		values := matches[2]

		valuesSplitted := strings.Split(values, ",")
		for i := range valuesSplitted {
			valuesSplitted[i] = strings.TrimSpace(valuesSplitted[i])
			valuesSplitted[i] = strings.Trim(valuesSplitted[i], "'")
			if strings.Contains(valuesSplitted[i], ",") {
				return "", fmt.Errorf("error: Values can't contain ',' symbol")
			}
		}

		if err := s.blockTables([]string{tableName}); err != nil {
			return "", fmt.Errorf("error: " + err.Error())
		}
		id, err := s.Insert(tableName, valuesSplitted)
		if err != nil {
			if errors.Is(err, ErrIncorrectNumberOfColumns) {
				return "", fmt.Errorf("error: Incorrect number of columns")
			}
			return "", fmt.Errorf("error: " + err.Error())
		}
		s.unBlockTables([]string{tableName})

		return id, nil
	} else if deleteRegexp.Match([]byte(str)) {
		matches := deleteRegexp.FindStringSubmatch(str)
		tables := matches[1]

		tablesSplitted := strings.Split(tables, ",")
		for i := range tablesSplitted {
			tablesSplitted[i] = strings.TrimSpace(tablesSplitted[i])
		}

		for _, tableName := range tablesSplitted {
			if err := s.blockTables([]string{tableName}); err != nil {
				return "", fmt.Errorf("error: " + err.Error())
			}
			err := s.Delete(tableName)
			if err != nil {
				return "", fmt.Errorf("error: " + err.Error())
			}
			s.unBlockTables([]string{tableName})
		}
	} else if deleteWhereRegexp.Match([]byte(str)) {
		matches := deleteWhereRegexp.FindStringSubmatch(str)
		tables := matches[1]
		condition := matches[2]

		tablesSplitted := strings.Split(tables, ",")
		for i := range tablesSplitted {
			tablesSplitted[i] = strings.TrimSpace(tablesSplitted[i])
		}
		count := 0
		for _, tableName := range tablesSplitted {
			if err := s.blockTables([]string{tableName}); err != nil {
				return "", fmt.Errorf("error: " + err.Error())
			}
			var err error
			err, count = s.DeleteWhere(tableName, condition)
			if err != nil {
				return "", fmt.Errorf("error: " + err.Error())
			}
			s.unBlockTables([]string{tableName})
		}

		return fmt.Sprintf("deleted %d rows", count), nil
	} else {
		return "", fmt.Errorf("error: Incorrect command")
	}

	return "", nil
}

// Insert adds a new row to the table with given values
func (s *Storage) Insert(table string, values []string) (string, error) {
	const op = "storage.Insert"
	log := s.log.With(
		slog.String("op", op),
		slog.String("table", table),
		slog.Any("values", values),
	)

	tablePath := s.TablePathes.Get(table).(string)
	if tablePath == "" {
		return "", ErrIncorectTable
	}

	// Validate columns count
	schemaColumns, _ := s.Schema.Tables.Get(table).([]string)
	if len(values) != len(schemaColumns)-1 {
		return "", ErrIncorrectNumberOfColumns
	}

	// Read pk and add to the columns
	pkPath := path.Join(tablePath, table+"_pk_sequence")
	idBytes, err := os.ReadFile(pkPath)
	if err != nil {
		log.Error(
			"PK reading error",
			slog.String("pkpath", pkPath),
		)
		return "", fmt.Errorf("%s: %w", op, err)
	}

	id := strings.TrimSpace(string(idBytes))
	idInt, err := strconv.Atoi(id)
	if err != nil {
		log.Error(
			"id isn't number",
			slog.String("pkpath", pkPath),
		)
		return "", fmt.Errorf("%s: %w", op, err)
	}
	values = append([]string{id}, values...)

	sheets, err := utils.GetSheetsFromFiles(tablePath)
	if err != nil {
		log.Error(
			"Sheets getting error",
			prettylogger.Err(err),
		)
		return "", fmt.Errorf("%s: %w", op, err)
	}

	for _, sheetName := range sheets {
		sheetPath := path.Join(tablePath, sheetName)
		// Check for rowCount
		_, rowCount, err := csv.ReadCSV(sheetPath, table)
		if err != nil {
			log.Error(
				"Sheet reading error",
				prettylogger.Err(err),
				slog.String("sheetpath", sheetPath),
			)
			return "", fmt.Errorf("%s: %v", op, err)
		}
		// If rowCount > tuples_limit, write to the next sheet
		if rowCount < s.Schema.TuplesLimit {
			err = csv.AddRow(sheetPath, values)
			if err != nil {
				log.Error(
					"Error adding row",
					prettylogger.Err(err),
					slog.String("sheetpath", sheetPath),
				)
				return "", fmt.Errorf("%s: %v", op, err)
			}

			err = os.WriteFile(pkPath, []byte(strconv.Itoa(idInt+1)), 0644)
			if err != nil {
				log.Error(
					"Error writing a new pk",
					slog.String("pkpath", pkPath),
				)
				return "", err
			}

			return id, nil
		}
	}
	log.Info("Creating new sheet")
	newSheetPath := path.Join(tablePath, fmt.Sprintf("%d.csv", len(sheets)+1))

	utils.WriteFile(newSheetPath, strings.Join(schemaColumns, ",")+"\n")

	err = csv.AddRow(newSheetPath, values)
	if err != nil {
		log.Error(
			"Error adding row",
			prettylogger.Err(err),
			slog.String("sheetpath", newSheetPath),
		)
		return "", fmt.Errorf("%s: %v", op, err)
	}
	err = os.WriteFile(pkPath, []byte(strconv.Itoa(idInt+1)), 0644)
	if err != nil {
		log.Error(
			"Error writing a new pk",
			slog.String("pkpath", pkPath),
		)
		return "", err
	}

	return id, nil
}

func (s *Storage) Select(fields []string, tables []string, condition string) (*mysl.MySl[*mysl.MySl[string]], error) {
	const op = "storage.Select"
	log := s.log.With(
		slog.String("op", op),
		slog.Any("fields", fields),
		slog.Any("tables", tables),
		slog.String("condition", condition),
	)

	// validate all tables
	for _, table := range tables {
		if _, ok := s.TablePathes.Get(table).(string); !ok {
			return nil, fmt.Errorf("table %s is not exists", table)
		}
	}

	// validate all fields
	for _, field := range fields {
		fieldSplitted := strings.Split(field, ".")
		if len(fieldSplitted) != 2 {
			return nil, fmt.Errorf("field %s is not valid", field)
		}
		table := fieldSplitted[0]
		column := fieldSplitted[1]
		if !slices.Contains(tables, table) {
			return nil, fmt.Errorf("field %s not in tables", field)
		}
		tableCols, ok := s.Schema.Tables.Get(table).([]string)
		if !ok || !slices.Contains(tableCols, column) {
			return nil, fmt.Errorf("column %s not exists in table %s", column, table)
		}
	}

	var result *mysl.MySl[*mysl.MySl[string]]
	// No Where
	if condition == "" {
		if len(tables) == 1 {
			result = mysl.New[*mysl.MySl[string]]()
			data, err := s.getAllColumns(tables[0])
			if err != nil {
				return nil, err
			}
			for i := 0; i < data.Len(); i++ {
				result.Append(mysl.New[string]())
			}

			for _, field := range fields {
				for j := 0; j < data.Len(); j++ {
					value := data.Get(j).Get(field).(string)
					result.Get(j).Append(value)
				}
			}
		} else { // Cross Join
			// Get all data
			allTablesData := mysl.New[*mysl.MySl[*mymap.CustomMap]]()
			for _, table := range tables {
				data, err := s.getAllColumns(table)
				if err != nil {
					return nil, err
				}
				tableData := mysl.New[*mymap.CustomMap]()
				for j := 0; j < data.Len(); j++ {
					tableData.Append(data.Get(j))
				}
				allTablesData.Append(tableData)
			}
			// get cross join fields
			joinedRows := crossJoin(allTablesData)
			// Get needed fields
			result = mysl.New[*mysl.MySl[string]]()
			for i := 0; i < joinedRows.Len(); i++ {
				selectedRow := mysl.New[string]()
				for _, field := range fields {
					value, ok := joinedRows.Get(i).Get(field).(string)
					if ok {
						selectedRow.Append(value)
					}
				}
				result.Append(selectedRow)
			}
		}
	} else { // For Where
		head := s.GetConditionTree(condition)
		result = mysl.New[*mysl.MySl[string]]()
		if len(tables) == 1 {
			for _, field := range fields {
				fieldSplitted := strings.Split(field, ".")
				tableName := fieldSplitted[0]
				tablePath := s.TablePathes.Get(tableName).(string)

				sheets, err := utils.GetSheetsFromFiles(tablePath)
				if err != nil {
					log.Error(
						"Error getting sheets from table",
						prettylogger.Err(err),
						slog.String("tableName", tableName),
						slog.String("tablePath", tablePath),
					)
					return nil, fmt.Errorf("error getting sheets from table %s", tableName)
				}
				for _, sheetName := range sheets {
					sheetpath := path.Join(tablePath, sheetName)

					data, _, err := csv.ReadCSV(sheetpath, tableName)
					if err != nil {
						slog.Error(
							"Error reading sheet",
							prettylogger.Err(err),
							slog.String("sheetname", sheetName),
							slog.String("sheetpath", sheetpath),
						)
						return nil, fmt.Errorf("error reading sheet %s", sheetpath)
					}
					for j := 0; j < data.Len(); j++ {
						if j >= result.Len() {
							result.Append(mysl.New[string]())
						}
						row := data.Get(j)
						if s.IsValidRow(head, row, tables, tableName) {
							value, ok := row.Get(field).(string)
							if ok {
								result.Get(j).Append(value)
							}
						}
					}
				}
			}
		} else { // Cross Join
			// get data from each field and validate it by condition
			validatedData := mysl.New[*mysl.MySl[*mymap.CustomMap]]()
			for i := 0; i < len(tables); i++ {
				validatedData.Append(mysl.New[*mymap.CustomMap]())
			}
			for _, field := range fields {
				fieldSplitted := strings.Split(field, ".")
				tableName := fieldSplitted[0]
				tableIndex := slices.Index(tables, tableName)
				tablePath := s.TablePathes.Get(tableName).(string)

				sheets, err := utils.GetSheetsFromFiles(tablePath)
				if err != nil {
					return nil, fmt.Errorf("error getting sheets from table %s", tableName)
				}

				// read sheet -> validate each row -> if valid append to validatedData
				for _, sheetName := range sheets {
					sheetpath := path.Join(tablePath, sheetName)

					data, _, err := csv.ReadCSV(sheetpath, tableName)
					if err != nil {
						return nil, fmt.Errorf("error reading sheet %s", sheetpath)
					}
					for j := 0; j < data.Len(); j++ {
						row := data.Get(j)
						if s.IsValidRow(head, row, tables, tableName) {
							validatedData.Get(tableIndex).Append(row)
						}
					}
				}
			}

			// get cross join fields
			joinedRows := crossJoin(validatedData)

			// get needed fields
			for i := 0; i < joinedRows.Len(); i++ {
				selectedRow := mysl.New[string]()
				for _, field := range fields {
					value, ok := joinedRows.Get(i).Get(field).(string)
					if !ok {
						continue
					}
					selectedRow.Append(value)
				}
				result.Append(selectedRow)
			}
		}
	}

	s.log.Info(
		"select completed successfully",
		slog.Any("fields", fields),
		slog.Any("tables", tables),
		slog.String("condition", condition),
	)

	return result, nil
}

// crossJoin gets Dekart mult of slice of tables
func crossJoin(tables *mysl.MySl[*mysl.MySl[*mymap.CustomMap]]) *mysl.MySl[*mymap.CustomMap] {
	if tables.Len() == 0 {
		return &mysl.MySl[*mymap.CustomMap]{}
	}

	result := tables.Get(0)

	for i := 1; i < tables.Len(); i++ {
		result = combine(result, tables.Get(i))
	}

	return result
}

// combine merges rows from two tables
func combine(table1, table2 *mysl.MySl[*mymap.CustomMap]) *mysl.MySl[*mymap.CustomMap] {
	result := mysl.New[*mymap.CustomMap]()

	for i := range table1.Len() {
		for j := range table2.Len() {
			row1, row2 := table1.Get(i), table2.Get(j)
			keys1, keys2 := row1.Keys(), row2.Keys()
			// merge rows from table2 and table1
			combinedMap := mymap.New()
			for i := 0; i < keys1.Len(); i++ {
				key := keys1.Get(i)
				val := row1.Get(key)
				combinedMap.Add(key, val)
			}
			for i := 0; i < keys2.Len(); i++ {
				key := keys2.Get(i)
				val := row2.Get(key)
				combinedMap.Add(key, val)
			}
			result.Append(combinedMap)
		}
	}

	return result
}

func (s *Storage) getAllColumns(table string) (*mysl.MySl[*mymap.CustomMap], error) {
	const op = "storage.getAllColumns"
	log := s.log.With(
		slog.String("op", op),
	)

	result := mysl.New[*mymap.CustomMap]()

	tablePath := s.TablePathes.Get(table).(string)
	sheets, err := utils.GetSheetsFromFiles(tablePath)
	if err != nil {
		return nil, err
	}
	for _, sheet := range sheets {
		sheetPath := path.Join(tablePath, sheet)
		data, _, err := csv.ReadCSV(sheetPath, table)
		if err != nil {
			log.Error(
				"Sheet reading error",
				prettylogger.Err(err),
				slog.String("sheetpath", sheetPath),
			)
			return nil, err
		}
		for i := 0; i < data.Len(); i++ {
			result.Append(data.Get(i))
		}
	}
	return result, nil
}

func (s *Storage) Delete(tableName string) error {
	tablePath, ok1 := s.TablePathes.Get(tableName).(string)
	columns, ok2 := s.Schema.Tables.Get(tableName).([]string)
	if !ok1 || !ok2 {
		return ErrIncorectTable
	}

	os.RemoveAll(tablePath)
	s.CreateTable(tableName, tablePath, columns)

	return nil
}

func (s *Storage) DeleteWhere(tableName string, condition string) (error, int) {
	const op = "storage.deleteWhere"
	log := s.log.With(
		slog.String("op", op),
		slog.String("tableName", tableName),
		slog.String("condition", condition),
	)

	head := s.GetConditionTree(condition)

	tablePath := s.TablePathes.Get(tableName).(string)
	if tablePath == "" {
		return ErrIncorectTable, 0
	}

	sheets, err := utils.GetSheetsFromFiles(tablePath)
	if err != nil {
		log.Error(
			"error getting sheets",
			prettylogger.Err(err),
			slog.String("tablePath", tablePath),
		)
		return fmt.Errorf("error getting sheets"), 0
	}
	deleted := 0
	for _, sheet := range sheets {
		sheetPath := path.Join(tablePath, sheet)
		rows, _, err := csv.ReadCSV(sheetPath, tableName)
		if err != nil {
			log.Error(
				"error reading csv",
				prettylogger.Err(err),
				slog.String("sheetPath", sheetPath),
			)
			return fmt.Errorf("error reading csv"), 0
		}
		for i := 0; i < rows.Len(); i++ {
			if s.IsValidRow(head, rows.Get(i), []string{tableName}, tableName) {
				rows.Delete(i)
				i--
				deleted++
			}
		}
		cols, ok := s.Schema.Tables.Get(tableName).([]string)
		if !ok {
			log.Error(
				"cols for table not found in schema",
				slog.String("tableName", tableName),
			)
			return fmt.Errorf("table %s not found", tableName), deleted
		}
		csv.WriteFile(sheetPath, tableName, rows, cols)
	}
	return nil, deleted
}
