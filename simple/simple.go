package simple

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var drivers = make(map[string]func(string) (Driver, error))

func RegisterDriver(driver string, newDriver func(string) (Driver, error)) {
	drivers[driver] = newDriver
}

func NewDriver(driver string, dsn string) (Driver, error) {
	if newDriver, ok := drivers[driver]; ok {
		return newDriver(dsn)
	} else {
		return nil, errors.New(fmt.Sprintf("%s driver is not exists", driver))
	}
}

type Driver interface {
	Connection() error
	Ping() error
	Close()
	InsertFromSlice(tableName string, fields []string, unSavedRow []interface{}) (sql.Result, error)
	InsertFromMap(tableName string, unSavedRow map[string]interface{}) (sql.Result, error)
	BulkInsertFromSliceSlice(tableName string, fields []string, unSavedRow [][]interface{}) (sql.Result, error)
	BulkInsertFromSliceMap(tableName string, unSavedRows []map[string]interface{}) (sql.Result, error)
	UpdateFromMapById(tableName string, keyName string, unSavedRow map[string]interface{}) error
	BulkUpdateFromSliceMapById(tableName string, keyName string, unSavedRows []map[string]interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Truncate(tableName string) error
	Drop(tableName string) error
	Copy(sourceTableName string, targetTableName string) error
	GetTableColumns(tableName string) ([]string, error)
	QueryInterfaceRow(query string, args ...interface{}) (map[string]interface{}, error)
	QueryInterface(query string, args ...interface{}) ([]map[string]interface{}, error)
	QueryStringRow(query string, args ...interface{}) (map[string]string, error)
	QueryString(query string, args ...interface{}) ([]map[string]string, error)
	QueryFieldInterface(field string, query string, args ...interface{}) (interface{}, error)
	QueryFieldInterfaceSlice(field string, query string, args ...interface{}) ([]interface{}, error)
}

var timeDefault time.Time
var timeType = reflect.TypeOf(timeDefault)

type Engine struct {
	Dsn string
	DB  *sql.DB
}

func (e *Engine) Rows2Strings(rows *sql.Rows) (resultsSlice []map[string]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := e.row2mapString(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func (e *Engine) row2mapString(rows *sql.Rows, fields []string) (resultsMap map[string]string, err error) {
	result := make(map[string]string)
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
		// if row is null then as empty string
		if rawValue.Interface() == nil {
			result[key] = ""
			continue
		}

		if data, err := e.value2String(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err
		}
	}

	return result, nil
}

func (e *Engine) value2String(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
			if str == "\x00" {
				str = "0"
			}
		default:
			err = fmt.Errorf("Unsupported struct type %v ", vv.Type().Name())
		}
		// time type
	case reflect.Struct:
		if aa.ConvertibleTo(timeType) {
			str = vv.Convert(timeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v ", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
		/* TODO: unsupported types below
		   case reflect.Map:
		   case reflect.Ptr:
		   case reflect.Uintptr:
		   case reflect.UnsafePointer:
		   case reflect.Chan, reflect.Func, reflect.Interface:
		*/
	default:
		err = fmt.Errorf("Unsupported struct type %v ", vv.Type().Name())
	}

	return
}

func (e *Engine) Rows2Interfaces(rows *sql.Rows) (resultsSlice []map[string]interface{}, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := e.row2mapInterface(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func (e *Engine) row2mapInterface(rows *sql.Rows, fields []string) (resultsMap map[string]interface{}, err error) {
	resultsMap = make(map[string]interface{}, len(fields))
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		resultsMap[key] = reflect.Indirect(reflect.ValueOf(scanResultContainers[ii])).Interface()
	}

	return
}

func (e *Engine) GenerateInsertPrepareQuery(tableName string, fields []string) string {
	placeholders := make([]string, len(fields), len(fields))
	for k := range fields {
		placeholders[k] = "?"
	}

	query := "INSERT INTO %s (`%s`) VALUES (%s)"
	return fmt.Sprintf(query, tableName, strings.Join(fields, "`, `"), strings.Join(placeholders, ","))
}

func (e *Engine) GenerateBulkInsertPrepareQuery(tableName string, fields []string, rowsAmount int) string {
	placeholders := make([]string, len(fields), len(fields))
	for k, _ := range placeholders {
		placeholders[k] = "?"
	}

	valueArg := "(" + strings.Join(placeholders, ",") + ")"

	valuesArgs := make([]string, 0, rowsAmount)
	for i := 0; i < rowsAmount; i++ {
		valuesArgs = append(valuesArgs, valueArg)
	}

	query := "INSERT INTO %s (`%s`) VALUES %s"
	return fmt.Sprintf(query, tableName, strings.Join(fields, "`, `"), strings.Join(valuesArgs, ","))
}

func (e *Engine) GenerateUpdatePrepareQuery(tableName string, keyName string, unSavedRow map[string]interface{}) (query string, fields []string, err error) {
	if len(unSavedRow) <= 1 {
		return "", nil, errors.New("number of key in the row is less then 1")
	}

	if _, ok := unSavedRow[keyName]; !ok {
		return "", nil, errors.New("row has no key name")
	}

	setPlaceholders := make([]string, 0, len(unSavedRow))
	fields = make([]string, 0, len(unSavedRow)-1)

	for k := range unSavedRow {
		if k == keyName {
			continue
		}

		setPlaceholders = append(setPlaceholders, "`"+k+"` = ?")
		fields = append(fields, k)
	}

	return fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = ?", tableName, strings.Join(setPlaceholders, ", "), keyName), fields, nil
}
