package simple

import (
	"github.com/mailru/dbr"
	"database/sql"
	"reflect"
	"strconv"
	"fmt"
	"time"
	"strings"
)

var timeDefault time.Time
var timeType = reflect.TypeOf(timeDefault)

type DB struct {
	connection *dbr.Connection
}

func NewDB() *DB {
	return &DB{}
}

func (s *DB) Connection(dsn string, driver string) {
	var err error
	s.connection, err = dbr.Open(dsn, driver, nil)
	if err != nil {
		panic(err)
	}
}

func (s *DB) Close() {
	s.connection.Close()
}

func (s *DB) BulkInsertFromSliceMap(tableName string, unsavedRows []map[string]interface{}) (error) {
	keys := make([]string, 0)
	placeholders := make([]string, 0)
	firstRow := unsavedRows[0]
	for k := range firstRow {
		keys = append(keys, k)
		placeholders = append(placeholders, "?")
	}
	//
	//valueStrings := make([]string, 0, len(unsavedRows))
	//valueArgs := make([]interface{}, 0, len(unsavedRows)*len(keys))
	//for _, row := range unsavedRows {
	//	valueStrings = append(valueStrings, "("+strings.Join(placeholders, ",")+")")
	//	for _, v := range keys {
	//		valueArgs = append(valueArgs, row[v])
	//	}
	//}

	query := "INSERT INTO %s (%s) VALUES (%s)"
	query = fmt.Sprintf(query, tableName, strings.Join(keys, ","), strings.Join(placeholders, ","))

	tx, err := s.connection.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	defer stmt.Close()
	for _, row := range unsavedRows {
		valueArgs := make([]interface{}, 0, len(keys))
		for _, v := range keys {
			valueArgs = append(valueArgs, row[v])
		}

		_, err := stmt.Exec(valueArgs...)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *DB) QueryString(sql string) ([]map[string]string, error) {
	rows, err := s.connection.DB.Query(sql)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return s.rows2Strings(rows)
}

func (s *DB) QueryStringOne(sql string) (map[string]string, error) {
	rows, err := s.QueryString(sql)
	if err != nil {
		return nil, err
	}

	return rows[0], err
}

func (s *DB) rows2Strings(rows *sql.Rows) (resultsSlice []map[string]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := s.row2mapStr(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func (s *DB) row2mapStr(rows *sql.Rows, fields []string) (resultsMap map[string]string, err error) {
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

		if data, err := s.value2String(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err
		}
	}

	return result, nil
}

func (s *DB) value2String(rawValue *reflect.Value) (str string, err error) {
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
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
		// time type
	case reflect.Struct:
		if aa.ConvertibleTo(timeType) {
			str = vv.Convert(timeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
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
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}
