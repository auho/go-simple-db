package simple

import (
	"database/sql"
	"errors"
	"fmt"
)

var drivers = make(map[string]func(string) (Driver, error))

func RegisterDriver(driver string, newDriver func(string) (Driver, error)) {
	drivers[driver] = newDriver
}

func NewDriver(driver string, dsn string) (Driver, error) {
	if newDriver, ok := drivers[driver]; ok {
		return newDriver(dsn)
	} else {
		return nil, errors.New(fmt.Sprintf("driver[%s] is not exists", driver))
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
