package mysql

import (
	"database/sql"

	"github.com/auho/go-simple-db/simple"
	_ "github.com/go-sql-driver/mysql"
)

func NewDriver(dsn string) simple.DB {
	m := &Mysql{}
	m.Connection("mysql", dsn)

	return m
}

type Mysql struct {
	simple.DbDriver
}

func (m *Mysql) Connection(driver string, dsn string) {
	var err error
	m.Db, err = sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}
}

func (m *Mysql) Close() {
	_ = m.Db.Close()
}

func (m *Mysql) InsertFromSlice(tableName string, fields []string, unSavedRow []interface{}) (sql.Result, error) {
	query := m.GenerateInsertPrepareQuery(tableName, fields)

	return m.Db.Exec(query, unSavedRow...)
}

func (m *Mysql) InsertFromMap(tableName string, unSavedRow map[string]interface{}) (sql.Result, error) {
	fields := make([]string, 0, len(unSavedRow))
	values := make([]interface{}, 0, len(unSavedRow))
	for field := range unSavedRow {
		fields = append(fields, field)
		values = append(values, unSavedRow[field])
	}

	return m.InsertFromSlice(tableName, fields, values)
}

func (m *Mysql) BulkInsertFromSliceSlice(tableName string, fields []string, unSavedRows [][]interface{}) (sql.Result, error) {
	valuesArgs := make([]interface{}, 0, len(unSavedRows)*len(fields))
	for _, row := range unSavedRows {
		valuesArgs = append(valuesArgs, row)
	}

	query := m.GenerateBulkInsertPrepareQuery(tableName, fields, len(unSavedRows))

	return m.Db.Exec(query, valuesArgs...)
}

func (m *Mysql) BulkInsertFromSliceMap(tableName string, unSavedRows []map[string]interface{}) (sql.Result, error) {
	fields := make([]string, 0, len(unSavedRows[0]))
	for k := range unSavedRows[0] {
		fields = append(fields, k)
	}

	valuesArgs := make([]interface{}, 0, len(unSavedRows)*len(fields))
	for _, row := range unSavedRows {
		for _, field := range fields {
			valuesArgs = append(valuesArgs, row[field])
		}
	}

	query := m.GenerateBulkInsertPrepareQuery(tableName, fields, len(unSavedRows))

	return m.Db.Exec(query, valuesArgs...)
}

func (m *Mysql) UpdateFromMapById(tableName string, keyName string, unSavedRow map[string]interface{}) error {
	unSavedRows := make([]map[string]interface{}, 0, 1)
	unSavedRows = append(unSavedRows, unSavedRow)

	return m.BulkUpdateFromSliceMapById(tableName, keyName, unSavedRows)
}

func (m *Mysql) BulkUpdateFromSliceMapById(tableName string, keyName string, unSavedRows []map[string]interface{}) error {
	query, fields := m.GenerateUpdatePrepareQuery(tableName, keyName, unSavedRows[0])
	stmt, err := m.Db.Prepare(query)
	if err != nil {
		return err
	}

	defer func() {
		_ = stmt.Close()
	}()

	for _, row := range unSavedRows {
		setArgs := make([]interface{}, 0, len(unSavedRows))
		for _, field := range fields {
			setArgs = append(setArgs, row[field])
		}

		setArgs = append(setArgs, unSavedRows)

		_, err := stmt.Exec(setArgs...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Mysql) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.Db.Exec(query, args...)
}

func (m *Mysql) QueryInterfaceRow(query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := m.QueryInterface(query, args...)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (m *Mysql) QueryInterface(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := m.Db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	return m.Rows2Interfaces(rows)
}

func (m *Mysql) QueryStringRow(query string, args ...interface{}) (map[string]string, error) {
	rows, err := m.QueryString(query, args...)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], err
}

func (m *Mysql) QueryString(query string, args ...interface{}) ([]map[string]string, error) {
	rows, err := m.Db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	return m.Rows2Strings(rows)
}
