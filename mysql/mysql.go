package mysql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/auho/go-simple-db/simple"
	_ "github.com/go-sql-driver/mysql"
)

func NewMysql(dsn string) *Mysql {
	m := &Mysql{}
	m.Dsn = dsn

	return m
}

func NewDriver(dsn string) (simple.Driver, error) {
	m := NewMysql(dsn)
	err := m.Connection()
	if err != nil {
		return nil, err
	}

	return m, nil
}

type Mysql struct {
	simple.Engine
}

func (m *Mysql) Connection() error {
	var err error
	m.DB, err = sql.Open("mysql", m.Dsn)
	if err != nil {
		return err
	}

	return nil
}
func (m *Mysql) Ping() error {
	return m.DB.Ping()
}

func (m *Mysql) Close() {
	_ = m.DB.Close()
}

func (m *Mysql) InsertFromSlice(tableName string, fields []string, unSavedRow []interface{}) (sql.Result, error) {
	query := m.GenerateInsertPrepareQuery(tableName, fields)

	return m.DB.Exec(query, unSavedRow...)
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
		valuesArgs = append(valuesArgs, row...)
	}

	query := m.GenerateBulkInsertPrepareQuery(tableName, fields, len(unSavedRows))

	return m.DB.Exec(query, valuesArgs...)
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

	return m.DB.Exec(query, valuesArgs...)
}

func (m *Mysql) UpdateFromMapById(tableName string, keyName string, unSavedRow map[string]interface{}) error {
	if len(unSavedRow) <= 1 {
		return errors.New("row is error")
	}

	unSavedRows := make([]map[string]interface{}, 0, 1)
	unSavedRows = append(unSavedRows, unSavedRow)

	return m.BulkUpdateFromSliceMapById(tableName, keyName, unSavedRows)
}

func (m *Mysql) BulkUpdateFromSliceMapById(tableName string, keyName string, unSavedRows []map[string]interface{}) error {
	query, fields, err := m.GenerateUpdatePrepareQuery(tableName, keyName, unSavedRows[0])
	if err != nil {
		return err
	}

	stmt, err := m.DB.Prepare(query)
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

		setArgs = append(setArgs, row[keyName])

		_, err := stmt.Exec(setArgs...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Mysql) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.DB.Exec(query, args...)
}

func (m *Mysql) Truncate(tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)
	_, err := m.Exec(query)

	return err
}

func (m *Mysql) Drop(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err := m.Exec(query)

	return err
}

func (m *Mysql) GetTableColumn(tableName string) ([]interface{}, error) {
	query := "SELECT `COLUMN_NAME` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_NAME` = ?"

	return m.QueryFieldInterfaceSlice(query, tableName)
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
	rows, err := m.DB.Query(query, args...)
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
	rows, err := m.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	return m.Rows2Strings(rows)
}

func (m *Mysql) QueryFieldInterfaceSlice(field string, query string, args ...interface{}) ([]interface{}, error) {
	rows, err := m.QueryInterface(query, args...)
	if err != nil {
		return nil, err
	}

	if rows == nil {
		return nil, nil
	}

	values := make([]interface{}, len(rows), len(rows))
	for k := range rows {
		values[k] = rows[k][field]
	}

	return values, nil
}

func (m *Mysql) QueryFieldInterface(field string, query string, args ...interface{}) (interface{}, error) {
	row, err := m.QueryInterfaceRow(query, args...)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	return row[field], nil
}
