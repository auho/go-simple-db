package gorm

import (
	"database/sql"
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/driver/mysql/internal"
	"github.com/auho/go-simple-db/v2/driver/util"
	"github.com/auho/go-simple-db/v2/schema"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewDialector
// 参考 https://github.com/go-sql-driver/mysql#dsn-data-source-name 获取详情
// "user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
func NewDialector(dsn string) gorm.Dialector {
	return gormmysql.Open(dsn)
}

var _ driver.Driver = (*MySQL)(nil)
var _ driver.GormProvider = (*MySQL)(nil)
var _ driver.SqlDBProvider = (*MySQL)(nil)

type MySQL struct {
	db    *gorm.DB
	sqlDb *sql.DB
	sql   internal.SQL
}

func NewMySQL(dsn string, opts ...gorm.Option) (driver.Driver, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &MySQL{db: db, sqlDb: sqlDb}, nil
}

func (m *MySQL) DriverName() string {
	return driver.MySQL
}

func (m *MySQL) GormDB() *gorm.DB {
	return m.db
}

func (m *MySQL) SqlDB() *sql.DB {
	return m.sqlDb
}

func (m *MySQL) Ping() error {
	return m.sqlDb.Ping()
}

func (m *MySQL) Close() error {
	return m.sqlDb.Close()
}

func (m *MySQL) Truncate(table string) error {
	return m.db.Exec(m.sql.Truncate(table)).Error
}

func (m *MySQL) Drop(table string) error {
	return m.db.Exec(m.sql.Drop(table)).Error
}

func (m *MySQL) CopyStructure(src string, dst string) error {
	return m.db.Exec(m.sql.CopyStructure(src, dst)).Error
}

func (m *MySQL) CopyData(src string, dst string) error {
	return m.db.Exec(m.sql.CopyData(src, dst)).Error
}

func (m *MySQL) RowCount(table string) (int, error) {
	var count int
	err := m.db.Raw(m.sql.RowCount(table)).Scan(&count).Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (m *MySQL) GetTableColumnsSchema(table string) ([]schema.Column, error) {
	database, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}

	var columns []schema.Column
	err = m.db.Raw(m.sql.GetTableColumnsSchema(), database, table).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *MySQL) GetTableColumns(table string) ([]string, error) {
	database, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}

	var columns []string
	err = m.db.Raw(m.sql.GetTableColumns(), database, table).Pluck("COLUMN_NAME", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *MySQL) GetDatabase() (string, error) {
	var database string
	err := m.db.Raw(m.sql.GetDatabase()).Scan(&database).Error
	if err != nil {
		return "", err
	}

	return database, nil
}

func (m *MySQL) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return m.db.Table(table).CreateInBatches(data, batchSize).Error
}

func (m *MySQL) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	return m.BulkInsertFromSliceMap(table, util.SliceSliceToSliceMap(fields, data), batchSize)
}

func (m *MySQL) BulkUpdateFromSliceMapByID(table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", table, id)
		}

		err := m.db.Table(table).Where(fmt.Sprintf("%s = ?", id), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", table, id, _id, err)
		}
	}

	return nil
}
