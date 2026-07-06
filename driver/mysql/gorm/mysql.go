package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/auho/go-simple-db/v3/driver/driver"
	"github.com/auho/go-simple-db/v3/driver/mysql/internal"
	"github.com/auho/go-simple-db/v3/driver/util"
	"github.com/auho/go-simple-db/v3/schema"
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
	dsn   string
	opts  []gorm.Option
	sql   internal.SQL
}

func NewMySQL(dsn string, opts ...gorm.Option) (*MySQL, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &MySQL{
		db:    db,
		sqlDb: sqlDb,
		dsn:   dsn,
		opts:  slices.Clone(opts),
	}, nil
}

func (m *MySQL) DriverName() string { return driver.MySQL }

func (m *MySQL) GormDB() *gorm.DB { return m.db }

func (m *MySQL) SqlDB() *sql.DB { return m.sqlDb }

func (m *MySQL) Ping(ctx context.Context) error { return m.sqlDb.PingContext(ctx) }

func (m *MySQL) Close() error {
	return m.sqlDb.Close()
}

func (m *MySQL) Clone() (driver.Driver, error) { return NewMySQL(m.dsn, m.opts...) }

func (m *MySQL) Truncate(ctx context.Context, table string) error {
	return m.db.WithContext(ctx).Exec(m.sql.Truncate(table)).Error
}

func (m *MySQL) Drop(ctx context.Context, table string) error {
	return m.db.WithContext(ctx).Exec(m.sql.Drop(table)).Error
}

func (m *MySQL) CopyStructure(ctx context.Context, src string, dst string) error {
	return m.db.WithContext(ctx).Exec(m.sql.CopyStructure(src, dst)).Error
}

func (m *MySQL) CopyData(ctx context.Context, src string, dst string) error {
	return m.db.WithContext(ctx).Exec(m.sql.CopyData(src, dst)).Error
}

func (m *MySQL) RowCount(ctx context.Context, table string) (int, error) {
	var count int
	err := m.db.WithContext(ctx).Raw(m.sql.RowCount(table)).Scan(&count).Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (m *MySQL) GetTableColumnsSchema(ctx context.Context, table string) ([]schema.Column, error) {
	database, err := m.GetDatabase(ctx)
	if err != nil {
		return nil, err
	}

	var columns []schema.Column
	err = m.db.WithContext(ctx).Raw(m.sql.GetTableColumnsSchema(), database, table).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *MySQL) GetTableColumns(ctx context.Context, table string) ([]string, error) {
	database, err := m.GetDatabase(ctx)
	if err != nil {
		return nil, err
	}

	var columns []string
	err = m.db.WithContext(ctx).Raw(m.sql.GetTableColumns(), database, table).Pluck("COLUMN_NAME", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *MySQL) GetDatabase(ctx context.Context) (string, error) {
	var database string
	err := m.db.WithContext(ctx).Raw(m.sql.GetDatabase()).Scan(&database).Error
	if err != nil {
		return "", err
	}

	return database, nil
}

func (m *MySQL) BulkInsertFromSliceMap(ctx context.Context, table string, data []map[string]any, batchSize int) error {
	return m.db.WithContext(ctx).Table(table).CreateInBatches(data, batchSize).Error
}

func (m *MySQL) BulkInsertFromSliceSlice(ctx context.Context, table string, fields []string, data [][]any, batchSize int) error {
	return m.BulkInsertFromSliceMap(ctx, table, util.SliceSliceToSliceMap(fields, data), batchSize)
}

func (m *MySQL) BulkUpdateFromSliceMapByID(ctx context.Context, table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", table, id)
		}

		err := m.db.WithContext(ctx).Table(table).Where(fmt.Sprintf("%s = ?", id), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", table, id, _id, err)
		}
	}

	return nil
}
