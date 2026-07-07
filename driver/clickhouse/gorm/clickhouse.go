package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/auho/go-simple-db/v3/driver/clickhouse/internal"
	"github.com/auho/go-simple-db/v3/driver/driver"
	"github.com/auho/go-simple-db/v3/driver/util"
	"github.com/auho/go-simple-db/v3/schema"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// NewDialector
// https://github.com/go-gorm/clickhouse
// "tcp://localhost:9000?database=gorm&username=gorm&password=gorm&read_timeout=10&write_timeout=20"
func NewDialector(dsn string) gorm.Dialector {
	return gormclickhouse.Open(dsn)
}

var _ driver.Driver = (*ClickHouse)(nil)
var _ driver.GormProvider = (*ClickHouse)(nil)
var _ driver.SqlDBProvider = (*ClickHouse)(nil)

type ClickHouse struct {
	db    *gorm.DB
	sqlDb *sql.DB
	dsn   string
	opts  []gorm.Option
	sql   internal.SQL
}

func NewClickHouse(dsn string, opts ...gorm.Option) (*ClickHouse, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &ClickHouse{
		db:    db,
		sqlDb: sqlDb,
		dsn:   dsn,
		opts:  slices.Clone(opts),
	}, nil
}

func (c *ClickHouse) DriverName() string {
	return driver.ClickHouse
}

func (c *ClickHouse) GormDB() *gorm.DB {
	return c.db
}

func (c *ClickHouse) SqlDB() *sql.DB {
	return c.sqlDb
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	return c.sqlDb.PingContext(ctx)
}

func (c *ClickHouse) Close() error {
	return c.sqlDb.Close()
}

func (c *ClickHouse) Clone() (driver.Driver, error) { return NewClickHouse(c.dsn, c.opts...) }

func (c *ClickHouse) Truncate(ctx context.Context, table string) error {
	return c.db.WithContext(ctx).Exec(c.sql.Truncate(table)).Error
}

func (c *ClickHouse) Drop(ctx context.Context, table string) error {
	return c.db.WithContext(ctx).Exec(c.sql.Drop(table)).Error
}

func (c *ClickHouse) CopyStructure(ctx context.Context, src string, dst string) error {
	return c.db.WithContext(ctx).Exec(c.sql.CopyStructure(src, dst)).Error
}

func (c *ClickHouse) CopyData(ctx context.Context, src string, dst string) error {
	return c.db.WithContext(ctx).Exec(c.sql.CopyData(src, dst)).Error
}

func (c *ClickHouse) RowCount(ctx context.Context, table string) (int, error) {
	var count int
	err := c.db.WithContext(ctx).Raw(c.sql.RowCount(table)).Scan(&count).Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (c *ClickHouse) GetTableColumnsSchema(ctx context.Context, table string) ([]schema.Column, error) {
	database, err := c.GetDatabase(ctx)
	if err != nil {
		return nil, err
	}

	var columns []schema.Column
	err = c.db.WithContext(ctx).Raw(c.sql.GetTableColumnsSchema(), database, table).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (c *ClickHouse) GetTableColumns(ctx context.Context, table string) ([]string, error) {
	database, err := c.GetDatabase(ctx)
	if err != nil {
		return nil, err
	}

	var columns []string
	err = c.db.WithContext(ctx).Raw(c.sql.GetTableColumns(), database, table).Pluck("name", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (c *ClickHouse) GetDatabase(ctx context.Context) (string, error) {
	var database string
	err := c.db.WithContext(ctx).Raw(c.sql.GetDatabase()).Scan(&database).Error
	if err != nil {
		return "", err
	}

	return database, nil
}

func (c *ClickHouse) BulkInsertFromSliceMap(ctx context.Context, table string, data []map[string]any, batchSize int) error {
	return c.db.WithContext(ctx).Table(table).CreateInBatches(data, batchSize).Error
}

func (c *ClickHouse) BulkInsertFromSliceSlice(ctx context.Context, table string, fields []string, data [][]any, batchSize int) error {
	dataMap, err := util.SliceSliceToSliceMap(fields, data)
	if err != nil {
		return err
	}
	return c.BulkInsertFromSliceMap(ctx, table, dataMap, batchSize)
}

func (c *ClickHouse) BulkUpdateFromSliceMapByID(ctx context.Context, table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("item[%s] not found in map for table[%s]", id, table)
		}

		err := c.db.WithContext(ctx).Table(table).Where(fmt.Sprintf("%s = ?", c.sql.QuoteIdentifier(id)), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("UpdateColumns table[%s] %s[%v]: %w", table, id, _id, err)
		}
	}

	return nil
}
