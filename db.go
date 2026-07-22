package simpledb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/auho/go-simple-db/v3/driver/driver"
	"github.com/auho/go-simple-db/v3/schema"
	"gorm.io/gorm"
)

type SimpleDB struct {
	driver driver.Driver
}

// NewSimple creates a SimpleDB with the given driver.
func NewSimple(d driver.Driver) *SimpleDB {
	return &SimpleDB{
		driver: d,
	}
}

func (s *SimpleDB) Driver() driver.Driver {
	return s.driver
}

func (s *SimpleDB) Clone() (*SimpleDB, error) {
	d, err := s.driver.Clone()
	if err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}

	return NewSimple(d), nil
}

// GormDB returns the underlying *gorm.DB if the driver implements GormProvider.
// Returns nil if the driver does not use GORM.
func (s *SimpleDB) GormDB() *gorm.DB {
	if gp, ok := s.driver.(driver.GormProvider); ok {
		return gp.GormDB()
	}

	return nil
}

// SqlDB returns the underlying *sql.DB if the driver implements SqlDBProvider.
// Returns nil if the driver does not support it.
func (s *SimpleDB) SqlDB() *sql.DB {
	if p, ok := s.driver.(driver.SqlDBProvider); ok {
		return p.SqlDB()
	}

	return nil
}

func (s *SimpleDB) Ping(ctx context.Context) error {
	return s.driver.Ping(ctx)
}

func (s *SimpleDB) Close() error {
	return s.driver.Close()
}

func (s *SimpleDB) Name(ctx context.Context) (string, error) {
	return s.driver.GetDatabase(ctx)
}

func (s *SimpleDB) DriverName() string {
	return s.driver.DriverName()
}

func (s *SimpleDB) Truncate(ctx context.Context, table string) error {
	return s.driver.Truncate(ctx, table)
}

func (s *SimpleDB) DropAndCopyStructure(ctx context.Context, src string, dst string) error {
	err := s.Drop(ctx, dst)
	if err != nil {
		return err
	}

	return s.CopyStructure(ctx, src, dst)
}

func (s *SimpleDB) Drop(ctx context.Context, table string) error {
	return s.driver.Drop(ctx, table)
}

func (s *SimpleDB) CopyStructure(ctx context.Context, src string, dst string) error {
	return s.driver.CopyStructure(ctx, src, dst)
}

func (s *SimpleDB) CopyData(ctx context.Context, src string, dst string) error {
	return s.driver.CopyData(ctx, src, dst)
}

func (s *SimpleDB) RowCount(ctx context.Context, table string) (int, error) {
	return s.driver.RowCount(ctx, table)
}

func (s *SimpleDB) GetTableColumnsSchema(ctx context.Context, table string) ([]schema.Column, error) {
	return s.driver.GetTableColumnsSchema(ctx, table)
}

func (s *SimpleDB) GetTableColumns(ctx context.Context, table string) ([]string, error) {
	return s.driver.GetTableColumns(ctx, table)
}

func (s *SimpleDB) GetDatabase(ctx context.Context) (string, error) {
	return s.driver.GetDatabase(ctx)
}

func (s *SimpleDB) BulkInsertFromSliceMap(ctx context.Context, table string, data []map[string]any, batchSize int) error {
	return s.driver.BulkInsertFromSliceMap(ctx, table, data, batchSize)
}

func (s *SimpleDB) BulkInsertFromSliceSlice(ctx context.Context, table string, fields []string, data [][]any, batchSize int) error {
	return s.driver.BulkInsertFromSliceSlice(ctx, table, fields, data, batchSize)
}

func (s *SimpleDB) BulkUpdateFromSliceMapByID(ctx context.Context, table string, id string, data []map[string]any) error {
	return s.driver.BulkUpdateFromSliceMapByID(ctx, table, id, data)
}
