package go_simple_db

import (
	"database/sql"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/schema"
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

func (s *SimpleDB) Ping() error {
	return s.driver.Ping()
}

func (s *SimpleDB) Close() error {
	return s.driver.Close()
}

func (s *SimpleDB) Name() string {
	database, _ := s.driver.GetDatabase()
	return database
}

func (s *SimpleDB) DriverName() string {
	return s.driver.DriverName()
}

func (s *SimpleDB) Truncate(table string) error {
	return s.driver.Truncate(table)
}

func (s *SimpleDB) DropAndCopyStructure(src string, dst string) error {
	err := s.Drop(dst)
	if err != nil {
		return err
	}

	return s.CopyStructure(src, dst)
}

func (s *SimpleDB) Drop(table string) error {
	return s.driver.Drop(table)
}

func (s *SimpleDB) CopyStructure(src string, dst string) error {
	return s.driver.CopyStructure(src, dst)
}

func (s *SimpleDB) CopyData(src string, dst string) error {
	return s.driver.CopyData(src, dst)
}

func (s *SimpleDB) RowCount(table string) (int, error) {
	return s.driver.RowCount(table)
}

func (s *SimpleDB) GetTableColumnsSchema(table string) ([]schema.Column, error) {
	return s.driver.GetTableColumnsSchema(table)
}

func (s *SimpleDB) GetTableColumns(table string) ([]string, error) {
	return s.driver.GetTableColumns(table)
}

func (s *SimpleDB) GetDatabase() (string, error) {
	return s.driver.GetDatabase()
}

func (s *SimpleDB) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return s.driver.BulkInsertFromSliceMap(table, data, batchSize)
}

func (s *SimpleDB) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	return s.driver.BulkInsertFromSliceSlice(table, fields, data, batchSize)
}

func (s *SimpleDB) BulkUpdateFromSliceMapByID(table string, id string, data []map[string]any) error {
	return s.driver.BulkUpdateFromSliceMapByID(table, id, data)
}
