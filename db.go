package go_simple_db

import (
	"database/sql"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/schema"
	"gorm.io/gorm"
)

type SimpleDB struct {
	*gorm.DB

	sqlDb  *sql.DB
	driver driver.Driver
}

// NewSimple
// new simple
func NewSimple(fn func() (driver.Driver, error)) (*SimpleDB, error) {
	d, err := fn()
	if err != nil {
		return nil, err
	}

	db := d.DB()

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &SimpleDB{
		DB:     db,
		sqlDb:  sqlDb,
		driver: d,
	}, nil
}

func (s *SimpleDB) Name() string {
	return s.DB.Name()
}

func (s *SimpleDB) DriverName() string {
	return s.driver.DriverName()
}

func (s *SimpleDB) GormDB() *gorm.DB {
	return s.DB
}

func (s *SimpleDB) SqlDB() *sql.DB {
	return s.sqlDb
}

func (s *SimpleDB) Ping() error {
	return s.sqlDb.Ping()
}

func (s *SimpleDB) Close() error {
	return s.sqlDb.Close()
}

func (s *SimpleDB) Truncate(table string) error {
	return s.driver.Truncate(table)
}

func (s *SimpleDB) DropAndCopy(src string, dst string) error {
	err := s.Drop(dst)
	if err != nil {
		return err
	}

	return s.Copy(src, dst)
}

func (s *SimpleDB) Drop(table string) error {
	return s.driver.Drop(table)
}

func (s *SimpleDB) Copy(src string, dst string) error {
	return s.driver.Copy(src, dst)
}

func (s *SimpleDB) CopyData(src string, dst string) error {
	return s.driver.CopyData(src, dst)
}

func (s *SimpleDB) TableAmount(table string) (int, error) {
	return s.driver.TableAmount(table)
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

func (s *SimpleDB) BulkUpdateFromSliceMapById(table string, id string, data []map[string]any) error {
	return s.driver.BulkUpdateFromSliceMapById(table, id, data)
}
