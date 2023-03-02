package go_simple_db

import (
	"database/sql"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"gorm.io/gorm"
)

type SimpleDB struct {
	*gorm.DB

	sqlDb  *sql.DB
	driver driver.Driver
}

// NewSimpleDB
// new simple db
func NewSimpleDB(fn func() (driver.Driver, error)) (*SimpleDB, error) {
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
		DB:    db,
		sqlDb: sqlDb,
	}, nil
}

func (d *SimpleDB) Name() string {
	return d.DB.Name()
}

func (d *SimpleDB) GormDB() *gorm.DB {
	return d.DB
}

func (d *SimpleDB) SqlDB() *sql.DB {
	return d.sqlDb
}

func (d *SimpleDB) Ping() error {
	return d.sqlDb.Ping()
}

func (d *SimpleDB) Truncate(table string) error {
	return d.driver.Truncate(table)
}

func (d *SimpleDB) Close() error {
	return d.sqlDb.Close()
}

func (d *SimpleDB) BulkInsertFromSliceMap(table string, data []map[string]interface{}, batchSize int) error {
	return d.Table(table).CreateInBatches(data, batchSize).Error
}
