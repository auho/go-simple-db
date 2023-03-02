package go_simple_db

import (
	"database/sql"

	"gorm.io/gorm"
)

type SimpleDB struct {
	*gorm.DB

	sqlDb *sql.DB
}

// NewSimpleDB
// new simple db
func NewSimpleDB(dial gorm.Dialector, c *gorm.Config) (*SimpleDB, error) {
	if c == nil {
		c = &gorm.Config{}
	}

	db, err := gorm.Open(dial, c)
	if err != nil {
		return nil, err
	}

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

func (d *SimpleDB) Close() error {
	return d.sqlDb.Close()
}

func (d *SimpleDB) BulkInsertFromSliceMap(tableName string, data []map[string]interface{}, batchSize int) error {
	return d.Table(tableName).CreateInBatches(data, batchSize).Error
}
