package simpledb

import (
	"github.com/auho/go-simple-db/v3/driver/clickhouse/gorm"
	mysqlgorm "github.com/auho/go-simple-db/v3/driver/mysql/gorm"
	gormlib "gorm.io/gorm"
)

// NewMySQLGorm
// new mysql with gorm driver
func NewMySQLGorm(dsn string, opts ...gormlib.Option) (*SimpleDB, *gormlib.DB, error) {
	d, err := mysqlgorm.NewMySQL(dsn, opts...)
	if err != nil {
		return nil, nil, err
	}

	return NewSimple(d), d.GormDB(), nil
}

// NewClickHouseGorm
// new clickhouse with gorm driver
func NewClickHouseGorm(dsn string, opts ...gormlib.Option) (*SimpleDB, *gormlib.DB, error) {
	d, err := gorm.NewClickHouse(dsn, opts...)
	if err != nil {
		return nil, nil, err
	}

	return NewSimple(d), d.GormDB(), nil
}
