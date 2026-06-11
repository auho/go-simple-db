package go_simple_db

import (
	"github.com/auho/go-simple-db/v2/driver/clickhouse/gorm"
	mysqlgorm "github.com/auho/go-simple-db/v2/driver/mysql/gorm"
	gormlib "gorm.io/gorm"
)

// NewMySQLGorm
// new mysql with gorm driver
func NewMySQLGorm(dsn string, opts ...gormlib.Option) (*SimpleDB, error) {
	d, err := mysqlgorm.NewMySQL(dsn, opts...)
	if err != nil {
		return nil, err
	}

	return NewSimple(d), nil
}

// NewClickHouseGorm
// new clickhouse with gorm driver
func NewClickHouseGorm(dsn string, opts ...gormlib.Option) (*SimpleDB, error) {
	d, err := gorm.NewClickHouse(dsn, opts...)
	if err != nil {
		return nil, err
	}

	return NewSimple(d), nil
}
