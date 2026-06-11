package go_simple_db

import (
	"github.com/auho/go-simple-db/v2/driver/clickhouse/gorm"
	mysqlgorm "github.com/auho/go-simple-db/v2/driver/mysql/gorm"

	"github.com/auho/go-simple-db/v2/driver/driver"
	gormlib "gorm.io/gorm"
)

// NewMySQL
// new mysql with gorm driver
func NewMySQL(dsn string, opts ...gormlib.Option) (*SimpleDB, error) {
	return NewSimple(func() (driver.Driver, error) {
		return mysqlgorm.NewMySQL(dsn, opts...)
	})
}

// NewClickHouse
// new clickhouse with gorm driver
func NewClickHouse(dsn string, opts ...gormlib.Option) (*SimpleDB, error) {
	return NewSimple(func() (driver.Driver, error) {
		return gorm.NewClickHouse(dsn, opts...)
	})
}
