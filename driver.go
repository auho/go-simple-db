package go_simple_db

import (
	"github.com/auho/go-simple-db/v2/driver/clickhouse"
	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/driver/mysql"
	"gorm.io/gorm"
)

// NewMySQL
// new mysql
func NewMySQL(dsn string, opts ...gorm.Option) (*SimpleDB, error) {
	return NewSimple(func() (driver.Driver, error) {
		return mysql.NewMySQL(dsn, opts...)
	})
}

// NewClickHouse
// new clickhouse
func NewClickHouse(dsn string, opts ...gorm.Option) (*SimpleDB, error) {
	return NewSimple(func() (driver.Driver, error) {
		return clickhouse.NewClickHouse(dsn, opts...)
	})
}
