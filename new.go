package go_simple_db

import (
	"github.com/auho/go-simple-db/v2/driver/clickhouse"
	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/driver/mysql"
	"gorm.io/gorm"
)

// NewMysql
// new mysql
func NewMysql(dsn string, opts ...gorm.Option) (*SimpleDB, error) {
	return NewSimpleDB(func() (driver.Driver, error) {
		return mysql.NewMysql(dsn, opts...)
	})
}

// NewClickhouse
// new clickhouse
func NewClickhouse(dsn string, opts ...gorm.Option) (*SimpleDB, error) {
	return NewSimpleDB(func() (driver.Driver, error) {
		return clickhouse.NewClickhouse(dsn, opts...)
	})
}
