package driver

import "gorm.io/gorm"

const Mysql = "mysql"
const Clickhouse = "clickhouse"

type Driver interface {
	DriverName() string
	DB() *gorm.DB
	Truncate(table string) error
}
