package go_simple_db

import (
	"github.com/auho/go-simple-db/v2/driver/clickhouse"
	"github.com/auho/go-simple-db/v2/driver/mysql"
	"gorm.io/gorm"
)

func NewMysql(dsn string, c *gorm.Config) (*SimpleDB, error) {
	return NewSimpleDB(mysql.NewDialector(dsn), c)
}

func NewClickhouse(dsn string, c *gorm.Config) (*SimpleDB, error) {
	return NewSimpleDB(clickhouse.NewDialector(dsn), c)
}
