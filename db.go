package go_simple_db

import (
	"github.com/auho/go-simple-db/simple"
	"github.com/auho/go-simple-db/mysql"
	"github.com/auho/go-simple-db/clickhouse"
	"fmt"
)

func New(driver string, dsn string) *simple.DB {
	switch driver {
	case simple.MYSQL:
		return mysql.NewEngine(dsn)
	case simple.CLICKHOUSE:
		return clickhouse.NewEngine(dsn)
	default:
		panic(fmt.Sprintf("driver[%s]is error", driver))
	}
}
