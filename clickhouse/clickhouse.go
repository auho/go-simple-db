package clickhouse

import (
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/auho/go-simple-db/simple"
)

func NewEngine(driver string) *simple.DB {
	s := simple.NewDB()
	s.Connection("clickhouse", driver)

	return s
}
