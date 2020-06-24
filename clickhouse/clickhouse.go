package clickhouse

import (
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/auho/go-simple-db/simple"
)

func NewEngine(dsn string) *simple.DB {
	s := simple.NewDB()
	s.Connection(simple.CLICKHOUSE, dsn)

	return s
}
