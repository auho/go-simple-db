package clickhouse

import (
	_ "github.com/kshvakov/clickhouse"
	"github.com/auho/go-simple-db/simple"
)

func NewEngine(driver string) *simple.DB {
	s := simple.NewDB()
	s.Connection("clickhouse", driver)

	return s
}
