package mysql

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/auho/go-simple-db/simple"
)

func NewEngine(dsn string) *simple.DB {
	s := simple.NewDB()
	s.Connection(simple.MYSQL, dsn)

	return s
}
