package mysql

import (
	_ "github.com/go-sql-driver/mysql"
	"auho/go-simple-db/simple"
)

func NewEngine(driver string) *simple.DB {
	s := simple.NewDB()
	s.Connection("mysql", driver)

	return s
}