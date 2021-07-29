package simple

import (
	"github.com/auho/go-simple-db/mysql"
)

func init() {
	RegisterMysqlDriver()
}

func NewMysqlDriver(dsn string) (Driver, error) {
	m := mysql.NewMysql(dsn)
	err := m.Connection()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func RegisterMysqlDriver() {
	RegisterDriver("mysql", func(dsn string) (Driver, error) {
		driver, err := NewMysqlDriver(dsn)
		if err != nil {
			return nil, err
		}

		return driver, nil
	})
}
