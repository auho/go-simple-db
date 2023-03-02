package mysql

import (
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewDialector
// 参考 https://github.com/go-sql-driver/mysql#dsn-data-source-name 获取详情
// "user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
func NewDialector(dsn string) gorm.Dialector {
	return mysql.Open(dsn)
}

var _ driver.Driver = (*Mysql)(nil)

type Mysql struct {
	db *gorm.DB
}

func NewMysql(dsn string, opts ...gorm.Option) (driver.Driver, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	return &Mysql{db: db}, nil
}

func (m *Mysql) DB() *gorm.DB {
	return m.db
}

func (m *Mysql) Truncate(table string) error {
	return m.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
}
