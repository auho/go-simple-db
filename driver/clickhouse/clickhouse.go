package clickhouse

import (
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// NewDialector
// https://github.com/go-gorm/clickhouse
// "tcp://localhost:9000?database=gorm&username=gorm&password=gorm&read_timeout=10&write_timeout=20"
func NewDialector(dsn string) gorm.Dialector {
	return clickhouse.Open(dsn)
}

var _ driver.Driver = (*Clickhouse)(nil)

type Clickhouse struct {
	db *gorm.DB
}

func NewClickhouse(dsn string, opts ...gorm.Option) (driver.Driver, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	return &Clickhouse{db: db}, nil
}

func (c *Clickhouse) DB() *gorm.DB {
	return c.db
}

func (c *Clickhouse) Truncate(table string) error {
	return c.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
}
