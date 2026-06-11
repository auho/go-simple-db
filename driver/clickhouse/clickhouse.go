package clickhouse

import (
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/schema"
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

func (c *Clickhouse) DriverName() string {
	return driver.Clickhouse
}

func (c *Clickhouse) Drop(table string) error {
	return c.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)).Error
}

func (c *Clickhouse) Copy(src string, dst string) error {
	return c.db.Exec(fmt.Sprintf("CREATE TABLE `%s` AS `%s` WITH NO DATA", dst, src)).Error
}

func (c *Clickhouse) CopyData(src string, dst string) error {
	return c.db.Exec(fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", dst, src)).Error
}

func (c *Clickhouse) TableAmount(table string) (int, error) {
	var row struct {
		Amount int
	}

	_sql := fmt.Sprintf("SELECT COUNT() AS amount FROM `%s`", table)
	err := c.db.Raw(_sql).Scan(&row).Error
	if err != nil {
		return 0, err
	}

	return row.Amount, nil
}

func (c *Clickhouse) GetTableColumnsSchema(table string) ([]schema.Column, error) {
	database, err := c.GetDatabase()
	if err != nil {
		return nil, err
	}

	query := "SELECT `name`, `type` AS `field_type` " +
		"FROM `system`.`columns` " +
		"WHERE `database` = ? AND `table` = ?"

	var columns []schema.Column
	err = c.db.Raw(query, database, table).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (c *Clickhouse) GetTableColumns(table string) ([]string, error) {
	database, err := c.GetDatabase()
	if err != nil {
		return nil, err
	}

	query := "SELECT `name` " +
		"FROM `system`.`columns` " +
		"WHERE `database` = ? AND `table` = ?"

	var columns []string
	err = c.db.Raw(query, database, table).Pluck("name", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (c *Clickhouse) GetDatabase() (string, error) {
	var row struct {
		Database string
	}

	err := c.db.Raw("SELECT currentDatabase() AS database").Scan(&row).Error
	if err != nil {
		return "", err
	}

	return row.Database, nil
}

func (c *Clickhouse) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return c.db.Table(table).CreateInBatches(data, batchSize).Error
}

func (c *Clickhouse) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for _, item := range data {
		m := make(map[string]any, fieldsLen)
		for k1, field := range fields {
			m[field] = item[k1]
		}

		sm = append(sm, m)
	}

	return c.BulkInsertFromSliceMap(table, sm, batchSize)
}

func (c *Clickhouse) BulkUpdateFromSliceMapById(table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", table, id)
		}

		err := c.db.Table(table).Where(fmt.Sprintf("%s = ?", id), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", table, id, _id, err)
		}
	}

	return nil
}
