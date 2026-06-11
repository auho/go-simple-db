package clickhouse

import (
	"database/sql"
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

var _ driver.Driver = (*ClickHouse)(nil)
var _ driver.GormProvider = (*ClickHouse)(nil)

type ClickHouse struct {
	db    *gorm.DB
	sqlDb *sql.DB
}

func NewClickHouse(dsn string, opts ...gorm.Option) (driver.Driver, error) {
	db, err := gorm.Open(NewDialector(dsn), opts...)
	if err != nil {
		return nil, err
	}

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &ClickHouse{db: db, sqlDb: sqlDb}, nil
}

func (c *ClickHouse) DriverName() string {
	return driver.ClickHouse
}

func (c *ClickHouse) GormDB() *gorm.DB {
	return c.db
}

func (c *ClickHouse) SqlDB() *sql.DB {
	return c.sqlDb
}

func (c *ClickHouse) Ping() error {
	return c.sqlDb.Ping()
}

func (c *ClickHouse) Close() error {
	return c.sqlDb.Close()
}

func (c *ClickHouse) Truncate(table string) error {
	return c.db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table)).Error
}

func (c *ClickHouse) Drop(table string) error {
	return c.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)).Error
}

func (c *ClickHouse) CopyStructure(src string, dst string) error {
	return c.db.Exec(fmt.Sprintf("CREATE TABLE `%s` AS `%s` WITH NO DATA", dst, src)).Error
}

func (c *ClickHouse) CopyData(src string, dst string) error {
	return c.db.Exec(fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", dst, src)).Error
}

func (c *ClickHouse) RowCount(table string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT() FROM `%s`", table)
	err := c.db.Raw(query).Scan(&count).Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (c *ClickHouse) GetTableColumnsSchema(table string) ([]schema.Column, error) {
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

func (c *ClickHouse) GetTableColumns(table string) ([]string, error) {
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

func (c *ClickHouse) GetDatabase() (string, error) {
	var database string
	err := c.db.Raw("SELECT currentDatabase()").Scan(&database).Error
	if err != nil {
		return "", err
	}

	return database, nil
}

func (c *ClickHouse) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return c.db.Table(table).CreateInBatches(data, batchSize).Error
}

func (c *ClickHouse) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for _, item := range data {
		m := make(map[string]any, fieldsLen)
		for k, field := range fields {
			m[field] = item[k]
		}

		sm = append(sm, m)
	}

	return c.BulkInsertFromSliceMap(table, sm, batchSize)
}

func (c *ClickHouse) BulkUpdateFromSliceMapByID(table string, id string, data []map[string]any) error {
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
