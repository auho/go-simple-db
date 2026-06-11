package mysql

import (
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"github.com/auho/go-simple-db/v2/schema"
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

func (m *Mysql) DriverName() string {
	return driver.Mysql
}

func (m *Mysql) Drop(table string) error {
	return m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)).Error
}

func (m *Mysql) Copy(src string, dst string) error {
	return m.db.Exec(fmt.Sprintf("CREATE TABLE `%s` LIKE `%s`", dst, src)).Error
}

func (m *Mysql) CopyData(src string, dst string) error {
	return m.db.Exec(fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`", dst, src)).Error
}

func (m *Mysql) TableAmount(table string) (int, error) {
	var row struct {
		Amount int
	}

	_sql := fmt.Sprintf("SELECT COUNT(*) AS 'amount' FROM `%s`", table)
	err := m.db.Raw(_sql).Scan(&row).Error
	if err != nil {
		return 0, err
	}

	return row.Amount, nil
}

func (m *Mysql) GetTableColumnsSchema(table string) ([]schema.Column, error) {
	database, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}

	query := "SELECT `COLUMN_NAME` AS 'name', `DATA_TYPE` AS `field_type` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	var columns []schema.Column
	err = m.db.Raw(query, database, table).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *Mysql) GetTableColumns(table string) ([]string, error) {
	database, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}

	query := "SELECT `COLUMN_NAME` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	var columns []string
	err = m.db.Raw(query, database, table).Pluck("COLUMN_NAME", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (m *Mysql) GetDatabase() (string, error) {
	var row struct {
		Database string
	}

	err := m.db.Raw("SELECT DATABASE() AS 'database'").Scan(&row).Error
	if err != nil {
		return "", err
	}

	return row.Database, nil
}

func (m *Mysql) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return m.db.Table(table).CreateInBatches(data, batchSize).Error
}

func (m *Mysql) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for _, item := range data {
		m := make(map[string]any, fieldsLen)
		for k1, field := range fields {
			m[field] = item[k1]
		}

		sm = append(sm, m)
	}

	return m.BulkInsertFromSliceMap(table, sm, batchSize)
}

func (m *Mysql) BulkUpdateFromSliceMapById(table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", table, id)
		}

		err := m.db.Table(table).Where(fmt.Sprintf("%s = ?", id), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", table, id, _id, err)
		}
	}

	return nil
}
