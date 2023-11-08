package go_simple_db

import (
	"database/sql"
	"fmt"

	"github.com/auho/go-simple-db/v2/driver/driver"
	"gorm.io/gorm"
)

type SimpleDB struct {
	*gorm.DB

	sqlDb  *sql.DB
	driver driver.Driver
}

// NewSimple
// new simple
func NewSimple(fn func() (driver.Driver, error)) (*SimpleDB, error) {
	d, err := fn()
	if err != nil {
		return nil, err
	}

	db := d.DB()

	sqlDb, err := db.DB()
	if err != nil {
		return nil, err
	}

	return &SimpleDB{
		DB:     db,
		sqlDb:  sqlDb,
		driver: d,
	}, nil
}

func (s *SimpleDB) Name() string {
	return s.DB.Name()
}

func (s *SimpleDB) DriverName() string {
	return s.driver.DriverName()
}

func (s *SimpleDB) GormDB() *gorm.DB {
	return s.DB
}

func (s *SimpleDB) SqlDB() *sql.DB {
	return s.sqlDb
}

func (s *SimpleDB) Ping() error {
	return s.sqlDb.Ping()
}

func (s *SimpleDB) Close() error {
	return s.sqlDb.Close()
}

func (s *SimpleDB) Truncate(table string) error {
	return s.driver.Truncate(table)
}

func (s *SimpleDB) Drop(table string) error {
	return s.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Error
}

func (s *SimpleDB) Copy(src string, dst string) error {
	return s.DB.Exec(fmt.Sprintf("CREATE TABLE %s LIKE %s", dst, src)).Error
}

func (s *SimpleDB) GetTableColumns(table string) ([]string, error) {
	var row struct {
		Database string
	}

	err := s.DB.Raw("SELECT DATABASE() AS 'database'").Scan(&row).Error
	if err != nil {
		return nil, err
	}

	query := "SELECT `COLUMN_NAME` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	var columns []string
	err = s.DB.Raw(query, row.Database, table).Pluck("COLUMN_NAME", &columns).Error
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (s *SimpleDB) BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error {
	return s.DB.Table(table).CreateInBatches(data, batchSize).Error
}

func (s *SimpleDB) BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for _, item := range data {
		m := make(map[string]any, fieldsLen)
		for k1, field := range fields {
			m[field] = item[k1]
		}

		sm = append(sm, m)
	}

	return s.BulkInsertFromSliceMap(table, sm, batchSize)
}

func (s *SimpleDB) BulkUpdateFromSliceMapById(table string, id string, data []map[string]any) error {
	for _, item := range data {
		_id, ok := item[id]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", table, id)
		}

		err := s.DB.Table(table).Where(fmt.Sprintf("%s = ?", id), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", table, id, _id, err)
		}
	}

	return nil
}
