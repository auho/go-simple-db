package driver

import (
	"github.com/auho/go-simple-db/v2/schema"
	"gorm.io/gorm"
)

const Mysql = "mysql"
const Clickhouse = "clickhouse"

type Driver interface {
	DriverName() string
	DB() *gorm.DB
	Truncate(table string) error
	Drop(table string) error
	Copy(src string, dst string) error
	CopyData(src string, dst string) error
	TableAmount(table string) (int, error)
	GetTableColumnsSchema(table string) ([]schema.Column, error)
	GetTableColumns(table string) ([]string, error)
	GetDatabase() (string, error)
	BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error
	BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error
	BulkUpdateFromSliceMapById(table string, id string, data []map[string]any) error
}
