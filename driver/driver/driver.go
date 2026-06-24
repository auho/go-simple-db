package driver

import "github.com/auho/go-simple-db/v3/schema"

const MySQL = "mysql"
const ClickHouse = "clickhouse"

type Driver interface {
	DriverName() string
	Ping() error
	Close() error
	Truncate(table string) error
	Drop(table string) error
	CopyStructure(src string, dst string) error
	CopyData(src string, dst string) error
	RowCount(table string) (int, error)
	GetTableColumnsSchema(table string) ([]schema.Column, error)
	GetTableColumns(table string) ([]string, error)
	GetDatabase() (string, error)
	BulkInsertFromSliceMap(table string, data []map[string]any, batchSize int) error
	BulkInsertFromSliceSlice(table string, fields []string, data [][]any, batchSize int) error
	BulkUpdateFromSliceMapByID(table string, id string, data []map[string]any) error
}
