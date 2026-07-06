package driver

import (
	"context"

	"github.com/auho/go-simple-db/v3/schema"
)

const MySQL = "mysql"
const ClickHouse = "clickhouse"

type Driver interface {
	DriverName() string
	Ping(ctx context.Context) error
	Close() error
	Clone() (Driver, error)
	Truncate(ctx context.Context, table string) error
	Drop(ctx context.Context, table string) error
	CopyStructure(ctx context.Context, src string, dst string) error
	CopyData(ctx context.Context, src string, dst string) error
	RowCount(ctx context.Context, table string) (int, error)
	GetTableColumnsSchema(ctx context.Context, table string) ([]schema.Column, error)
	GetTableColumns(ctx context.Context, table string) ([]string, error)
	GetDatabase(ctx context.Context) (string, error)
	BulkInsertFromSliceMap(ctx context.Context, table string, data []map[string]any, batchSize int) error
	BulkInsertFromSliceSlice(ctx context.Context, table string, fields []string, data [][]any, batchSize int) error
	BulkUpdateFromSliceMapByID(ctx context.Context, table string, id string, data []map[string]any) error
}
