package gorm_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	mysqlgorm "github.com/auho/go-simple-db/v3/driver/mysql/gorm"
	"github.com/auho/go-simple-db/v3/internal/testutil/mysql"
	mysqlgormtest "github.com/auho/go-simple-db/v3/internal/testutil/mysql/gorm"
)

const testTable = "test_gorm"

func createTableDDL(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE `%s` (`id` BIGINT NOT NULL AUTO_INCREMENT, `name` VARCHAR(100) DEFAULT NULL, `age` INT DEFAULT NULL, PRIMARY KEY (`id`))",
		table,
	)
}

func recreateTable(t *testing.T, m *mysqlgorm.MySQL, table string) {
	t.Helper()
	if err := mysqlgormtest.RecreateTable(m.GormDB(), table, createTableDDL(table)); err != nil {
		t.Fatal(err)
	}
}

var testDB *mysqlgorm.MySQL

func TestMain(m *testing.M) {
	fullDSN, err := mysqlgormtest.Setup()
	if err != nil {
		log.Fatalf("setup dsn: %v", err)
	}

	db, err := mysqlgorm.NewMySQL(fullDSN)
	if err != nil {
		log.Fatalf("new mysql: %v", err)
	}

	testDB = db
	code := m.Run()
	if err = testDB.Close(); err != nil {
		log.Printf("close: %v", err)
	}

	os.Exit(code)
}

func TestMySQL_DriverName(t *testing.T) {
	if got := testDB.DriverName(); got != "mysql" {
		t.Fatalf("DriverName = %q, want %q", got, "mysql")
	}
}

func TestMySQL_Ping(t *testing.T) {
	if err := testDB.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMySQL_Clone(t *testing.T) {
	cloned, err := testDB.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer cloned.Close()
	if err := cloned.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMySQL_GetDatabase(t *testing.T) {
	got, err := testDB.GetDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != mysqltestutil.TestDatabase {
		t.Fatalf("GetDatabase = %q, want %q", got, mysqltestutil.TestDatabase)
	}
}

func TestMySQL_GetTableColumns(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	columns, err := testDB.GetTableColumns(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "name", "age"}
	if len(columns) != len(want) {
		t.Fatalf("GetTableColumns = %v, want %v", columns, want)
	}
	for i, c := range columns {
		if c != want[i] {
			t.Fatalf("GetTableColumns[%d] = %q, want %q", i, c, want[i])
		}
	}
}

func TestMySQL_GetTableColumnsSchema(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	columns, err := testDB.GetTableColumnsSchema(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 3 {
		t.Fatalf("GetTableColumnsSchema returned %d columns, want 3", len(columns))
	}
	if columns[0].Name != "id" {
		t.Fatalf("columns[0].Name = %q, want %q", columns[0].Name, "id")
	}
}

func TestMySQL_RowCount(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	if err := testDB.BulkInsertFromSliceMap(ctx, testTable, []map[string]any{
		{"name": "alice", "age": 30},
		{"name": "bob", "age": 25},
	}, 100); err != nil {
		t.Fatal(err)
	}
	count, err := testDB.RowCount(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("RowCount = %d, want 2", count)
	}
}

func TestMySQL_Truncate(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	if err := testDB.BulkInsertFromSliceMap(ctx, testTable, []map[string]any{
		{"name": "alice", "age": 30},
	}, 100); err != nil {
		t.Fatal(err)
	}
	if err := testDB.Truncate(ctx, testTable); err != nil {
		t.Fatal(err)
	}
	count, err := testDB.RowCount(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("RowCount after Truncate = %d, want 0", count)
	}
}

func TestMySQL_Drop(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	if err := testDB.Drop(ctx, testTable); err != nil {
		t.Fatal(err)
	}
	if _, err := testDB.RowCount(ctx, testTable); err == nil {
		t.Fatal("RowCount after Drop: unexpected success, want error")
	}
}

func TestMySQL_CopyStructure(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	dst := testTable + "_copy"
	defer testDB.Drop(ctx, dst)

	if err := testDB.CopyStructure(ctx, testTable, dst); err != nil {
		t.Fatal(err)
	}
	columns, err := testDB.GetTableColumns(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 3 {
		t.Fatalf("GetTableColumns(dst) = %v, want 3 columns", columns)
	}
}

func TestMySQL_CopyData(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	if err := testDB.BulkInsertFromSliceMap(ctx, testTable, []map[string]any{
		{"name": "alice", "age": 30},
		{"name": "bob", "age": 25},
	}, 100); err != nil {
		t.Fatal(err)
	}
	dst := testTable + "_copy"
	defer testDB.Drop(ctx, dst)
	if err := testDB.CopyStructure(ctx, testTable, dst); err != nil {
		t.Fatal(err)
	}
	if err := testDB.CopyData(ctx, testTable, dst); err != nil {
		t.Fatal(err)
	}
	count, err := testDB.RowCount(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("RowCount(dst) = %d, want 2", count)
	}
}

func TestMySQL_BulkInsertFromSliceMap(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	data := []map[string]any{
		{"name": "alice", "age": 30},
		{"name": "bob", "age": 25},
		{"name": "carol", "age": 28},
	}
	if err := testDB.BulkInsertFromSliceMap(ctx, testTable, data, 2); err != nil {
		t.Fatal(err)
	}
	count, err := testDB.RowCount(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("RowCount = %d, want 3", count)
	}
}

func TestMySQL_BulkInsertFromSliceSlice(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	fields := []string{"name", "age"}
	data := [][]any{
		{"alice", 30},
		{"bob", 25},
	}
	if err := testDB.BulkInsertFromSliceSlice(ctx, testTable, fields, data, 100); err != nil {
		t.Fatal(err)
	}
	count, err := testDB.RowCount(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("RowCount = %d, want 2", count)
	}
}

func TestMySQL_BulkUpdateFromSliceMapByID(t *testing.T) {
	ctx := context.Background()
	recreateTable(t, testDB, testTable)
	if err := testDB.BulkInsertFromSliceMap(ctx, testTable, []map[string]any{
		{"id": 1, "name": "alice", "age": 30},
	}, 100); err != nil {
		t.Fatal(err)
	}
	if err := testDB.BulkUpdateFromSliceMapByID(ctx, testTable, "id", []map[string]any{
		{"id": 1, "name": "alice2", "age": 31},
	}); err != nil {
		t.Fatal(err)
	}
	var row struct {
		Name string
		Age  int
	}
	if err := testDB.GormDB().WithContext(ctx).Table(testTable).Where("id = ?", 1).Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Name != "alice2" || row.Age != 31 {
		t.Fatalf("updated row = (%q,%d), want (%q,%d)", row.Name, row.Age, "alice2", 31)
	}
}
