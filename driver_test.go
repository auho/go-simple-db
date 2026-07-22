package simpledb_test

import (
	"context"
	"testing"

	simpledb "github.com/auho/go-simple-db/v3"
	mysqlgormtest "github.com/auho/go-simple-db/v3/internal/testutil/mysql/gorm"
)

func TestNewMySQLGorm(t *testing.T) {
	fullDSN, err := mysqlgormtest.Setup()
	if err != nil {
		t.Fatalf("setup dsn: %v", err)
	}

	sdb, gdb, err := simpledb.NewMySQLGorm(fullDSN)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = sdb.Close() }()

	if sdb == nil {
		t.Fatal("SimpleDB = nil")
	}

	if gdb == nil {
		t.Fatal("gorm.DB = nil")
	}

	if got := sdb.DriverName(); got != "mysql" {
		t.Fatalf("DriverName = %q, want %q", got, "mysql")
	}

	if err = sdb.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}
