package mysqlgormtest

import (
	"fmt"

	mysqltestutil "github.com/auho/go-simple-db/v3/internal/testutil/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Setup loads TEST_MYSQL_DSN, ensures the test database exists, and returns a dsn pointing at it.
func Setup() (string, error) {
	dsn, err := mysqltestutil.LoadDSN()
	if err != nil {
		return "", fmt.Errorf("load dsn: %w", err)
	}

	if err = EnsureDatabase(dsn); err != nil {
		return "", fmt.Errorf("ensure database: %w", err)
	}

	return dsn, nil
}

// RecreateTable drops and recreates the given table using the provided DDL.
func RecreateTable(db *gorm.DB, table string, ddl string) error {
	if err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)).Error; err != nil {
		return err
	}

	return db.Exec(ddl).Error
}

// EnsureDatabase connects with the bare dsn (no database selected) and creates
// the unified test database if it does not exist.
func EnsureDatabase(dsn string) error {
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	defer func() { _ = sqlDB.Close() }()

	return db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqltestutil.TestDatabase)).Error
}
