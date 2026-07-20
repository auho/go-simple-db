package mysqlgormtest

import (
	"errors"
	"fmt"

	mysqltestutil "github.com/auho/go-simple-db/v3/internal/testutil/mysql"
	"github.com/auho/go-toolkit-testutil"
	testmysql "github.com/auho/go-toolkit-testutil/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// RecreateTable drops and recreates the given table using the provided DDL.
func RecreateTable(db *gorm.DB, table string, ddl string) error {
	if err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)).Error; err != nil {
		return err
	}

	return db.Exec(ddl).Error
}

// LoadDSN loads .env.test and returns the TEST_MYSQL_DSN value (without database name).
// It returns an error if .env.test exists but fails to parse.
func LoadDSN() (string, error) {
	if err := testutil.LoadEnv(); err != nil {
		return "", fmt.Errorf("load env file: %w", err)
	}

	return testmysql.LoadDSN(mysqltestutil.TestDatabase)
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

	defer sqlDB.Close()

	return db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", mysqltestutil.TestDatabase)).Error
}

// SetupDSN loads TEST_MYSQL_DSN, ensures the test database exists, and returns a dsn pointing at it.
func SetupDSN() (string, error) {
	dsn, err := LoadDSN()
	if err != nil {
		return "", fmt.Errorf("load dsn: %w", err)
	}

	if dsn == "" {
		return "", errors.New("TEST_MYSQL_DSN not set; create .env.test with TEST_MYSQL_DSN")
	}

	if err := EnsureDatabase(dsn); err != nil {
		return "", fmt.Errorf("ensure database: %w", err)
	}

	return dsn, nil
}
