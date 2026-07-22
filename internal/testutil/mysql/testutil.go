package mysqltestutil

import (
	"errors"
	"fmt"

	"github.com/auho/go-simple-db/v3/internal/testutil"
	testmysql "github.com/auho/go-toolkit-testutil/mysql"
)

// TestDatabase is the unified database name shared by all mysql test
// implementations (gorm, raw sql, etc.).
const TestDatabase = "_test_simpledb"

// LoadDSN loads .env.test and returns the TEST_MYSQL_DSN value (without database name).
// It returns an error if .env.test exists but fails to parse.
func LoadDSN() (string, error) {
	err := testutil.LoadEnv()
	if err != nil {
		return "", fmt.Errorf("load env: %v", err)
	}

	dsn, err := testmysql.LoadDSN(TestDatabase)
	if err != nil {
		return "", fmt.Errorf("load dsn: %w", err)
	}

	if dsn == "" {
		return "", errors.New("TEST_MYSQL_DSN not set; create .env.test with TEST_MYSQL_DSN")
	}

	return dsn, nil
}
