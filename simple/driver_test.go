package simple

import (
	"testing"
)

func TestNewMysqlDriver(t *testing.T) {
	d, err := NewMysqlDriver(mysqlDsn)
	if err != nil {
		t.Error(err)
	}

	d.Close()
}
