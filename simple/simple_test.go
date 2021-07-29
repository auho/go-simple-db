package simple

import (
	"testing"
)

func TestNewDriver(t *testing.T) {
	d, err := NewDriver("mysql", mysqlDsn)
	if err != nil {
		t.Error(err)
	}

	d.Close()
}
