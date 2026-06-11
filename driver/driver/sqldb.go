package driver

import "database/sql"

// SqlDBProvider is an optional interface that drivers can implement
// to expose the underlying *sql.DB instance.
type SqlDBProvider interface {
	SqlDB() *sql.DB
}
