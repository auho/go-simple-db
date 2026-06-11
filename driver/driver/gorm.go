package driver

import (
	"database/sql"
	"gorm.io/gorm"
)

// GormProvider is an optional interface that drivers can implement
// to expose the underlying *gorm.DB instance.
type GormProvider interface {
	GormDB() *gorm.DB
}

// SqlDBProvider is an optional interface that drivers can implement
// to expose the underlying *sql.DB instance.
type SqlDBProvider interface {
	SqlDB() *sql.DB
}
