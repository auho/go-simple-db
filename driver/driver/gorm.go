package driver

import "gorm.io/gorm"

// GormProvider is an optional interface that drivers can implement
// to expose the underlying *gorm.DB instance.
type GormProvider interface {
	GormDB() *gorm.DB
}
