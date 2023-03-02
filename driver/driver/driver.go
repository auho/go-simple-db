package driver

import "gorm.io/gorm"

type Driver interface {
	DB() *gorm.DB
	Truncate(table string) error
}
