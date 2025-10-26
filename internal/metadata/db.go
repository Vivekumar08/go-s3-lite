package metadata

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func OpenDB(path string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&NodeModel{}, &FileMapping{}); err != nil {
		return nil, err
	}
	return db, nil
}
