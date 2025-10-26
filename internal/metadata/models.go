package metadata

import (
	"time"

	utils "github.com/vivekumar08/go-s3-lite/internal/utils"
	"gorm.io/gorm"
)

type NodeModel struct {
	ID        string    `gorm:"primaryKey;size:128"`
	Address   string    `gorm:"size:256;not null"`
	LastSeen  time.Time `gorm:"index"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

type FileMapping struct {
	Key       string `gorm:"primaryKey;size:512"`
	Replicas  string `gorm:"size:1024"` // CSV of node IDs
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

// helper to parse CSV replicas (if needed)
func (fm *FileMapping) ReplicaIDs() []string {
	if fm == nil || fm.Replicas == "" {
		return []string{}
	}
	return utils.SplitCSV(fm.Replicas)
}
