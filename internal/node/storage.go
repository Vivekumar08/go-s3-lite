package node

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type FileStorage struct {
	basePath string
}

// NewFileStorage creates a storage folder for the node
func NewFileStorage(basePath string) *FileStorage {
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		panic(fmt.Errorf("failed to create storage dir: %v", err))
	}
	return &FileStorage{basePath: basePath}
}

// SaveFile saves a file chunk to disk
func (fs *FileStorage) SaveFile(filename string, data []byte) error {
	path := filepath.Join(fs.basePath, filename)
	return ioutil.WriteFile(path, data, 0644)
}

// ReadFile reads a file from disk
func (fs *FileStorage) ReadFile(filename string) ([]byte, error) {
	path := filepath.Join(fs.basePath, filename)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %v", err)
	}
	return data, nil
}
