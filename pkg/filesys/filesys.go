// Package filesys provides a collection of utility functions for common file system operations.
package filesys

import (
	"errors"
	"os"
	"path/filepath"
)

var (
	ErrIsNotDir = errors.New("path isn't a directory")
)

// CreateDir creates a directory at the specified path with the given permissions.
func CreateDir(dirPath string, permission os.FileMode, force bool) error {
	stat, err := os.Stat(dirPath)
	if !force && !os.IsNotExist(err) {
		return err
	}

	if stat != nil && !stat.IsDir() {
		return ErrIsNotDir
	}

	if err := os.MkdirAll(dirPath, permission); err != nil {
		return err
	}

	return os.Chmod(dirPath, 0755)
}

// ReadDir reads the directory specified by `dirName` and returns a list of matching file paths.
// It uses `filepath.Glob` which means `dirName` can contain glob patterns (e.g., "mydir/*.txt").
func ReadDir(dirName string) ([]string, error) {
	files, err := filepath.Glob(dirName)
	return files, err
}
