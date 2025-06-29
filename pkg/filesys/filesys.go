package filesys

import (
	"errors"
	"os"
	"path/filepath"
)

var (
	ErrIsNotDir = errors.New("path isn't a directory")
)

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

func ReadDir(dirName string) ([]string, error) {
	files, err := filepath.Glob(dirName)
	return files, err
}
