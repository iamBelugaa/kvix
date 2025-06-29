// Package seginfo provides utilities for managing sequential segment files in a file-based storage system.
package seginfo

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/iamNilotpal/ignite/pkg/filesys"
)

// GetLastSegmentInfo discovers and analyzes the most recent segment file in the specified directory.
func GetLastSegmentInfo(segmentDir, prefix string) (uint16, os.FileInfo, error) {
	if segmentDir == "" || prefix == "" {
		return 0, nil, fmt.Errorf("all parameters (segmentDir, prefix) must be non-empty")
	}

	lastSegmentPath, err := GetLastSegmentName(segmentDir, prefix)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to discover latest segment: %w", err)
	}

	if lastSegmentPath == "" {
		return 1, nil, nil
	}

	segmentID, err := ParseSegmentID(lastSegmentPath, prefix)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to parse segment ID from %s: %w", lastSegmentPath, err)
	}

	fileInfo, err := getFileInfo(lastSegmentPath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to retrieve file info for %s: %w", lastSegmentPath, err)
	}

	return segmentID, fileInfo, nil
}

// GetLastSegmentName searches the segment directory and identifies the file with the highest sequence ID.
func GetLastSegmentName(segmentDir, prefix string) (string, error) {
	if segmentDir == "" || prefix == "" {
		return "", fmt.Errorf("all parameters (segmentDir, prefix) must be non-empty")
	}

	// Example: "/var/data/segments/segment_*.seg"
	searchPattern := filepath.Join(segmentDir, prefix+"*.seg")

	matchingFiles, err := filesys.ReadDir(searchPattern)
	if err != nil {
		return "", fmt.Errorf("failed to read segment directory with pattern %s: %w", searchPattern, err)
	}

	if len(matchingFiles) == 0 {
		return "", nil
	}

	slices.Sort(matchingFiles)
	return matchingFiles[len(matchingFiles)-1], nil
}

// ParseSegmentID extracts the sequence ID from a segment filename.
func ParseSegmentID(fullPath, prefix string) (uint16, error) {
	_, filename := filepath.Split(fullPath)

	if !strings.HasPrefix(filename, prefix) {
		return 0, fmt.Errorf("filename %s does not start with expected prefix %s", filename, prefix)
	}

	// Example: "segment_00001_1678881234567890.seg" -> "00001_1678881234567890"
	withoutPrefix := strings.TrimPrefix(filename, prefix)
	withoutExtension := strings.Split(withoutPrefix, ".")[0]

	// Example: "00001_1678881234567890" -> ["", "00001", "1678881234567890"]
	parts := strings.Split(withoutExtension, "_")

	// We expect: ["", "ID", "timestamp"] (empty first element due to leading underscore).
	if len(parts) < 3 {
		return 0, fmt.Errorf("filename %s has unexpected format, expected prefix_ID_timestamp.seg", filename)
	}

	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse segment ID '%s' as integer: %w", parts[1], err)
	}

	return uint16(id), nil
}

// getFileInfo safely retrieves file system metadata for a given path.
func getFileInfo(filePath string) (os.FileInfo, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Printf("Warning: failed to close file %s: %v\n", filePath, closeErr)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	}

	return stat, nil
}
