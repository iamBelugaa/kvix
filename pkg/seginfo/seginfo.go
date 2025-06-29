package seginfo

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/iamBelugaa/kvix/pkg/filesys"
)

func GetLastSegmentInfo(segmentDir, prefix string) (uint16, os.FileInfo, error) {
	lastSegmentPath, err := GetLastSegmentName(segmentDir, prefix)
	if err != nil {
		return 0, nil, err
	}

	if lastSegmentPath == "" {
		return 1, nil, nil
	}

	segmentID, err := ParseSegmentID(lastSegmentPath, prefix)
	if err != nil {
		return 0, nil, err
	}

	file, err := os.OpenFile(lastSegmentPath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get file info for %s: %w", lastSegmentPath, err)
	}

	return segmentID, stat, nil
}

func GetLastSegmentName(segmentDir, prefix string) (string, error) {
	searchPattern := filepath.Join(segmentDir, prefix+"*.seg")
	matchingFiles, err := filesys.ReadDir(searchPattern)
	if err != nil {
		return "", err
	}

	if len(matchingFiles) == 0 {
		return "", nil
	}

	slices.Sort(matchingFiles)
	return matchingFiles[len(matchingFiles)-1], nil
}

func ParseSegmentID(fullPath, prefix string) (uint16, error) {
	_, filename := filepath.Split(fullPath)

	if !strings.HasPrefix(filename, prefix) {
		return 0, fmt.Errorf("filename %s does not start with expected prefix %s", filename, prefix)
	}

	withoutPrefix := strings.TrimPrefix(filename, prefix)
	withoutExtension := strings.Split(withoutPrefix, ".")[0]

	parts := strings.Split(withoutExtension, "_")
	if len(parts) < 3 {
		return 0, fmt.Errorf("filename %s has unexpected format, expected prefix_ID_timestamp.seg", filename)
	}

	id, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}

	return uint16(id), nil
}

func GenerateName(id uint16, prefix string) string {
	return GenerateNameWithTimestamp(id, prefix, time.Now().UnixNano())
}

func GenerateNameWithTimestamp(id uint16, prefix string, timestamp int64) string {
	if prefix == "" {
		return ""
	}
	return fmt.Sprintf("%s_%05d_%d.seg", prefix, id, timestamp)
}

func ParseSegmentTimestamp(fullPath, prefix string) (int64, error) {
	_, filename := filepath.Split(fullPath)

	if !strings.HasPrefix(filename, prefix) {
		return 0, fmt.Errorf("filename %s does not start with expected prefix %s", filename, prefix)
	}

	withoutPrefix := strings.TrimPrefix(filename, prefix)
	withoutExtension := strings.Split(withoutPrefix, ".")[0]

	parts := strings.Split(withoutExtension, "_")
	if len(parts) < 3 {
		return 0, fmt.Errorf("filename %s has unexpected format, expected prefix_ID_timestamp.seg", filename)
	}

	timestamp, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, err
	}
	return int64(timestamp), nil
}
