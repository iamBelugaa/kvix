// Package storage provides a comprehensive file-based storage mechanism for managing segments of data
// in high-throughput, append-only scenarios.
package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/iamNilotpal/ignite/internal/storage/segmentpool"
	"github.com/iamNilotpal/ignite/pkg/checksum"
	"github.com/iamNilotpal/ignite/pkg/errors"
	"github.com/iamNilotpal/ignite/pkg/filesys"
	"github.com/iamNilotpal/ignite/pkg/options"
	"github.com/iamNilotpal/ignite/pkg/seginfo"
)

// New creates and initializes a new Storage instance.
func New(ctx context.Context, log *zap.SugaredLogger, options *options.Options) (*Storage, error) {
	log.Infow(
		"Initializing storage system",
		"dataDir", options.DataDir,
		"maxSegmentSize", options.SegmentOptions.Size,
		"segmentDir", options.SegmentOptions.Directory,
		"segmentPrefix", options.SegmentOptions.Prefix,
	)

	segmentDirPath := filepath.Join(options.SegmentOptions.Directory)
	if err := filesys.CreateDir(segmentDirPath, 0755, true); err != nil {
		return nil, errors.ClassifyDirectoryCreationError(err, segmentDirPath)
	}

	log.Infow("Segment directory created successfully", "path", segmentDirPath)

	segmentPool := segmentpool.New(int64((time.Minute * 30).Seconds()), options, log)
	storage := &Storage{
		log:         log,
		options:     options,
		segmentPool: segmentPool,
		checksummer: checksum.NewCRC32IEEE(),
	}

	log.Infow(
		"Discovering existing segments",
		"dataDir", options.DataDir,
		"prefix", options.SegmentOptions.Prefix,
		"segmentDir", options.SegmentOptions.Directory,
	)

	lastSegmentID, lastSegmentInfo, err := seginfo.GetLastSegmentInfo(
		options.SegmentOptions.Directory,
		options.SegmentOptions.Prefix,
	)
	if err != nil {
		return nil, errors.NewStorageError(
			err, errors.ErrSystemInternal,
			"Failed to discover existing segments during initialization",
		).
			WithPath(segmentDirPath)
	}

	// Determine the appropriate segment to use based on discovery results.
	var targetOffset int64
	var targetSegmentID uint16
	var segmentTimestamp int64

	// No existing segments found, start with ID 1
	if lastSegmentInfo == nil {
		targetSegmentID = 1
		segmentTimestamp = time.Now().UnixNano()
		log.Infow("No existing segments found, starting fresh", "newSegmentID", targetSegmentID)
	} else {
		// Existing segments found, check if we need to rotate to a new segment.
		currentSize := lastSegmentInfo.Size()
		targetOffset = currentSize
		maxSize := int64(options.SegmentOptions.Size)

		if currentSize >= maxSize {
			// Current segment is full, create a new one.
			targetOffset = 0
			targetSegmentID = lastSegmentID + 1
			segmentTimestamp = time.Now().UnixNano()

			log.Infow(
				"Current segment is full, creating new segment",
				"maxSize", maxSize,
				"currentSize", currentSize,
				"newSegmentID", targetSegmentID,
				"currentSegmentID", lastSegmentID,
			)
		} else {
			// Current segment has space, continue using it.
			targetSegmentID = lastSegmentID
			segmentTimestamp, err = seginfo.ParseSegmentTimestamp(lastSegmentInfo.Name(), options.SegmentOptions.Prefix)
			if err != nil {
				return nil, errors.NewStorageError(
					err, errors.ErrSystemInternal,
					"Failed to parse timestamp from existing segment filename",
				).
					WithPath(lastSegmentInfo.Name())
			}

			log.Infow(
				"Continuing with existing segment",
				"maxSize", maxSize,
				"currentSize", currentSize,
				"segmentID", targetSegmentID,
				"remainingCapacity", maxSize-currentSize,
			)
		}
	}

	segmentFile, err := storage.openSegmentFile(targetSegmentID, segmentTimestamp, targetOffset == 0)
	if err != nil {
		return nil, err
	}

	storage.activeSegment = segmentFile
	storage.currentOffset = targetOffset
	storage.activeSegmentID = targetSegmentID
	storage.activeSegmentCreatedAt = segmentTimestamp

	log.Infow(
		"Storage system initialized successfully with offset tracking",
		"currentOffset", targetOffset,
		"isNewSegment", targetOffset == 0,
		"activeSegmentID", targetSegmentID,
		"activeSegmentTimestamp", segmentTimestamp,
	)

	return storage, nil
}

// SegmentID returns the current active segment ID.
func (s *Storage) SegmentID() uint16 {
	return s.activeSegmentID
}

// Offset returns the current active segment write offset.
func (s *Storage) Offset() int64 {
	return s.currentOffset
}

// SegmentTimestamp returns the creation timestamp of the current active segment.
func (s *Storage) SegmentTimestamp() int64 {
	return s.activeSegmentCreatedAt
}

// Handles the complex process of opening a segment file for writing.
func (s *Storage) openSegmentFile(segmentID uint16, timestamp int64, isNewSegment bool) (*os.File, error) {
	fileName := seginfo.GenerateNameWithTimestamp(segmentID, s.options.SegmentOptions.Prefix, timestamp)
	filePath := filepath.Join(s.options.SegmentOptions.Directory, fileName)

	var flags int
	if isNewSegment {
		flags = os.O_CREATE | os.O_RDWR | os.O_APPEND
	} else {
		flags = os.O_RDWR | os.O_APPEND
	}

	s.log.Infow(
		"Opening segment file",
		"path", filePath,
		"filename", fileName,
		"segmentID", segmentID,
		"isNewSegment", isNewSegment,
	)

	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return nil, errors.ClassifyFileOpenError(err, filePath, fileName)
	}

	// Position the file pointer at the end of the file.
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			s.log.Errorw("Failed to close file after seek error", "seekError", err, "closeError", closeErr)
		}
		return nil, errors.NewStorageError(
			err, errors.ErrIOSeekFailed, "Failed to seek to end of segment file",
		).
			WithPath(filePath).
			WithFileName(fileName).
			WithDetail("seekOffset", 0).
			WithDetail("whence", io.SeekEnd)
	}

	s.log.Infow(
		"Segment file opened successfully",
		"path", filePath,
		"currentOffset", offset,
		"isNewSegment", isNewSegment,
	)

	return file, nil
}
