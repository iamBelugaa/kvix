package segmentpool

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/iamNilotpal/ignite/pkg/errors"
	"github.com/iamNilotpal/ignite/pkg/options"
	"github.com/iamNilotpal/ignite/pkg/seginfo"
	"go.uber.org/zap"
)

// New creates an efficient segment pool with minimal memory overhead.
func New(maxIdleTime int64, options *options.Options, log *zap.SugaredLogger) *SegmentPool {
	if maxIdleTime <= 0 {
		maxIdleTime = int64((time.Minute * 30).Seconds())
	}

	log.Infow("Initializing lightweight segment pool", "maxIdleTime", maxIdleTime)
	return &SegmentPool{
		log:         log,
		options:     options,
		maxIdleTime: maxIdleTime,
		handles:     make(map[string]*SegmentHandle),
	}
}

// GetSegmentHandle provides optimized access to segment files.
func (sp *SegmentPool) GetSegmentHandle(segmentID uint16, timestamp int64) (*os.File, error) {
	cacheKey := seginfo.GenerateNameWithTimestamp(segmentID, sp.options.SegmentOptions.Prefix, timestamp)

	sp.mu.RLock()
	if handle, exists := sp.handles[cacheKey]; exists {
		file := handle.file
		handle.lastUsed = time.Now().Unix()
		sp.mu.RUnlock()

		sp.log.Infow("Segment pool hit", "segmentID", segmentID)
		return file, nil
	}

	sp.mu.RUnlock()
	sp.log.Infow("Opening new segment file", "segmentID", segmentID, "timestamp", timestamp)

	fileName := seginfo.GenerateNameWithTimestamp(segmentID, sp.options.SegmentOptions.Prefix, timestamp)
	filePath := filepath.Join(sp.options.SegmentOptions.Directory, fileName)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.NewStorageError(
			err, errors.ErrSegmentOpenFailed,
			fmt.Sprintf("Failed to open segment file: %s", fileName),
		).
			WithPath(filePath).
			WithSegmentID(int(segmentID))
	}

	sp.mu.Lock()
	sp.handles[cacheKey] = &SegmentHandle{file: file, lastUsed: time.Now().Unix()}
	sp.mu.Unlock()

	sp.log.Infow(
		"Segment file opened and cached", "segmentID", segmentID, "fileName", fileName, "poolSize", len(sp.handles),
	)
	return file, nil
}

// CleanupIdleHandles removes file handles that haven't been used recently.
func (sp *SegmentPool) CleanupIdleHandles() int {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var cleanedCount int
	var closeErrors []error
	currentTime := time.Now().Unix()

	for key, handle := range sp.handles {
		if currentTime-handle.lastUsed > sp.maxIdleTime {
			if err := handle.file.Close(); err != nil {
				closeErrors = append(closeErrors, err)
				sp.log.Errorw("Failed to close idle segment file", "cacheKey", key, "error", err)
			}

			delete(sp.handles, key)
			cleanedCount++
			handle = nil
		}
	}

	sp.log.Infow(
		"Idle handle cleanup completed",
		"cleanedCount", cleanedCount,
		"maxIdleTime", sp.maxIdleTime,
		"closeErrors", len(closeErrors),
		"remainingCount", len(sp.handles),
	)

	return cleanedCount
}

// Close safely closes all cached file handles and cleans up resources.
func (sp *SegmentPool) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var closeErrors []error
	handleCount := len(sp.handles)

	for key, handle := range sp.handles {
		if err := handle.file.Close(); err != nil {
			closeErrors = append(closeErrors, err)
			sp.log.Errorw("Failed to close segment file during shutdown", "cacheKey", key, "error", err)
		}
		handle = nil
	}

	clear(sp.handles)
	if len(closeErrors) > 0 {
		return fmt.Errorf("failed to close %d out of %d segment handles during shutdown", len(closeErrors), handleCount)
	}

	sp.log.Infow("Segment pool closed successfully", "handlesCleared", handleCount)
	return nil
}
