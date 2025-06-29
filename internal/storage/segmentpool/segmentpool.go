package segmentpool

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/options"
	"github.com/iamBelugaa/kvix/pkg/seginfo"
	"go.uber.org/zap"
)

func New(maxIdleTime int64, options *options.Options, log *zap.SugaredLogger) *SegmentPool {
	if maxIdleTime <= 0 {
		maxIdleTime = int64((time.Minute * 30).Seconds())
	}

	return &SegmentPool{
		options:     options,
		maxIdleTime: maxIdleTime,
		handles:     make(map[string]*SegmentHandle),
	}
}

func (sp *SegmentPool) GetSegmentHandle(segmentID uint16, timestamp int64) (*os.File, error) {
	cacheKey := seginfo.GenerateNameWithTimestamp(segmentID, sp.options.SegmentOptions.Prefix, timestamp)

	sp.mu.RLock()
	if handle, exists := sp.handles[cacheKey]; exists {
		file := handle.file
		handle.lastUsed = time.Now().Unix()
		sp.mu.RUnlock()
		return file, nil
	}

	sp.mu.RUnlock()

	fileName := seginfo.GenerateNameWithTimestamp(segmentID, sp.options.SegmentOptions.Prefix, timestamp)
	filePath := filepath.Join(sp.options.SegmentOptions.Directory, fileName)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.NewStorageError(
			err, errors.ErrIOGeneral, fmt.Sprintf("Failed to open segment file: %s", fileName),
		).
			WithPath(filePath).
			WithSegmentID(int(segmentID))
	}

	sp.mu.Lock()
	sp.handles[cacheKey] = &SegmentHandle{file: file, lastUsed: time.Now().Unix()}
	sp.mu.Unlock()

	return file, nil
}

func (sp *SegmentPool) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var closeErrors []error
	handleCount := len(sp.handles)

	for _, handle := range sp.handles {
		if err := handle.file.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
		handle = nil
	}

	clear(sp.handles)
	if len(closeErrors) > 0 {
		return fmt.Errorf(
			"failed to close %d out of %d segment handles during shutdown", len(closeErrors), handleCount,
		)
	}

	return nil
}
