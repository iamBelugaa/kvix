package segmentpool

import (
	"os"
	"sync"

	"github.com/iamBelugaa/kvix/pkg/options"
	"go.uber.org/zap"
)

// SegmentHandle represents a minimal file handle entry with zero-overhead tracking.
type SegmentHandle struct {
	lastUsed int64
	file     *os.File
}

// SegmentPool implements ultra-lightweight lazy loading for segment files.
type SegmentPool struct {
	maxIdleTime int64
	mu          sync.RWMutex
	options     *options.Options
	log         *zap.SugaredLogger
	handles     map[string]*SegmentHandle
}
