package segmentpool

import (
	"os"
	"sync"

	"github.com/iamBelugaa/kvix/pkg/options"
)

type SegmentHandle struct {
	lastUsed int64
	file     *os.File
}

type SegmentPool struct {
	maxIdleTime int64
	mu          sync.RWMutex
	options     *options.Options
	handles     map[string]*SegmentHandle
}
