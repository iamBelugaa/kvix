package index

import (
	"sync"
	"time"
)

type RecordPointer struct {
	ExpiresAt        int64
	Offset           int64
	SegmentTimestamp int64
	SegmentID        uint16
}

func (rp *RecordPointer) IsExpired() bool {
	if rp.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixMilli() > rp.ExpiresAt
}

type Index struct {
	dataDir       string
	mu            sync.RWMutex
	recordPointer map[string]*RecordPointer
}
