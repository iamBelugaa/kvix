package index

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// RecordPointer contains the  metadata required to locate and retrieve a data entry from disk storage
type RecordPointer struct {
	ExpiresAt        int64  // ExpiresAt stores the Unix nanoseconds timestamp at which this entry becomes invalid.
	Offset           int64  // Offset specifies the exact byte position within the segment file where this record begins.
	SegmentTimestamp int64  // SegmentTimestamp stores the Unix nanosecond timestamp when the segment was created.
	SegmentID        uint16 // SegmentID identifies which segment file contains this entry.
}

// IsExpired checks if the record has expired based on the current time.
func (rp *RecordPointer) IsExpired() bool {
	if rp.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixMilli() > rp.ExpiresAt
}

// Index represents the in-memory hash table that maps keys to their disk locations.
type Index struct {
	dataDir       string
	mu            sync.RWMutex
	log           *zap.SugaredLogger
	recordPointer map[string]*RecordPointer
}
