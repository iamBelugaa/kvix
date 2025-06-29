package storage

import (
	stdErrors "errors"
	"os"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	ignitepb "github.com/iamNilotpal/ignite/internal/storage/__proto__"
	"github.com/iamNilotpal/ignite/internal/storage/segmentpool"
	"github.com/iamNilotpal/ignite/pkg/checksum"
	"github.com/iamNilotpal/ignite/pkg/options"
)

var (
	ErrNilHeader       = stdErrors.New("nil header")       // Indicates a nil EntryHeader struct.
	ErrNilValue        = stdErrors.New("nil value")        // Indicates a nil []byte in ErrNilValue.
	ErrInvalidChecksum = stdErrors.New("invalid checksum") // Indicates checksum mismatch during validation.
	ErrNilKey          = stdErrors.New("nil key")          // Occurs when attempting to process a nil Entry struct.
)

// Storage represents the core file-based storage component responsible for managing segment files
// and handling data persistence operations.
type Storage struct {
	options                *options.Options
	log                    *zap.SugaredLogger
	currentOffset          int64
	activeSegmentCreatedAt int64
	activeSegmentID        uint16
	activeSegment          *os.File
	checksummer            *checksum.CRC32IEEE
	segmentPool            *segmentpool.SegmentPool
}

// Record represents a complete key-value entry as it exists in our storage system.
type Record struct {
	// Header contains all metadata about this record including size information,
	// version details, and integrity checksums.
	Header *RecordHeader

	// Key holds the user-provided key data as raw bytes.
	Key []byte

	// Value contains the user-provided value data as raw bytes.
	Value []byte
}

// RecordHeader contains essential metadata for each stored record.
type RecordHeader struct {
	Checksum    uint32 // Checksum provides data integrity verification using CRC32 algorithm.
	PayloadSize uint32 // Size of the protobuf payload.
	Timestamp   int64  // Timestamp records when this record was created.
	Version     uint8  // Version enables forward and backward compatibility as the storage format evolves.
}

// Serializes a record to its Protocol Buffer representation.
func (r *Record) MarshalProto() ([]byte, error) {
	record := ignitepb.Record{
		Key:   r.Key,
		Value: r.Value,
	}
	opts := proto.MarshalOptions{Deterministic: true}
	return opts.Marshal(&record)
}

// Deserializes a record from its Protocol Buffer representation.
func (r *Record) UnMarshalProto(data []byte) error {
	var record ignitepb.Record
	opts := proto.UnmarshalOptions{DiscardUnknown: true}

	if err := opts.Unmarshal(data, &record); err != nil {
		return err
	}

	if record.Key == nil {
		return ErrNilKey
	}

	if record.Value == nil {
		return ErrNilValue
	}

	r.Key = record.Key
	r.Value = record.Value
	return nil
}
