package storage

import (
	stdErrors "errors"
	"os"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	kvixpb "github.com/iamBelugaa/kvix/internal/storage/__proto__"
	"github.com/iamBelugaa/kvix/internal/storage/segmentpool"
	"github.com/iamBelugaa/kvix/pkg/checksum"
	"github.com/iamBelugaa/kvix/pkg/options"
)

var (
	ErrNilKey          = stdErrors.New("nil key")
	ErrNilValue        = stdErrors.New("nil value")
	ErrNilHeader       = stdErrors.New("nil header")
	ErrInvalidChecksum = stdErrors.New("invalid checksum")
)

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

type Record struct {
	Header *RecordHeader
	Key    []byte
	Value  []byte
}

type RecordHeader struct {
	Checksum    uint32
	PayloadSize uint32
	Timestamp   int64
	Version     uint8
}

func (r *Record) MarshalProto() ([]byte, error) {
	record := kvixpb.Record{
		Key:   r.Key,
		Value: r.Value,
	}
	opts := proto.MarshalOptions{Deterministic: true}
	return opts.Marshal(&record)
}

func (r *Record) UnMarshalProto(data []byte) error {
	var record kvixpb.Record
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
