package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	stdErrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/iamBelugaa/kvix/internal/storage/segmentpool"
	"github.com/iamBelugaa/kvix/pkg/checksum"
	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/filesys"
	"github.com/iamBelugaa/kvix/pkg/options"
	"github.com/iamBelugaa/kvix/pkg/seginfo"
)

func New(ctx context.Context, log *zap.SugaredLogger, options *options.Options) (*Storage, error) {
	segmentDirPath := filepath.Join(options.SegmentOptions.Directory)
	if err := filesys.CreateDir(segmentDirPath, 0755, true); err != nil {
		return nil, errors.NewStorageError(err, errors.ErrIOGeneral, err.Error())
	}

	segmentPool := segmentpool.New(int64((time.Minute * 30).Seconds()), options, log)
	storage := &Storage{
		log:         log,
		options:     options,
		segmentPool: segmentPool,
		checksummer: checksum.NewCRC32IEEE(),
	}

	lastSegmentID, lastSegmentInfo, err := seginfo.GetLastSegmentInfo(
		options.SegmentOptions.Directory,
		options.SegmentOptions.Prefix,
	)
	if err != nil {
		return nil, errors.NewStorageError(err, errors.ErrSystemInternal, err.Error()).WithPath(segmentDirPath)
	}

	var targetOffset int64
	var targetSegmentID uint16
	var segmentTimestamp int64

	if lastSegmentInfo == nil {
		targetSegmentID = 1
		segmentTimestamp = time.Now().UnixNano()
		log.Infow("No existing segments found, starting fresh", "newSegmentID", targetSegmentID)
	} else {
		currentSize := lastSegmentInfo.Size()
		targetOffset = currentSize
		maxSize := int64(options.SegmentOptions.Size)

		if currentSize >= maxSize {
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

	isNewSegment := targetOffset == 0
	fileName := seginfo.GenerateNameWithTimestamp(targetSegmentID, options.SegmentOptions.Prefix, segmentTimestamp)
	filePath := filepath.Join(options.SegmentOptions.Directory, fileName)

	var flags int
	if isNewSegment {
		flags = os.O_CREATE | os.O_RDWR | os.O_APPEND
	} else {
		flags = os.O_RDWR | os.O_APPEND
	}

	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return nil, errors.NewStorageError(err, errors.ErrIOGeneral, err.Error())
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			log.Errorw("Failed to close file after seek error", "seekError", err, "closeError", closeErr)
		}

		return nil, errors.NewStorageError(
			err, errors.ErrIOSeekFailed, "Failed to seek to end of segment file",
		).
			WithPath(filePath).
			WithFileName(fileName).
			WithDetail("seekOffset", 0).
			WithDetail("whence", io.SeekEnd)
	}

	storage.activeSegment = file
	storage.currentOffset = targetOffset
	storage.activeSegmentID = targetSegmentID
	storage.activeSegmentCreatedAt = segmentTimestamp

	log.Infow(
		"Storage system initialized successfully",
		"currentOffset", targetOffset,
		"isNewSegment", targetOffset == 0,
		"activeSegmentID", targetSegmentID,
		"activeSegmentTimestamp", segmentTimestamp,
	)

	return storage, nil
}

func (s *Storage) SegmentID() uint16 {
	return s.activeSegmentID
}

func (s *Storage) Offset() int64 {
	return s.currentOffset
}

func (s *Storage) SegmentTimestamp() int64 {
	return s.activeSegmentCreatedAt
}

func (s *Storage) Set(ctx context.Context, key, value []byte) (*Record, int64, error) {
	recordOffset := s.currentOffset
	record := &Record{
		Key:   key,
		Value: value,
		Header: &RecordHeader{
			Timestamp: time.Now().Unix(),
			Version:   options.CurrentSchemaVersion,
		},
	}

	encoded, err := record.MarshalProto()
	if err != nil {
		return nil, 0, errors.NewStorageError(
			err, errors.ErrRecordSerialization, "Failed to marshal payload",
		).
			WithDetail("record", record)
	}

	record.Header.PayloadSize = uint32(len(encoded))
	record.Header.Checksum = s.checksummer.Calculate(encoded)

	s.log.Infow(
		"Record prepared successfully",
		"version", record.Header.Version,
		"checksum", record.Header.Checksum,
		"payloadSize", record.Header.PayloadSize,
	)

	s.log.Infow(
		"Writing record to active segment",
		"actualPayloadLength", len(encoded),
		"binaryHeaderSize", binary.Size(record.Header),
		"headerPayloadSize", record.Header.PayloadSize,
	)

	headerSize := binary.Size(record.Header)
	totalSize := headerSize + len(encoded)

	if err := binary.Write(s.activeSegment, binary.LittleEndian, record.Header); err != nil {
		return nil, 0, errors.NewStorageError(
			err, errors.ErrRecordHeaderWriteFailed, "Failed to write record header",
		).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	bytesWritten, err := s.activeSegment.Write(encoded)
	if err != nil {
		return nil, 0, errors.NewStorageError(
			err, errors.ErrRecordPayloadWriteFailed, "Failed to write record",
		).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	if bytesWritten != len(encoded) {
		return nil, 0, errors.NewStorageError(
			err, errors.ErrIOWriteFailed,
			fmt.Sprintf("Short write occurred: %d written, expected %d", bytesWritten, len(encoded)),
		).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	s.log.Infow(
		"Record written successfully",
		"headerBytes", headerSize,
		"totalBytes", totalSize,
		"currentOffset", s.currentOffset,
	)

	return record, recordOffset, nil
}

func (s *Storage) Get(
	ctx context.Context, key []byte, segmentID uint16, segmentTimestamp int64, offset int64,
) (record *Record, err error) {
	s.log.Infow("Starting Get operation", "requestedKey", string(key), "readOffset", offset)

	isActiveSegment := segmentID == s.activeSegmentID
	if isActiveSegment {
		defer func() {
			_, err = s.activeSegment.Seek(0, io.SeekEnd)
		}()
	}

	var segmentFile *os.File
	if isActiveSegment {
		segmentFile = s.activeSegment
	} else {
		segmentFile, err = s.segmentPool.GetSegmentHandle(segmentID, segmentTimestamp)
		if err != nil {
			return nil, err
		}
	}

	var header RecordHeader
	headerSize := int64(binary.Size(header))
	headerReader := io.NewSectionReader(segmentFile, offset, headerSize)

	if err := binary.Read(headerReader, binary.LittleEndian, &header); err != nil {
		if stdErrors.Is(err, io.EOF) {
			return nil, errors.NewStorageError(
				err, errors.ErrSystemInternal, "Reached end of file while reading record header",
			).
				WithDetail("offset", offset).
				WithSegmentID(int(s.activeSegmentID))
		}

		return nil, errors.NewStorageError(
			err, errors.ErrRecordHeaderReadFailed,
			"Failed to read record header from segment file",
		).
			WithDetail("offset", offset).
			WithDetail("headerSize", headerSize).
			WithSegmentID(int(s.activeSegmentID))
	}

	s.log.Infow(
		"Header read successfully",
		"version", header.Version,
		"checksum", header.Checksum,
		"timestamp", header.Timestamp,
		"payloadSize", header.PayloadSize,
	)

	if header.PayloadSize == 0 {
		return nil, errors.NewValidationError(
			nil, errors.ErrValidationInvalidData, "Record header contains zero payload size",
		).
			WithDetail("header", header).
			WithDetail("offset", offset)
	}

	if header.PayloadSize > options.MaxValueSize {
		return nil, errors.NewValidationError(
			nil, errors.ErrRecordPayloadTooLarge,
			fmt.Sprintf("Payload size %d exceeds maximum allowed size %d", header.PayloadSize, options.MaxValueSize),
		).
			WithDetail("offset", offset).
			WithDetail("payloadSize", header.PayloadSize)
	}

	if header.Version < options.MinSchemaVersion || header.Version > options.MaxSchemaVersion {
		return nil, errors.NewValidationError(
			nil, errors.ErrSystemUnsupportedVersion, "Unsupported schema version",
		).
			WithDetail("version", header.Version).
			WithDetail("minVersion", options.MinSchemaVersion).
			WithDetail("maxSchemaVersion", options.MaxSchemaVersion)
	}

	var payloadBuffer []byte
	payloadOffset := offset + headerSize
	payloadSize := int64(header.PayloadSize)

	if payloadSize < 1048576 {
		payloadBuffer, err = s.readSmallPayload(segmentFile, payloadOffset, payloadSize)
		if err != nil {
			return nil, err
		}
	} else {
		payloadSectionReader := io.NewSectionReader(segmentFile, payloadOffset, payloadSize)
		payloadBuffer, err = s.readLargePayload(payloadSectionReader, payloadSize)
		if err != nil {
			if stdErrors.Is(err, io.EOF) || stdErrors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.NewStorageError(
					err, errors.ErrSystemInternal, "Reached end of file while reading record payload",
				).
					WithDetail("offset", payloadOffset).
					WithSegmentID(int(s.activeSegmentID)).
					WithDetail("expectedBytes", payloadSize)
			}

			return nil, errors.NewStorageError(
				err, errors.ErrRecordPayloadReadFailed, "Failed to read record payload.",
			).
				WithDetail("offset", payloadOffset).
				WithSegmentID(int(s.activeSegmentID)).
				WithDetail("payloadSize", payloadSize)
		}
	}

	record = &Record{Header: &header}
	if err := record.UnMarshalProto(payloadBuffer); err != nil {
		return nil, errors.NewStorageError(
			err, errors.ErrRecordDeserialization,
			"Failed to deserialize record from protobuf payload",
		).
			WithDetail("offset", offset).
			WithSegmentID(int(s.activeSegmentID)).
			WithDetail("payloadSize", len(payloadBuffer))
	}

	if isValid, err := s.VerifyChecksum(record); err != nil {
		return nil, err
	} else if !isValid {
		return nil, errors.NewValidationError(
			ErrInvalidChecksum, errors.ErrRecordChecksumMismatch,
			"Record checksum validation failed",
		).
			WithDetail("offset", offset).
			WithDetail("storedChecksum", record.Header.Checksum)
	}

	s.log.Infow(
		"Get operation completed successfully",
		"keyLength", len(record.Key),
		"valueLength", len(record.Value),
		"payloadSize", record.Header.PayloadSize,
	)

	return record, nil
}

func (s *Storage) VerifyChecksum(record *Record) (bool, error) {
	encoded, err := record.MarshalProto()
	if err != nil {
		return false, errors.NewStorageError(
			err, errors.ErrRecordSerialization, "Failed to marshal payload for checksum verification",
		).
			WithDetail("record", record)
	}

	if s.checksummer.Verify(encoded, record.Header.Checksum) {
		return true, nil
	}

	return false, errors.NewValidationError(
		ErrInvalidChecksum, errors.ErrRecordChecksumMismatch, "Invalid checksum",
	)
}

func (s *Storage) Close() error {
	s.log.Infow("Closing storage system")

	var currentFileName string
	var currentFilePath string
	if stat, err := s.activeSegment.Stat(); err == nil {
		currentFileName = stat.Name()
		currentFilePath = filepath.Join(s.options.SegmentOptions.Directory, currentFileName)
	}

	if err := s.activeSegment.Sync(); err != nil {
		s.log.Infow(
			"Failed to sync file before closing",
			"error", err,
			"fileName", currentFileName,
			"filePath", currentFilePath,
		)

		if closeErr := s.activeSegment.Close(); closeErr != nil {
			s.log.Infow(
				"Failed to close file after sync error",
				"syncError", err,
				"closeError", closeErr,
				"fileName", currentFileName,
				"filePath", currentFilePath,
			)
		}

		return errors.NewStorageError(err, errors.ErrIOCloseFailed, err.Error())
	}

	if err := s.activeSegment.Close(); err != nil {
		return errors.NewStorageError(
			err, errors.ErrIOCloseFailed, "Failed to close segment file",
		).
			WithPath(currentFilePath).
			WithFileName(currentFileName)
	}

	s.activeSegment = nil
	s.log.Infow("Storage system closed successfully", "fileName", currentFileName, "filePath", currentFilePath)
	return nil
}

func (s *Storage) readSmallPayload(file *os.File, offset, size int64) ([]byte, error) {
	buffer := make([]byte, size)

	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		if stdErrors.Is(err, io.EOF) && int64(n) == size {
			return buffer, nil
		}
		return nil, errors.NewStorageError(err, errors.ErrRecordPayloadReadFailed, "Failed to read payload")
	}

	if int64(n) != size {
		return nil, errors.NewStorageError(nil, errors.ErrRecordPayloadReadFailed, "Incomplete read of payload")
	}
	return buffer, nil
}

func (s *Storage) readLargePayload(reader io.Reader, expectedSize int64) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(expectedSize))

	copied, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload : %w", err)
	}

	if copied != expectedSize {
		return nil, fmt.Errorf("payload size mismatch")
	}

	return buf.Bytes(), nil
}
