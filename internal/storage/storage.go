// Package storage provides a comprehensive file-based storage mechanism for managing segments of data
// in high-throughput, append-only scenarios.
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

// Set stores a key-value pair in the storage system, returning the created record.
func (s *Storage) Set(ctx context.Context, key, value []byte) (*Record, int64, error) {
	s.log.Infow(
		"Starting optimized Set operation",
		"keyLength", len(key),
		"valueLength", len(value),
		"currentOffset", s.currentOffset,
	)

	recordOffset := s.currentOffset
	s.log.Infow(
		"Record will be written at tracked offset",
		"offset", recordOffset,
		"segmentID", s.activeSegmentID,
	)

	record, encoded, err := s.prepareRecord(key, value)
	if err != nil {
		return nil, 0, errors.NewStorageError(
			err, errors.ErrRecordPreparationFailed, "Failed to prepare record for storage",
		).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	bytesWritten, err := s.writeRecord(record, encoded)
	if err != nil {
		return nil, 0, err
	}

	s.currentOffset += int64(bytesWritten)
	s.log.Infow(
		"Set operation completed with offset tracking",
		"recordOffset", recordOffset,
		"segmentID", s.activeSegmentID,
		"newCurrentOffset", s.currentOffset,
	)

	return record, recordOffset, nil
}

// Get retrieves a record from the storage system starting at the specified offset.
func (s *Storage) Get(
	ctx context.Context, key []byte, segmentID uint16, segmentTimestamp int64, offset int64,
) (record *Record, err error) {
	s.log.Infow("Starting Get operation", "requestedKey", string(key), "readOffset", offset)

	// Only manage append position for active segment reads.
	isActiveSegment := segmentID == s.activeSegmentID
	if isActiveSegment {
		defer func() {
			if e := s.ensureAppendPosition(); e != nil {
				err = e
			}
		}()
	}

	var segmentFile *os.File
	if isActiveSegment {
		segmentFile = s.activeSegment
		s.log.Infow("Reading from active segment", "segmentID", segmentID)
	} else {
		segmentFile, err = s.segmentPool.GetSegmentHandle(segmentID, segmentTimestamp)
		if err != nil {
			return nil, err
		}
		s.log.Infow("Retrieved segment from pool", "segmentID", segmentID)
	}

	// Step 1: Read the binary header from the segment file.
	var header RecordHeader
	headerSize := int64(binary.Size(header))

	s.log.Infow("Reading header", "headerSize", headerSize, "offset", offset)
	headerReader := io.NewSectionReader(segmentFile, offset, headerSize)

	if err := binary.Read(headerReader, binary.LittleEndian, &header); err != nil {
		if stdErrors.Is(err, io.EOF) {
			return nil, errors.NewStorageError(
				err, errors.ErrSegmentUnexpectedEOF, "Reached end of file while reading record header",
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

	// Step 2: Validate header fields for basic sanity checks.
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
			fmt.Sprintf(
				"Payload size %s exceeds maximum allowed size %s",
				options.FormatBytes(uint64(header.PayloadSize)), options.FormatBytes(uint64(options.MaxValueSize)),
			),
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

	// Step 3: Read the protobuf payload.
	var payloadBuffer []byte
	payloadOffset := offset + headerSize
	payloadSize := int64(header.PayloadSize)

	s.log.Infow(
		"payloadSize", payloadSize,
		"payloadOffset", payloadOffset,
		"Reading payload using stored size",
	)

	// Small payloads (< 1MB): Direct allocation and read for minimal overhead.
	// Large payloads (>= 1MB): Section reader for memory efficiency.
	if payloadSize < 1048576 {
		payloadBuffer, err = s.readSmallPayload(segmentFile, payloadOffset, payloadSize)
		if err != nil {
			return nil, err
		}
	} else {
		payloadSectionReader := io.NewSectionReader(segmentFile, payloadOffset, payloadSize)
		payloadBuffer, err = s.readLargePayloadWithBuffer(payloadSectionReader, payloadSize)
		if err != nil {
			if stdErrors.Is(err, io.EOF) || stdErrors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.NewStorageError(
					err, errors.ErrSegmentUnexpectedEOF, "Reached end of file while reading record payload",
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

	s.log.Infow("Payload read successfully using efficient strategy", "bytesRead", len(payloadBuffer))

	// Step 4: Deserialize the protobuf payload back into a Record structure.
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

	s.log.Infow("Record reconstructed successfully", "keyLength", len(record.Key), "valueLength", len(record.Value))

	// Step 5: Validate that the deserialized record matches our expectations.
	if !bytes.Equal(record.Key, key) {
		return nil, errors.NewValidationError(
			nil, errors.ErrRecordKeyMismatch, "Retrieved key does not match requested key",
		).
			WithDetail("offset", offset).
			WithDetail("requestedKey", string(key)).
			WithDetail("retrievedKey", string(record.Key))
	}

	// Step 6: Verify data integrity using checksum validation.
	if isValid, err := s.VerifyChecksum(record); err != nil {
		return nil, err
	} else if !isValid {
		return nil, errors.NewValidationError(
			ErrInvalidChecksum, errors.ErrRecordChecksumMismatch,
			"Record checksum validation failed - data may be corrupted",
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

// VerifyChecksum validates the integrity of a stored record by recalculating
// its checksum and comparing it against the stored checksum value.
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

// Close gracefully shuts down the storage system, ensuring all buffered data is written
// to disk and all resources are properly released.
func (s *Storage) Close() error {
	s.log.Infow("Closing storage system")

	var currentFileName string
	var currentFilePath string
	if stat, err := s.activeSegment.Stat(); err == nil {
		currentFileName = stat.Name()
		currentFilePath = filepath.Join(s.options.SegmentOptions.Directory, currentFileName)
	}

	if err := s.activeSegment.Sync(); err != nil {
		s.log.Errorw(
			"Failed to sync file before closing",
			"error", err,
			"fileName", currentFileName,
			"filePath", currentFilePath,
		)

		if closeErr := s.activeSegment.Close(); closeErr != nil {
			s.log.Errorw(
				"Failed to close file after sync error",
				"syncError", err,
				"closeError", closeErr,
				"fileName", currentFileName,
				"filePath", currentFilePath,
			)
		}

		return errors.ClassifySyncError(err, currentFileName, currentFilePath)
	}

	if err := s.activeSegment.Close(); err != nil {
		return errors.NewStorageError(
			err, errors.ErrSegmentCloseFailed, "Failed to close segment file handle",
		).
			WithPath(currentFilePath).
			WithFileName(currentFileName)
	}

	s.activeSegment = nil
	s.log.Infow("Storage system closed successfully", "fileName", currentFileName, "filePath", currentFilePath)

	return nil
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

// After any read/open operation, ensure file pointer is positioned for next append.
func (s *Storage) ensureAppendPosition() error {
	offset, err := s.activeSegment.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.NewStorageError(
			err, errors.ErrIOSeekFailed, "Failed to position file pointer for append operations",
		).
			WithDetail("seekOffset", 0).
			WithDetail("whence", io.SeekEnd).
			WithFileName(s.activeSegment.Name())
	}

	s.log.Infow("File pointer positioned for append operations", "appendOffset", offset)
	return nil
}

// Transforms a raw Record into a structured Record ready for storage.
func (s *Storage) prepareRecord(key, value []byte) (*Record, []byte, error) {
	s.log.Infow("Preparing record", "keyLength", len(key), "valueLength", len(value))

	record := &Record{
		Key:   key,
		Value: value,
		Header: &RecordHeader{
			Version:   1,
			Timestamp: time.Now().Unix(),
		},
	}

	encoded, err := record.MarshalProto()
	if err != nil {
		return nil, nil, errors.NewStorageError(
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

	return record, encoded, nil
}

// writeRecord performs the low-level operation of writing a prepared record
// to the segment's underlying writer.
func (s *Storage) writeRecord(record *Record, encoded []byte) (int, error) {
	s.log.Infow(
		"Writing record to active segment",
		"actualPayloadLength", len(encoded),
		"binaryHeaderSize", binary.Size(record.Header),
		"headerPayloadSize", record.Header.PayloadSize,
	)

	headerSize := binary.Size(record.Header)
	totalSize := headerSize + len(encoded)

	if err := binary.Write(s.activeSegment, binary.LittleEndian, record.Header); err != nil {
		return 0, errors.NewStorageError(
			err, errors.ErrRecordHeaderWriteFailed, "Failed to write record header",
		).
			WithDetail("header", record.Header).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	bytesWritten, err := s.activeSegment.Write(encoded)
	if err != nil {
		return 0, errors.NewStorageError(
			err, errors.ErrRecordPayloadWriteFailed, "Failed to write record",
		).
			WithDetail("record", record).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	if bytesWritten != len(encoded) {
		return bytesWritten, errors.NewStorageError(
			err, errors.ErrIOWriteFailed,
			fmt.Sprintf(
				"Short write occurred: %s written, expected %s",
				options.FormatBytes(uint64(bytesWritten)), options.FormatBytes(uint64(len(encoded))),
			),
		).
			WithFileName(s.activeSegment.Name()).
			WithSegmentID(int(s.activeSegmentID)).
			WithDetail("bytesWritten", bytesWritten).
			WithDetail("encodedLength", len(encoded)).
			WithPath(s.options.SegmentOptions.Directory)
	}

	s.log.Infow(
		"Record written successfully",
		"headerBytes", headerSize,
		"totalBytes", totalSize,
		"currentOffset", s.currentOffset,
	)

	return totalSize, nil
}

// readSmallPayload handles payloads under 1MB with minimal overhead.
func (s *Storage) readSmallPayload(file *os.File, offset, size int64) ([]byte, error) {
	buffer := make([]byte, size)

	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		if stdErrors.Is(err, io.EOF) && int64(n) == size {
			return buffer, nil
		}
		return nil, errors.NewStorageError(
			err, errors.ErrRecordPayloadReadFailed, "Failed to read small payload",
		).
			WithDetail("actualBytes", n).
			WithDetail("offset", offset).
			WithDetail("expectedBytes", size)
	}

	if int64(n) != size {
		return nil, errors.NewStorageError(
			nil, errors.ErrRecordPayloadReadFailed, "Incomplete read of small payload",
		).
			WithDetail("actualBytes", n).
			WithDetail("offset", offset).
			WithDetail("expectedBytes", size)
	}

	return buffer, nil
}

// readLargePayloadWithBuffer implements buffer based reading for all payload sizes using bytes.Buffer.
func (s *Storage) readLargePayloadWithBuffer(reader io.Reader, expectedSize int64) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(expectedSize))

	s.log.Infow(
		"Using buffer-based reading strategy",
		"expectedSize", expectedSize,
		"initialCapacity", buf.Cap(),
		"approach", "predictive_growth",
	)

	copied, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload with buffer approach: %w", err)
	}

	if copied != expectedSize {
		return nil, fmt.Errorf("payload size mismatch with buffer strategy: expected %d bytes, got %d", expectedSize, copied)
	}

	s.log.Infow(
		"Buffer based reading completed successfully with optimal memory utilization",
		"bytesRead", copied,
		"finalBufferSize", buf.Len(),
		"finalBufferCapacity", buf.Cap(),
		"allocationOverhead", buf.Cap()-buf.Len(),
		"memoryEfficiency", float64(buf.Len())/float64(buf.Cap())*100,
	)

	return buf.Bytes(), nil
}
