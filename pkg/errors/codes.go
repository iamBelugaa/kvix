package errors

// ErrorCode represents a standardized way to categorize different types of errors.
type ErrorCode string

const (
	// ErrSystemInternal represents unexpected system failures.
	ErrSystemInternal ErrorCode = "SYSTEM_INTERNAL"

	// ErrSystemInvalidInput represents client-side errors.
	ErrSystemInvalidInput ErrorCode = "SYSTEM_INVALID_INPUT"

	// ErrSystemPermissionDenied indicates insufficient permissions to access a resource.
	ErrSystemPermissionDenied ErrorCode = "SYSTEM_PERMISSION_DENIED"

	// ErrSystemDiskFull indicates that the storage device has run out of space.
	ErrSystemDiskFull ErrorCode = "SYSTEM_DISK_FULL"

	// ErrSystemFilesystemReadonly indicates that the filesystem is mounted read-only.
	ErrSystemFilesystemReadonly ErrorCode = "SYSTEM_FILESYSTEM_READONLY"

	// ErrSystemUnsupportedVersion indicates a version incompatibility between the data format
	ErrSystemUnsupportedVersion ErrorCode = "SYSTEM_UNSUPPORTED_VERSION"
)

const (
	// ErrIOGeneral represents failures in input/output operations across any
	// system boundary.
	ErrIOGeneral ErrorCode = "IO_GENERAL"

	// ErrIOWriteFailed indicates specific problems with write operations.
	ErrIOWriteFailed ErrorCode = "IO_WRITE_FAILED"

	// ErrIOSyncFailed occurs when the system cannot ensure data has been
	// written to persistent storage.
	ErrIOSyncFailed ErrorCode = "IO_SYNC_FAILED"

	// ErrIOSeekFailed indicates that positioning the file pointer failed.
	ErrIOSeekFailed ErrorCode = "IO_SEEK_FAILED"
)

const (
	// ErrValidationInvalidData indicates that data values are outside acceptable ranges.
	ErrValidationInvalidData ErrorCode = "VALIDATION_INVALID_DATA"
)

const (
	// ErrRecordHeaderReadFailed occurs when the system cannot read the header
	// portion of a record.
	ErrRecordHeaderReadFailed ErrorCode = "RECORD_HEADER_READ_FAILED"

	// ErrRecordHeaderWriteFailed occurs when the system cannot write a record header.
	ErrRecordHeaderWriteFailed ErrorCode = "RECORD_HEADER_WRITE_FAILED"

	// ErrRecordSerialization represents failures when converting structured
	// objects into binary data for storage.
	ErrRecordSerialization ErrorCode = "RECORD_SERIALIZATION"

	// ErrRecordDeserialization represents failures when converting stored
	// binary data back into structured objects.
	ErrRecordDeserialization ErrorCode = "RECORD_DESERIALIZATION"

	// ErrRecordPreparationFailed indicates that the system could not
	// prepare a record for storage.
	ErrRecordPreparationFailed ErrorCode = "RECORD_PREPARATION_FAILED"

	// ErrRecordKeyMismatch occurs when a retrieved record's key doesn't
	// match the requested key.
	ErrRecordKeyMismatch ErrorCode = "RECORD_KEY_MISMATCH"

	// ErrRecordChecksumMismatch occurs when the checksum stored in a record
	// doesn't match.
	ErrRecordChecksumMismatch ErrorCode = "RECORD_CHECKSUM_MISMATCH"

	// ErrRecordPayloadReadFailed indicates problems reading the actual data.
	ErrRecordPayloadReadFailed ErrorCode = "RECORD_PAYLOAD_READ_FAILED"

	// ErrRecordPayloadWriteFailed indicates that the payload data could not be written
	ErrRecordPayloadWriteFailed ErrorCode = "RECORD_PAYLOAD_WRITE_FAILED"

	// ErrRecordPayloadTooLarge indicates that the payload exceeds the maximum size.
	ErrRecordPayloadTooLarge ErrorCode = "RECORD_PAYLOAD_TOO_LARGE"
)

const (
	// ErrSegmentUnexpectedEOF indicates that an operation reached the end of file unexpectedly
	ErrSegmentUnexpectedEOF ErrorCode = "SEGMENT_UNEXPECTED_EOF"

	// ErrSegmentOpenFailed indicates that a segment file could not be opened for reading or writing.
	ErrSegmentOpenFailed ErrorCode = "SEGMENT_OPEN_FAILED"

	// ErrSegmentCloseFailed indicates that a segment file could not be properly closed.
	ErrSegmentCloseFailed ErrorCode = "SEGMENT_CLOSE_FAILED"
)

const (
	// ErrIndexKeyNotFound indicates that a requested key doesn't exist in the index.
	ErrIndexKeyNotFound ErrorCode = "INDEX_KEY_NOT_FOUND"
)
