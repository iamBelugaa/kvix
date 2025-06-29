package errors

type ErrorCode string

const (
	ErrIOGeneral     ErrorCode = "IO_GENERAL"
	ErrIOSyncFailed  ErrorCode = "IO_SYNC_FAILED"
	ErrIOSeekFailed  ErrorCode = "IO_SEEK_FAILED"
	ErrIOWriteFailed ErrorCode = "IO_WRITE_FAILED"
	ErrIOCloseFailed ErrorCode = "IO_CLOSE_FAILED"

	ErrSystemInternal           ErrorCode = "SYSTEM_INTERNAL"
	ErrSystemInvalidInput       ErrorCode = "SYSTEM_INVALID_INPUT"
	ErrSystemUnsupportedVersion ErrorCode = "SYSTEM_UNSUPPORTED_VERSION"

	ErrIndexKeyNotFound      ErrorCode = "INDEX_KEY_NOT_FOUND"
	ErrValidationInvalidData ErrorCode = "VALIDATION_INVALID_DATA"

	ErrRecordKeyMismatch        ErrorCode = "RECORD_KEY_MISMATCH"
	ErrRecordHeaderReadFailed   ErrorCode = "RECORD_HEADER_READ_FAILED"
	ErrRecordHeaderWriteFailed  ErrorCode = "RECORD_HEADER_WRITE_FAILED"
	ErrRecordSerialization      ErrorCode = "RECORD_SERIALIZATION"
	ErrRecordDeserialization    ErrorCode = "RECORD_DESERIALIZATION"
	ErrRecordChecksumMismatch   ErrorCode = "RECORD_CHECKSUM_MISMATCH"
	ErrRecordPayloadTooLarge    ErrorCode = "RECORD_PAYLOAD_TOO_LARGE"
	ErrRecordPayloadReadFailed  ErrorCode = "RECORD_PAYLOAD_READ_FAILED"
	ErrRecordPayloadWriteFailed ErrorCode = "RECORD_PAYLOAD_WRITE_FAILED"
)
