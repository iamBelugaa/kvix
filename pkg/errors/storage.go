package errors

// StorageError is a specialized error type for storage-related operations.
type StorageError struct {
	*baseError
	segmentId int
	offset    int
	fileName  string
	path      string
}

// NewStorageError creates a new storage-specific error with the provided context.
func NewStorageError(err error, code ErrorCode, msg string) *StorageError {
	return &StorageError{baseError: NewBaseError(err, code, msg)}
}

// WithMessage updates the error message.
func (se *StorageError) WithMessage(msg string) *StorageError {
	se.baseError.WithMessage(msg)
	return se
}

// WithCode sets the error code.
func (se *StorageError) WithCode(code ErrorCode) *StorageError {
	se.baseError.WithCode(code)
	return se
}

// WithDetail adds contextual information.
func (se *StorageError) WithDetail(key string, value any) *StorageError {
	se.baseError.WithDetail(key, value)
	return se
}

// WithSegmentID sets which storage segment was involved in the error.
func (se *StorageError) WithSegmentID(id int) *StorageError {
	se.segmentId = id
	return se
}

// WithOffset records the byte position where the error occurred.
func (se *StorageError) WithOffset(offset int) *StorageError {
	se.offset = offset
	return se
}

// WithFileName captures which file was being processed when the error occurred.
func (se *StorageError) WithFileName(fileName string) *StorageError {
	se.fileName = fileName
	return se
}

// WithPath captures which filesystem path was being processed during the error.
func (se *StorageError) WithPath(path string) *StorageError {
	se.path = path
	return se
}

// SegmentId returns the segment identifier where the error occurred.
func (se *StorageError) SegmentId() int {
	return se.segmentId
}

// Offset returns the byte offset within the segment where the error happened.
func (se *StorageError) Offset() int {
	return se.offset
}

// FileName returns the name of the file that was being processed.
func (se *StorageError) FileName() string {
	return se.fileName
}

// Path returns the full filesystem path of the file that was being processed.
func (se *StorageError) Path() string {
	return se.path
}
