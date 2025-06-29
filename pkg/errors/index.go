package errors

// IndexError provides specialized error handling for index-related operations.
type IndexError struct {
	*baseError
	operation string
	segmentID uint16
	key       string
}

// NewIndexError creates a new index-specific error with the provided context.
func NewIndexError(err error, code ErrorCode, msg string) *IndexError {
	return &IndexError{
		baseError: NewBaseError(err, code, msg),
	}
}

// WithMessage updates the error message.
func (ie *IndexError) WithMessage(msg string) *IndexError {
	ie.baseError.WithMessage(msg)
	return ie
}

// WithCode sets the error code.
func (ie *IndexError) WithCode(code ErrorCode) *IndexError {
	ie.baseError.WithCode(code)
	return ie
}

// WithDetail adds contextual information.
func (ie *IndexError) WithDetail(key string, value any) *IndexError {
	ie.baseError.WithDetail(key, value)
	return ie
}

// WithKey records which key was being processed when the error occurred.
func (ie *IndexError) WithKey(key string) *IndexError {
	ie.key = key
	return ie
}

// WithSegmentID captures which segment was involved in the error.
func (ie *IndexError) WithSegmentID(segmentID uint16) *IndexError {
	ie.segmentID = segmentID
	return ie
}

// WithOperation records what index operation was being performed.
func (ie *IndexError) WithOperation(operation string) *IndexError {
	ie.operation = operation
	return ie
}

// Key returns the key that was being processed when the error occurred.
func (ie *IndexError) Key() string {
	return ie.key
}

// SegmentID returns the segment identifier associated with the error.
func (ie *IndexError) SegmentID() uint16 {
	return ie.segmentID
}

// Operation returns the name of the operation that was being performed.
func (ie *IndexError) Operation() string {
	return ie.operation
}
