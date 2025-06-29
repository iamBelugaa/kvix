package errors

type IndexError struct {
	*baseError
	operation string
	segmentID uint16
	key       string
}

func NewIndexError(err error, code ErrorCode, msg string) *IndexError {
	return &IndexError{
		baseError: NewBaseError(err, code, msg),
	}
}

func (ie *IndexError) WithMessage(msg string) *IndexError {
	ie.baseError.WithMessage(msg)
	return ie
}

func (ie *IndexError) WithCode(code ErrorCode) *IndexError {
	ie.baseError.WithCode(code)
	return ie
}

func (ie *IndexError) WithDetail(key string, value any) *IndexError {
	ie.baseError.WithDetail(key, value)
	return ie
}

func (ie *IndexError) WithKey(key string) *IndexError {
	ie.key = key
	return ie
}

func (ie *IndexError) WithSegmentID(segmentID uint16) *IndexError {
	ie.segmentID = segmentID
	return ie
}

func (ie *IndexError) WithOperation(operation string) *IndexError {
	ie.operation = operation
	return ie
}

func (ie *IndexError) Key() string {
	return ie.key
}

func (ie *IndexError) SegmentID() uint16 {
	return ie.segmentID
}

func (ie *IndexError) Operation() string {
	return ie.operation
}
