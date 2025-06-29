package errors

type StorageError struct {
	*baseError
	segmentId int
	offset    int
	fileName  string
	path      string
}

func NewStorageError(err error, code ErrorCode, msg string) *StorageError {
	return &StorageError{baseError: NewBaseError(err, code, msg)}
}

func (se *StorageError) WithMessage(msg string) *StorageError {
	se.baseError.WithMessage(msg)
	return se
}

func (se *StorageError) WithCode(code ErrorCode) *StorageError {
	se.baseError.WithCode(code)
	return se
}

func (se *StorageError) WithDetail(key string, value any) *StorageError {
	se.baseError.WithDetail(key, value)
	return se
}

func (se *StorageError) WithSegmentID(id int) *StorageError {
	se.segmentId = id
	return se
}

func (se *StorageError) WithOffset(offset int) *StorageError {
	se.offset = offset
	return se
}

func (se *StorageError) WithFileName(fileName string) *StorageError {
	se.fileName = fileName
	return se
}

func (se *StorageError) WithPath(path string) *StorageError {
	se.path = path
	return se
}

func (se *StorageError) SegmentId() int {
	return se.segmentId
}

func (se *StorageError) Offset() int {
	return se.offset
}

func (se *StorageError) FileName() string {
	return se.fileName
}

func (se *StorageError) Path() string {
	return se.path
}
