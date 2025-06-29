package errors

import (
	stdErrors "errors"
	"os"
	"syscall"
)

// AsValidationError safely extracts a ValidationError from an error chain, providing access
// to validation-specific context.
func AsValidationError(err error) (*ValidationError, bool) {
	var ve *ValidationError
	if stdErrors.As(err, &ve) {
		return ve, true
	}
	return nil, false
}

// AsStorageError extracts StorageError context from an error chain, providing access to
// storage-specific information.
func AsStorageError(err error) (*StorageError, bool) {
	var se *StorageError
	if stdErrors.As(err, &se) {
		return se, true
	}
	return nil, false
}

// AsIndexError extracts IndexError context, providing access to index-specific information.
func AsIndexError(err error) (*IndexError, bool) {
	var ie *IndexError
	if stdErrors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}

// GetErrorCode extracts the error code from any error that supports it.
func GetErrorCode(err error) ErrorCode {
	if ve, ok := AsValidationError(err); ok {
		return ve.Code()
	}

	if se, ok := AsStorageError(err); ok {
		return se.Code()
	}

	if ie, ok := AsIndexError(err); ok {
		return ie.Code()
	}

	return ErrSystemInternal
}

// GetErrorDetails extracts structured details from any error.
func GetErrorDetails(err error) map[string]any {
	if ve, ok := AsValidationError(err); ok {
		if details := ve.Details(); details != nil {
			return details
		}
	}

	if se, ok := AsStorageError(err); ok {
		if details := se.Details(); details != nil {
			return details
		}
	}

	if ie, ok := AsIndexError(err); ok {
		if details := ie.Details(); details != nil {
			return details
		}
	}

	return make(map[string]any)
}

// ClassifyDirectoryCreationError analyzes directory creation failures and returns appropriate error.
func ClassifyDirectoryCreationError(err error, path string) error {
	if os.IsPermission(err) {
		return NewStorageError(
			err, ErrSystemPermissionDenied,
			"Insufficient permissions to create segment directory",
		).
			WithPath(path)
	}

	if pathErr, ok := err.(*os.PathError); ok {
		if errno, ok := pathErr.Err.(syscall.Errno); ok {
			switch errno {
			case syscall.ENOSPC:
				{
					return NewStorageError(
						err, ErrSystemDiskFull,
						"Insufficient disk space to create segment directory",
					).
						WithPath(path)
				}
			case syscall.EROFS:
				{
					return NewStorageError(
						err, ErrSystemFilesystemReadonly,
						"Cannot create directory on read-only filesystem",
					).
						WithPath(path)
				}
			}
		}
	}

	return NewStorageError(err, ErrIOGeneral, "Failed to create segment directory").WithPath(path)
}

// ClassifyFileOpenError analyzes file opening failures and returns appropriate error
func ClassifyFileOpenError(err error, filePath, fileName string) error {
	if os.IsPermission(err) {
		return NewStorageError(
			err, ErrSystemPermissionDenied, "Insufficient permissions to open segment file",
		).
			WithPath(filePath).
			WithFileName(fileName)
	}

	if pathErr, ok := err.(*os.PathError); ok {
		if errno, ok := pathErr.Err.(syscall.Errno); ok {
			switch errno {
			case syscall.ENOSPC:
				{
					return NewStorageError(
						err, ErrSystemDiskFull, "Insufficient disk space to create segment file",
					).
						WithPath(filePath).
						WithFileName(fileName)
				}
			case syscall.EROFS:
				{
					return NewStorageError(
						err, ErrSystemFilesystemReadonly, "Cannot create file on read-only filesystem",
					).
						WithPath(filePath).
						WithFileName(fileName)
				}
			}
		}
	}

	return NewStorageError(
		err, ErrIOGeneral, "Failed to open segment file",
	).
		WithPath(filePath).
		WithFileName(fileName)
}

// ClassifySyncError analyzes sync operation failures and returns appropriate error codes.
func ClassifySyncError(err error, fileName, filePath string) error {
	if pathErr, ok := err.(*os.PathError); ok {
		if errno, ok := pathErr.Err.(syscall.Errno); ok {
			switch errno {
			case syscall.ENOSPC:
				{
					return NewStorageError(
						err, ErrSystemDiskFull,
						"Cannot sync file: insufficient disk space",
					).
						WithPath(filePath).
						WithFileName(fileName)
				}
			case syscall.EROFS:
				{
					return NewStorageError(
						err, ErrSystemFilesystemReadonly,
						"Cannot sync file: filesystem is read-only",
					).
						WithPath(filePath).
						WithFileName(fileName)
				}
			case syscall.EIO:
				{
					return NewStorageError(
						err, ErrIOGeneral,
						"I/O error during file sync - possible hardware or corruption issue",
					).
						WithPath(filePath).
						WithFileName(fileName)
				}
			}
		}
	}

	return NewStorageError(
		err, ErrIOSyncFailed, "Failed to sync segment file to disk",
	).
		WithPath(filePath).
		WithFileName(fileName).
		WithDetail("operation", "file_sync")
}
