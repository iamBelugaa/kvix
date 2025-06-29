package errors

import (
	stdErrors "errors"
)

func AsValidationError(err error) (*ValidationError, bool) {
	var ve *ValidationError
	if stdErrors.As(err, &ve) {
		return ve, true
	}
	return nil, false
}

func AsStorageError(err error) (*StorageError, bool) {
	var se *StorageError
	if stdErrors.As(err, &se) {
		return se, true
	}
	return nil, false
}

func AsIndexError(err error) (*IndexError, bool) {
	var ie *IndexError
	if stdErrors.As(err, &ie) {
		return ie, true
	}
	return nil, false
}
