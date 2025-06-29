package kvix

import (
	"fmt"

	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/options"
)

func isValidKey(key []byte) error {
	if len(key) == 0 {
		return errors.NewValidationError(nil, errors.ErrSystemInvalidInput, "Key is required")
	}

	if len(key) > int(options.MaxKeySize) {
		return errors.NewValidationError(
			nil, errors.ErrValidationInvalidData, fmt.Sprintf(
				"Key size %d exceeds maximum allowed size of %d", len(key), options.MaxKeySize,
			),
		)
	}

	return nil
}

func isValidValue(value []byte) error {
	if len(value) == 0 {
		return errors.NewValidationError(nil, errors.ErrSystemInvalidInput, "Value is required")
	}

	if len(value) > int(options.MaxValueSize) {
		return errors.NewValidationError(
			nil, errors.ErrValidationInvalidData, fmt.Sprintf(
				"Value size %d exceeds maximum allowed size of %d", len(value), options.MaxValueSize,
			),
		)
	}

	return nil
}
