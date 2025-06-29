package kvix

import (
	"fmt"

	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/options"
)

func isValidKey(key []byte) error {
	if len(key) == 0 {
		return errors.NewRequiredFieldError("key").WithExpected(1).WithProvided(0)
	}

	if len(key) > int(options.MaxKeySize) {
		return errors.NewFieldRangeError("key", len(key), 1, options.MaxKeySize).
			WithMessage(
				fmt.Sprintf(
					"Key size %s exceeds maximum allowed size of %s",
					options.FormatBytes(uint64(len(key))), options.FormatBytes(uint64(options.MaxKeySize)),
				),
			)
	}

	return nil
}

func isValidValue(value []byte) error {
	if len(value) == 0 {
		return errors.NewRequiredFieldError("value").WithExpected(1).WithProvided(0)
	}

	if len(value) > int(options.MaxValueSize) {
		return errors.NewFieldRangeError("value", len(value), 1, int(options.MaxValueSize)).
			WithMessage(
				fmt.Sprintf(
					"Value size %s exceeds maximum allowed size of %s",
					options.FormatBytes(uint64(len(value))), options.FormatBytes(uint64(options.MaxValueSize)),
				),
			)
	}

	return nil
}
