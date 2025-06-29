package errors

// ValidationError is a specialized error type for input validation failures.
type ValidationError struct {
	*baseError
	provided any
	expected any
	field    string
}

// NewValidationError creates a new validation-specific error with the provided context.
func NewValidationError(err error, code ErrorCode, msg string) *ValidationError {
	return &ValidationError{baseError: NewBaseError(err, code, msg)}
}

// WithMessage updates the error message.
func (ve *ValidationError) WithMessage(msg string) *ValidationError {
	ve.baseError.WithMessage(msg)
	return ve
}

// WithCode sets the error code.
func (ve *ValidationError) WithCode(code ErrorCode) *ValidationError {
	ve.baseError.WithCode(code)
	return ve
}

// WithDetail adds contextual information.
func (ve *ValidationError) WithDetail(key string, value any) *ValidationError {
	ve.baseError.WithDetail(key, value)
	return ve
}

// WithField sets which field failed validation.
func (ve *ValidationError) WithField(field string) *ValidationError {
	ve.field = field
	return ve
}

// WithProvided captures what value was provided that failed validation.
func (ve *ValidationError) WithProvided(value any) *ValidationError {
	ve.provided = value
	return ve
}

// WithExpected describes what would have been a valid value.
func (ve *ValidationError) WithExpected(value any) *ValidationError {
	ve.expected = value
	return ve
}

// Field returns the field name that failed validation.
func (ve *ValidationError) Field() string {
	return ve.field
}

// Provided returns the value that was provided and failed validation.
func (ve *ValidationError) Provided() any {
	return ve.provided
}

// Expected returns what would have been a valid value.
func (ve *ValidationError) Expected() any {
	return ve.expected
}

// NewRequiredFieldError creates a specialized error for missing required fields.
func NewRequiredFieldError(fieldName string) *ValidationError {
	return NewValidationError(
		nil, ErrSystemInvalidInput, "Required field is missing or empty",
	).
		WithField(fieldName)
}

// NewFieldRangeError creates an error for fields that are outside acceptable ranges.
func NewFieldRangeError(fieldName string, provided any, min, max any) *ValidationError {
	return NewValidationError(
		nil, ErrValidationInvalidData, "Field value is outside acceptable range",
	).
		WithField(fieldName).
		WithProvided(provided).
		WithDetail("minValue", min).
		WithDetail("maxValue", max)
}
