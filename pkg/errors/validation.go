package errors

type ValidationError struct {
	*baseError
	provided any
	expected any
}

func NewValidationError(err error, code ErrorCode, msg string) *ValidationError {
	return &ValidationError{baseError: NewBaseError(err, code, msg)}
}

func (ve *ValidationError) WithMessage(msg string) *ValidationError {
	ve.baseError.WithMessage(msg)
	return ve
}

func (ve *ValidationError) WithCode(code ErrorCode) *ValidationError {
	ve.baseError.WithCode(code)
	return ve
}

func (ve *ValidationError) WithDetail(key string, value any) *ValidationError {
	ve.baseError.WithDetail(key, value)
	return ve
}

func (ve *ValidationError) WithProvided(value any) *ValidationError {
	ve.provided = value
	return ve
}

func (ve *ValidationError) WithExpected(value any) *ValidationError {
	ve.expected = value
	return ve
}

func (ve *ValidationError) Provided() any {
	return ve.provided
}

func (ve *ValidationError) Expected() any {
	return ve.expected
}
