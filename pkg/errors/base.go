package errors

// baseError is a custom error type that can hold extra information.
type baseError struct {
	cause   error          // The original error that caused this one.
	message string         // The error message that will be displayed to users.
	code    ErrorCode      // Error code for categorizing the error type programmatically.
	details map[string]any // Additional context information like request IDs, timestamps, etc.
}

func NewBaseError(err error, code ErrorCode, msg string) *baseError {
	return &baseError{cause: err, code: code, message: msg}
}

func (be *baseError) WithMessage(msg string) *baseError {
	be.message = msg
	return be
}

func (be *baseError) WithCode(code ErrorCode) *baseError {
	be.code = code
	return be
}

func (be *baseError) WithDetail(key string, value any) *baseError {
	if be.details == nil {
		be.details = make(map[string]any)
	}
	be.details[key] = value
	return be
}

func (b *baseError) Error() string {
	return b.message
}

func (b *baseError) Unwrap() error {
	return b.cause
}

func (b *baseError) Code() ErrorCode {
	return b.code
}

func (b *baseError) Details() map[string]any {
	return b.details
}
