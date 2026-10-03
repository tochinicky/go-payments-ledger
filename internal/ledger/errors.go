package ledger

// Error is a business-rule failure with a stable code. The HTTP layer maps codes to problem+json responses
// (the error catalogue); callers compare with errors.Is against the sentinels below.
type Error struct {
	Code    string
	Message string
	// sentinel is the error this one was made from (newError), so a detailed error still matches its sentinel.
	sentinel *Error
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Is matches the sentinel this error is, or was made from. It deliberately does NOT match on Code: several
// sentinels share an API code (ErrInvalidAmount, ErrInvalidCurrency and ErrOverflow are all "validation_failed"),
// and an overflow must never pass for an ordinary invalid amount in a test or a log.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && (t == e || (e.sentinel != nil && t == e.sentinel))
}

// Sentinels for errors.Is. The codes are part of the API contract: never rename one.
var (
	ErrInvalidAmount      = &Error{Code: "validation_failed", Message: "amount must be positive"}
	ErrInvalidCurrency    = &Error{Code: "validation_failed", Message: "unknown or malformed currency"}
	ErrOverflow           = &Error{Code: "validation_failed", Message: "amount out of range"}
	ErrNotFound           = &Error{Code: "not_found", Message: "not found"}
	ErrSameAccount        = &Error{Code: "same_account", Message: "source and destination are the same account"}
	ErrCurrencyMismatch   = &Error{Code: "currency_mismatch", Message: "currencies differ"}
	ErrInsufficientFunds  = &Error{Code: "insufficient_funds", Message: "available balance too low"}
	ErrAccountNotActive   = &Error{Code: "account_not_active", Message: "account is not active"}
	ErrAccountNotEmpty    = &Error{Code: "account_not_empty", Message: "an account can only be closed with nothing posted or held"}
	ErrAccountNotClosable = &Error{Code: "account_not_closable", Message: "a settlement account can't be closed"}
	ErrHoldNotActive      = &Error{Code: "hold_not_active", Message: "hold is not active"}
	ErrHoldNotCapturable  = &Error{Code: "hold_not_capturable", Message: "the hold's destination account is not active"}
	ErrCaptureExceedsHold = &Error{Code: "capture_exceeds_hold", Message: "capture amount exceeds the hold"}
	ErrUnbalanced         = &Error{Code: "internal", Message: "postings do not sum to zero"}
)

// newError returns a copy of a sentinel with a more specific message; it still matches the sentinel with errors.Is.
func newError(sentinel *Error, message string) *Error {
	return &Error{Code: sentinel.Code, Message: message, sentinel: sentinel}
}
