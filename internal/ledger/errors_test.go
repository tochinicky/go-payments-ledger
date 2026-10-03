package ledger

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelsSharingAnAPICodeAreStillDistinct(t *testing.T) {
	// Three sentinels share the API code "validation_failed"; errors.Is must still tell them apart.
	if errors.Is(ErrOverflow, ErrInvalidAmount) || errors.Is(ErrInvalidAmount, ErrOverflow) || errors.Is(ErrInvalidCurrency, ErrInvalidAmount) {
		t.Error("sentinels with the same code must not match each other")
	}
}

func TestADetailedErrorMatchesItsOwnSentinelOnly(t *testing.T) {
	detailed := newError(ErrOverflow, "posted balance would overflow")

	if !errors.Is(detailed, ErrOverflow) {
		t.Error("a detailed error must match the sentinel it was made from")
	}
	if errors.Is(detailed, ErrInvalidAmount) {
		t.Error("…and not another sentinel with the same code")
	}
	if !errors.Is(fmt.Errorf("transfer: %w", detailed), ErrOverflow) {
		t.Error("…also when wrapped")
	}
	if detailed.Code != "validation_failed" {
		t.Errorf("the API code is unchanged: %q", detailed.Code)
	}
}

// The API codes are a public contract: pin each sentinel to its documented code.
func TestEachSentinelHasItsDocumentedAPICode(t *testing.T) {
	want := map[*Error]string{
		ErrInvalidAmount: "validation_failed", ErrInvalidCurrency: "validation_failed", ErrOverflow: "validation_failed",
		ErrNotFound: "not_found", ErrSameAccount: "same_account", ErrCurrencyMismatch: "currency_mismatch",
		ErrInsufficientFunds: "insufficient_funds", ErrAccountNotActive: "account_not_active",
		ErrAccountNotEmpty: "account_not_empty", ErrAccountNotClosable: "account_not_closable",
		ErrHoldNotActive: "hold_not_active", ErrHoldNotCapturable: "hold_not_capturable",
		ErrCaptureExceedsHold: "capture_exceeds_hold", ErrUnbalanced: "internal",
	}
	for sentinel, code := range want {
		if sentinel.Code != code {
			t.Errorf("%s has code %q, documented %q", sentinel.Message, sentinel.Code, code)
		}
	}
}
