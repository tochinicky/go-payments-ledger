package ledger

import (
	"time"

	"github.com/google/uuid"
)

// HoldStatus is where a hold is in its life. A hold starts active and ends exactly once: captured, released or
// expired (invariant 4: never captured or released twice).
type HoldStatus string

// The hold statuses.
const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldReleased HoldStatus = "released"
	HoldExpired  HoldStatus = "expired"
)

// Hold reserves money on an account (like a card authorisation) for a destination fixed when it's placed.
type Hold struct {
	ID          uuid.UUID
	AccountID   uuid.UUID
	ToAccountID uuid.UUID
	Amount      Money
	Status      HoldStatus
	ExpiresAt   time.Time
	// Captured is the amount moved when the hold was captured (≤ Amount); the rest was released.
	Captured int64
}
