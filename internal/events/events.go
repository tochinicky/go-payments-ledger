// Package events is the contract between the ledger and its consumers: the topic, its fixed partitioning, and the
// JSON of an account entry event.
package events

import (
	"time"

	"github.com/google/uuid"
)

// Topic carries one event per account entry, keyed by account id. Internal only: the payload exposes balances.
const Topic = "ledger.account-entries.v1"

// Partitions is fixed: adding partitions later remaps keys to partitions and breaks per-account order for accounts
// in flight. Topics are created explicitly with this count; broker auto-creation is off.
const Partitions = 6

// The event types, one per kind of account entry.
const (
	AccountDebited  = "account.debited"
	AccountCredited = "account.credited"
	HoldPlaced      = "hold.placed"
	HoldCaptured    = "hold.captured"
	HoldReleased    = "hold.released"
	HoldExpired     = "hold.expired"
)

// SchemaVersion is the payload version. A breaking change gets a new topic (…v2), not a new number here.
const SchemaVersion = 1

// Event is one account entry, as published. AmountMinor is the entry's amount from the account's point of view:
// negative for money leaving (a debit, a capture's source) or being reserved (a hold placed), positive for money
// arriving (a credit) or a reservation given back (a hold released or expired). It is a summary, not a balance
// change: summed across event types it is neither posted nor held. PostedDeltaMinor and HeldDeltaMinor are the
// exact changes (account.* events change posted only, hold.placed/released/expired change held only,
// hold.captured changes both), so summing either one per account gives that balance. PostedBalanceAfter and
// HeldBalanceAfter are exact; AccountSeq is the account's version after the entry, gap-free per account.
type Event struct {
	EventID            uuid.UUID  `json:"event_id"`
	EventType          string     `json:"event_type"`
	SchemaVersion      int        `json:"schema_version"`
	OccurredAt         time.Time  `json:"occurred_at"`
	PartnerID          uuid.UUID  `json:"partner_id"`
	AccountID          uuid.UUID  `json:"account_id"`
	TransactionID      *uuid.UUID `json:"transaction_id"`
	TransactionKind    *string    `json:"transaction_kind"`
	HoldID             *uuid.UUID `json:"hold_id,omitempty"`
	AmountMinor        int64      `json:"amount_minor"`
	PostedDeltaMinor   int64      `json:"posted_delta_minor"`
	HeldDeltaMinor     int64      `json:"held_delta_minor"`
	Currency           string     `json:"currency"`
	PostedBalanceAfter int64      `json:"posted_balance_after"`
	HeldBalanceAfter   int64      `json:"held_balance_after"`
	AccountSeq         int64      `json:"account_seq"`
}
