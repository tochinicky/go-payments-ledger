// Package ledger is the double-entry core: money, postings, accounts, holds and the rules (invariants) that keep
// money from being created, lost or moved twice. It does no I/O: no database, no network, no clock of its own.
// The store (slice 2) persists what this package decides; the in-memory Book here is also the reference model the
// property-based tests compare the real database against.
package ledger
