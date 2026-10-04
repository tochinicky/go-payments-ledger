package store

// SlowTransfers reports how many transfers have taken the slow path, for tests.
func SlowTransfers() int64 { return slowTransfers.Load() }
