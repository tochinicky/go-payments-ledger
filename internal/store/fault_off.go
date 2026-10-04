//go:build !faultinject

package store

func commitAckLost() error { return nil }
