// Package faultinject crashes a process at a named point, for the failure-injection tests: a real exit, never a
// context cancellation, so nothing graceful runs. It is only compiled into binaries built with -tags faultinject;
// CI checks that production binaries contain none of it.
package faultinject
