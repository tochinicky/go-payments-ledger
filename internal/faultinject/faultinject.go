//go:build faultinject

package faultinject

import (
	"os"
	"sync"
)

// ExitCode is what a process exits with at an injected crash, so a test can tell it from a real failure.
const ExitCode = 86

// Point exits the process at once if FAULT_POINT names this point.
func Point(name string) {
	if os.Getenv("FAULT_POINT") == name {
		os.Exit(ExitCode)
	}
}

var fired sync.Map

// Once reports true the first time it is called for the point FAULT_POINT names, and false otherwise: for faults
// that must happen exactly once, such as a lost commit acknowledgement.
func Once(name string) bool {
	if os.Getenv("FAULT_POINT") != name {
		return false
	}
	_, already := fired.LoadOrStore(name, true)
	return !already
}
