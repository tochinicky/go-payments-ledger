//go:build faultinject

package faultinject

import "os"

// ExitCode is what a process exits with at an injected crash, so a test can tell it from a real failure.
const ExitCode = 86

// Point exits the process at once if FAULT_POINT names this point.
func Point(name string) {
	if os.Getenv("FAULT_POINT") == name {
		os.Exit(ExitCode)
	}
}
