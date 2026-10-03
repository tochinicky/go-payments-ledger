// Package version reports which build of the ledger is running.
package version

import "fmt"

// Version and Commit are set at build time:
//
//	go build -ldflags "-X github.com/tochinicky/go-payments-ledger/internal/version.Version=v0.1.0 -X ...Commit=$(git rev-parse --short HEAD)"
//
// Left unset, they read "dev" and "unknown".
var (
	Version = "dev"
	Commit  = "unknown"
)

// String returns a one-line description, e.g. "v0.1.0 (a1b2c3d)".
func String() string {
	return fmt.Sprintf("%s (%s)", Version, Commit)
}
