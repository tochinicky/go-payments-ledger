// Command ledger-api serves the ledger's HTTP API. Slice 0: a skeleton that starts, logs and exits.
// The API arrives in slice 2.
package main

import (
	"log/slog"
	"os"

	"github.com/tochinicky/go-payments-ledger/internal/version"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("ledger-api skeleton", "version", version.String())
}
