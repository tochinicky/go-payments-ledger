//go:build faultinject

package store

import (
	"errors"
	"net"

	"github.com/tochinicky/go-payments-ledger/internal/faultinject"
)

// commitAckLost simulates scenario 8, "commit outcome unknown": the transaction really committed, but the
// connection drops before the acknowledgement arrives, so the caller sees a network error.
func commitAckLost() error {
	if faultinject.Once("store.commit_ack_lost") {
		return &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer (injected after COMMIT)")}
	}
	return nil
}
