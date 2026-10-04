//go:build faultinject

package relay

import "github.com/tochinicky/go-payments-ledger/internal/faultinject"

func fault(point string) { faultinject.Point(point) }
