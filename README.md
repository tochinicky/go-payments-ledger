# go-payments-ledger

A double-entry ledger service in Go: accounts, idempotent transfers and holds, a transactional outbox to Kafka, and an inbox-based consumer, built so that money can't be created, lost or moved twice.

> **Status:** slice 1 (money and the double-entry core) built; the database, API, events and operations slices follow.

## Toolchain

The Go toolchain runs in Docker, so nothing needs installing beyond Docker:

```sh
scripts/go.sh go test -race ./...     # tests, with the race detector
scripts/go.sh go build ./...
scripts/go.sh golangci-lint run       # lint (the golangci-lint image)
```

- [Decisions](docs/decisions.md)
