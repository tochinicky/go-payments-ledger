# go-payments-ledger

A double-entry ledger service in Go: accounts, idempotent transfers and holds, a transactional outbox to Kafka, and an inbox-based consumer, built so that money can't be created, lost or moved twice.

> **Status:** the double-entry core, the Postgres schema, the accounts and transfers API and idempotent writes are built; holds, events and operations follow.

## Toolchain

The Go toolchain runs in Docker, so nothing needs installing beyond Docker:

```sh
scripts/go.sh go test -race ./...     # tests, with the race detector
scripts/go.sh go build ./...
scripts/go.sh golangci-lint run       # lint (the golangci-lint image)
scripts/go.sh sqlc generate           # regenerate internal/store/db from the SQL (the sqlc image)
```

The store and API tests start a throwaway Postgres with Testcontainers, so Docker must be running.

## API

All routes need `Authorization: Bearer <partner API key>`; errors are `application/problem+json` with a stable `code`.

Every write (`POST`) needs an `Idempotency-Key` header (at most 255 characters, scoped to the partner). A retry with the same key and request gets the original response, byte for byte, with `Idempotent-Replayed: true`. The same key with a different request gets `422 idempotency_key_reused`. While the first attempt is still running, a retry gets `409 idempotency_in_progress` with `Retry-After: 1`. Completed keys are kept for 24 hours (`IDEMPOTENCY_RETENTION`).

| Route | |
|---|---|
| `POST /v1/accounts` `{currency, customer_ref?}` | open a customer account |
| `GET /v1/accounts/{id}` | the account |
| `GET /v1/accounts/{id}/balance` | posted, held and available, in minor units |
| `GET /v1/accounts/{id}/statement?cursor=&limit=` | entries oldest first, keyset-paginated |
| `POST /v1/transfers` `{from, to, amount_minor, currency, reference?}` | move money between two of the partner's accounts |

`ledger-api migrate` applies the migrations (owner role); `ledger-api` serves on `LISTEN_ADDR` (default `:8080`). Both read `DATABASE_URL`.

- [Decisions](docs/decisions.md)
