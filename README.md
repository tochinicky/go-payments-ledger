# go-payments-ledger

A double-entry ledger service in Go: accounts, idempotent transfers and holds, a transactional outbox to Kafka, and an inbox-based consumer, built so that money can't be created, lost or moved twice.

> **Status:** the double-entry core, the Postgres schema, accounts, transfers, holds, idempotent writes and the event pipeline (outbox, relay, notifier) are built; security, observability and operations follow.

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
| `POST /v1/holds` `{account, to_account, amount_minor, currency, expires_in}` | reserve money for a destination fixed now (`expires_in` in seconds, up to 30 days) |
| `GET /v1/holds/{id}` | the hold |
| `POST /v1/holds/{id}/capture` `{amount_minor}` | move up to the held amount to the destination; the rest is released |
| `POST /v1/holds/{id}/release` | free the reservation (empty body or `{}`) |

`ledger-api migrate` applies the migrations (owner role); `ledger-api` serves on `LISTEN_ADDR` (default `:8080`). Both read `DATABASE_URL`.

## Events

Every change to an account's balance writes one event to an outbox table in the same database transaction. `relay` publishes the outbox to Kafka (topic `ledger.account-entries.v1`, 6 partitions, keyed by account id); run several for availability, and one leads. `notifier` is a sample consumer that records one notification per event, exactly once.

```sh
KAFKA_BROKERS=localhost:9092 relay create-topic                 # once: the topic, with its fixed partition count
DATABASE_URL=... KAFKA_BROKERS=localhost:9092 relay
DATABASE_URL=... KAFKA_BROKERS=localhost:9092 notifier          # the notifier's own database role
```

The end-to-end tests (`internal/e2e`) run the pipeline against real Postgres and Redpanda containers, including crash and outage scenarios.

## Operations

Each binary has an admin listener apart from its main port: `ledger-api` on `:9090`, `relay` on `:9091`, `notifier` on `:9092` (`ADMIN_ADDR`). It serves `/metrics` (Prometheus), `/healthz` (liveness: the process is up) and `/readyz` (readiness; for `ledger-api`, the database answers and its schema is at least this build's migration version). Traces go to `OTEL_EXPORTER_OTLP_ENDPOINT` when it is set.

On SIGTERM, `ledger-api` fails readiness, keeps serving for `SHUTDOWN_DRAIN` (5 s), then stops accepting and lets in-flight requests finish. The relay stops within `SHUTDOWN_TIMEOUT` (15 s) even if Kafka is unreachable. Every write and every authentication failure is recorded in `audit_log`; each partner is rate-limited by its `rate_limit_per_min`.

- [Decisions](docs/decisions.md)

## Licence

MIT, see [LICENSE](LICENSE).
