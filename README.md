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

## Load test

`load/transfers.js` (k6, an open model: constant arrival rate, so a slow server can't hide its latency by slowing
the clients) runs 80% transfers and 20% hold place-then-capture across 2 partners and 100 funded accounts, ramped to
200 requests/s and held for 5 minutes. 5% of writes are retried with the same key: half after the answer (must replay
byte for byte), half concurrently (a replay or 409, never a second transaction).

```sh
LEDGER_DB_HOST=postgres:5432 docker compose up -d --build   # the load test bypasses Toxiproxy
load/setup.sh && load/run.sh                                # RATE and HOLD override 200/s and 5m
```

**Measured** on one laptop (Apple Silicon, Docker Desktop with 8 vCPUs and 4 GB for the whole stack: Postgres,
Redpanda, the three services, the observability stack and k6 itself), 2026-10-05:

| | Result |
|---|---|
| Correctness | **0 errors** in 67,579 checked requests, **0 replay mismatches**, reconciliation clean |
| Throughput | **166 iterations/s sustained** of the 200/s offered (k6 dropped the rest) |
| Transfer latency (k6) | median 16 ms, **p99 5.4 s** |
| Transfers within 100 ms (ledger-api's own histogram) | **64%** |
| At 100/s for 60 s | p99 317 ms, nothing dropped |

**SLO verdict: missed.** The target is 99% of `POST /v1/transfers` under 100 ms at 200/s; this machine sustains
about 166/s and its tail is far above 100 ms even at 100/s. What the investigation found (details in
[decisions](docs/decisions.md)):

- **The service isn't CPU-bound:** a CPU profile mid-run shows ledger-api using about half a core, mostly in network syscalls.
- **Postgres's write-ahead log is the bottleneck:** sampled wait events are mostly `WALWrite` and `WalSync` (commits queueing for the log flush), then row-lock waits. Each transfer holds its two balance locks across about a dozen sequential round trips, so slow commits lengthen lock hold times and build convoys.
- **Done:** the notifier now commits once per batch instead of once per event (p99 at 200/s fell from 8.4 s to 2.5 s in a 60 s run). Postgres group commit (`commit_delay`) was tried and made it worse: waiting to share a flush while holding row locks feeds the convoy.
- **Next:** fewer round trips while the locks are held (pipelining a transfer's statements), and a real database disk rather than a laptop VM's.

