# Decisions

Each design choice, why it was made, and what it costs.

## Slice 0: toolchain and skeleton, 2026-10-03

- **Go 1.27.1**, the latest stable release on 2026-10-03 (`go.mod`: `go 1.27.1`). Pinning the exact version makes local, Docker and CI builds identical.
- **The toolchain runs in Docker** (`scripts/go.sh`), so the only prerequisite is Docker and every machine uses the same Go version. The image is pinned to `golang:1.27.1`. Module and build caches live in named volumes (`gpl-gomod`, `gpl-gocache`), so repeat runs take seconds. The race detector needs cgo, and the `golang` image has gcc, so `-race` works in it.
- **Module path `github.com/tochinicky/go-payments-ledger`**: the repo the code will live at, so imports don't change when it's pushed.
- **Lint: golangci-lint v2.14.0**, also run from its Docker image. Beyond the standard set (errcheck, govet, staticcheck, unused, ineffassign) it enables linters that matter for this service: `errorlint` (wrap with `%w`, compare with `errors.Is/As`), `gosec`, `bodyclose`, `noctx`, `sqlclosecheck`, `rowserrcheck`, and `revive` for idiomatic naming and doc comments. `gofmt` and `goimports` are enforced as formatters. Checked that it really fails: an ignored `os.Stdout.Write` error was reported by errcheck.
- **CI** (`.github/workflows/ci.yml`, checked with actionlint): lint, `go test -race -count=1 ./...`, and build with the commit stamped in. `-count=1` turns off Go's test cache, so CI always really runs the tests. No deploy, no registry push.
- **Version stamping:** `internal/version` holds `Version` and `Commit`, set at build time with `-ldflags -X`. It's the Go way of stamping a build without generated files.

---

## Design decisions, 2026-10-03

The decisions the build depends on, each with its reason:

**Money**
- **Settlement account per partner and currency; funding is an ordinary transfer.** Postings sum to zero, so money has to come from an account. A settlement account is the partner's position at the bank, and needs no special endpoint.
- **Every account has a floor, `min_balance_minor`; the settlement floor is minus the partner's funding limit.** An unbounded floor lets a partner mint money; a bank regulator asks about exactly this control. One check for every account: `posted − held ≥ min_balance`.
- **A hold's destination is fixed when it's placed** (like a card authorisation, which knows the merchant). Capture to an inactive destination → `409 hold_not_capturable`.
- **No cross-partner movement; another partner's id → 404**, so tenant isolation never leaks existence.

**Idempotency**
- **Hash of the canonical typed request**, not of raw bytes: whitespace and key order mustn't turn a retry into "key reused".
- **Only final outcomes are stored** (2xx, deterministic 4xx). A transient failure must be retryable; a stored `insufficient_funds` stays failed, and a new key is how you try again (Stripe's semantics).
- **A 30 s lease on the DB clock, with a fencing token locked at the start of the ledger transaction.** A takeover blocks on the row lock instead of running in parallel, and replicas with skewed clocks can't steal a live lease.
- **Why the database and not Redis holds the keys here:** the key's completion and the money movement commit in one transaction, so no crash between "moved money" and "remembered the key" exists. With a separate key store (such as Redis), there is a window between moving the money and recording the key.

**Process and storage**
- **ledger-api runs the API plus hold expiry and key cleanup; relay and notifier are separate binaries.** Background loops are batched and replica-safe; the relay is separate so a test can kill it alone.
- **Append-only twice over: role grants and triggers** (UPDATE, DELETE and TRUNCATE). Permissions stop the app; triggers stop even the owner from quietly rewriting history.

**Events**
- **franz-go v1.22.1, Redpanda v26.2.3** (research: the idempotent producer is on by default and keeps per-partition order; Redpanda is the only broker with a working Testcontainers module).
- **One event per account entry.** A transfer touches two accounts, and one event can only have one partition key.
- **One active relay (an advisory lock on a dedicated connection), publishing `ORDER BY seq`.** `seq` is an IDENTITY drawn while the account's row lock is held, so per account, seq order is commit order. Rejected alternatives: UUIDv7 id order (ids can be generated before the lock, so it isn't commit order); `ORDER BY account_id, account_seq` (correct per account, but `LIMIT n` starves high account ids); `SKIP LOCKED` across replicas (reorders an account's events). This is the classic outbox ordering bug.
- **6 partitions, fixed; topic auto-creation off.** Adding partitions remaps keys and breaks per-account order.
- **Inbox in its own `notifier` schema and role** (production: a separate database). The notifier checks `account_seq` for gaps.

**Build defaults**
- **Error codes follow the IETF Idempotency-Key draft** (draft-ietf-httpapi-idempotency-key-header): 400 missing key, 409 concurrent, 422 reused.
- **The SLO histogram has a bucket at exactly 100 ms**, so "under 100 ms" is a count, not an interpolation.
- **k6 uses constant-arrival-rate (an open model).** A closed VU loop slows down with the server and hides latency (coordinated omission).
- **`/readyz` fails until the expected migration version is present**, so readiness doesn't depend on Job ordering in kind.

**Failure injection**
- **A `faultinject` build tag; CI checks the production binary has no `faultinject` package symbols.** "Not in production" is proven, not claimed.
- **Crashes are real (a subprocess exit or SIGKILL), never a context cancellation**, which runs the graceful paths and tests a polite stop.
- **Kafka outage = broker stop/start with a fixed host port** (the advertised listener survives); pause/unpause is a second variant. Not Toxiproxy: Kafka hands out advertised addresses.
- **New scenario 8, "commit outcome unknown":** freeing a key after a 5xx is conditional on its own lease token, and never touches a completed row. **The test proves the logic** (a hook does the real commit, then reports a lost ack); **the demo proves the mechanism** (Toxiproxy resets the connection after COMMIT).
