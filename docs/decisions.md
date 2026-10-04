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

---

## Slice 1: money and the double-entry core

- **`Money` is `int64` minor units + a currency; arithmetic is checked.** `Add`, `Sub` and `Neg` return an error instead of wrapping round: in Go, `math.MaxInt64 + 1` silently becomes a huge negative number, which in a ledger would be a balance flipping sign. `Neg(math.MinInt64)` is the subtle case: it has no positive counterpart.
- **Supported currencies are a fixed table** (EUR, USD, GBP, CHF: 2 decimals; JPY: 0). Unknown codes are refused rather than guessed.
- **`ParseMoney` is strict:** an optional `-`, digits, and at most the currency's decimals. No `+`, spaces, exponents or separators. The API takes `amount_minor` as an integer; parsing is for the CLI and tests, and strictness is safer than guessing. It accumulates as a negative number, so even the most negative amount parses without overflowing on the way.
- **`FormatAmount` works on the decimal digits as text** (`strconv.FormatInt`). It needs no integer conversion, so it also formats `math.MinInt64`. The first version converted to `uint64`; the linter (gosec G115) flagged it and the rewrite is simpler.
- **Business errors are values with stable codes** (`*ledger.Error{Code, Message}`, matched with `errors.Is` against sentinels). The codes are the API's error codes; a detailed message still matches its sentinel.
- **An in-memory `Book` is the reference model.** It applies exactly the rules the database-backed ledger will. The property test checks it against the invariants now; from slice 2 the database must agree with it.
- **`CheckInvariants` is the reconciliation logic in miniature:** postings sum to zero per transaction (1), posted = sum of postings (2), available ≥ floor (3), held = sum of active holds and captures ≤ holds (4).
- **A capture releases the whole reservation and posts the captured part**, so it needs no funds check (the money was reserved when the hold was placed), and available can only go up. It's all-or-nothing: if posting fails, the reservation is restored.
- **Account `Version` goes up by one on every change** (posted or held). It becomes `account_seq` in events.
- **Tests:**
  - **unit tests** per rule;
  - **a rapid property test**: random sequences of transfers, holds, captures, releases, expiries and closures over two partners, checking all invariants plus per-partner money conservation **after every step**, and that every refusal is a known business error. It runs **1,000 sequences** by default (`TestMain`; an explicit `-rapid.checks` wins). Removing the funds check on holds made it fail at once, shrunk to the minimal case (a single hold one cent too large);
  - **fuzz tests** for parsing (round trip) and addition (against exact `math/big` arithmetic).

**Slice 1 follow-up: closure rules and error identity**
- **Only an empty customer account can be closed** (posted = 0 and held = 0, which also means no active holds). Otherwise `409 account_not_empty`: the money would be stranded, and an active hold on it could still be captured. **Settlement accounts can't be closed** (`409 account_not_closable`); their balance mirrors the customers' money. Closing a closed account is a no-op success. Consequence: a closed account can never change balance. Capture also checks its source is active, as a guard in case the closure rule is ever loosened. Two new stable codes: `account_not_empty`, `account_not_closable` (409).
- **Error identity is separate from the API code.** `errors.Is` matches the sentinel an error is, or was made from, never the code: `ErrInvalidAmount`, `ErrInvalidCurrency` and `ErrOverflow` share `validation_failed`, and matching on the code made an overflow look like an ordinary invalid amount. The property test would have accepted a real overflow bug as a normal refusal. It now treats any overflow as a failure. A test pins each sentinel to its documented code.
- **Version rule:** exactly one bump per account per ledger operation that changes its money (place, capture, release and expire of a hold; each side of a transfer). Each bump becomes one account entry with the next `account_seq`.
- `CheckInvariants` sums with checked addition, so the checker itself can't overflow silently.

## Slice 2: Postgres, sqlc, accounts and transfers

- **Postgres 18, pgx/v5, sqlc 1.31.1, goose 3.28.** sqlc turns the SQL in `internal/store/queries` into typed Go (`internal/store/db`), so every query is visible SQL and checked against the schema at generate time. Migrations are embedded in the binary (`ledger-api migrate`) and shared with the tests.
- **Ordered row locks at READ COMMITTED.** A transfer locks both balance rows with one `SELECT … ORDER BY account id FOR UPDATE`, then checks the funds against the locked rows. Two transfers A→B and B→A take the locks in the same order, so neither can wait for the other while holding a lock: no deadlock. Alternatives considered: optimistic locking on `version` (retries under contention, and hot accounts like settlement are always contended); SERIALIZABLE with retry (correct, but every conflict becomes a retry the client never asked for). The lock query is scoped to the partner, so a request can never lock another partner's row.
- **The rules live in package `ledger`; the store calls them on the locked rows**, in the same order as the in-memory Book, so both refuse the same command with the same error.
- **Overflow is checked before anything is written**: every new posted balance is computed with checked arithmetic first. Postgres would refuse a `bigint` overflow too, but as a 500, not a business error.
- **Invariant 1 is also enforced by the database**: a deferred constraint trigger checks at COMMIT that a transaction's postings sum to zero per currency, whatever wrote them.
- **Append-only (invariant 8) twice over**: the app's role has no UPDATE/DELETE grant on transactions and postings, and triggers refuse UPDATE, DELETE and TRUNCATE even for the owner. Tests prove both.
- **The app connects as a login role in `ledger_writer`, never as the owner.** The migration creates the grant-holding role without a password; each deployment (and the tests) creates its own login role in it.
- **Statements are ordered by `account_seq`, a per-account sequence, not by a timestamp.** Each posting stores the balance version it produced (`account_seq`), assigned under the account's row lock, so it follows commit order by construction. A clock can't promise that: `now()` is when the transaction began, before it waited for the lock, and even a timestamp taken after the lock goes wrong if the server clock steps backwards (an NTP correction, a VM migration). Then keyset pagination would skip or repeat rows. `UNIQUE (account_id, account_seq)` is the statement's index and also turns a lost update into a hard error. The same number goes on the account's events, so statements and events share one order. A hold takes a version without a posting, so a statement can skip a number, and the event stream fills it. `created_at` (`clock_timestamp()`) is for display only.
- **Statements use keyset pagination** on `account_seq` with an opaque base64 cursor; a page costs the same at any depth, and new rows can't shift a page. `limit` is 1–200 (default 50).
- **Invariant 3 is also enforced by the database:** a trigger on `balances` refuses a write that leaves `posted − held` below the account's floor **and** lower than it was. The store checks first and answers `insufficient_funds`; the trigger turns a bug in that check into a failed transaction instead of a committed overdraft. A write that improves the position is always allowed, matching `CanSpend`, which only guards spending.
- **A funding limit may be lowered below a settlement account's current position.** The account then can't fund anything, but withdrawals into it still go through until it's back within its floor. Refusing the change would make a partner's risk limit impossible to tighten while it's in use, which is exactly when it matters.
- **Migration `00001` was edited in place while slice 2 was under review, since it had never shipped. It becomes the frozen baseline at the first push: from then on, only new migrations.**
- **Settlement accounts are created with their partner** (per currency, floor = −funding limit); the API opens customer accounts only.
- **The settlement account is a deliberate hot spot:** every funding transfer locks its row, so funding is serialised per partner and currency. The load test funds its accounts up front, so it measures customer transfers, not settlement contention. A real system would batch funding, or split settlement across several sub-accounts.
- **Partners authenticate with `Bearer <key>`, looked up by its SHA-256.** Keys are random and high-entropy, so a slow password hash (bcrypt) adds nothing; the lookup is by hash, so timing reveals nothing about the key. Audit logging and rate limits come with the security slice.
- **Strict request decoding**: unknown fields, trailing data and bodies over 64 KiB are refused (`malformed_json`, `body_too_large`), and `validation_failed` lists every bad field at once. A malformed id in a path is `404`: it can't name anything.
- **Tests**: one Postgres container per test package (Testcontainers), each test isolated by its own partner.
  - **Concurrency:** 1,000 random transfers among 10 accounts, 50 workers, sent as opposite-direction pairs. No error but `insufficient_funds` (so no deadlock), money conserved, nothing below its floor, posted = sum of postings, `-race` clean.
  - **Store vs Book:** a rapid property test runs random transfers against both, and requires the same outcome and the same balances after every step. 100 sequences in the default run; a separate CI job runs 1,000.
  - **Database guarantees:** append-only (app: permission denied; owner: trigger), an unbalanced transaction can't commit, a balance can't be written below its floor, a duplicate `account_seq` is refused, overflow writes nothing.
  - **Statement order:** entries carry account_seq 1…n; after a hold-like version bump, the next entry's account_seq equals the balance version.
  - **API:** the happy path, statement pagination, and the error catalogue (401, 404 across partners, 400, 413, 422).

## Slice 3: idempotency

- **Keys live in Postgres, and the response is stored in the same transaction as the money movement.** A crash can't separate "moved the money" from "remembered the answer".
- **Claim, then work.** A request claims `(partner, key)` with an `INSERT … ON CONFLICT DO NOTHING`, in its own short transaction, with a 30 s lease timed on the database clock and a random lease token. If the key exists, a plain read (no lock, so it never waits on the running attempt) decides:
  - a different request hash: `422 idempotency_key_reused`, whatever the key's state;
  - completed: replay;
  - a live lease: `409 idempotency_in_progress`;
  - a lapsed lease: take over with a conditional UPDATE.
  A short loop covers keys freed or completed while we looked.
- **Fencing.** The ledger transaction starts with `SELECT … FOR UPDATE` on its own claim (`lease_token = mine AND status = 'in_progress'`). If the claim is gone, it stops before touching money. While it works, the row lock makes any takeover wait; once it commits, the takeover's UPDATE re-checks its conditions, finds the key completed, and replays instead. A slow attempt and its replacement can never both post.
- **What is stored:** 2xx responses and business refusals (`insufficient_funds`, `not_found`, …). A refusal stays a refusal for its key, and a new key is how you try again. Validation (400) and auth (401) happen before the claim, so they store nothing and a corrected request can reuse its key. 5xx failures aren't stored: the key is freed so a retry runs again.
- **Freeing a key is conditional:** `DELETE … WHERE status = 'in_progress' AND lease_token = mine`, on a fresh context (the request's own may be what failed). If the failure was a lost COMMIT acknowledgement, the key is already completed and stays, and the retry gets the stored answer. If the database is unreachable, the lease simply expires.
- **The request hash is SHA-256 of the route, the actual path and the canonical JSON of the validated request**, so whitespace, field order and formatting don't turn a retry into "key reused", and the same key on another route is a different request.
- **Replays are byte for byte**: the stored body is the exact bytes first sent, with `Idempotent-Replayed: true`. The `Location` header on account creation was dropped so a replay matches the original exactly.
- **Why 409 instead of waiting for the first attempt:** waiting holds a connection and a server goroutine per retry for as long as the slowest attempt, and a retry storm would multiply that. A 409 with `Retry-After: 1` costs one read, and the client retries when the answer is likely ready. Waiting would be friendlier for clients that don't retry, at the cost of the server's capacity under exactly the load that causes retries.
- **Cleanup:** ledger-api deletes completed keys older than the retention (24 h, `IDEMPOTENCY_RETENTION`), and in-progress claims abandoned that long, once a minute in batches of 1,000 with `FOR UPDATE SKIP LOCKED`, so replicas share the work. A live claim is never touched.
- **A 503 `unavailable` (with `Retry-After: 1`) when the database can't be reached or doesn't answer in time**, a 500 `internal` otherwise; neither is stored.
- **The lease always outlives the request, by construction.** The timeouts nest: idempotency lease (30 s) > request deadline (10 s, `REQUEST_TIMEOUT`) > Postgres `statement_timeout` (8 s) ≥ `lock_timeout` (5 s). The request's context reaches pgx, so a stuck query is cancelled rather than abandoned; the database gives up on a lock wait or a slow statement before the request does. `statement_timeout` and `lock_timeout` bound each statement, not a transaction of several: the request deadline is what bounds the whole transaction, and "lease > request deadline" is the relation that keeps the lease alive. `idle_in_transaction_session_timeout` (15 s, longer than any request) is a backstop: a leaked transaction can't hold balance locks indefinitely. These are session settings on every connection of the app's pool, so a background job that needs longer waits (the relay's lock connection, a long batch) must get its own. ledger-api refuses to start with any other order. Fencing would still keep money safe if a request outlived its lease, but the client would see a confusing 409 and the work would be wasted. A lock or statement timeout is a 503 with `Retry-After`; the key is freed, so the retry runs.
- **Tests:**
  - **Store:** the claim lifecycle (new, in progress, reused, replay, per partner); a lapsed lease is taken over and the old attempt is fenced (one transaction); a takeover waits for an attempt still inside its transaction, then replays; release frees only its own in-progress claim (a completed key and a taken-over claim survive); cleanup deletes only expired keys, across batches.
  - **API:** the key is required; a replay is byte-identical even with the JSON reordered; a different request with the same key is refused, including on another route; a refusal is stored but a validation error isn't.
  - **Lock timeout:** a request behind a held balance lock gets a 503 after about 5 s, its key is freed, and once the lock is gone a retry with the same key succeeds (one transaction). The timeout ordering is unit-tested.
  - **Retry storm (scenario 1):** 50 identical requests with one key, with the settlement row held locked so the first request is really in flight. Result: exactly one 201 and 49 409s with `Retry-After`, a final retry gets the identical 201, and exactly one transaction.

## Slice 4: holds and expiry

- **A hold reserves money by raising `held`; it posts nothing until it's captured.** Placing a hold runs the same checks as a transfer (both accounts the partner's, active, the amount's currency, enough available), on the same ordered locks.
- **A capture is one entry on the source account:** it releases the whole reservation and posts the captured part in a single balance update, so the version goes up once (`ApplyToBalance` changes posted and held together). A partial capture releases the remainder. No funds check is needed, and `available` can only go up.
- **A hold past its `expires_at` is no longer active, even before the job has ended it.** An expired authorisation can't be captured or released (`409 hold_not_active`); the expiry job then gives the reservation back. The in-memory Book has no clock in capture, so the property test keeps its holds far from expiry, and expiry has its own tests.
- **Expiry runs in ledger-api every 5 s**, in batches of 100, each one transaction: `FOR UPDATE SKIP LOCKED` on due holds (replicas share the work, and a hold a request is ending right now is left to it), then the balances in account-id order.
- **Lock order everywhere: hold row first, then balance rows in account-id order.** Placing a hold takes only balance rows (the hold is new); capture, release and expiry take the hold row, then balances. The expiry job never waits on a hold row (SKIP LOCKED), and balances are always taken in the same order, so there's no cycle.
- **Invariant 4 in the database:** a trigger refuses any change to a hold that has already ended, even by the owner, and CHECK constraints tie the status to its fields (`captured` ⇔ a transaction and captured > 0; `active` ⇔ no end time; captured ≤ amount). The app role may update only the four columns that end a hold.
- **`expires_in` is 1 s to 30 days**, and `expires_at` is computed on the database clock, like the idempotency lease.
- **Capture and release answer 200** (the hold changed state; a capture's transaction is its `transaction_id`); placing answers 201. All three take an Idempotency-Key. Release accepts an empty body.
- **Tests:**
  - **Rules:** placing reserves without posting; a partial capture posts the part and releases the rest; the refusals (insufficient funds, exceeds the hold, inactive destination leaves the hold active, another partner's hold is 404, twice is `hold_not_active`); release; a hold past its time can't be captured or released.
  - **Statement and versions agree:** placing a hold takes version 2 with no posting, and the capture's posting carries account_seq 3.
  - **Expiry:** only due holds end, across batches of 2; a capture racing the job, 20 times: exactly one ends each hold, and money is conserved.
  - **Database:** an ended hold can't be revived.
  - **Property:** the store-vs-Book test now draws transfers, holds, captures and releases (1,000 sequences in its CI job).
  - **API:** place, capture and release end to end, the error catalogue for holds, and an idempotent capture that posts once.

