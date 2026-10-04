# Runbook

How to run the ledger, and what to do when something looks wrong. Each section says what you see, what it means,
and what to do. Commands assume the Compose stack; in Kubernetes the same binaries run in the `ledger` namespace.

## The pieces

| Process | Does | Admin port | Replicas |
|---|---|---|---|
| `ledger-api` | the HTTP API, hold expiry, idempotency key cleanup | 9090 | 2 or more |
| `relay` | publishes the outbox to Kafka; one leads (advisory lock), the rest wait | 9091 | 2 |
| `notifier` | sample consumer: one notification per event, exactly once | 9092 | 1 or more (one group) |
| `reconcile` | one-shot invariant check over the whole database | | job |
| `ledgerctl` | partners, API keys, outbox status, API calls | | CLI |

Every admin port serves `/metrics`, `/healthz` (the process is up) and `/readyz`. For ledger-api, ready means the
database answers and its schema is at this build's migration version. `/debug/pprof` is on the admin port too.

## Deploying

1. **Migrations first:** `ledger-api migrate` as the owner role (a Job in Kubernetes). Migrations are additive:
   a running older build keeps working on the newer schema. New replicas stay unready until the schema reaches
   their version, so the order of the Job and the rollout doesn't matter.
2. **Roll ledger-api.** The rollout never has fewer than the desired replicas (maxUnavailable 0), and the
   PodDisruptionBudget keeps one up during node drains. On SIGTERM a replica fails readiness, keeps serving for
   `SHUTDOWN_DRAIN` (5 s) while load balancers move away, then finishes in-flight requests and exits.
3. **Roll relay and notifier** any time. A stopping relay stops within `SHUTDOWN_TIMEOUT` even if Kafka is down. A
   stopping notifier finishes its batch and leaves the group, so its partitions move at once.

Once the first push has happened, migration `00001` is frozen: changes are always new migrations.

## Routine tasks

- **A new partner:** `DATABASE_URL=<owner> ledgerctl partner create --name "…" --funding-limit 100000000 --currencies EUR`.
  The API key is printed once and only its hash is stored, so hand it over now. The settlement accounts are
  created with the partner.
- **A leaked key:** `ledgerctl partner rotate-key --partner <id>`. The old key stops working at once.
- **Lower a funding limit:** update `partners.funding_limit_minor` and the settlement accounts' `min_balance_minor`
  (owner role). Lowering it below the current position is allowed: the account can't fund anything until money
  comes back, and withdrawals into it always go through.
- **A partner's rate limit:** `partners.rate_limit_per_min`. Each replica enforces it separately, so N replicas
  allow up to N times the limit.
- **Reconcile:** `DATABASE_URL=<any reader> PUBLISHED_WITHIN=60s reconcile`. Exit 0 means clean, 1 means an
  invariant broke (the report names the rows), 2 means it couldn't run.

## Alerts and what to do

### Reconciliation fails

**This is the most serious alert.** The ledger's own invariants are broken. Don't "fix" balances with UPDATEs:
`postings` and `transactions` are append-only, and balances must stay derivable from them.

1. Stop and read the report: which invariant, and which rows.
2. Each check means something different:
   - **2 (posted ≠ postings):** the materialised balance drifted. Find what wrote it (the audit log by time, deploys).
   - **1 (a transaction doesn't sum to zero):** this should be impossible, because a deferred trigger refuses it at COMMIT. Treat it as a database integrity incident.
   - **6a/6b (events ≠ versions):** a change was written without its event, or the other way round.
3. Correct with a **new reversing transaction** through the API, never by editing rows. Keep the report with the
   incident.

### `notifier_gaps_total` or `notifier_regressions_total` above zero

The event stream lost an event, or delivered an account's events out of order. Duplicates don't count here: the
inbox absorbs them before the sequence check. Check the relay first: was more than one leader publishing (two pods
with `relay_leader` = 1), and did a failover happen at that time? Compare the notifier's `account_progress` with
the outbox for the account in the log line. The outbox is the source of truth, and events can be re-published from
it.

### The outbox backlog grows (`ledgerctl outbox status`, `relay_failed_total` rising)

- **No leader:** no pod has `relay_leader` = 1. Check the relays' logs and the database connection; a standby
  takes the lock within a second of a leader losing its connection.
- **Kafka unreachable:** the relay logs "kafka refused records". The ledger keeps working and the outbox keeps the
  events. When Kafka returns, everything is delivered in order. Nothing to do but fix Kafka.
- A backlog older than your consumers' tolerance is an incident for them, not data loss: nothing is dropped.

### Many 409 `idempotency_in_progress`

Clients retried while their first attempt was running, which is expected under load. A key stays in progress after
a failed attempt only if releasing it also failed (the database was unreachable). It then frees itself when its
30 s lease expires. Don't delete idempotency rows by hand: a completed row is the stored answer a retry must get.

### 503 `unavailable`

The database didn't answer in time or dropped the connection (deadline 10 s, statement timeout 8 s, lock timeout
5 s). Nothing was stored, and clients should retry with the same key. A burst during a database failover is normal.
A steady rate means the database is overloaded or a lock is being held. Look for long transactions in
`pg_stat_activity`. `idle_in_transaction_session_timeout` (15 s) ends leaked ones.

### p99 latency above 100 ms

Check in this order:
1. `db_pool_waits` and `db_pool_wait_time_seconds` (pool too small?).
2. Postgres wait events in `pg_stat_activity`: `WALWrite` or `WalSync` mean the commit path (disk), `Lock:*`
   means contention on hot accounts.
3. A CPU profile from `/debug/pprof/profile` on the admin port.

The load test's findings are in the README.

### Many 429s

- **`rate_limited` for a partner:** it's over its own limit. The aggregated audit rows (`action = 'rate_limited'`)
  give counts per minute.
- **`ledger_auth_throttled_total` rising:** an address is sending bad API keys. The throttle answers without
  touching the database. The audit rows with `action = 'auth_throttled'` name the address.

## Failure behaviour (what the system guarantees)

| Failure | What clients see | What the ledger guarantees |
|---|---|---|
| Database lost mid-request | 503, retry with the same key | never half-applied; a retry gets the stored answer if the commit happened |
| Commit acknowledgement lost | 503, then the stored 201 on retry | exactly one transaction |
| Kafka down | nothing: writes succeed | events wait in the outbox, then go out in order |
| Relay crash | nothing | the next leader re-publishes unmarked rows; consumers deduplicate |
| Notifier crash | nothing | the batch is redelivered and skipped by the inbox |
| Retry storm (one key) | one answer, or 409 while it runs | one transaction |
