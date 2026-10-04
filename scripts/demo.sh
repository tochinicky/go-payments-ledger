#!/usr/bin/env bash
# Runs the eight failure scenarios against the Compose stack and prints PASS or FAIL for each, from concrete checks
# (HTTP answers, SQL counts, the notifier's metrics), then the reconciliation. Exit status 0 only if all pass.
#
#   docker compose up --build -d && scripts/demo.sh
#
# KAFKA_DOWN (default 60) is how long scenario 3 keeps the broker stopped, in seconds.
set -uo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8080
TOXI=http://localhost:8474
OWNER_DSN='postgres://owner:owner@postgres:5432/ledger?sslmode=disable'
KAFKA_DOWN=${KAFKA_DOWN:-60}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"; curl -s -X POST "$TOXI/proxies/postgres" -d "{\"enabled\":true}" >/dev/null; curl -s -X DELETE "$TOXI/proxies/postgres/toxics/lat" >/dev/null; curl -s -X DELETE "$TOXI/proxies/postgres/toxics/reset" >/dev/null' EXIT
RESULTS=()
FAILED=0
RUN=$(date +%s) # references carry the run, so a second run on the same database counts only its own rows

say() { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }
result() { # result NUMBER NAME OK DETAIL
  if [ "$3" = ok ]; then RESULTS+=("PASS  $1. $2"); else RESULTS+=("FAIL  $1. $2: $4"); FAILED=1; fi
  printf '   -> %s\n' "${RESULTS[${#RESULTS[@]}-1]}"
}
sql() { docker compose exec -T postgres psql -U owner -d ledger -tAc "$1"; }
ledgerctl() { docker compose run --rm -T --no-deps --entrypoint /usr/local/bin/ledgerctl -e "DATABASE_URL=$OWNER_DSN" ledger-api "$@" 2>/dev/null; }

# post KEY PATH JSON BODYFILE -> prints the HTTP status ("000" if the connection failed)
post() {
  curl -s -o "$4" -w '%{http_code}\n' -X POST "$API$2" -H "Authorization: Bearer $KEY" -H "Idempotency-Key: $1" \
    -H 'Content-Type: application/json' -d "$3"
}
transfer_json() { printf '{"from":"%s","to":"%s","amount_minor":%s,"currency":"EUR","reference":"%s"}' "$1" "$2" "$3" "$4"; }

# retry_transfer FROM TO AMOUNT REF: one transfer, retried with the same key until it gets a final answer. Prints
# the statuses seen along the way.
retry_transfer() {
  local key status seen="" i
  key=$(uuidgen)
  for i in $(seq 1 120); do
    status=$(post "$key" /v1/transfers "$(transfer_json "$1" "$2" "$3" "$4")" "$WORK/r-$key")
    seen="$seen $status"
    case $status in 201|422) echo "$seen"; return 0 ;; esac
    sleep 0.5
  done
  echo "$seen"
}

wait_published() { # wait until the outbox is drained (max 120 s)
  local i
  for i in $(seq 1 240); do
    [ "$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ] && return 0
    sleep 0.5
  done
  return 1
}
wait_notified() { # wait until every event has its notification (max 120 s)
  local i
  for i in $(seq 1 240); do
    [ "$(sql "SELECT count(*) FROM outbox o WHERE NOT EXISTS (SELECT 1 FROM notifier.notifications n WHERE n.event_id = o.id)")" = 0 ] && return 0
    sleep 0.5
  done
  return 1
}
metric() { curl -s "$1" | awk -v m="$2" '$1 ~ "^"m {s += $NF} END {print s + 0}'; }

say "Setting up two partners"
docker compose ps --status running --services | grep -q ledger-api || { echo "the stack isn't running: docker compose up --build -d"; exit 2; }
A=$(ledgerctl partner create --name "Demo Partner A" --funding-limit 100000000)
KEY=$(echo "$A" | jq -r .api_key); PARTNER=$(echo "$A" | jq -r .partner_id)
B=$(ledgerctl partner create --name "Demo Partner B" --funding-limit 100000000)
KEY_B=$(echo "$B" | jq -r .api_key)
sql "UPDATE partners SET rate_limit_per_min = 1000000 WHERE id = '$PARTNER'" >/dev/null
SETTLEMENT=$(sql "SELECT id FROM accounts WHERE partner_id = '$PARTNER' AND kind = 'settlement'")
ACCOUNTS=()
for i in $(seq 1 10); do
  post "$(uuidgen)" /v1/accounts '{"currency":"EUR"}' "$WORK/acct" >/dev/null
  ACCOUNTS+=("$(jq -r .id "$WORK/acct")")
  post "$(uuidgen)" /v1/transfers "$(transfer_json "$SETTLEMENT" "${ACCOUNTS[$((i-1))]}" 100000 fund)" "$WORK/f" >/dev/null
done
note "partner A $PARTNER with 10 funded accounts; partner B for the isolation check"

# 1. Retry storm --------------------------------------------------------------------------------------------------
say "1. Retry storm: 50 identical requests with the same key at once"
KEY1=$(uuidgen); REF1="storm-$KEY1"
BODY1=$(transfer_json "${ACCOUNTS[0]}" "${ACCOUNTS[1]}" 7 "$REF1")
for i in $(seq 1 50); do
  (post "$KEY1" /v1/transfers "$BODY1" "$WORK/storm-$i" > "$WORK/storm-$i.status") &
done
wait
FINAL1=$(post "$KEY1" /v1/transfers "$BODY1" "$WORK/storm-final")
CREATED=$(cat "$WORK"/storm-*.status | grep -c 201); INFLIGHT=$(cat "$WORK"/storm-*.status | grep -c 409)
DIFFERENT=0
for i in $(seq 1 50); do
  if [ "$(cat "$WORK/storm-$i.status")" = 201 ] && ! cmp -s "$WORK/storm-$i" "$WORK/storm-final"; then DIFFERENT=$((DIFFERENT+1)); fi
done
TX1=$(sql "SELECT count(*) FROM transactions WHERE reference = '$REF1'")
note "$CREATED x 201, $INFLIGHT x 409 in flight; final retry $FINAL1; transactions: $TX1"
[ "$TX1" = 1 ] && [ $((CREATED+INFLIGHT)) = 50 ] && [ "$DIFFERENT" = 0 ] && [ "$FINAL1" = 201 ] \
  && result 1 "retry storm" ok || result 1 "retry storm" no "$TX1 transactions, $DIFFERENT differing bodies"

# 2. Relay crash between commit and publish ------------------------------------------------------------------------
say "2. Crash between commit and publish: the relay is killed, transfers commit, the relay restarts"
docker compose kill -s SIGKILL relay >/dev/null 2>&1
BEFORE2=$(sql "SELECT count(*) FROM outbox")
for i in 1 2 3 4 5; do post "$(uuidgen)" /v1/transfers "$(transfer_json "${ACCOUNTS[2]}" "${ACCOUNTS[3]}" 1 relay-crash-$RUN)" "$WORK/t" >/dev/null; done
WAITING2=$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")
docker compose start relay >/dev/null 2>&1
wait_published && wait_notified
DUP2=$(sql "SELECT count(*) - count(DISTINCT event_id) FROM notifier.notifications")
NEW2=$(( $(sql "SELECT count(*) FROM outbox") - BEFORE2 ))
note "$NEW2 events committed while the relay was down ($WAITING2 unpublished); duplicates after restart: $DUP2"
[ "$WAITING2" -ge 10 ] && [ "$DUP2" = 0 ] && [ "$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ] \
  && result 2 "relay crash" ok || result 2 "relay crash" no "waiting $WAITING2, duplicates $DUP2"

# 3. Kafka down ----------------------------------------------------------------------------------------------------
say "3. Kafka down for ${KAFKA_DOWN}s: transfers keep succeeding, the outbox grows, then everything is delivered"
docker compose stop redpanda >/dev/null 2>&1
OK3=0
END3=$(( $(date +%s) + KAFKA_DOWN ))
while [ "$(date +%s)" -lt "$END3" ]; do
  [ "$(post "$(uuidgen)" /v1/transfers "$(transfer_json "${ACCOUNTS[4]}" "${ACCOUNTS[5]}" 1 kafka-down-$RUN)" "$WORK/t")" = 201 ] && OK3=$((OK3+1))
  sleep 1
done
GREW3=$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")
docker compose start redpanda >/dev/null 2>&1
wait_published && wait_notified
GAPS3=$(metric http://localhost:9092/metrics notifier_gaps_total); REG3=$(metric http://localhost:9092/metrics notifier_regressions_total)
note "$OK3 transfers succeeded with the broker down; outbox grew to $GREW3; after recovery gaps $GAPS3, regressions $REG3"
[ "$OK3" -gt 0 ] && [ "$GREW3" -gt 0 ] && [ "$GAPS3" = 0 ] && [ "$REG3" = 0 ] && [ "$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ] \
  && result 3 "Kafka down" ok || result 3 "Kafka down" no "ok $OK3, grew $GREW3, gaps $GAPS3"

# 4. Consumer redelivery ------------------------------------------------------------------------------------------
say "4. Consumer redelivery: the notifier is killed mid-stream and restarted"
for i in $(seq 1 200); do post "$(uuidgen)" /v1/transfers "$(transfer_json "${ACCOUNTS[6]}" "${ACCOUNTS[7]}" 1 redelivery-$RUN)" "$WORK/t" >/dev/null & done
sleep 0.5
docker compose kill -s SIGKILL notifier >/dev/null 2>&1
wait
docker compose start notifier >/dev/null 2>&1
wait_published && wait_notified
EVENTS4=$(sql "SELECT count(*) FROM outbox"); NOTES4=$(sql "SELECT count(*) FROM notifier.notifications")
note "$EVENTS4 events, $NOTES4 notifications (one each, however often an event was delivered)"
[ "$EVENTS4" = "$NOTES4" ] && result 4 "consumer redelivery" ok || result 4 "consumer redelivery" no "$EVENTS4 events vs $NOTES4 notifications"

# 5. Concurrency --------------------------------------------------------------------------------------------------
say "5. Concurrency: 1,000 random transfers among 10 accounts, 50 at a time"
TOTAL_BEFORE=$(sql "SELECT sum(posted_minor) FROM balances b JOIN accounts a ON a.id = b.account_id WHERE a.partner_id = '$PARTNER' AND a.kind = 'customer'")
for i in $(seq 1 1000); do
  f=$((RANDOM % 10)); t=$(( (f + 1 + RANDOM % 9) % 10 ))
  echo "${ACCOUNTS[$f]} ${ACCOUNTS[$t]} $((1 + RANDOM % 500))"
done > "$WORK/pairs"
export API KEY
xargs -P 50 -L 1 sh -c 'curl -s -o /dev/null -w "%{http_code}\n" -X POST "$API/v1/transfers" -H "Authorization: Bearer $KEY" \
  -H "Idempotency-Key: $(uuidgen)" -d "{\"from\":\"$0\",\"to\":\"$1\",\"amount_minor\":$2,\"currency\":\"EUR\"}"' \
  < "$WORK/pairs" > "$WORK/conc"
TOTAL_AFTER=$(sql "SELECT sum(posted_minor) FROM balances b JOIN accounts a ON a.id = b.account_id WHERE a.partner_id = '$PARTNER' AND a.kind = 'customer'")
NEG5=$(sql "SELECT count(*) FROM balances b JOIN accounts a ON a.id = b.account_id WHERE a.partner_id = '$PARTNER' AND a.kind = 'customer' AND b.posted_minor < 0")
OTHER5=$(grep -v -c -E '^(201|422)$' "$WORK/conc")
note "$(grep -c 201 "$WORK/conc") x 201, $(grep -c 422 "$WORK/conc") x 422 insufficient funds, $OTHER5 other; customer total $TOTAL_BEFORE -> $TOTAL_AFTER"
[ "$OTHER5" = 0 ] && [ "$TOTAL_BEFORE" = "$TOTAL_AFTER" ] && [ "$NEG5" = 0 ] \
  && result 5 "concurrency" ok || result 5 "concurrency" no "$OTHER5 unexpected answers, total $TOTAL_BEFORE -> $TOTAL_AFTER"

# 6. Postgres connection loss -------------------------------------------------------------------------------------
say "6. Postgres connection loss: ledger-api's connections are reset in bursts while transfers retry"
# 10 clients, each making 10 transfers in a row (so the transfers span the bursts), each retried with its key.
for c in $(seq 1 10); do
  (for i in $(seq 1 10); do retry_transfer "${ACCOUNTS[8]}" "${ACCOUNTS[9]}" 1 "conn-loss-$RUN"; sleep 0.2; done > "$WORK/cl-$c") &
done
for burst in 1 2 3 4; do
  sleep 0.5
  curl -s -X POST "$TOXI/proxies/postgres/toxics" -d '{"name":"reset","type":"reset_peer","stream":"downstream","attributes":{"timeout":0}}' >/dev/null
  sleep 0.3
  curl -s -X DELETE "$TOXI/proxies/postgres/toxics/reset" >/dev/null
done
wait
SEEN6=$(cat "$WORK"/cl-* | tr ' ' '\n' | grep -v '^$' | sort | uniq -c | awk '{printf "%s x %s, ", $1, $2}')
BAD6=$(cat "$WORK"/cl-* | tr ' ' '\n' | grep -v '^$' | grep -v -c -E '^(201|503|409|000)$')
TX6=$(sql "SELECT count(*) FROM transactions WHERE reference = 'conn-loss-$RUN'")
note "answers seen: ${SEEN6%, }; transactions: $TX6 (want 100: one per key)"
[ "$BAD6" = 0 ] && [ "$TX6" = 100 ] && result 6 "Postgres connection loss" ok || result 6 "Postgres connection loss" no "$BAD6 bad answers, $TX6 transactions"

# 7. Tenant isolation ---------------------------------------------------------------------------------------------
say "7. Tenant isolation: partner B's key against partner A's accounts"
S7A=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY_B" "$API/v1/accounts/${ACCOUNTS[0]}/balance")
S7B=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $KEY_B" -H "Idempotency-Key: $(uuidgen)" \
  -d "$(transfer_json "${ACCOUNTS[0]}" "${ACCOUNTS[1]}" 1 theft-$RUN)" "$API/v1/transfers")
note "read A's balance: $S7A; move A's money: $S7B"
[ "$S7A" = 404 ] && [ "$S7B" = 404 ] && [ "$(sql "SELECT count(*) FROM transactions WHERE reference = 'theft-$RUN'")" = 0 ] \
  && result 7 "tenant isolation" ok || result 7 "tenant isolation" no "answers $S7A, $S7B"

# 8. Commit outcome unknown ---------------------------------------------------------------------------------------
say "8. Commit outcome unknown: the connection drops after COMMIT, before its acknowledgement arrives"
# Postgres's replies to ledger-api are delayed by 700 ms; the moment the database shows the key completed (the commit
# happened), its acknowledgement is still inside Toxiproxy, and disabling the proxy drops it.
OK8=no
for attempt in 1 2 3; do
  KEY8=$(uuidgen); REF8="unknown-$KEY8"; BODY8=$(transfer_json "${ACCOUNTS[1]}" "${ACCOUNTS[2]}" 3 "$REF8")
  curl -s -X POST "$TOXI/proxies/postgres/toxics" -d '{"name":"lat","type":"latency","stream":"downstream","attributes":{"latency":700}}' >/dev/null
  (post "$KEY8" /v1/transfers "$BODY8" "$WORK/u8" > "$WORK/u8.status") &
  printf "SELECT 'state:' || coalesce((SELECT status FROM idempotency WHERE key = '%s'), 'none')\n\\\\watch i=0.05 c=300\n" "$KEY8" \
    | docker compose exec -T postgres psql -U owner -d ledger -tA 2>/dev/null \
    | while read -r line; do case $line in state:completed) curl -s -X POST "$TOXI/proxies/postgres" -d '{"enabled":false}' >/dev/null; break ;; esac; done
  wait
  curl -s -X DELETE "$TOXI/proxies/postgres/toxics/lat" >/dev/null
  curl -s -X POST "$TOXI/proxies/postgres" -d '{"enabled":true}' >/dev/null
  FIRST8=$(cat "$WORK/u8.status")
  sleep 1
  RETRY8=$(post "$KEY8" /v1/transfers "$BODY8" "$WORK/u8r")
  TX8=$(sql "SELECT count(*) FROM transactions WHERE reference = '$REF8'")
  note "attempt $attempt: first answer $FIRST8, retry $RETRY8, transactions $TX8"
  if [ "$FIRST8" = 503 ] && [ "$RETRY8" = 201 ] && [ "$TX8" = 1 ]; then OK8=ok; break; fi
  # Only a missed timing window is worth another attempt: the acknowledgement got through before the proxy was cut,
  # so the first answer was simply 201. Anything else (a 500, two transactions) is a real failure.
  [ "$FIRST8" = 201 ] && [ "$TX8" = 1 ] || break
done
[ "$OK8" = ok ] && result 8 "commit outcome unknown" ok || result 8 "commit outcome unknown" no "first $FIRST8, retry $RETRY8, $TX8 transactions"

# Reconciliation --------------------------------------------------------------------------------------------------
say "Reconciliation over the whole database"
wait_published && wait_notified
docker compose run --rm -T --no-deps --entrypoint /usr/local/bin/reconcile -e "DATABASE_URL=$OWNER_DSN" -e PUBLISHED_WITHIN=30s ledger-api 2>/dev/null \
  | sed 's/^/   /'
RECON=${PIPESTATUS[0]}

printf '\n== Summary\n'
for r in "${RESULTS[@]}"; do printf '   %s\n' "$r"; done
if [ "$RECON" = 0 ]; then printf '   PASS  reconciliation\n'; else printf '   FAIL  reconciliation\n'; FAILED=1; fi
exit $FAILED
