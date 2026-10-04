#!/usr/bin/env bash
# Runs the load test against the Compose stack and reports the SLO from both sides: k6 (client-side, with its
# thresholds) and ledger-api's own histogram (server-side, the 0.1 s bucket). Takes a 30 s CPU profile mid-run,
# then runs the reconciliation. RATE (default 200/s) and HOLD (default 5m) shape the run.
#
#   LEDGER_DB_HOST=postgres:5432 docker compose up -d --build   # the load test bypasses Toxiproxy
#   load/setup.sh && load/run.sh
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=load/results; mkdir -p "$OUT"
RATE=${RATE:-200}; HOLD=${HOLD:-5m}
bucket() { # bucket LE: cumulative count of 201 transfers at or under LE seconds, as ledger-api counts them
  curl -s localhost:9090/metrics | grep '^http_server_request_duration_seconds_bucket' | grep 'http_route="POST /v1/transfers"' \
    | grep 'status_code="201"' | grep "le=\"$1\"" | awk '{s += $NF} END {print s + 0}'
}
under_before=$(bucket 0.1); all_before=$(bucket +Inf)

( sleep 90; curl -s -o "$OUT/cpu.pprof" "localhost:9090/debug/pprof/profile?seconds=30" ) &
docker run --rm --network go-payments-ledger_default -v "$PWD/load:/load" grafana/k6:2.3.0 run \
  -e API=http://ledger-api:8080 -e RATE="$RATE" -e HOLD="$HOLD" --summary-export=/load/results/summary.json /load/transfers.js \
  > "$OUT/k6.txt" 2>&1
K6=$?
wait

under=$(( $(bucket 0.1) - under_before )); all=$(( $(bucket +Inf) - all_before ))
echo "k6 (client side):"
grep -E "✓|✗|http_req_duration\{name:transfer\}|http_reqs\.|iterations\.|errors\.|dropped" "$OUT/k6.txt" | sed 's/^/  /'
echo "ledger-api (server side): $under of $all successful transfers within 100 ms ($(awk -v u="$under" -v a="$all" 'BEGIN {printf "%.2f", 100*u/a}')%)"
echo "CPU profile: $OUT/cpu.pprof (go tool pprof -top $OUT/cpu.pprof)"
OWNER_DSN='postgres://owner:owner@postgres:5432/ledger?sslmode=disable'
docker compose run --rm -T --no-deps --entrypoint /usr/local/bin/reconcile -e "DATABASE_URL=$OWNER_DSN" -e PUBLISHED_WITHIN=60s ledger-api 2>/dev/null | tail -1
exit $K6
