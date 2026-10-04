#!/usr/bin/env bash
# Creates the load test's data against the Compose stack: 2 partners, 50 funded customer accounts each, rate limits
# lifted for the test. Writes load/data.json (API keys included: it is ignored by git and only for this local stack).
set -euo pipefail
cd "$(dirname "$0")/.."
OWNER_DSN='postgres://owner:owner@postgres:5432/ledger?sslmode=disable'
API=http://localhost:8080
sql() { docker compose exec -T postgres psql -U owner -d ledger -tAc "$1"; }
partners="[]"
for p in 1 2; do
  created=$(docker compose run --rm -T --no-deps --entrypoint /usr/local/bin/ledgerctl -e "DATABASE_URL=$OWNER_DSN" ledger-api \
    partner create --name "Load Partner $p" --funding-limit 100000000000 2>/dev/null)
  key=$(echo "$created" | jq -r .api_key); id=$(echo "$created" | jq -r .partner_id)
  sql "UPDATE partners SET rate_limit_per_min = 10000000 WHERE id = '$id'" >/dev/null
  settlement=$(sql "SELECT id FROM accounts WHERE partner_id = '$id' AND kind = 'settlement'")
  accounts="[]"
  for i in $(seq 1 50); do
    acct=$(curl -s -X POST "$API/v1/accounts" -H "Authorization: Bearer $key" -H "Idempotency-Key: $(uuidgen)" -d '{"currency":"EUR"}' | jq -r .id)
    curl -s -o /dev/null -X POST "$API/v1/transfers" -H "Authorization: Bearer $key" -H "Idempotency-Key: $(uuidgen)" \
      -d "{\"from\":\"$settlement\",\"to\":\"$acct\",\"amount_minor\":1000000000,\"currency\":\"EUR\"}"
    accounts=$(echo "$accounts" | jq --arg a "$acct" '. + [$a]')
  done
  partners=$(echo "$partners" | jq --arg k "$key" --argjson a "$accounts" '. + [{"key": $k, "accounts": $a}]')
done
echo "$partners" > load/data.json
echo "load/data.json: 2 partners, $(echo "$partners" | jq '[.[].accounts | length] | add') funded accounts"
