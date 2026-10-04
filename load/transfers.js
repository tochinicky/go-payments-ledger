// The load test: an open model (constant arrival rate), so a slow server can't lower the request rate and hide its
// latency (coordinated omission). 80% transfers, 20% hold place-then-capture, across 2 partners and 100 funded
// accounts, ramped to 200 requests/s and held for 5 minutes. 5% of writes are retried with the same key: half after
// the original answered (the replay must be byte-identical), half concurrently (a replay or 409, never a second
// transaction; the reconciliation afterwards proves it).
//
//   load/setup.sh && docker run --rm --network go-payments-ledger_default -v "$PWD/load:/load" \
//     grafana/k6:2.3.0 run -e API=http://ledger-api:8080 /load/transfers.js
import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const data = JSON.parse(open('./data.json'));
const API = __ENV.API || 'http://ledger-api:8080';
const RATE = parseInt(__ENV.RATE || '200', 10);
const HOLD = __ENV.HOLD || '5m';

export const options = {
  scenarios: {
    ledger: {
      executor: 'ramping-arrival-rate',
      startRate: 20,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 400,
      stages: [
        { target: RATE, duration: '30s' },
        { target: RATE, duration: HOLD },
      ],
    },
  },
  thresholds: {
    'http_req_duration{name:transfer}': ['p(99)<100'],   // the SLO: 99% of transfers under 100 ms
    errors: ['rate<0.001'],                              // under 0.1%, insufficient_funds excluded
    replay_mismatch: ['count==0'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const errors = new Rate('errors');
const replayMismatch = new Counter('replay_mismatch');
const concurrentRetries = new Counter('concurrent_retries');
const holdCapture = new Trend('hold_capture_duration', true);

function headers(key, idem) {
  return { headers: { Authorization: `Bearer ${key}`, 'Idempotency-Key': idem, 'Content-Type': 'application/json' } };
}

// ok is a final, expected answer: success, or a business refusal that isn't an error (insufficient funds).
function ok(res, success) {
  return res.status === success || (res.status === 422 && res.body.includes('insufficient_funds'));
}

function write(name, key, path, body, success) {
  const idem = `${__VU}-${__ITER}-${name}-${Math.random()}`;
  const params = Object.assign(headers(key, idem), { tags: { name } });
  const roll = Math.random();
  if (roll < 0.025) {
    // A concurrent retry: the same request twice at once. Each must be the stored answer or 409 in progress.
    const [a, b] = http.batch([
      ['POST', `${API}${path}`, body, params],
      ['POST', `${API}${path}`, body, Object.assign(headers(key, idem), { tags: { name: `${name}_retry` } })],
    ]);
    concurrentRetries.add(1);
    for (const r of [a, b]) {
      errors.add(!(ok(r, success) || r.status === 409));
    }
    return a.status === 409 ? b : a;
  }
  const res = http.post(`${API}${path}`, body, params);
  errors.add(!ok(res, success));
  if (roll < 0.05) {
    // A retry after completion: must replay the stored answer, byte for byte.
    const again = http.post(`${API}${path}`, body, Object.assign(headers(key, idem), { tags: { name: `${name}_retry` } }));
    if (again.status !== res.status || again.body !== res.body || again.headers['Idempotent-Replayed'] !== 'true') {
      replayMismatch.add(1);
    }
  }
  return res;
}

export default function () {
  const partner = data[randomIntBetween(0, data.length - 1)];
  const accounts = partner.accounts;
  const from = accounts[randomIntBetween(0, accounts.length - 1)];
  let to = accounts[randomIntBetween(0, accounts.length - 1)];
  while (to === from) to = accounts[randomIntBetween(0, accounts.length - 1)];
  const amount = randomIntBetween(1, 5000);

  if (Math.random() < 0.8) {
    const res = write('transfer', partner.key, '/v1/transfers',
      JSON.stringify({ from, to, amount_minor: amount, currency: 'EUR', reference: 'load' }), 201);
    check(res, { 'transfer answered': (r) => r.status === 201 || r.status === 422 || r.status === 409 });
    return;
  }
  const started = Date.now();
  const placed = write('hold_place', partner.key, '/v1/holds',
    JSON.stringify({ account: from, to_account: to, amount_minor: amount, currency: 'EUR', expires_in: 600 }), 201);
  if (placed.status !== 201) return;
  const hold = JSON.parse(placed.body).id;
  write('hold_capture', partner.key, `/v1/holds/${hold}/capture`,
    JSON.stringify({ amount_minor: randomIntBetween(1, amount) }), 200);
  holdCapture.add(Date.now() - started);
}
