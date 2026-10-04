package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// We audit decisions per request and abuse in aggregate. A write that ran, a replay, a validation error: one audit
// row each, bounded by the rate limiter and backed by real work. A failed authentication or a rate-limited request
// is the protection doing its job, and a row per request would turn a flood into database writes (unauthenticated
// ones, for bad keys). Those are counted in memory per (actor, outcome) and flushed as one row per minute with a
// count and a time span, plus a metric and a log line sampled at most once a second per actor.

var (
	apiMeter            = otel.Meter("github.com/tochinicky/go-payments-ledger/internal/api")
	authFailuresMetric  = must(apiMeter.Int64Counter("ledger.auth_failures", metric.WithDescription("Requests refused for a missing or unknown API key.")))
	rateLimitedMetric   = must(apiMeter.Int64Counter("ledger.rate_limited", metric.WithDescription("Requests refused by a partner's rate limit.")))
	authThrottledMetric = must(apiMeter.Int64Counter("ledger.auth_throttled", metric.WithDescription("Requests refused before any lookup: too many failed authentications from one address.")))
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type abuseKey struct {
	actor   string
	action  string // "auth_failure", "auth_throttled" or "rate_limited"
	partner uuid.UUID
}

type abuseCount struct {
	count       int
	first, last time.Time
	lastLogged  time.Time
}

// abuse aggregates refusals between flushes.
type abuse struct {
	mu     sync.Mutex
	counts map[abuseKey]*abuseCount
	log    *slog.Logger
}

func newAbuse(log *slog.Logger) *abuse { return &abuse{counts: map[abuseKey]*abuseCount{}, log: log} }

func (a *abuse) record(ctx context.Context, k abuseKey) {
	now := time.Now()
	a.mu.Lock()
	c, ok := a.counts[k]
	if !ok {
		c = &abuseCount{first: now}
		a.counts[k] = c
	}
	c.count++
	c.last = now
	logNow := now.Sub(c.lastLogged) >= time.Second
	if logNow {
		c.lastLogged = now
	}
	a.mu.Unlock()
	if logNow {
		a.log.WarnContext(ctx, "request refused", slog.String("outcome", k.action), slog.String("actor", k.actor))
	}
}

// drain takes everything counted so far, as audit rows.
func (a *abuse) drain() []store.AuditAggregate {
	a.mu.Lock()
	counts := a.counts
	a.counts = map[abuseKey]*abuseCount{}
	a.mu.Unlock()
	rows := make([]store.AuditAggregate, 0, len(counts))
	for k, c := range counts {
		e := store.AuditAggregate{Count: c.count, FirstAt: c.first, LastAt: c.last}
		e.Actor, e.Action, e.Resource, e.RequestID = k.actor, k.action, "aggregate", "aggregate"
		e.Status = http.StatusUnauthorized
		if k.action == "auth_throttled" {
			e.Status = http.StatusTooManyRequests
		}
		if k.action == "rate_limited" {
			e.Status = http.StatusTooManyRequests
			p := k.partner
			e.PartnerID = &p
		}
		rows = append(rows, e)
	}
	return rows
}

// FlushAudit writes the refusals counted since the last flush as aggregated audit rows. ledger-api calls it every
// minute and once more on shutdown.
func (s *Server) FlushAudit(ctx context.Context) error {
	rows := s.abuse.drain()
	if len(rows) == 0 {
		return nil
	}
	return s.store.AuditAggregates(ctx, rows)
}

// RunAuditFlush flushes every interval until ctx ends, then once more.
func (s *Server) RunAuditFlush(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := s.FlushAudit(final); err != nil {
				s.log.ErrorContext(final, "audit flush failed", slog.Any("error", err))
			}
			return
		case <-ticker.C:
			if err := s.FlushAudit(ctx); err != nil {
				s.log.ErrorContext(ctx, "audit flush failed", slog.Any("error", err))
			}
		}
	}
}

// failedAuth throttles authentication failures per client address, before any database access: each failure
// takes a token (10 per second, a burst of 20); with none left, the address gets 429 at once, without a lookup.
// Successful requests take nothing, so a busy partner is never slowed by this. The map is reset when it grows
// large, so a spoofed-address flood can't exhaust memory. Behind a proxy, the address must come from the proxy's
// verified header instead of RemoteAddr.
type failedAuth struct {
	mu sync.Mutex
	m  map[string]*rate.Limiter
}

func newFailedAuth() *failedAuth { return &failedAuth{m: map[string]*rate.Limiter{}} }

func (f *failedAuth) limiter(r *http.Request) *rate.Limiter {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.m[ip]
	if !ok {
		if len(f.m) >= 100_000 {
			f.m = map[string]*rate.Limiter{}
		}
		l = rate.NewLimiter(10, 20)
		f.m[ip] = l
	}
	return l
}

// blocked reports whether the address has used up its failed authentications.
func (f *failedAuth) blocked(r *http.Request) bool { return f.limiter(r).Tokens() < 1 }

// failed takes a token for a failed authentication.
func (f *failedAuth) failed(r *http.Request) { f.limiter(r).Allow() }

func (s *Server) authFailure(r *http.Request, actor string) {
	s.failedAuth.failed(r)
	authFailuresMetric.Add(r.Context(), 1)
	s.abuse.record(r.Context(), abuseKey{actor: actor, action: "auth_failure"})
}

func (s *Server) authThrottled(r *http.Request) {
	authThrottledMetric.Add(r.Context(), 1)
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	s.abuse.record(r.Context(), abuseKey{actor: "address:" + ip, action: "auth_throttled"})
}

func (s *Server) rateLimited(r *http.Request, partner uuid.UUID) {
	rateLimitedMetric.Add(r.Context(), 1, metric.WithAttributes(attribute.String("partner", partner.String())))
	s.abuse.record(r.Context(), abuseKey{actor: "partner:" + partner.String(), action: "rate_limited", partner: partner})
}
