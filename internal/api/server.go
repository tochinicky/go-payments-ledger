// Package api is the ledger's HTTP API: JSON over net/http, with problem+json errors.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// maxBodyBytes caps a request body. Every request this API takes is a few hundred bytes.
const maxBodyBytes = 64 << 10

// DefaultMaxAmountMinor caps a single amount: 10 billion in minor units (100 million euros), far above any real
// transfer here, so a typo or an attack can't move absurd sums in one request.
const DefaultMaxAmountMinor = 10_000_000_000

// Config configures the API.
type Config struct {
	Store          *store.Store
	Log            *slog.Logger
	RequestTimeout time.Duration // every request's deadline (store.Timeouts.Request)
	MaxAmountMinor int64         // the largest amount_minor accepted (default DefaultMaxAmountMinor)
}

// Server serves the API from a store.
type Server struct {
	store          *store.Store
	log            *slog.Logger
	requestTimeout time.Duration
	maxAmount      int64
	limiters       *limiters
}

// New returns a server.
func New(cfg Config) *Server {
	if cfg.MaxAmountMinor == 0 {
		cfg.MaxAmountMinor = DefaultMaxAmountMinor
	}
	return &Server{
		store: cfg.Store, log: cfg.Log, requestTimeout: cfg.RequestTimeout, maxAmount: cfg.MaxAmountMinor, limiters: newLimiters(),
	}
}

// Handler returns the routes. Every request gets a request id and a deadline, then must authenticate (failures are
// audited); every write is audited with its outcome, including a refusal by the per-partner rate limit.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /v1/accounts":               s.openAccount,
		"GET /v1/accounts/{id}":           s.getAccount,
		"GET /v1/accounts/{id}/balance":   s.getBalance,
		"GET /v1/accounts/{id}/statement": s.getStatement,
		"POST /v1/transfers":              s.createTransfer,
		"POST /v1/holds":                  s.placeHold,
		"GET /v1/holds/{id}":              s.getHold,
		"POST /v1/holds/{id}/capture":     s.captureHold,
		"POST /v1/holds/{id}/release":     s.releaseHold,
	} {
		mux.Handle(pattern, route(pattern, h))
	}
	chain := withRequestID(s.withDeadline(s.authenticate(s.auditWrites(mux, s.rateLimit(mux)))))
	// Outermost, so the latency histogram (http.server.request.duration) and the trace span cover everything,
	// including authentication and rate limiting.
	return otelhttp.NewHandler(chain, "ledger-api")
}

// route labels a request's span and its latency metrics with the route pattern ("POST /v1/transfers"), not the
// path, so metrics stay one series per route. (otelhttp can't see the pattern itself: the mux sets it on the
// request copy it hands the handler.)
func route(pattern string, h http.Handler) http.Handler {
	attr := semconv.HTTPRoute(pattern)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if labeler, ok := otelhttp.LabelerFromContext(r.Context()); ok {
			labeler.Add(attr)
		}
		span := trace.SpanFromContext(r.Context())
		span.SetName(pattern)
		span.SetAttributes(attr)
		h.ServeHTTP(w, r)
	})
}

// withDeadline gives each request a context deadline. pgx cancels a query when its context ends, so a request stuck
// on the database is stopped, not merely abandoned (http.Server's WriteTimeout doesn't cancel the handler).
func (s *Server) withDeadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type partnerKey struct{}

// authenticate resolves "Authorization: Bearer <key>" to a partner, by the SHA-256 of the key. Keys are random and
// high-entropy, so a fast hash is enough (bcrypt exists to slow down guessing low-entropy passwords). The lookup is
// by hash in an index, so request timing can reveal nothing about the key itself. Every failure is audited, with
// at most a short fingerprint of the key's hash: never the key.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			s.auditFailure(r, "anonymous")
			writeProblem(w, "unauthenticated", "an API key is required: Authorization: Bearer <key>")
			return
		}
		hash := sha256.Sum256([]byte(key))
		p, found, err := s.store.PartnerByKeyHash(r.Context(), hash[:])
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		if !found {
			s.auditFailure(r, "key:"+hex.EncodeToString(hash[:4]))
			writeProblem(w, "unauthenticated", "unknown API key")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), partnerKey{}, p)))
	})
}

func partnerOf(r *http.Request) store.Partner {
	return r.Context().Value(partnerKey{}).(store.Partner) // set by authenticate on every route
}

// decode reads a JSON body strictly: one object, no unknown fields, at most maxBodyBytes. A typo in a field name
// is refused rather than silently ignored. It writes the error response itself and reports whether to go on.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeBody(w, r, dst, false)
}

// decodeOptional is decode for requests whose body may be empty (no fields to send), such as a release.
func decodeOptional(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeBody(w, r, dst, true)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any, emptyOK bool) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if emptyOK && errors.Is(err, io.EOF) {
		return true
	}
	if err == nil && !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
		err = errors.New("unexpected data after the JSON object")
	}
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooLarge):
		writeProblem(w, "body_too_large", fmt.Sprintf("the body is limited to %d bytes", maxBodyBytes))
	default:
		writeProblem(w, "malformed_json", err.Error())
	}
	return false
}

// pathID parses the {id} in a path. An id that isn't a UUID can't name anything, so it is simply not found.
func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeProblem(w, "not_found", "not found")
		return uuid.Nil, false
	}
	return id, true
}

// validAmount reports whether an amount_minor was given, is positive and is within the configured maximum.
func (s *Server) validAmount(a *int64) bool {
	return a != nil && *a > 0 && *a <= s.maxAmount
}

func (s *Server) amountError() fieldError {
	return fieldError{"amount_minor", fmt.Sprintf("a positive integer number of minor units, at most %d", s.maxAmount)}
}
