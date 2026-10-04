package api

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

type requestIDKey struct{}

// validRequestID is what a client may send as X-Request-Id: short and plain, so it can't smuggle anything into
// logs or the audit table.
var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// withRequestID gives every request an id, the client's X-Request-Id if it's plain enough, or a new one, and
// echoes it in the response. It ties a client's report to the logs and the audit log.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if !validRequestID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

func requestID(r *http.Request) string {
	id, _ := r.Context().Value(requestIDKey{}).(string)
	return id
}

// auditState lets a write's own transaction record its audit row; the middleware then records nothing more.
type auditState struct{ done bool }

type auditStateKey struct{}

// statusRecorder remembers the status a handler answered with.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// auditWrites records every write attempt with its outcome. A write that ran is audited inside its own
// transaction (see idempotent), so the money and its audit row commit together; anything else (a validation
// error, a replay, a 409 in progress, a failure) gets its row here, after the answer.
// It wraps the mux itself, to resolve a request's route pattern (the mux sets r.Pattern only on the request it hands
// the handler).
func (s *Server) auditWrites(routes *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			routes.ServeHTTP(w, r)
			return
		}
		state := &auditState{}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		routes.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), auditStateKey{}, state)))
		if !state.done {
			_, pattern := routes.Handler(r)
			e := auditEntry(r, rec.status)
			e.Action = pattern
			s.auditAfter(r, e)
		}
	})
}

// auditEntry is the audit row for a write that ran in a transaction; markAudited tells the middleware it's done.
func auditEntry(r *http.Request, status int) store.AuditEntry {
	p := partnerOf(r)
	return store.AuditEntry{
		PartnerID: &p.ID, Actor: "partner:" + p.ID.String(), Action: r.Pattern, Resource: r.URL.Path,
		Status: status, RequestID: requestID(r),
	}
}

func markAudited(r *http.Request) {
	if state, ok := r.Context().Value(auditStateKey{}).(*auditState); ok {
		state.done = true
	}
}

// auditAfter writes an audit row outside any ledger transaction, on a fresh context (the request's may be done).
// A failure is logged: the answer has already been decided.
func (s *Server) auditAfter(r *http.Request, e store.AuditEntry) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	if err := s.store.Audit(ctx, e); err != nil {
		s.log.ErrorContext(ctx, "audit write failed", slog.String("request_id", e.RequestID), slog.Any("error", err))
	}
}
