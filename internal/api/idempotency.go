package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// work is a write's business logic, run inside the transaction that also completes its idempotency key. It returns
// the success status and body; a business refusal is returned as an error and stored like a success.
type work func(ctx context.Context, tx store.Tx) (status int, body any, err error)

// idempotent runs a write under its Idempotency-Key (the header is required on every write):
//   - a new key: run the work and store its response in the same transaction as the money movement;
//   - the same key and request after completion: replay the stored response, byte for byte;
//   - the same key, a different request: 422 idempotency_key_reused;
//   - the same key while the first attempt is still running: 409 idempotency_in_progress, with Retry-After.
//
// It must be called after authentication and validation: a 400 or 401 stores nothing, so a corrected request can
// reuse its key. request is the validated request, re-encoded to hash it: whitespace, field order and spelling
// variants like "EUR" in different JSON layouts don't make a retry look like a different request.
func (s *Server) idempotent(w http.ResponseWriter, r *http.Request, request any, run work) {
	key := r.Header.Get("Idempotency-Key")
	switch {
	case key == "":
		writeProblem(w, "idempotency_key_required", "writes need an Idempotency-Key header")
		return
	case utf8.RuneCountInString(key) > 255:
		writeProblem(w, "validation_failed", "the request has invalid fields", fieldError{"Idempotency-Key", "at most 255 characters"})
		return
	}
	partner := partnerOf(r)
	claim, err := s.store.ClaimKey(r.Context(), partner.ID, key, requestHash(r, request))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	switch claim.Result {
	case store.Replay:
		w.Header().Set("Idempotent-Replayed", "true")
		writeBody(w, claim.Response.Status, claim.Response.Body)
		return
	case store.InProgress:
		writeProblem(w, "idempotency_in_progress", "a request with this Idempotency-Key is still being processed")
		return
	case store.KeyReused:
		writeProblem(w, "idempotency_key_reused", "this Idempotency-Key was used for a different request")
		return
	}

	resp, err := s.store.RunWithLease(r.Context(), claim.Lease, func(tx store.Tx) (store.Response, error) {
		resp, err := response(run(r.Context(), tx))
		if err != nil {
			return store.Response{}, err
		}
		// The audit row commits with the write (or its stored refusal), in the same transaction.
		if err := tx.Audit(r.Context(), auditEntry(r, resp.Status)); err != nil {
			return store.Response{}, err
		}
		return resp, nil
	})
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		writeProblem(w, "idempotency_in_progress", "a request with this Idempotency-Key is still being processed")
	case err != nil:
		s.release(r, claim.Lease)
		s.writeError(w, r, err)
	default:
		markAudited(r)
		writeBody(w, resp.Status, resp.Body)
	}
}

// response renders a write's outcome for storing: its success body, or a business refusal as problem+json.
// Any other error isn't a final outcome and is passed on.
func response(status int, body any, err error) (store.Response, error) {
	if err != nil {
		if status, problemBody, ok := businessError(err); ok {
			return store.Response{Status: status, Body: problemBody}, nil
		}
		return store.Response{}, err
	}
	return store.Response{Status: status, Body: mustJSON(body)}, nil
}

// release frees a failed attempt's key so a retry can run, on a fresh context: the request's own may be what
// failed (a client disconnect, a timeout). If it fails too, the lease expires on its own.
func (s *Server) release(r *http.Request, lease store.Lease) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	if err := s.store.ReleaseKey(ctx, lease); err != nil {
		s.log.WarnContext(ctx, "could not free idempotency key; its lease will expire", slog.Any("error", err))
	}
}

// requestHash is SHA-256 over the method and route, the actual path (its parameters), and the canonical JSON of
// the validated request.
func requestHash(r *http.Request, request any) []byte {
	h := sha256.New()
	h.Write([]byte(r.Pattern + "\n" + r.URL.EscapedPath() + "\n"))
	h.Write(mustJSON(request))
	return h.Sum(nil)
}
