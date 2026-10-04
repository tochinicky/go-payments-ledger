// Package api is the ledger's HTTP API: JSON over net/http, with problem+json errors.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/tochinicky/go-payments-ledger/internal/store"
)

// maxBodyBytes caps a request body. Every request this API takes is a few hundred bytes.
const maxBodyBytes = 64 << 10

// Server serves the API from a store.
type Server struct {
	store *store.Store
	log   *slog.Logger
}

// New returns a server.
func New(st *store.Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log}
}

// Handler returns the routes, every one behind authentication.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/accounts", s.openAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/balance", s.getBalance)
	mux.HandleFunc("GET /v1/accounts/{id}/statement", s.getStatement)
	mux.HandleFunc("POST /v1/transfers", s.createTransfer)
	return s.authenticate(mux)
}

type partnerKey struct{}

// authenticate resolves "Authorization: Bearer <key>" to a partner, by the SHA-256 of the key. Keys are random and
// high-entropy, so a fast hash is enough (bcrypt exists to slow down guessing low-entropy passwords). The lookup is
// by hash in an index, so request timing can reveal nothing about the key itself.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
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
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
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
		writeProblem(w, "not_found", "no such account")
		return uuid.Nil, false
	}
	return id, true
}
