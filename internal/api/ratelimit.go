package api

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// limiters holds one token bucket per partner, sized from the partner's rate_limit_per_min: tokens refill at
// limit/60 per second, and a burst of up to ten seconds' worth is allowed. It lives in this process, so with N
// replicas a partner gets up to N times its limit; an exact multi-replica limit needs a shared store (Redis, or a
// limiter in the gateway in front of the replicas).
type limiters struct {
	mu sync.Mutex
	m  map[uuid.UUID]*partnerLimiter
}

type partnerLimiter struct {
	perMin  int
	limiter *rate.Limiter
}

func newLimiters() *limiters { return &limiters{m: map[uuid.UUID]*partnerLimiter{}} }

// allow takes a token for the partner. If none is left it reports how long until one is.
func (l *limiters) allow(partner uuid.UUID, perMin int) (bool, time.Duration) {
	l.mu.Lock()
	pl, ok := l.m[partner]
	if !ok || pl.perMin != perMin { // a changed limit starts a fresh bucket
		pl = &partnerLimiter{perMin: perMin, limiter: rate.NewLimiter(rate.Limit(float64(perMin)/60), max(1, perMin/6))}
		l.m[partner] = pl
	}
	l.mu.Unlock()
	r := pl.limiter.Reserve()
	if d := r.Delay(); d > 0 {
		r.Cancel() // don't spend a future token on a request we refuse
		return false, d
	}
	return true, 0
}

// rateLimit refuses a partner's request with 429 rate_limited once its bucket is empty.
func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := partnerOf(r)
		if ok, wait := s.limiters.allow(p.ID, p.RateLimitPerMin); !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
			writeProblem(w, "rate_limited", "too many requests for this API key; retry later")
			return
		}
		next.ServeHTTP(w, r)
	})
}
