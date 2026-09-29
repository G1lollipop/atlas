package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter applies a request quota to an HTTP handler. Implementations may be
// process-local or shared across API replicas.
type RateLimiter interface {
	Middleware(next http.Handler) http.Handler
}

// bucket tracks one client's tokens plus when it was last topped up, so refill can be
// computed lazily from elapsed wall-clock time on each request rather than requiring a
// background goroutine/ticker per client (which wouldn't scale with client cardinality).
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// rateLimiter is retained for local development and unit tests. Production API
// instances use redisRateLimiter whenever REDIS_ADDR is configured.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rps     float64
	burst   int
}

func newRateLimiter(rps float64, burst int) *rateLimiter {
	return &rateLimiter{
		buckets: make(map[string]*bucket),
		rps:     rps,
		burst:   burst,
	}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(rl.burst), lastRefill: now}
		rl.buckets[key] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now

	b.tokens += elapsed * rl.rps
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return rl.Middleware(next)
}

// Middleware returns the local limiter middleware.
func (rl *rateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.allow(clientIP(r)) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

const redisBucketPrefix = "atlas:rate-limit:v1:"

// redisTokenBucketScript makes each token-bucket decision atomic across API
// processes. Redis TIME avoids skew between replica clocks; the single hash key
// keeps the script compatible with Redis Cluster's one-key EVAL requirement.
var redisTokenBucketScript = redis.NewScript(`
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local values = redis.call('HMGET', KEYS[1], 'tokens', 'last_ms')
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local tokens = tonumber(values[1])
local last = tonumber(values[2])

if tokens == nil or last == nil then
  tokens = burst
  last = now
end

local current = math.max(now, last)
local elapsed = current - last
tokens = math.min(burst, tokens + elapsed * rate / 1000)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_ms', current)
redis.call('PEXPIRE', KEYS[1], ttl)
return allowed
`)

type redisRateLimiter struct {
	client redis.UniversalClient
	rps    float64
	burst  int
	ttlMS  int64
}

// NewRedisRateLimiter returns a token bucket backed by Redis. All API replicas
// must use the same Redis deployment and settings to enforce one quota per client
// IP. Backend errors fail closed as HTTP 503; requests are never silently checked
// against a per-process fallback bucket.
func NewRedisRateLimiter(client redis.UniversalClient, rps float64, burst int) (RateLimiter, error) {
	if client == nil {
		return nil, fmt.Errorf("redis rate limiter requires a Redis client")
	}
	if math.IsNaN(rps) || math.IsInf(rps, 0) || rps <= 0 {
		return nil, fmt.Errorf("rate limit requests per second must be finite and greater than zero")
	}
	if burst <= 0 {
		return nil, fmt.Errorf("rate limit burst must be greater than zero")
	}

	// Retain an idle bucket for twice the full refill window. Once that window has
	// passed, the bucket would be full anyway, so expiration is behavior-preserving.
	ttl := math.Ceil(float64(burst) / rps * 2000)
	if ttl < 1000 {
		ttl = 1000
	}
	var ttlMS int64
	if ttl >= float64(math.MaxInt64) {
		ttlMS = math.MaxInt64
	} else {
		ttlMS = int64(ttl)
	}

	return &redisRateLimiter{client: client, rps: rps, burst: burst, ttlMS: ttlMS}, nil
}

func (rl *redisRateLimiter) allow(ctx context.Context, key string) (bool, error) {
	redisKey := redisBucketKey(key)
	result, err := redisTokenBucketScript.Run(ctx, rl.client, []string{redisKey}, rl.burst, rl.rps, rl.ttlMS).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func redisBucketKey(key string) string {
	keyHash := sha256.Sum256([]byte(key))
	return redisBucketPrefix + hex.EncodeToString(keyHash[:])
}

// Middleware returns distributed rate-limit middleware. A Redis failure returns
// 503 so the service does not accidentally grant a larger quota on an API replica.
func (rl *redisRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, err := rl.allow(r.Context(), clientIP(r))
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "rate limiter unavailable")
			return
		}
		if !allowed {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
