package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestRedisRateLimiterFailsClosed(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1,
	})
	t.Cleanup(func() { _ = client.Close() })

	limiter, err := NewRedisRateLimiter(client, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var nextCalls atomic.Int32
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.RemoteAddr = "198.51.100.22:4321"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if nextCalls.Load() != 0 {
		t.Fatalf("next handler called %d times after Redis error, want 0", nextCalls.Load())
	}
}

func TestRedisRateLimiterSharedAcrossInstances(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	firstClient := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 300 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := firstClient.Ping(ctx).Err()
	cancel()
	if err != nil {
		_ = firstClient.Close()
		t.Skipf("Redis is unavailable at %s: %v", addr, err)
	}
	secondClient := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 300 * time.Millisecond})
	t.Cleanup(func() {
		_ = firstClient.Close()
		_ = secondClient.Close()
	})

	const burst = 37
	// Two independent clients model two API processes. The very low refill rate
	// keeps the expected allowance stable for the duration of the concurrent check.
	first, err := NewRedisRateLimiter(firstClient, 0.0001, burst)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRedisRateLimiter(secondClient, 0.0001, burst)
	if err != nil {
		t.Fatal(err)
	}
	key := "integration-" + uuid.NewString()
	t.Cleanup(func() {
		_ = firstClient.Del(context.Background(), redisBucketKey(key)).Err()
	})

	const requestCount = burst * 2
	var allowed atomic.Int32
	var failures atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			limiter := first
			if i%2 != 0 {
				limiter = second
			}
			ok, err := limiter.(*redisRateLimiter).allow(context.Background(), key)
			if err != nil {
				t.Errorf("distributed allow: %v", err)
				failures.Add(1)
				return
			}
			if ok {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d Redis limiter calls failed", failures.Load())
	}
	if got := allowed.Load(); got != burst {
		t.Fatalf("combined allowed requests = %d, want shared burst %d", got, burst)
	}
	if allowed.Load() > burst {
		t.Fatalf("replicas granted more than the shared burst: %d > %d", allowed.Load(), burst)
	}
}
