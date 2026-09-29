// Package cache wraps a store.Store with a Redis-backed, cache-aside layer for the
// two read-heavy lookups the API serves directly to callers: GetJob and GetRun. It is
// used only by the api service — worker's LeaseNextRun and scheduler's PromoteOnce
// deliberately go straight to Postgres, since job leasing and promotion need
// up-to-the-moment consistency that a cache would undermine.
//
// Two different caching strategies are used, matched to how each entity actually
// changes:
//
//   - Job: cached with a short TTL and invalidated on every status write
//     (UpdateJobStatus and CancelJob). A job's status can change at any time, so a
//     time-boxed cache with active invalidation is the only safe option — this is the
//     classic "get this wrong and you serve a paused job as active" bug class.
//   - JobRun: cached only after it reaches succeeded or canceled. Failed runs
//     can be retried, and dead runs can be manually requeued from the dead-letter
//     API, so neither is stable enough to cache. A final run has a longer TTL.
package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

const (
	jobTTL       = 30 * time.Second
	runTTL       = 1 * time.Hour
	jobKeyPrefix = "atlas:job:"
	runKeyPrefix = "atlas:run:"
)

// Store decorates a store.Store with cache-aside reads for GetJob/GetRun. All other
// methods (including writes other than UpdateJobStatus) pass straight through.
type Store struct {
	store.Store
	redis *redis.Client
	// group deduplicates concurrent cache misses for the same key into a single
	// underlying fetch — without this, N requests arriving during the same cold
	// window each miss the cache and each hit Postgres, the classic thundering-herd
	// bug on a popular job right after its cache entry expires.
	group singleflight.Group
}

// Wrap returns a Store that caches GetJob/GetRun reads in Redis at redisAddr.
func Wrap(underlying store.Store, redisAddr string) *Store {
	return &Store{
		Store: underlying,
		redis: redis.NewClient(&redis.Options{Addr: redisAddr}),
	}
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
	key := jobKeyPrefix + id

	if raw, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var job model.Job
		if jsonErr := json.Unmarshal(raw, &job); jsonErr == nil {
			metrics.CacheHits.WithLabelValues("job").Inc()
			return &job, nil
		}
	}
	metrics.CacheMisses.WithLabelValues("job").Inc()

	v, err, _ := s.group.Do(key, func() (any, error) {
		job, err := s.Store.GetJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if raw, jsonErr := json.Marshal(job); jsonErr == nil {
			s.redis.Set(ctx, key, raw, jobTTL)
		}
		return job, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*model.Job), nil
}

func (s *Store) UpdateJobStatus(ctx context.Context, id string, status model.JobStatus) error {
	if err := s.Store.UpdateJobStatus(ctx, id, status); err != nil {
		return err
	}
	// Invalidate rather than update-in-place: simpler to reason about, and the next
	// GetJob repopulates it from Postgres anyway. A stale cached job served between
	// this write and the invalidation call below is the real (small) risk window this
	// strategy accepts - see docs/ARCHITECTURE.md's caching section.
	s.redis.Del(ctx, jobKeyPrefix+id)
	return nil
}

// CancelJob changes both the job and any active runs. A cached job must be
// invalidated after the underlying transactional cancellation commits.
func (s *Store) CancelJob(ctx context.Context, id string) error {
	if err := s.Store.CancelJob(ctx, id); err != nil {
		return err
	}
	_ = s.redis.Del(ctx, jobKeyPrefix+id).Err()
	return nil
}

// RetryDeadLetter also clears a terminal run cached by an older API instance.
// Dead runs are no longer newly cached because operators can requeue them.
func (s *Store) RetryDeadLetter(ctx context.Context, id string) (*model.JobRun, error) {
	run, err := s.Store.RetryDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}
	_ = s.redis.Del(ctx, runKeyPrefix+run.ID).Err()
	return run, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (*model.JobRun, error) {
	key := runKeyPrefix + id

	if raw, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var run model.JobRun
		if jsonErr := json.Unmarshal(raw, &run); jsonErr == nil {
			metrics.CacheHits.WithLabelValues("run").Inc()
			return &run, nil
		}
	}
	metrics.CacheMisses.WithLabelValues("run").Inc()

	v, err, _ := s.group.Do(key, func() (any, error) {
		run, err := s.Store.GetRun(ctx, id)
		if err != nil {
			return nil, err
		}
		if run.Status == model.RunStatusSucceeded || run.Status == model.RunStatusCanceled {
			if raw, jsonErr := json.Marshal(run); jsonErr == nil {
				s.redis.Set(ctx, key, raw, runTTL)
			}
		}
		return run, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*model.JobRun), nil
}

func (s *Store) Close() {
	_ = s.redis.Close()
	s.Store.Close()
}
