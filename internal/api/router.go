// Package api implements atlas's HTTP surface: job/run/worker CRUD-ish endpoints
// behind JWT auth and a per-IP rate limiter, plus unauthenticated /healthz and /metrics.
package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/store"
)

// NewRouter wires the full HTTP API. jwtSecret is the shared HS256 secret used to
// verify Authorization: Bearer tokens on /v1/*; the optional limiter may enforce
// quotas across API replicas, while the local token bucket supports tests/dev.
func NewRouter(st store.Store, log *slog.Logger, jwtSecret string, rateRPS float64, rateBurst int, supplied ...RateLimiter) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", healthzHandler)
	r.Handle("/metrics", metrics.Handler())

	h := &handler{store: st, log: log}
	var limiter RateLimiter = newRateLimiter(rateRPS, rateBurst)
	if len(supplied) > 0 && supplied[0] != nil {
		limiter = supplied[0]
	}

	r.Route("/v1", func(r chi.Router) {
		r.Use(chimiddleware.Recoverer)
		r.Use(requestLogger(log))
		r.Use(limiter.Middleware)
		r.Use(jwtAuth(jwtSecret))

		r.Post("/jobs", h.createJob)
		r.Get("/jobs", h.listJobs)
		r.Get("/jobs/{id}", h.getJob)
		r.Post("/jobs/{id}/pause", h.pauseJob)
		r.Post("/jobs/{id}/resume", h.resumeJob)
		r.Post("/jobs/{id}/cancel", h.cancelJob)
		r.Get("/jobs/{id}/runs", h.listJobRuns)
		r.Get("/runs/{id}", h.getRun)
		r.Get("/workers", h.listWorkers)
		r.Get("/dead-letters", h.listDeadLetters)
		r.Post("/dead-letters/{id}/retry", h.retryDeadLetter)
		r.Delete("/dead-letters/{id}", h.deleteDeadLetter)
	})

	return r
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
