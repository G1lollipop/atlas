package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/G1lollipop/atlas/internal/lock"
	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

var tracer = otel.Tracer("atlas/scheduler")

// activeJobsPageSize bounds each keyset page without capping the number of active
// jobs visited in a promotion cycle.
const activeJobsPageSize = 1000

// Promoter is the leader-only loop that turns due, dependency-satisfied jobs into
// queued job_runs and dispatches scheduled runs to compatible workers. Only one
// replica's Promoter should be actively promoting at a time; Elector enforces that.
type Promoter struct {
	Store    store.Store
	Elector  lock.LeaderElector
	Logger   *slog.Logger
	Interval time.Duration
	Policy   SchedulingPolicy
}

// NewPromoter constructs a Promoter ready to Run.
func NewPromoter(st store.Store, el lock.LeaderElector, log *slog.Logger, interval time.Duration) *Promoter {
	return &Promoter{
		Store:    st,
		Elector:  el,
		Logger:   log,
		Interval: interval,
	}
}

// PromoteOnce scans active jobs and creates a queued run for each one that is due and whose
// dependencies are satisfied. It is safe to call directly (e.g. from tests or an
// admin "promote now" endpoint) without going through Run/Elector.
func (p *Promoter) PromoteOnce(ctx context.Context) (int, error) {
	ctx, span := tracer.Start(ctx, "scheduler.PromoteOnce")
	defer span.End()

	promoted := 0
	now := time.Now()
	if pager, ok := p.Store.(store.ActiveJobPager); ok {
		afterID := ""
		for {
			if err := ctx.Err(); err != nil {
				return promoted, err
			}
			jobs, err := pager.ListActiveJobsAfter(ctx, afterID, activeJobsPageSize)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return promoted, err
			}
			if len(jobs) == 0 {
				break
			}
			stop := false
			for _, job := range jobs {
				created, hitGlobalLimit := p.promoteJob(ctx, job, now, true)
				if created {
					promoted++
				}
				if hitGlobalLimit {
					stop = true
					break
				}
			}
			afterID = jobs[len(jobs)-1].ID
			if stop || len(jobs) < activeJobsPageSize {
				break
			}
		}
	} else {
		// Keep compatibility with Store implementations that predate keyset paging.
		// Production PostgresStore implements ActiveJobPager and avoids OFFSET scans.
		activeStatus := model.JobStatusActive
		for offset := 0; ; {
			jobs, err := p.Store.ListJobs(ctx, &activeStatus, activeJobsPageSize, offset)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return promoted, err
			}
			stop := false
			for _, job := range jobs {
				created, hitGlobalLimit := p.promoteJob(ctx, job, now, false)
				if created {
					promoted++
				}
				if hitGlobalLimit {
					stop = true
					break
				}
			}
			offset += len(jobs)
			if stop || len(jobs) < activeJobsPageSize {
				break
			}
		}
	}

	if depth, err := p.Store.CountPendingRuns(ctx); err != nil {
		p.Logger.Warn("count pending runs failed", "error", err)
	} else {
		metrics.QueueDepth.Set(float64(depth))
	}

	span.SetAttributes(attribute.Int("promoted.count", promoted))
	return promoted, nil
}

// promoteJob checks a single job and creates its next run when due. eligibleOnly
// means the keyset query has already excluded active runs and completed one-shots.
func (p *Promoter) promoteJob(ctx context.Context, job *model.Job, now time.Time, eligibleOnly bool) (created, hitGlobalLimit bool) {
	if job == nil {
		return false, false
	}
	if !eligibleOnly {
		hasActive, err := p.Store.HasActiveRun(ctx, job.ID)
		if err != nil {
			p.Logger.Error("check active run failed", "job_id", job.ID, "error", err)
			return false, false
		}
		if hasActive {
			return false, false
		}
	}

	var lastScheduledAt *time.Time
	if !eligibleOnly || job.CronExpr != nil {
		lastRun, err := p.Store.LatestRunForJob(ctx, job.ID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			lastScheduledAt = nil
		case err != nil:
			p.Logger.Error("latest run lookup failed", "job_id", job.ID, "error", err)
			return false, false
		default:
			lastScheduledAt = &lastRun.ScheduledAt
		}
	}

	due, err := NextRunDue(job.CronExpr, lastScheduledAt, job.CreatedAt, now)
	if err != nil {
		p.Logger.Error("next run due check failed", "job_id", job.ID, "error", err)
		return false, false
	}
	if !due {
		return false, false
	}

	satisfied, err := DependenciesSatisfied(ctx, p.Store, job.ID)
	if err != nil {
		p.Logger.Error("dependency check failed", "job_id", job.ID, "error", err)
		return false, false
	}
	if !satisfied {
		return false, false
	}

	if _, err := p.Store.CreateRun(ctx, job.ID, job.Priority, now); err != nil {
		if errors.Is(err, store.ErrRunAlreadyExists) {
			return false, false
		}
		if errors.Is(err, store.ErrQueueCapacityExceeded) {
			var capacityErr *store.QueueCapacityError
			p.Logger.Warn("job promotion deferred by queue backpressure", "job_id", job.ID, "error", err)
			return false, errors.As(err, &capacityErr) && capacityErr.Dimension == "global backlog"
		}
		p.Logger.Error("create run failed", "job_id", job.ID, "error", err)
		return false, false
	}

	p.Logger.Info("promoted job", "job_id", job.ID, "name", job.Name)
	return true, false
}

// DispatchOnce recovers expired ownership, schedules queued runs, and assigns work
// to compatible workers. Run invokes it only while this replica holds leadership.
func (p *Promoter) DispatchOnce(ctx context.Context) (int, error) {
	return NewDispatcherWithPolicy(p.Store, p.Logger, p.Policy).DispatchOnce(ctx)
}

// Run drives promotion and assignment until ctx is cancelled. Leadership is
// re-checked on every tick because leadership can change hands at any time, for
// example if this replica's advisory-lock connection drops.
func (p *Promoter) Run(ctx context.Context) error {
	if p.Elector == nil {
		return errors.New("scheduler: promoter elector is nil")
	}
	interval := p.Interval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	campaignCtx, cancelCampaign := context.WithCancel(ctx)
	defer cancelCampaign()
	campaignDone := make(chan error, 1)
	startCampaign := func() {
		go func() { campaignDone <- p.Elector.Campaign(campaignCtx) }()
	}
	startCampaign()

	for {
		select {
		case <-ctx.Done():
			cancelCampaign()
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer releaseCancel()
			if err := p.Elector.Release(releaseCtx); err != nil {
				p.Logger.Warn("release leadership failed", "error", err)
			}
			return ctx.Err()

		case err := <-campaignDone:
			if ctx.Err() != nil {
				continue
			}
			metrics.IsLeader.Set(0)
			if err != nil {
				p.Logger.Error("leader campaign stopped", "error", err)
			} else {
				p.Logger.Warn("leader campaign stopped unexpectedly")
			}
			retry := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				retry.Stop()
			case <-retry.C:
				startCampaign()
			}

		case <-ticker.C:
			if p.Elector.IsLeader() {
				metrics.IsLeader.Set(1)
				decisionStartedAt := time.Now()
				if _, err := p.PromoteOnce(ctx); err != nil {
					p.Logger.Error("promote once failed", "error", err)
				}
				if _, err := p.DispatchOnce(ctx); err != nil {
					p.Logger.Error("dispatch once failed", "error", err)
				}
				metrics.SchedulerDecisionSeconds.Observe(time.Since(decisionStartedAt).Seconds())
			} else {
				metrics.IsLeader.Set(0)
			}
		}
	}
}
