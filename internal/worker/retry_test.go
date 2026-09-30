package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

func TestRetryPolicyDelayUsesExponentialCapAndDeterministicJitter(t *testing.T) {
	policy := RetryPolicy{
		BaseDelay:  2 * time.Second,
		Multiplier: 3,
		MaxDelay:   10 * time.Second,
		Jitter:     0.25,
	}
	for _, test := range []struct {
		attempt int16
		sample  float64
		want    time.Duration
	}{
		{attempt: 1, sample: 0.5, want: 2*time.Second + 250*time.Millisecond},
		{attempt: 2, sample: 0, want: 6 * time.Second},
		{attempt: 3, sample: 0.5, want: 11*time.Second + 250*time.Millisecond},
		{attempt: 10, sample: 0, want: 10 * time.Second},
	} {
		t.Run(time.Duration(test.attempt).String(), func(t *testing.T) {
			got, err := policy.Delay(test.attempt, func() float64 { return test.sample })
			if err != nil {
				t.Fatalf("Delay(%d) error = %v", test.attempt, err)
			}
			if got != test.want {
				t.Fatalf("Delay(%d) = %s, want %s", test.attempt, got, test.want)
			}
		})
	}
}

func TestRetryPolicyDelaySaturatesWithoutDurationOverflow(t *testing.T) {
	policy := RetryPolicy{
		BaseDelay:  time.Duration(math.MaxInt64 / 4),
		Multiplier: math.MaxFloat64,
		MaxDelay:   time.Duration(math.MaxInt64),
		Jitter:     1,
	}
	got, err := policy.Delay(3, func() float64 { return 0.999999 })
	if err != nil {
		t.Fatalf("Delay() error = %v", err)
	}
	if got <= 0 {
		t.Fatalf("Delay() = %s, want a positive saturated duration", got)
	}
	if got > time.Duration(math.MaxInt64) {
		t.Fatalf("Delay() = %s, exceeds max time.Duration", got)
	}
}

func TestRetryPolicyRejectsInvalidConfigurationAndRandomSamples(t *testing.T) {
	valid := DefaultRetryPolicy()
	invalid := []RetryPolicy{
		{BaseDelay: 0, Multiplier: 2, MaxDelay: time.Second},
		{BaseDelay: time.Second, Multiplier: 2, MaxDelay: time.Millisecond},
		{BaseDelay: time.Second, Multiplier: math.Inf(1), MaxDelay: time.Minute},
		{BaseDelay: time.Second, Multiplier: 2, MaxDelay: time.Minute, Jitter: -0.1},
		{BaseDelay: time.Second, Multiplier: 2, MaxDelay: time.Minute, Jitter: 1.1},
		{BaseDelay: time.Second, Multiplier: 2, MaxDelay: time.Minute, MaxAttempts: -1},
	}
	for i, policy := range invalid {
		if err := policy.Validate(); err == nil {
			t.Errorf("invalid policy %d unexpectedly validated", i)
		}
	}
	if _, err := valid.Delay(0, nil); err == nil {
		t.Fatal("Delay(0) unexpectedly succeeded")
	}
	if _, err := valid.Delay(1, math.NaN); err == nil {
		t.Fatal("NaN random sample unexpectedly succeeded")
	}
	if _, err := valid.Delay(1, func() float64 { return 1 }); err == nil {
		t.Fatal("out-of-range random sample unexpectedly succeeded")
	}
}

func TestEffectiveMaxAttemptsUsesJobLimitAndOptionalWorkerCeiling(t *testing.T) {
	for _, test := range []struct {
		job, policy, want int16
	}{
		{job: 5, policy: 0, want: 5},
		{job: 2, policy: 5, want: 2},
		{job: 5, policy: 3, want: 3},
		{job: 0, policy: 4, want: 1},
	} {
		if got := effectiveMaxAttempts(test.job, test.policy); got != test.want {
			t.Errorf("effectiveMaxAttempts(%d, %d) = %d, want %d", test.job, test.policy, got, test.want)
		}
	}
}

func TestPoolUsesRetryPolicyForHandlerAndAdmissionFailures(t *testing.T) {
	policy := RetryPolicy{BaseDelay: 50 * time.Millisecond, Multiplier: 2, MaxDelay: time.Second, Jitter: 0.5}
	for _, test := range []struct {
		name      string
		admission bool
	}{
		{name: "handler failure"},
		{name: "resource admission rejection", admission: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := newFakeStore()
			p := NewPool(fs, "test-worker", 1, 0, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err := p.SetRetryPolicy(policy); err != nil {
				t.Fatalf("SetRetryPolicy() error = %v", err)
			}
			p.SetRetryRandomSource(func() float64 { return 0.5 })
			p.RegisterHandler("job", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
				return nil, errors.New("retry me")
			})
			job := &model.Job{ID: "job-1", Name: "job", MaxAttempts: 3, TimeoutSeconds: 2}
			if test.admission {
				job.RequiredCPUMillis = 1
			}

			p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, job)

			if len(fs.failRunCalls) != 1 || !fs.failRunCalls[0].requeue {
				t.Fatalf("FailRun calls = %#v, want one scheduled retry", fs.failRunCalls)
			}
			if got, want := fs.failRunCalls[0].backoff, 62*time.Millisecond+500*time.Microsecond; got != want {
				t.Fatalf("retry backoff = %s, want %s", got, want)
			}
		})
	}
}

func TestPoolRetryPolicyMaxAttemptsCapsJobLimit(t *testing.T) {
	fs := newFakeStore()
	p := NewPool(fs, "test-worker", 1, 0, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.SetRetryPolicy(RetryPolicy{
		BaseDelay: time.Millisecond, Multiplier: 2, MaxDelay: time.Second, MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("SetRetryPolicy() error = %v", err)
	}
	p.RegisterHandler("job", func(context.Context, *model.Job, *model.JobRun) (map[string]any, error) {
		return nil, errors.New("terminal failure")
	})
	p.executeOne(context.Background(), &model.JobRun{ID: "run-1", Attempt: 1}, &model.Job{
		ID: "job-1", Name: "job", MaxAttempts: 5, TimeoutSeconds: 2,
	})
	if len(fs.failRunCalls) != 1 || fs.failRunCalls[0].requeue {
		t.Fatalf("FailRun calls = %#v, want terminal failure at policy cap", fs.failRunCalls)
	}
	if len(fs.markDeadCalls) != 1 {
		t.Fatalf("MarkDead calls = %d, want 1 at policy cap", len(fs.markDeadCalls))
	}
}
