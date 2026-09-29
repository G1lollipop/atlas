package worker

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy controls bounded exponential retry delays. Jitter is a fraction
// of the capped exponential delay, sampled uniformly from [0, delay*jitter].
// MaxAttempts is an optional worker-wide ceiling; a job's own MaxAttempts can
// make the ceiling stricter but never looser. Zero leaves each job's value in
// control.
type RetryPolicy struct {
	BaseDelay   time.Duration
	Multiplier  float64
	MaxDelay    time.Duration
	Jitter      float64
	MaxAttempts int16
}

// DefaultRetryPolicy keeps the initial retry responsive while spreading later
// retries across a small window to avoid synchronized retry bursts.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseDelay:  time.Second,
		Multiplier: 2,
		MaxDelay:   5 * time.Minute,
		Jitter:     0.2,
	}
}

func (p RetryPolicy) Validate() error {
	switch {
	case p.BaseDelay <= 0:
		return fmt.Errorf("retry base delay must be positive")
	case p.MaxDelay < p.BaseDelay:
		return fmt.Errorf("retry max delay must be at least the base delay")
	case math.IsNaN(p.Multiplier) || math.IsInf(p.Multiplier, 0) || p.Multiplier < 1:
		return fmt.Errorf("retry multiplier must be a finite number greater than or equal to 1")
	case math.IsNaN(p.Jitter) || math.IsInf(p.Jitter, 0) || p.Jitter < 0 || p.Jitter > 1:
		return fmt.Errorf("retry jitter must be between 0 and 1")
	case p.MaxAttempts < 0:
		return fmt.Errorf("retry max attempts must be zero or greater")
	}
	return nil
}

// Delay returns the backoff after the given failed attempt. Attempt 1 uses the
// base delay. The exponential part is capped before positive jitter is added;
// the final sum saturates at the largest duration representable by time.Duration.
// Pass randomFloat64 to make jitter deterministic in tests; nil uses the
// concurrency-safe package random source.
func (p RetryPolicy) Delay(attempt int16, randomFloat64 func() float64) (time.Duration, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if attempt < 1 {
		return 0, fmt.Errorf("retry attempt must be positive, got %d", attempt)
	}

	delay := float64(p.BaseDelay)
	maxDelay := float64(p.MaxDelay)
	for n := int16(1); n < attempt && delay < maxDelay; n++ {
		if delay >= maxDelay/p.Multiplier {
			delay = maxDelay
			break
		}
		delay *= p.Multiplier
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	base := durationFromFloat(delay)
	if base < p.BaseDelay {
		base = p.BaseDelay
	}
	if base > p.MaxDelay {
		base = p.MaxDelay
	}
	if p.Jitter == 0 {
		return base, nil
	}
	if randomFloat64 == nil {
		randomFloat64 = rand.Float64
	}
	sample := randomFloat64()
	if math.IsNaN(sample) || sample < 0 || sample >= 1 {
		return 0, fmt.Errorf("retry random sample must be in [0, 1), got %v", sample)
	}
	extraLimit := durationFromFloat(float64(base) * p.Jitter)
	extra := durationFromFloat(sample * float64(extraLimit))
	if extra > 0 && base > time.Duration(math.MaxInt64)-extra {
		return time.Duration(math.MaxInt64), nil
	}
	return base + extra, nil
}

func durationFromFloat(value float64) time.Duration {
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(value)
}

// effectiveMaxAttempts applies the optional worker-wide ceiling while keeping
// the persisted job limit authoritative whenever it is stricter.
func effectiveMaxAttempts(jobMax, policyMax int16) int16 {
	if jobMax < 1 {
		jobMax = 1
	}
	if policyMax > 0 && policyMax < jobMax {
		return policyMax
	}
	return jobMax
}
