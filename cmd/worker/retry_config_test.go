package main

import (
	"testing"
	"time"
)

func clearRetryEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"WORKER_RETRY_BASE_DELAY",
		"WORKER_RETRY_MULTIPLIER",
		"WORKER_RETRY_MAX_DELAY",
		"WORKER_RETRY_JITTER",
		"WORKER_RETRY_MAX_ATTEMPTS",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadRetryPolicyDefaultsAndEnvironmentOverrides(t *testing.T) {
	clearRetryEnvironment(t)
	policy, err := loadRetryPolicy()
	if err != nil {
		t.Fatalf("load default retry policy: %v", err)
	}
	if policy.BaseDelay != time.Second || policy.Multiplier != 2 || policy.MaxDelay != 5*time.Minute || policy.Jitter != 0.2 || policy.MaxAttempts != 0 {
		t.Fatalf("default retry policy = %+v", policy)
	}

	t.Setenv("WORKER_RETRY_BASE_DELAY", "250ms")
	t.Setenv("WORKER_RETRY_MULTIPLIER", "1.5")
	t.Setenv("WORKER_RETRY_MAX_DELAY", "15s")
	t.Setenv("WORKER_RETRY_JITTER", "0.35")
	t.Setenv("WORKER_RETRY_MAX_ATTEMPTS", "4")
	policy, err = loadRetryPolicy()
	if err != nil {
		t.Fatalf("load configured retry policy: %v", err)
	}
	if policy.BaseDelay != 250*time.Millisecond || policy.Multiplier != 1.5 || policy.MaxDelay != 15*time.Second || policy.Jitter != 0.35 || policy.MaxAttempts != 4 {
		t.Fatalf("configured retry policy = %+v", policy)
	}
}

func TestLoadRetryPolicyRejectsInvalidValues(t *testing.T) {
	clearRetryEnvironment(t)
	t.Setenv("WORKER_RETRY_JITTER", "1.5")
	if _, err := loadRetryPolicy(); err == nil {
		t.Fatal("invalid jitter unexpectedly accepted")
	}
	clearRetryEnvironment(t)
	t.Setenv("WORKER_RETRY_MAX_ATTEMPTS", "40000")
	if _, err := loadRetryPolicy(); err == nil {
		t.Fatal("max attempts outside int16 range unexpectedly accepted")
	}
}
