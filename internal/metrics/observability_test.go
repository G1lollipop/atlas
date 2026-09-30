package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type observabilitySnapshotSourceFunc func(context.Context, time.Duration) (model.ObservabilitySnapshot, error)

func (f observabilitySnapshotSourceFunc) ObservabilitySnapshot(ctx context.Context, heartbeatTTL time.Duration) (model.ObservabilitySnapshot, error) {
	return f(ctx, heartbeatTTL)
}

func scrapeObservabilityCollector(t *testing.T, source ObservabilitySource, timeout time.Duration) string {
	t.Helper()
	registry := prometheus.NewRegistry()
	collector := NewObservabilityCollector(source, timeout, 30*time.Second)
	if err := registry.Register(collector); err != nil {
		t.Fatalf("register collector: %v", err)
	}
	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("scrape status = %d, want 200", response.Code)
	}
	return response.Body.String()
}

func hasMetric(output, name, value string, labels map[string]string) bool {
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, name+"{") || !strings.HasSuffix(line, " "+value) {
			continue
		}
		matched := true
		for label, labelValue := range labels {
			if !strings.Contains(line, label+`="`+labelValue+`"`) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func TestObservabilityCollectorExportsQueueAndWorkerSnapshots(t *testing.T) {
	output := scrapeObservabilityCollector(t, observabilitySnapshotSourceFunc(func(_ context.Context, _ time.Duration) (model.ObservabilitySnapshot, error) {
		return model.ObservabilitySnapshot{
			QueueDepths: []model.QueueDepthSnapshot{
				{Queue: "image", ResourceClass: "cpu", Depth: 0},
				{Queue: "image", ResourceClass: "gpu", Depth: 3},
			},
			Workers: []model.WorkerReservationSnapshot{{
				WorkerID: "worker-a", Alive: true,
				CPUCapacityMillis: 2000, CPUReservedMillis: 500,
				MemoryCapacityMB: 8000, MemoryReservedMB: 2000,
				GPUCapacity: 2, GPUReservedCount: 1,
			}},
		}, nil
	}), time.Second)

	for _, expected := range []string{
		"atlas_observability_up 1",
		`atlas_job_queue_depth{queue="image",resource_class="cpu"} 0`,
		`atlas_job_queue_depth{queue="image",resource_class="gpu"} 3`,
		`atlas_worker_alive{worker_id="worker-a"} 1`,
		`atlas_worker_cpu_reserved{worker_id="worker-a"} 500`,
		`atlas_worker_memory_reserved{worker_id="worker-a"} 2000`,
		`atlas_worker_gpu_reserved{worker_id="worker-a"} 1`,
	} {
		if !strings.Contains(output, expected) {
			t.Errorf("scrape is missing %q:\n%s", expected, output)
		}
	}
	if !hasMetric(output, "atlas_worker_utilization", "0.25", map[string]string{"worker_id": "worker-a", "resource": "cpu"}) {
		t.Errorf("scrape is missing CPU reservation utilization:\n%s", output)
	}
	if !hasMetric(output, "atlas_worker_utilization", "0.5", map[string]string{"worker_id": "worker-a", "resource": "gpu"}) {
		t.Errorf("scrape is missing GPU reservation utilization:\n%s", output)
	}
}

func TestObservabilityCollectorOmitsSnapshotSeriesOnFailure(t *testing.T) {
	output := scrapeObservabilityCollector(t, observabilitySnapshotSourceFunc(func(_ context.Context, _ time.Duration) (model.ObservabilitySnapshot, error) {
		return model.ObservabilitySnapshot{QueueDepths: []model.QueueDepthSnapshot{{Queue: "image", ResourceClass: "cpu", Depth: 0}}}, errors.New("database unavailable")
	}), time.Second)

	if !strings.Contains(output, "atlas_observability_up 0") {
		t.Fatalf("failed scrape did not expose observability_up=0:\n%s", output)
	}
	if strings.Contains(output, "atlas_job_queue_depth{") || strings.Contains(output, "atlas_worker_alive{") {
		t.Fatalf("failed scrape exposed stale snapshot series:\n%s", output)
	}
}

func TestObservabilityCollectorAppliesSnapshotTimeout(t *testing.T) {
	output := scrapeObservabilityCollector(t, observabilitySnapshotSourceFunc(func(ctx context.Context, _ time.Duration) (model.ObservabilitySnapshot, error) {
		<-ctx.Done()
		return model.ObservabilitySnapshot{}, ctx.Err()
	}), 10*time.Millisecond)

	if !strings.Contains(output, "atlas_observability_up 0") {
		t.Fatalf("timed out scrape did not expose observability_up=0:\n%s", output)
	}
	if strings.Contains(output, "atlas_job_queue_depth{") {
		t.Fatalf("timed out scrape exposed queue depth:\n%s", output)
	}
}

func TestQueueWaitDurationExcludesRetryBackoff(t *testing.T) {
	startedAt := time.Unix(100, 0)
	createdAt := startedAt.Add(-10 * time.Second)
	scheduledAt := startedAt.Add(-2 * time.Second)

	if got := QueueWaitDurationSeconds(createdAt, scheduledAt, startedAt, true); got != 10 {
		t.Fatalf("one-shot wait = %v, want acceptance-to-start 10 seconds", got)
	}
	if got := QueueWaitDurationSeconds(createdAt, scheduledAt, startedAt, false); got != 2 {
		t.Fatalf("run wait = %v, want due-to-start 2 seconds", got)
	}
	if got := QueueWaitDurationSeconds(createdAt, startedAt.Add(time.Second), startedAt, false); got != 0 {
		t.Fatalf("future retry wait = %v, want clamped zero", got)
	}
}

func TestSchedulingMetricNamesAndQueueWaitLabels(t *testing.T) {
	JobQueueWaitSeconds.WithLabelValues("cpu").Observe(0.25)
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather default metrics: %v", err)
	}
	want := map[string]bool{
		"atlas_jobs_submitted_total":       false,
		"atlas_jobs_completed_total":       false,
		"atlas_jobs_failed_total":          false,
		"atlas_job_queue_wait_seconds":     false,
		"atlas_job_execution_seconds":      false,
		"atlas_scheduler_decision_seconds": false,
		"atlas_retry_total":                false,
		"atlas_dead_letter_total":          false,
		"atlas_lease_expired_total":        false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
		if family.GetName() == "atlas_job_queue_wait_seconds" {
			if len(family.Metric) == 0 || len(family.Metric[0].Label) != 1 || family.Metric[0].Label[0].GetName() != "resource_class" {
				t.Errorf("queue wait labels = %v, want only resource_class", family.Metric)
			}
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %q was not registered", name)
		}
	}
}
