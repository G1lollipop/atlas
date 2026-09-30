package metrics

import (
	"context"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

// ObservabilitySource is deliberately local to the metrics package so this
// scrape-only capability does not expand the Store interface used by other
// services and test fakes.
type ObservabilitySource interface {
	ObservabilitySnapshot(ctx context.Context, heartbeatTTL time.Duration) (model.ObservabilitySnapshot, error)
}

// ObservabilityCollector refreshes scheduler queue and worker gauges directly
// from Postgres on each scrape. A failed or timed-out snapshot reports
// atlas_observability_up=0 and leaves all snapshot series out of that scrape.
type ObservabilityCollector struct {
	source       ObservabilitySource
	timeout      time.Duration
	heartbeatTTL time.Duration

	observabilityUpDesc   *prometheus.Desc
	queueDepthDesc        *prometheus.Desc
	workerAliveDesc       *prometheus.Desc
	workerUtilizationDesc *prometheus.Desc
	workerCPUReservedDesc *prometheus.Desc
	workerMemoryDesc      *prometheus.Desc
	workerGPUDesc         *prometheus.Desc
}

// NewObservabilityCollector creates a fresh database-backed collector. The
// snapshot timeout defaults to three seconds; heartbeatTTL must match the
// scheduler's worker freshness rule.
func NewObservabilityCollector(source ObservabilitySource, timeout, heartbeatTTL time.Duration) *ObservabilityCollector {
	if timeout <= 0 {
		timeout = DefaultObservabilityTimeout
	}
	if heartbeatTTL <= 0 {
		heartbeatTTL = 30 * time.Second
	}
	return &ObservabilityCollector{
		source:       source,
		timeout:      timeout,
		heartbeatTTL: heartbeatTTL,
		observabilityUpDesc: prometheus.NewDesc(
			"atlas_observability_up", "1 if the most recent scheduler database snapshot succeeded.", nil, nil,
		),
		queueDepthDesc: prometheus.NewDesc(
			"atlas_job_queue_depth", "Accepted, due, and assigned work waiting for execution slots.",
			[]string{"queue", "resource_class"}, nil,
		),
		workerAliveDesc: prometheus.NewDesc(
			"atlas_worker_alive", "1 if the worker heartbeat is current and its stored status is alive or draining.",
			[]string{"worker_id"}, nil,
		),
		workerUtilizationDesc: prometheus.NewDesc(
			"atlas_worker_utilization", "Reserved worker capacity divided by advertised capacity; this is not hardware usage.",
			[]string{"worker_id", "resource"}, nil,
		),
		workerCPUReservedDesc: prometheus.NewDesc(
			"atlas_worker_cpu_reserved", "CPU capacity reserved by active assignments and leases, in millicores.",
			[]string{"worker_id"}, nil,
		),
		workerMemoryDesc: prometheus.NewDesc(
			"atlas_worker_memory_reserved", "Memory reserved by active assignments and leases, in megabytes.",
			[]string{"worker_id"}, nil,
		),
		workerGPUDesc: prometheus.NewDesc(
			"atlas_worker_gpu_reserved", "GPU count reserved by active assignments and leases.",
			[]string{"worker_id"}, nil,
		),
	}
}

func (c *ObservabilityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.observabilityUpDesc
	ch <- c.queueDepthDesc
	ch <- c.workerAliveDesc
	ch <- c.workerUtilizationDesc
	ch <- c.workerCPUReservedDesc
	ch <- c.workerMemoryDesc
	ch <- c.workerGPUDesc
}

func (c *ObservabilityCollector) Collect(ch chan<- prometheus.Metric) {
	if c.source == nil {
		ch <- prometheus.MustNewConstMetric(c.observabilityUpDesc, prometheus.GaugeValue, 0)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	snapshot, err := c.source.ObservabilitySnapshot(ctx, c.heartbeatTTL)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.observabilityUpDesc, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.observabilityUpDesc, prometheus.GaugeValue, 1)
	for _, depth := range snapshot.QueueDepths {
		ch <- prometheus.MustNewConstMetric(c.queueDepthDesc, prometheus.GaugeValue, float64(depth.Depth),
			depth.Queue, depth.ResourceClass)
	}
	for _, worker := range snapshot.Workers {
		alive := 0.0
		if worker.Alive {
			alive = 1
		}
		ch <- prometheus.MustNewConstMetric(c.workerAliveDesc, prometheus.GaugeValue, alive, worker.WorkerID)
		ch <- prometheus.MustNewConstMetric(c.workerUtilizationDesc, prometheus.GaugeValue,
			utilization(worker.CPUReservedMillis, worker.CPUCapacityMillis), worker.WorkerID, "cpu")
		ch <- prometheus.MustNewConstMetric(c.workerUtilizationDesc, prometheus.GaugeValue,
			utilization(worker.MemoryReservedMB, worker.MemoryCapacityMB), worker.WorkerID, "memory")
		ch <- prometheus.MustNewConstMetric(c.workerUtilizationDesc, prometheus.GaugeValue,
			utilization(worker.GPUReservedCount, worker.GPUCapacity), worker.WorkerID, "gpu")
		ch <- prometheus.MustNewConstMetric(c.workerCPUReservedDesc, prometheus.GaugeValue,
			float64(worker.CPUReservedMillis), worker.WorkerID)
		ch <- prometheus.MustNewConstMetric(c.workerMemoryDesc, prometheus.GaugeValue,
			float64(worker.MemoryReservedMB), worker.WorkerID)
		ch <- prometheus.MustNewConstMetric(c.workerGPUDesc, prometheus.GaugeValue,
			float64(worker.GPUReservedCount), worker.WorkerID)
	}
}

func utilization(reserved, capacity int64) float64 {
	if capacity <= 0 {
		return 0
	}
	return float64(reserved) / float64(capacity)
}
