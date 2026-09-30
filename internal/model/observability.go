package model

// ObservabilitySnapshot is the database-backed view used to publish scheduler
// queue and worker reservation gauges. It contains no run or tenant identifiers.
type ObservabilitySnapshot struct {
	QueueDepths []QueueDepthSnapshot
	Workers     []WorkerReservationSnapshot
}

// QueueDepthSnapshot is the due, unpromoted, and assigned backlog for one queue
// and resource class. Queue is intentionally the only potentially variable label.
type QueueDepthSnapshot struct {
	Queue         string
	ResourceClass string
	Depth         int64
}

// WorkerReservationSnapshot records current reserved capacity alongside the
// worker's advertised capacity. Reservations are zeroed when the heartbeat is stale.
type WorkerReservationSnapshot struct {
	WorkerID          string
	Alive             bool
	CPUCapacityMillis int64
	MemoryCapacityMB  int64
	GPUCapacity       int64
	CPUReservedMillis int64
	MemoryReservedMB  int64
	GPUReservedCount  int64
}
