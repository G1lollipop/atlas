package worker

import (
	"fmt"
	"strings"
	"sync"

	"github.com/G1lollipop/atlas/internal/model"
)

// resourceAdmission mirrors a worker's advertised capacity inside one process.
// The database remains authoritative across assignments and worker processes;
// these counters only prevent this process from running jobs beyond its own
// configured CPU, memory, or accelerator capacity.
type resourceAdmission struct {
	mu sync.Mutex

	cpuCapacityMillis int64
	memoryCapacityMB  int64
	gpuCapacity       int64
	gpuMemoryPerGPU   int64
	gpuMemoryCapacity int64
	gpuType           string

	cpuReservedMillis int64
	memoryReservedMB  int64
	gpuReserved       int64
	gpuMemoryReserved int64
}

func newResourceAdmission(capabilities model.Worker) *resourceAdmission {
	a := &resourceAdmission{}
	a.setCapacity(capabilities)
	return a
}

func (a *resourceAdmission) setCapacity(capabilities model.Worker) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cpuCapacityMillis = int64(capabilities.CPUCapacity)
	a.memoryCapacityMB = int64(capabilities.MemoryCapacityMB)
	a.gpuCapacity = int64(capabilities.GPUCount)
	a.gpuMemoryPerGPU = int64(capabilities.GPUMemoryMB)
	a.gpuMemoryCapacity = int64(capabilities.GPUCount) * int64(capabilities.GPUMemoryMB)
	a.gpuType = capabilities.GPUType
}

// tryAcquire is non-blocking so a leased run never waits locally with its DB
// reservation held. A rejected run is returned to the store by Pool.executeOne.
func (a *resourceAdmission) tryAcquire(job *model.Job) (func(), error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	if job.RequiredCPUMillis < 0 || job.RequiredMemoryMB < 0 ||
		job.RequiredGPUCount < 0 || job.RequiredGPUMemoryMB < 0 {
		return nil, fmt.Errorf("job resource requirements must not be negative")
	}
	if job.RequiredGPUCount == 0 && (job.RequiredGPUMemoryMB != 0 || job.RequiredAccelerator != "") {
		return nil, fmt.Errorf("GPU memory and accelerator requirements need at least one GPU")
	}

	requestCPU := int64(job.RequiredCPUMillis)
	requestMemory := int64(job.RequiredMemoryMB)
	requestGPU := int64(job.RequiredGPUCount)
	requestGPUMemory := requestGPU * int64(job.RequiredGPUMemoryMB)

	a.mu.Lock()
	defer a.mu.Unlock()

	availableCPU := remaining(a.cpuCapacityMillis, a.cpuReservedMillis)
	if requestCPU > availableCPU {
		return nil, fmt.Errorf(
			"CPU request %d millicores exceeds %d available", requestCPU, availableCPU,
		)
	}
	availableMemory := remaining(a.memoryCapacityMB, a.memoryReservedMB)
	if requestMemory > availableMemory {
		return nil, fmt.Errorf(
			"memory request %d MB exceeds %d MB available", requestMemory, availableMemory,
		)
	}
	availableGPU := remaining(a.gpuCapacity, a.gpuReserved)
	if requestGPU > availableGPU {
		return nil, fmt.Errorf("GPU request %d exceeds %d available", requestGPU, availableGPU)
	}
	if requestGPU > 0 && int64(job.RequiredGPUMemoryMB) > a.gpuMemoryPerGPU {
		return nil, fmt.Errorf(
			"GPU memory request %d MB per GPU exceeds %d MB per GPU",
			job.RequiredGPUMemoryMB, a.gpuMemoryPerGPU,
		)
	}
	availableGPUMemory := remaining(a.gpuMemoryCapacity, a.gpuMemoryReserved)
	if requestGPUMemory > availableGPUMemory {
		return nil, fmt.Errorf(
			"aggregate GPU memory request %d MB exceeds %d MB available",
			requestGPUMemory, availableGPUMemory,
		)
	}
	if job.RequiredAccelerator != "" && !strings.EqualFold(
		strings.TrimSpace(job.RequiredAccelerator), strings.TrimSpace(a.gpuType),
	) {
		return nil, fmt.Errorf(
			"accelerator %q does not match worker accelerator %q", job.RequiredAccelerator, a.gpuType,
		)
	}

	a.cpuReservedMillis += requestCPU
	a.memoryReservedMB += requestMemory
	a.gpuReserved += requestGPU
	a.gpuMemoryReserved += requestGPUMemory

	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.cpuReservedMillis -= requestCPU
			a.memoryReservedMB -= requestMemory
			a.gpuReserved -= requestGPU
			a.gpuMemoryReserved -= requestGPUMemory
		})
	}, nil
}

func remaining(capacity, reserved int64) int64 {
	if reserved >= capacity {
		return 0
	}
	return capacity - reserved
}
