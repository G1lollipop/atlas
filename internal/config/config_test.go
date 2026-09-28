package config

import "testing"

func setWorkerEnv(t *testing.T, cpu, memory, gpuCount, gpuType, gpuMemory, labels string) {
	t.Helper()
	t.Setenv("WORKER_CPU_CAPACITY_MILLIS", cpu)
	t.Setenv("WORKER_MEMORY_CAPACITY_MB", memory)
	t.Setenv("WORKER_GPU_COUNT", gpuCount)
	t.Setenv("WORKER_GPU_TYPE", gpuType)
	t.Setenv("WORKER_GPU_MEMORY_MB", gpuMemory)
	t.Setenv("WORKER_LABELS", labels)
}

func TestLoadWorkerCapabilityDefaults(t *testing.T) {
	setWorkerEnv(t, "", "", "", "", "", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerCPUCapacityMillis != 4000 || cfg.WorkerMemoryCapacityMB != 8192 {
		t.Fatalf("default CPU/memory capacity = %d/%d, want 4000/8192", cfg.WorkerCPUCapacityMillis, cfg.WorkerMemoryCapacityMB)
	}
	if cfg.WorkerGPUCount != 0 || cfg.WorkerGPUType != "" || cfg.WorkerGPUMemoryMB != 0 {
		t.Fatalf("default GPU capability = %d/%q/%d, want 0/empty/0", cfg.WorkerGPUCount, cfg.WorkerGPUType, cfg.WorkerGPUMemoryMB)
	}
	if len(cfg.WorkerLabels) != 0 {
		t.Fatalf("default worker labels = %#v, want empty", cfg.WorkerLabels)
	}
}

func TestLoadWorkerCapabilitiesFromEnvironment(t *testing.T) {
	setWorkerEnv(t, "8000", "32768", "2", "H100", "81920", `{"region":"us-central","pool":"gpu"}`)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerCPUCapacityMillis != 8000 || cfg.WorkerMemoryCapacityMB != 32768 {
		t.Errorf("CPU/memory capacity = %d/%d, want 8000/32768", cfg.WorkerCPUCapacityMillis, cfg.WorkerMemoryCapacityMB)
	}
	if cfg.WorkerGPUCount != 2 || cfg.WorkerGPUType != "H100" || cfg.WorkerGPUMemoryMB != 81920 {
		t.Errorf("GPU capability = %d/%q/%d, want 2/H100/81920", cfg.WorkerGPUCount, cfg.WorkerGPUType, cfg.WorkerGPUMemoryMB)
	}
	if cfg.WorkerLabels["region"] != "us-central" || cfg.WorkerLabels["pool"] != "gpu" {
		t.Errorf("worker labels = %#v, want both configured labels", cfg.WorkerLabels)
	}
}

func TestLoadRejectsInvalidWorkerCapabilities(t *testing.T) {
	tests := []struct {
		name                     string
		cpu, memory, gpuCount    string
		gpuType, gpuMemory, tags string
	}{
		{name: "negative CPU", cpu: "-1", memory: "8192", gpuCount: "0", gpuMemory: "0", tags: "{}"},
		{name: "negative GPU memory", cpu: "4000", memory: "8192", gpuCount: "1", gpuType: "A10", gpuMemory: "-1", tags: "{}"},
		{name: "GPU needs type", cpu: "4000", memory: "8192", gpuCount: "1", gpuMemory: "16384", tags: "{}"},
		{name: "GPU needs memory", cpu: "4000", memory: "8192", gpuCount: "1", gpuType: "A10", gpuMemory: "0", tags: "{}"},
		{name: "type without GPU", cpu: "4000", memory: "8192", gpuCount: "0", gpuType: "A10", gpuMemory: "0", tags: "{}"},
		{name: "invalid labels", cpu: "4000", memory: "8192", gpuCount: "0", gpuMemory: "0", tags: `{"region":4}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setWorkerEnv(t, tt.cpu, tt.memory, tt.gpuCount, tt.gpuType, tt.gpuMemory, tt.tags)
			if _, err := Load(); err == nil {
				t.Fatal("Load() succeeded for invalid worker capabilities")
			}
		})
	}
}
