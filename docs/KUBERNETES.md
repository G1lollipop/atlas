# Kubernetes deployment

The core manifest set deploys the API, the leader-elected scheduler, and two
separate worker pools. It uses ordinary Kubernetes resources, so it can be
rendered or applied without installing KEDA CRDs:

```powershell
kubectl kustomize k8s
kubectl apply -k k8s
kubectl get deployments,services,hpa
```

Before applying to a shared cluster, replace the placeholder values in
`k8s/base/secret.yaml` with values supplied through the deployment's secret manager.
The API also needs a working Postgres service at `DATABASE_URL`; the manifest
does not provision a database. The API HPA needs the Kubernetes Metrics Server.
The worker Deployments have fixed initial replica counts until the optional KEDA
configuration is installed.

## Worker pools and placement

`atlas-worker-cpu` starts with two replicas and advertises CPU and host memory.
`atlas-worker-gpu` starts with one replica and advertises one simulated
`simulated-nvidia-l4` device with 24,576 MB of simulated VRAM. The pools have
different Kubernetes labels and different `WORKER_LABELS` values, which makes
them visible as separate inventory groups. CPU and memory requests and limits
bound each pod at the Kubernetes container level; the `WORKER_*_CAPACITY_*`
environment values advertise per-worker capacity to Atlas's scheduler.

The job's `queue` remains its workload class. It does not select a worker. The
scheduler assigns a run only when the worker's advertised resource capacity
fits that run's CPU, memory, GPU count, GPU memory, and optional accelerator
type. Worker labels are descriptive inventory metadata and do not constrain
placement. A CPU-only run can therefore use spare CPU capacity on either pool;
a run that asks for a GPU requires a compatible advertised GPU capability.

The default GPU pool is for scheduling and placement demonstrations. Its
`WORKER_GPU_*` values are simulated declarations: the pod has no
`nvidia.com/gpu` limit and receives no NVIDIA device. The built-in `inference`
handler is a small simulated CPU classifier. Neither the advertised GPU
capacity nor the simulated handler runs a real GPU model.

## Optional NVIDIA device allocation example

The `k8s/gpu-nvidia` Kustomize overlay changes the GPU pool to request one
`nvidia.com/gpu`, selects nodes labeled for NVIDIA L4 devices, and advertises
the hardware pool as `nvidia-l4`. Use it only after the NVIDIA drivers and a
Kubernetes NVIDIA device plugin or GPU Operator have made `nvidia.com/gpu`
available on those nodes. Kubernetes schedules extended GPU resources from the
container's `limits`; the device plugin is required for the resource to exist.
See the [Kubernetes GPU scheduling guide](https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/).

First label only nodes that have the matching GPU model and enough memory for
the advertised capacity, then apply the overlay:

```powershell
kubectl label node <gpu-node> atlas.io/accelerator=nvidia-l4
kubectl apply -k k8s/gpu-nvidia
kubectl get pods -l atlas.io/worker-pool=gpu -o wide
```

The overlay is an allocation and placement example. The repository's current
worker image and built-in handlers do not load CUDA or execute a real model.
Use a GPU-capable image and handler before treating hardware allocation as GPU
model execution. If your nodes use another GPU model, update the node label,
`WORKER_GPU_TYPE`, and `WORKER_GPU_MEMORY_MB` together to match the homogeneous
pool. The label selector and the extended resource limit serve separate jobs:
the selector chooses the model-specific node pool, while `nvidia.com/gpu: 1`
reserves a device from the Kubernetes device plugin.

## Prometheus scraping

The API Service exposes `http` and `metrics` ports. The scheduler and each
worker pool have separate metrics Services. Every scrape Service is labeled
with `app.kubernetes.io/name: atlas` and a component label, and the scrape port
is named `metrics` and targets the container's named `metrics` port on 9090.
The scheduler's `/metrics` endpoint is the source for the database-derived
global queue gauges; scrape every scheduler endpoint so Prometheus has a
current sample during leader changes.

Merge the job in [`k8s/prometheus-scrape.yml`](../k8s/prometheus-scrape.yml)
into the Prometheus configuration. It uses Kubernetes Endpoints discovery and
keeps Atlas Services' named metrics ports. The Prometheus service account needs
`get`, `list`, and `watch` permission for Services, Endpoints, and Pods in the
namespace that contains Atlas. The example watches `default`; change that
namespace if the Atlas manifests are applied elsewhere. Set the same Prometheus
query endpoint in the KEDA resources that the KEDA operator can reach from its
namespace.

The KEDA queries first gate queue-depth samples with
`and on (job, instance) (up == 1)` and
`and on (job, instance) (atlas_observability_up == 1)`. The first gate rejects
samples from a scheduler endpoint whose scrape failed; the second rejects
samples when the scheduler's database snapshot failed. This prevents a last-good
zero gauge from being treated as current backlog during a source outage. The
queries then use `sum(max by (queue, resource_class) (...))`: `max` collapses
copies of the same queue/resource-class gauge exposed by scheduler replicas,
and `sum` adds distinct queue/class series into one scalar. The `queue` label
remains a workload class dimension in the metric, while worker execution still
depends on capability matching. Before enabling KEDA, confirm that Prometheus exposes the expected
`atlas_job_queue_depth` series for both `resource_class="cpu"` and
`resource_class="gpu"`. The core deployment can still run without these
series, but queue-based scaling requires them.

## Optional independent KEDA scaling

The files under `k8s/keda` are intentionally separate from the core
Kustomization because `ScaledObject` is a KEDA custom resource. Install KEDA,
make sure its operator can query Prometheus, edit the `serverAddress` values in
`k8s/keda/scaledobjects.yaml`, confirm both resource-class series exist, then
apply:

```powershell
kubectl apply -k k8s/keda
kubectl get scaledobjects,hpa
```

KEDA manages a separate HPA for `atlas-worker-cpu` and `atlas-worker-gpu`.
There is no CPU HPA on either worker Deployment, so each has only one autoscaler
owner. The example keeps at least two CPU workers and one GPU worker, scales up
around eight queued runs per replica (`AverageValue`), and caps the pools at 20
CPU workers and 8 GPU workers. The target and caps are starting points; tune
them to the handler duration, worker concurrency, database capacity, and node
budget. See the [KEDA Prometheus scaler documentation](https://keda.sh/docs/2.18/scalers/prometheus/)
and [ScaledObject fallback reference](https://keda.sh/docs/2.18/reference/scaledobject-spec/#fallback).

`ignoreNullValues: "false"` makes an empty Prometheus result an error instead
of treating it as a zero backlog. Each scaler also falls back after three
consecutive errors, using its minimum replica count or retaining a higher
current count. This covers a missing series as well as an unavailable monitoring
backend; it prevents a monitoring outage from appearing as an empty queue and
triggering scale-in.

The two pools scale independently from class-level backlog, while Atlas places
runs by actual resource fit. A CPU run can use a GPU worker's available CPU, so
the CPU backlog signal can grow the CPU pool even when a GPU worker still has
headroom. KEDA does not inspect worker reservations or forecast cross-pool
placement. Keep capacity headroom for this sharing, and tune each threshold and
maximum with the combined pool in mind. A GPU-required run still needs a worker
that advertises compatible GPU count, type, and memory.

## Static validation boundary

Run the standalone validator to render all three configurations and check
their expected resource boundaries:

```powershell
./k8s/validate.ps1
```

This validates Kustomize output and manifest structure locally. It does not
install KEDA, contact Prometheus, schedule a pod on an NVIDIA node, or prove
live autoscaling. Those checks require the corresponding cluster components and
hardware.
