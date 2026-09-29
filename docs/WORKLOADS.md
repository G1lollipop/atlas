# Workload classes and artifact-backed examples

Atlas treats `queue` as a workload class, not a fixed worker destination. A job
in `embedding`, `inference`, `batch`, `cpu-default`, or any other queue is placed
on a worker whose advertised CPU, memory, and GPU capacity meets its explicit
requirements. Queue names are free-form; adding a new name does not require a
code change or a hardcoded machine mapping. `workload_type` describes the work
for clients, while `name` chooses a registered handler.

The three built-in infrastructure examples read `payload.input_uri` and write
their full output to the configured artifact store. The job result in Postgres
contains an `output_uri` and small metadata such as counts and a model label.
Input and output objects are limited by `ARTIFACT_MAX_BYTES` (16 MiB by
default). The examples deliberately do not train a model: `embedding` computes
deterministic feature-hash vectors, `inference` runs a small simulated sentiment
classifier, and `batch_transform` performs bounded CPU work on JSONL records.

| Handler name | Input object | Suggested queue and resource request |
| --- | --- | --- |
| `embedding` | JSON `{"texts":["hello","world"]}` | `embedding`, CPU and RAM |
| `inference` | JSON `{"texts":["good result","slow result"]}` | `inference`, optionally GPU and VRAM for placement demonstrations |
| `batch_transform` | One JSON/text record per line | `batch`, CPU and RAM |

For a local S3-compatible demonstration, start Compose with the `artifacts`
profile and set `ARTIFACT_STORE=s3` for the worker. The profile starts MinIO and
creates the `atlas-artifacts` bucket. Upload `input.json` to
`atlas-artifacts/inputs/input.json` with the MinIO console on port 9001 or an
S3-compatible client; the resulting job URI is
`s3://atlas-artifacts/inputs/input.json`. For example:

```json
{
  "name": "embedding",
  "queue": "embedding",
  "workload_type": "embedding",
  "required_cpu_millis": 2000,
  "required_memory_mb": 4096,
  "payload": {"input_uri": "s3://atlas-artifacts/inputs/input.json"}
}
```

The local backend uses `local://<root-relative-key>` URIs under
`ARTIFACT_LOCAL_DIR`; use it when one worker can access the objects on its own
filesystem. An S3/MinIO backend provides shared object access across worker
replicas. In S3 mode, configure `ARTIFACT_S3_BUCKET` and optionally
`ARTIFACT_S3_ENDPOINT` for MinIO, together with AWS SDK credentials and region.
Artifact writes do not make handler execution exactly once; a retried run may
write the same key again. Consumers should use the run's `execution_key` when
they need to deduplicate downstream effects.
