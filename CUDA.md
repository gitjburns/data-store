# CUDA Verification

This document is the NVIDIA CUDA verification procedure for the standalone
Data Store service. It is not a completed verification record until these steps
are run on a machine with supported NVIDIA hardware and a working CUDA runtime.

The local development path currently uses Metal. CUDA support is compile-gated
through the Rust `cuda` Cargo feature and selected at runtime through service
config.

## Scope

Use this procedure to verify that the service can:

- compile with CUDA support
- initialize the configured CUDA device
- load local model artifacts onto the CUDA-backed Candle device
- run inference startup smoke checks
- start HTTP with CUDA selected
- report CUDA readiness through `/v1/health`

Storage readiness is checked by `/v1/health`, but a storage setup error is not
itself a CUDA verification failure.

## Config

Use a local `service/data-store/config.toml` with CUDA selected:

```toml
[inference]
device = "cuda"
device_index = 0
```

Set `device_index` to the NVIDIA device that should run inference. The model,
Docling, storage, request-limit, retrieval, admission, and logging config fields
must still be valid exactly as they are for Metal.

## Compile Check

From the repository root, verify the CUDA feature build:

```bash
cargo check --manifest-path service/data-store/Cargo.toml --features cuda
```

If config requests CUDA but the binary is built without `--features cuda`,
startup must fail readiness with an explicit message like:

```text
config requested cuda:0, but this binary was not built with --features cuda
```

## Inference Smoke

Run the one-shot inference smoke path:

```bash
cargo run --manifest-path service/data-store/Cargo.toml --features cuda -- --config service/data-store/config.toml --smoke-dense
```

Expected indicators:

- command exits successfully
- output includes `device ready: cuda:<device_index>`
- dense runtime reports vector dimension and smoke norm
- ColBERT readiness details are printed
- reranker readiness details are printed

This command initializes inference and exits without binding HTTP.

## Storage Setup

If the target machine uses a fresh index root, create the SQLite schema
deliberately:

```bash
cargo run --manifest-path service/data-store/Cargo.toml -- --config service/data-store/config.toml --setup-storage
```

Runtime startup will not create or migrate schema. A missing database, stale
schema version, or missing table should be treated as storage setup work, not
as CUDA inference failure.

## Service Startup

Start the HTTP service with CUDA support:

```bash
cargo run --manifest-path service/data-store/Cargo.toml --features cuda -- --config service/data-store/config.toml
```

Startup should print bootstrap details to stdout, including:

- config path
- configured and resolved log path
- log level
- file logging initialization
- bind address
- `admin_shutdown_token=<token>`

Capture the admin token for shutdown and protected admin endpoints. The token
is intentionally not written to service logs or durable storage.

## Health Check

Check readiness:

```bash
curl http://127.0.0.1:8091/v1/health
```

Expected CUDA-specific indicator:

```text
device ready: cuda:<device_index>
```

Overall health is ready only when readiness-critical components are ready:

- `inference`
- `storage_cache`

Diagnostic-only components, such as `admission` and `logging`, should remain
visible without changing the top-level readiness flag.

## Functional Smoke

After `/v1/health` reports ready, run a small ingest/search cycle against a
known corpus-relative PDF:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"source":"The_Elements_of_Style.pdf"}' \
  http://127.0.0.1:8091/v1/ingest
```

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"query":"clear writing style rules","topK":3}' \
  http://127.0.0.1:8091/v1/search
```

The search response should include public results and raw diagnostics for the
dense, BM25, RRF, ColBERT, reranker, and final-result stages.

## Shutdown

Use the startup-scoped admin token:

```bash
curl -X POST \
  -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8091/admin/shutdown
```

The service should return `{"status":"shutting_down"}` and exit through Axum
graceful shutdown.

## Failure Interpretation

- `config requested cuda:<index>, but this binary was not built with --features cuda`:
  rebuild or run with `--features cuda`.
- `failed to initialize CUDA device <index>`:
  check NVIDIA driver, CUDA runtime availability, visible devices, and
  `device_index`.
- `Candle backend panicked` during CUDA initialization:
  treat as a CUDA/Candle backend initialization failure; preserve the panic
  diagnostic.
- Missing model config, tokenizer, or safetensors:
  fix the model paths in `config.toml`; do not switch devices as a fallback.
- `/v1/health` shows inference ready but storage not ready:
  CUDA verification may have passed; fix storage setup separately.
- `503 Service Unavailable` during ingest/search:
  the configured admission gate is saturated, not a CUDA initialization error.
