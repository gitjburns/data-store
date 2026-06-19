# Data Store Service

Standalone HTTP service for document ingestion and retrieval. This service is
intentionally independent from the existing Node backend and frontend. It owns
its own config, corpus, Docling conversion, SQLite storage, dense vector cache,
and retrieval pipeline, and it currently runs alongside the existing
Node-backed Data Store.

Backend/frontend integration is later scope. Do not expect this service to be
started or managed by the Node app yet.

Run commands in this document from `service/data-store/` unless stated
otherwise.

## Capabilities

- Axum/Tokio confined to the HTTP transport shell for health, limits, ingest,
  search, protected admin version controls, and protected shutdown.
- Synchronous operation pipelines behind the transport shell for storage,
  model calls, Docling process handling, admission, shutdown state, and CLI
  operation.
- Interactive `data-store` CLI client for operating the documented HTTP API.
- Explicit Metal/CUDA accelerator selection for local Candle inference with no
  CPU fallback.
- Local Qwen3 dense embedding and ColBERT runtimes through Candle, plus a
  config-selected final reranker backend: local Candle ModernBERT or an HTTP
  Cohere-compatible rerank endpoint.
- PDF ingest through Docling, deterministic unit splitting, dense and ColBERT
  embedding, SQLite persistence, and FTS5 population.
- Startup-loaded in-memory dense vector cache with exact cosine retrieval.
- SQLite FTS5 BM25, dense/BM25 over-fetch, RRF fusion, persisted ColBERT
  MaxSim reranking, and config-selected final reranking.
- Immutable source-document versions with active-version publish and protected
  rollback.
- Explicit `--setup-storage` schema setup; normal runtime validates existing
  storage without creating or migrating schema.
- Config-backed request limits, fail-fast admission gates, and file logging.

## Prerequisites

Create a local `config.toml` from `config.example.toml`. The local config is
machine-specific and should point at:

- local model directories for Qwen3 embedding and ColBERT-Zero
- either a local ModernBERT reranker model directory or a remote
  Cohere-compatible rerank endpoint with model name, timeout, and optional API
  key file
- a service-owned corpus root for source files
- a service-owned index root for SQLite and Docling conversion artifacts
- the Python executable for environment diagnostics and the directly launched Docling executable
- the CLI operation timeout and explicit Docling document timeout, PDF backend,
  OCR mode, Docling device, thread count, and page batch size
- the selected Rust inference accelerator backend and device index for local
  Candle inference
- required request, retrieval, admission, logging, and admin token-file settings

The service has no CPU fallback for local Candle inference. If
`inference.device = "metal"`, run with `--features metal`. CUDA support is
separate operational verification work; this runbook documents the locally used
Metal path.

Docling's `[docling].device` setting is passed to the Python Docling CLI as
`--device` and is separate from `[inference].device`, which controls the
Rust/Candle dense embedding, ColBERT, and local reranker runtimes.

`[models.reranker].backend` selects exactly one final reranker backend. Use
`backend = "local"` with `path` and `max_tokens` for the in-process Candle
ModernBERT runtime, or `backend = "http"` with `endpoint`, `model`,
`timeout_seconds`, and optional `api_key_file_path` for a remote endpoint
speaking the Cohere-compatible rerank contract. The service never falls back
between reranker backends; local misconfiguration or an unreachable HTTP
backend fails readiness or search explicitly.

## First-Time Storage Setup

Run schema setup deliberately before normal service startup:

```bash
cargo run -- --config config.toml --setup-storage
```

Normal runtime never creates tables, runs migrations, or repairs stale schemas.
If startup health reports a missing database, missing table, or schema-version
error, stop the service and run the setup command intentionally.

## Start The Service

```bash
cargo run --features metal -- --config config.toml
```

Normal startup forks a detached background service after printing bootstrap
handoff details to stdout. Use `--foreground` to keep the service attached to
the current terminal while debugging startup:

```bash
cargo run --features metal -- --config config.toml --foreground
```

## Build A Binary

`cargo run` is convenient during development because it builds and starts the
service in one command. For sustained manual operation, build a reusable release
binary and run it directly. By default, the release binary backgrounds itself
after the startup handoff completes:

```bash
cargo build --release --features metal
./target/release/data-store-service --config config.toml
```

Run the release binary in the foreground when needed:

```bash
./target/release/data-store-service --config config.toml --foreground
```

The compiled binary uses the same command-line flags as `cargo run`. Run
first-time storage setup with the release binary when needed:

```bash
./target/release/data-store-service --config config.toml --setup-storage
```

Startup prints bootstrap and readiness details to stdout before the invoking
parent process exits. It includes the current `admin_shutdown_token=<token>` for
manual protected operations and a `/v1/health` compatibility URL for readiness
checks.

The default example config uses:

```toml
[admin]
token_file_path = ".data-store-admin-token"
```

The CLI client reads the same startup-scoped admin token from this file.
Relative admin token-file paths resolve from the Rust service root.

Operational events after file logging initialization are written to the
configured `[logging].file_path`. Relative log paths resolve from the Rust
service root, so `logs/data-store.log` resolves to `logs/data-store.log` inside
this directory.

## Use The CLI Client

The `data-store` client supports both an interactive REPL and one-shot command
invocation over the same `POST /v1/operations` stream API shown in this
runbook. Start the service first, then start the interactive client from this
directory:

```bash
cargo run --bin data-store -- --config config.toml
```

For a release build, run:

```bash
./target/release/data-store --config config.toml
```

When no operation flag is supplied, the client opens the interactive REPL.
For non-interactive use, pass exactly one operation flag:

```bash
./target/release/data-store --config config.toml --help
./target/release/data-store --config config.toml --health
./target/release/data-store --config config.toml --limits
./target/release/data-store --config config.toml --sources
./target/release/data-store --config config.toml --ingest The_Elements_of_Style.pdf
./target/release/data-store --config config.toml --ingest The_Elements_of_Style.pdf --force
./target/release/data-store --config config.toml --search "clear writing style rules" 3
./target/release/data-store --config config.toml --search-full "clear writing style rules" 3
./target/release/data-store --config config.toml --versions
./target/release/data-store --config config.toml --rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
./target/release/data-store --config config.toml --shutdown
```

The same one-shot command surface is available during development:

```bash
cargo run --bin data-store -- --config config.toml --health
```

The client reads `server.bind_address`, `admin.token_file_path`, and
`client.operation_timeout_seconds` from the same config file. Public commands
send unauthenticated operations. Protected commands read the configured token
file immediately before sending the operation. If the token file is missing,
start or restart the service.

At the prompt, run `help` to show the available commands and syntax:

```text
data-store> help
```

Common commands:

```text
data-store> health
data-store> limits
data-store> sources
data-store> ingest The_Elements_of_Style.pdf
data-store> ingest The_Elements_of_Style.pdf --force
data-store> search "clear writing style rules" 3
data-store> search-full "clear writing style rules" 3
data-store> versions
data-store> rollback The_Elements_of_Style.pdf 2026-06-01T21:37:22.184Z
data-store> shutdown
data-store> help
data-store> exit
```

`search` prints excerpts. `search-full` prints the full matched unit content.
Both search commands print a server-authoritative `Benchmarks:` summary after
the rendered search results, computed by the server and rendered by the client
with no client-side timing. It includes `search_preparation`, the streamed
search stages with the nested `retrieving_candidates` substages, and a
server-reported `Total` (the whole-operation duration). Rows may not sum exactly
to `Total`; the small unattributed remainder is shown rather than hidden.
`ingest` prints a server-authoritative `Benchmarks:` summary after the ingest
result, computed by the server and rendered by the client with no client-side
timing. It includes `docling_converting`, `unit_splitting`, `dense_embedding`,
`colbert_embedding`, and `storage_publishing` with its nested
`vector_validation`, `document_persistence`, `cache_preparation`, and `commit`
substages, and a server-reported `Total` (the whole-operation duration). Rows
may not sum exactly to `Total`; the small unattributed remainder is shown rather
than hidden. Quote multi-word queries and any argument containing spaces. Long
operations stream status and counted progress while they run. The CLI updates
the current stage line in place and prints a newline when each stage completes.
The `shutdown` command sends the protected operation directly and prints the
server-authored `shutdown_complete` confirmation from the terminal result
event. Any shutdown stream error or non-completion status is reported as an
operator-visible error.

The client prints human-readable output and stores readline history in
`.data-store.history`.

## Operation API

The documented consumer API is one streamed operation endpoint:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Each request starts one operation. The response is newline-delimited JSON with
`status`, `progress`, `result`, and `error` events. The stream ends after one
terminal `result` or `error` event. See `PROTOCOL.md` for the complete event
contract.

Public operations do not require authentication. Protected operations require
the startup-scoped bearer token printed as `admin_shutdown_token=<token>` or
written to the configured admin token file.

Route-specific endpoints such as `/v1/health`, `/v1/limits`, `/v1/sources`,
`/v1/ingest`, `/v1/search`, and `/admin/...` remain available during migration as
compatibility routes. New consumers should use `/v1/operations`.

## Health And Limits

Check readiness:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"health","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

The top-level `ready` flag is based on readiness-critical components:

- `inference`: accelerator, model artifacts, tokenizer/model load, configured
  reranker backend, and startup smoke checks, including ColBERT max-capacity
  document encoding and reranker backend smoke scoring
- `storage_cache`: SQLite schema validation and active dense-cache load

Diagnostic-only components remain visible without making the service unready:

- `admission`: current ingest/search in-flight counters
- `logging`: initialized file log path and level

Fetch request and retrieval limits before constructing ingest or search
requests:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"limits","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Oversized bodies return `413 Payload Too Large`. Oversized fields, unknown JSON
fields, and invalid request values return `400 Bad Request`.

## Ingest A Source

Ingest uses a corpus-relative source reference. File bytes do not cross the
HTTP API; the service resolves the source inside its configured corpus root.

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"ingest","payload":{"source":"The_Elements_of_Style.pdf"}}' \
  http://127.0.0.1:8091/v1/operations
```

A successful ingest creates a new immutable source-document version, persists
units and vectors, updates the active-version map, and publishes a new active
search snapshot only after the new version is durable and cache-ready.
If the resolved source already has an active version, ingest aborts before
conversion unless the request includes `force: true`. The CLI and REPL expose
that override as `--force`.

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"ingest","payload":{"source":"The_Elements_of_Style.pdf","force":true}}' \
  http://127.0.0.1:8091/v1/operations
```

PDF conversion progress comes from Docling stderr progress lines when Docling
emits them. The service passes `[docling].document_timeout_seconds` to Docling
as `--document-timeout`, `[docling].pdf_backend` as `--pdf-backend`,
`[docling].device` as `--device`, `[docling].num_threads` as `--num-threads`,
and `[docling].page_batch_size` as `--page-batch-size`. OCR behavior is
explicitly config-backed: `ocr_mode = "on"` passes `--ocr`, `ocr_mode = "off"`
passes `--no-ocr`, and `ocr_mode = "auto"` leaves Docling's CLI default in
control. The interactive CLI uses `[client].operation_timeout_seconds` as the
HTTP stream timeout for one operation.

## Search

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"operation":"search","payload":{"query":"clear writing style rules","topK":3}}' \
  http://127.0.0.1:8091/v1/operations
```

Search is synchronous. It captures the active document-version snapshot at
request admission and uses that same snapshot through dense retrieval, BM25,
RRF, ColBERT MaxSim, final reranking, and result materialization. The terminal
`result` event contains public results plus raw stage diagnostics. Reranker raw
diagnostics expose a backend-dependent `mode`: `modernbert_sequence_classifier`
for the local backend and `http_rerank` for the HTTP backend. Local diagnostics
include raw `logit` and `tokenCount` values when available; HTTP diagnostics
omit those fields and use the provider `relevance_score` as the public score.

If the configured ingest/search admission gate is saturated, the endpoint
emits a terminal `error` event with status `503 Service Unavailable`. The
service does not hide work in an unbounded queue.

## Version Administration

Protected operations require the startup-scoped bearer token printed as
`admin_shutdown_token=<token>` or written to the configured admin token file.

List retained document versions:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"versions","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Rollback one source document to an already-retained version:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"operation":"rollback","payload":{"source":"The_Elements_of_Style.pdf","versionLabel":"2026-06-01T21:37:22.184Z"}}' \
  http://127.0.0.1:8091/v1/operations
```

Rollback repoints `active_document_versions` and publishes a new active search
snapshot. It does not delete versions, rebuild embeddings, or run automatic
cleanup.

## Shutdown

Use the startup token for graceful shutdown:

```bash
curl -N -X POST \
  -H "Accept: application/x-ndjson" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"operation":"shutdown","payload":{}}' \
  http://127.0.0.1:8091/v1/operations
```

Shutdown requests drain through Axum graceful shutdown instead of requiring a
process signal. The operation stream's terminal result is the authoritative
shutdown confirmation and contains `status: "shutdown_complete"` plus a
server-authored message emitted as the final confirmation before process
termination. If shutdown cannot be requested, the stream emits a terminal
`error` with the reason.

The CLI `shutdown` command sends the same protected operation and requires the
`shutdown_complete` terminal result before displaying the confirmation.

## Verify

```bash
cargo fmt
cargo check
cargo check --features metal
cargo clippy
```

## Common Operator Diagnostics

- Missing SQLite database: run `--setup-storage` deliberately, then restart the
  service.
- SQLite schema version or table mismatch: the runtime will not migrate; rebuild
  or set up the development database intentionally.
- Requested accelerator unavailable or not compiled: rebuild with the matching
  Cargo feature and verify the configured device.
- Missing model artifacts or tokenizer files: fix local model paths in
  `config.toml`; readiness must fail clearly rather than falling back.
- HTTP reranker startup or search failure: inspect the API error and service
  log for endpoint, status, score-count, elapsed-time, and bounded response-body
  context. The service does not silently retry through the local reranker.
- Docling conversion failure: inspect the API error and service log. The service
  does not silently switch PDF backends or OCR modes.
- `401 Unauthorized` on protected operations: use the current startup token
  from stdout or the configured admin token file; tokens do not survive restart.
- Missing admin token file for the CLI: start or restart the service with a
  config that includes `[admin].token_file_path`.
- `503 Service Unavailable` on ingest/search: the configured in-flight limit is
  saturated. Retry after active work finishes or change the config deliberately.
