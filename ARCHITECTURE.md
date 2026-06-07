# Data Store Architecture

This document describes the standalone Data Store service architecture for
developers and operators working on the service itself.

## Purpose

The Data Store service provides document ingestion and retrieval over a
service-owned operation-stream HTTP API. It owns source-file resolution, PDF
conversion, unit splitting, model inference, durable storage, search indexes,
active document versioning, retrieval ranking, service config, readiness,
logging, and protected admin controls.

The service is an independent Rust application under `service/data-store/`.
It is not a library facade over another process, and its storage, corpus,
model runtime, and operational lifecycle are service-local concerns.

## Runtime Stack

- HTTP server: Axum on Tokio.
- Inference: Candle plus tokenizers, using an explicitly selected accelerator.
- Storage: SQLite with FTS5 for durable data and lexical search.
- Dense retrieval: exact cosine scan over an in-memory active-vector cache.
- Conversion: Docling launched as a configured executable.
- Logging: file-backed `tracing` events after bootstrap stdout output.
- CLI: separate `data-store` REPL binary over the documented HTTP API.

CPU inference is intentionally unsupported. The configured accelerator must be
available and compiled into the binary through the matching Cargo feature:

- `metal` for Apple Silicon Metal.
- `cuda` for NVIDIA CUDA.

There is no automatic device fallback. If the requested accelerator cannot be
initialized, inference readiness fails explicitly.

## Configuration Ownership

All operational service behavior is config-backed in the service TOML config:

- HTTP bind address, request body limits, field limits, and admission limits.
- File logging path and level.
- Admin token-file path for local startup-scoped credential handoff.
- CLI operation-stream timeout.
- Accelerator device kind and device index.
- Corpus root and index root.
- Docling executable, document timeout, and PDF conversion defaults.
- Local model artifact paths and model shape limits.
- Retrieval defaults, candidate-pool sizes, and unit sizing.

Required paths are absolute except for `logging.file_path` and
`admin.token_file_path`, where relative paths resolve against the Rust service
root. Normal runtime validates config before binding HTTP. Missing required
limits, missing admin token-file configuration, or invalid cross-field values
are startup configuration errors.

## Model Runtime

The service loads local model artifacts at startup and reports readiness through
the `health` operation. The retained `/v1/health` route reports the same
readiness data during migration compatibility.

| Runtime | Model role | Output |
|---|---|---|
| Dense | Qwen3 embedding | One normalized dense vector per query or passage |
| ColBERT | Late interaction | One 128-dimensional vector per token |
| Reranker | ModernBERT sequence classifier | Raw relevance logit and sigmoid score per candidate |

Dense embeddings use query instruction formatting for queries, raw passage text
for documents, last-token pooling, and L2 normalization.

ColBERT formatting is part of the runtime contract. Queries use the configured
query prompt and marker, documents use the configured document prompt and
marker, and MaxSim scores are computed only over a bounded candidate pool.
Startup smoke checks include max-capacity ColBERT document encoding so
long-sequence accelerator failures are reported through readiness rather than
after ingest work has already completed conversion and dense embedding.

The reranker tokenizes query/document pairs as ModernBERT sequence pairs,
scores one raw single-label relevance logit per candidate, and converts that
logit to the public search score with a sigmoid.

## Operation Protocol

The documented consumer API is:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Each request starts one named operation. The service streams operation-scoped
newline-delimited JSON events with monotonic per-operation sequence numbers.
Events are `status`, `progress`, `result`, and `error`; `result` and `error`
are terminal.

Supported operations are `health`, `limits`, `ingest`, `search`, `versions`,
`rollback`, and `shutdown`. `versions`, `rollback`, and `shutdown` require the
startup-scoped bearer token. The route-specific `/v1/...` and `/admin/...`
endpoints remain available during migration as compatibility routes, but the
operation stream is the documented consumer contract.

## Storage Model

SQLite is the durable source of truth. Schema setup is an explicit operator
action via `--setup-storage`; normal startup never creates tables, runs
migrations, repairs schemas, or backfills data.

The current schema version is `PRAGMA user_version = 3`.

Durable tables:

- `document_versions`: immutable source-document versions and ingest metadata.
- `active_document_versions`: active version label per source path.
- `units`: searchable retrieval units for each document version.
- `dense_vectors`: dense vector blobs and dense embedding metadata per unit.
- `colbert_document_vectors`: persisted ColBERT document-token matrices per
  unit.
- `units_fts`: SQLite FTS5 index over unit content.

Vector blobs are contiguous little-endian `f32` values. Dense vectors store
their dimension and norm. ColBERT document vectors store token count and
dimension. Loads and writes validate dimensions, byte length, finite values,
and domain-specific invariants.

## Document Versions

Every successful ingest creates a new immutable version for one corpus-relative
source document. `versionLabel` is a source-document-scoped timestamp string.

Older versions are retained. By default, ingest aborts when the resolved source
already has an active version; callers must set `force: true` to create and
publish a replacement version. Force re-ingest never overwrites or deletes a
previous version. First-time ingest remains invisible to search until the new
version is fully durable and cache-ready. Force re-ingest keeps the previously
active version searchable until publish completes.

Publishing a version updates `active_document_versions` and swaps the active
in-memory dense cache snapshot after durable writes and cache preparation have
succeeded.

Rollback is an admin operation that repoints one source document to an already
retained version. It does not delete versions, rebuild embeddings, or mutate
immutable version rows.

## Dense Cache And Search Snapshots

The active dense cache contains only active source-document versions:

- one row-major `Vec<f32>` for dense vectors;
- parallel arrays for unit IDs, source paths, version labels, and norms;
- active-version metadata;
- load duration, load timestamp, dimension, and memory diagnostics.

Startup loads and validates the active cache from SQLite. An empty valid
database is ready. A missing or stale database is not repaired at runtime.

Search captures the active cache and active version map exactly once at request
admission. Dense retrieval, BM25 filtering, candidate materialization, ColBERT
document-vector loading, reranking provenance, and raw diagnostics all use that
same captured snapshot for the lifetime of the request.

## Ingestion Pipeline

The `ingest` operation is synchronous and streamed. The high-level stages are:

1. Validate JSON shape and field limits.
2. Acquire the non-queueing ingest admission permit.
3. Resolve the corpus-relative source inside the configured corpus root.
4. Convert PDF to markdown through Docling using service-configured options,
   including the configured document timeout.
5. Split markdown into deterministic retrieval units.
6. Allocate a version label and versioned document/unit IDs.
7. Generate dense passage vectors for every unit.
8. Generate ColBERT document-token vectors for every unit.
9. Persist the immutable document version, units, dense vectors, ColBERT
   vectors, and FTS rows in SQLite.
10. Publish the active version and swap the active dense cache.
11. Emit the terminal ingest result.

Ingest progress is part of the operation stream contract. The service emits
Docling conversion progress parsed from Docling stderr when available, unit
counts after splitting, per-unit dense and ColBERT embedding progress, and
storage/publish checkpoints. Clients may render progress compactly, but must
not replace the raw NDJSON events as the authoritative record.

Conversion failures, source-resolution failures, model failures, and storage
failures are explicit. The service does not silently switch PDF backends, OCR
modes, devices, models, or vector sources.

## Retrieval Pipeline

The `search` operation is synchronous and streamed. The high-level stages are:

1. Validate JSON shape and field limits.
2. Acquire the non-queueing search admission permit.
3. Capture the active search snapshot.
4. Embed the query with the dense runtime.
5. Validate query vector values and norm.
6. Run exact dense cosine scan over the captured cache.
7. Run SQLite FTS5 BM25 filtered to captured active versions.
8. Fuse dense and BM25 candidate lists with Reciprocal Rank Fusion.
9. Load persisted ColBERT document vectors for the bounded RRF pool.
10. Embed the query with ColBERT and MaxSim-rerank the candidate pool.
11. Rerank the ColBERT-ranked candidates with the ModernBERT sequence-classification reranker.
12. Emit public top-K results and raw diagnostics for every stage.

Dense tie-breaking is deterministic by `unitId` ascending. Public result scores
are final reranker scores, while dense, BM25, RRF, and ColBERT scores remain
visible in `raw`.

## Admission And Backpressure

Ingest and search have separate config-backed maximum in-flight counts.
Admission uses immediate permit acquisition. Saturated operations emit terminal
errors with status `503 Service Unavailable` rather than waiting in a hidden
queue.

`health`, `limits`, and protected admin operations do not consume ingest/search
admission permits.

## Readiness And Logging

The `health` operation reports top-level readiness and component diagnostics.
Readiness-critical components are:

- `inference`: accelerator, model artifacts, model loading, and startup smoke,
  including ColBERT max-capacity document encoding.
- `storage_cache`: SQLite validation and active dense-cache load.

Diagnostic-only components include admission counters and logging state.

Normal startup forks a detached background service after printing bootstrap
handoff and readiness details to stdout, including the one-time admin token.
The service process also writes that same token to the configured owner-only
admin token file for the local CLI client. `--foreground` keeps the service
attached to the current terminal for debugging.

The startup handoff reports config/log paths, file logging initialization,
bind address, background child PID, admin token-file path/write status,
inference progress and readiness, storage/cache readiness, HTTP bind/listening
state, final top-level readiness, and the `/v1/health` URL. Inference progress
uses an updating terminal line and includes accelerator, artifact, model-load,
every model layer, and smoke-check milestones so long model initialization does
not appear frozen. After file logging is initialized, operational events go to
the configured log file. Logs summarize operation status, counts, and timings;
they must not store the admin token, document contents, vector values, or
oversized retrieval internals.

Operation-stream events are live client feedback, not the only diagnostic
record. Long-running operation stages must also write durable service-log
boundaries so a client disconnect, timeout, or terminal delivery failure does
not leave operators blind. Ingest logs include source resolution, Docling
conversion, unit splitting, dense embedding, ColBERT document embedding,
storage publishing, terminal result/error readiness, event delivery outcome,
and operation task finish. Persistence-affecting workflows log durable
transaction boundaries separately from active-version/cache publish boundaries.
These logs preserve compact operational facts such as operation ID, source
reference, version label, unit/vector counts, stage elapsed milliseconds, status,
and error kind/message without logging contents, vectors, tokens, or large raw
payloads.

## Admin Token

Each service start generates one cryptographically random admin token. The
token is printed once as `admin_shutdown_token=<token>` during bootstrap and
written to the configured admin token file for local client use. The token file
is replaced on startup, created with owner-only permissions, and removed on
graceful shutdown when it still contains the current service token. If the
service crashes, a stale token file may remain; that stale token is not accepted
by any later service process and is replaced on the next startup.

Protected operations require `Authorization: Bearer <token>`. Missing,
malformed, or invalid authorization fails explicitly and does not trigger
shutdown or version changes.

The protected shutdown operation emits a terminal operation-stream result with
`status: "shutdown_complete"` and a server-authored message as the final
confirmation before process termination. If shutdown cannot be requested, the
operation emits a terminal error event with the reason instead of leaving the
client to infer completion.

## CLI Client

The `data-store` binary is a CLI client over the documented HTTP API. With no
operation flag, it starts the interactive REPL. With one operation flag such as
`--health`, `--ingest`, `--search`, or `--shutdown`, it executes that operation
non-interactively and exits after the terminal result or error. Both modes read
`server.bind_address`, `admin.token_file_path`, and
`client.operation_timeout_seconds` from the service config, construct
`http://<bind_address>`, and send operation requests to the service. The client
does not share process memory, bypass authorization, access SQLite directly, or
reimplement domain behavior.

Public commands send unauthenticated operations. Protected commands read the
current token file immediately before sending the request in either mode and use
the same bearer-token header required by curl clients. Client output is
human-readable: it renders streamed status/progress events in place for the
active stage, prints a newline when each stage completes, and then prints
terminal results/errors. The `shutdown` command sends the protected operation
directly and displays only the server-authored `shutdown_complete` terminal
result as confirmation. Raw protocol payloads remain available through the HTTP
API itself.

## Hard Invariants

- Normal runtime must never create, migrate, or repair SQLite schema.
- Search must use one captured active-version snapshot for the full request.
- Raw retrieval diagnostics must preserve per-stage provenance rather than
  replacing it with summaries.
- No silent fallbacks across accelerators, models, vector sources, Docling
  backends, OCR modes, or search-time ColBERT document-vector recomputation.
- Durable state and in-memory active cache updates must publish together.
- Long-running operation stages and persistence publish boundaries must be
  visible in durable service logs; stream events alone are not sufficient.
- Source files are addressed by corpus-relative references; ingest request
  bodies never carry source file bytes.
- Admin tokens are startup-scoped secrets exposed only through bootstrap stdout
  and the configured owner-only runtime token file.
- The CLI client operates through the documented HTTP API and must not bypass
  service validation, storage, or authentication.
- Public API strings and persisted metadata values are contracts; change them
  deliberately.
- Every operation writes meaningful lifecycle facts to the service log at
  `service/data-store/logs/data-store.log`.
- Storage transactions log begin, each persistence phase, commit attempt,
  commit success or failure, rollback or abort when visible, and publish
  success or failure.
- External process calls and model calls log start, completion, elapsed time,
  and failure with source context.
- Operation streams are reporting channels only; they must not control
  authoritative execution or outcome logging.
- Health and CLI diagnostics must surface active operation counts and last known
  operation facts when available.
