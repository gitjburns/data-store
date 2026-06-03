# Data Store Architecture

This document describes the standalone Data Store service architecture for
developers and operators working on the service itself.

## Purpose

The Data Store service provides document ingestion and retrieval over a
service-owned HTTP API. It owns source-file resolution, PDF conversion, unit
splitting, model inference, durable storage, search indexes, active document
versioning, retrieval ranking, service config, readiness, logging, and protected
admin controls.

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
- Accelerator device kind and device index.
- Corpus root and index root.
- Docling executable and PDF conversion defaults.
- Local model artifact paths and model shape limits.
- Retrieval defaults, candidate-pool sizes, and unit sizing.

Required paths are absolute except for `logging.file_path`, where relative paths
resolve against the Rust service root. Normal runtime validates config before
binding HTTP. Missing required limits or invalid cross-field values are startup
configuration errors.

## Model Runtime

The service loads local model artifacts at startup and reports readiness through
`/v1/health`.

| Runtime | Model role | Output |
|---|---|---|
| Dense | Qwen3 embedding | One normalized dense vector per query or passage |
| ColBERT | Late interaction | One 128-dimensional vector per token |
| Reranker | Qwen3 yes/no reranker | Final yes-probability score per candidate |

Dense embeddings use query instruction formatting for queries, raw passage text
for documents, last-token pooling, and L2 normalization.

ColBERT formatting is part of the runtime contract. Queries use the configured
query prompt and marker, documents use the configured document prompt and
marker, and MaxSim scores are computed only over a bounded candidate pool.

The reranker renders the service-local Qwen3 chat prompt shape and scores final
next-token logits for the configured yes/no token IDs. Public search scores are
the reranker yes probabilities.

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

Older versions are retained. Re-ingest never overwrites or deletes a previous
version. First-time ingest remains invisible to search until the new version is
fully durable and cache-ready. Re-ingest keeps the previously active version
searchable until publish completes.

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

`POST /v1/ingest` is synchronous. The high-level stages are:

1. Validate JSON shape and field limits.
2. Acquire the non-queueing ingest admission permit.
3. Resolve the corpus-relative source inside the configured corpus root.
4. Convert PDF to markdown through Docling using service-configured options.
5. Split markdown into deterministic retrieval units.
6. Allocate a version label and versioned document/unit IDs.
7. Generate dense passage vectors for every unit.
8. Generate ColBERT document-token vectors for every unit.
9. Persist the immutable document version, units, dense vectors, ColBERT
   vectors, and FTS rows in SQLite.
10. Publish the active version and swap the active dense cache.
11. Return the ingest response.

Conversion failures, source-resolution failures, model failures, and storage
failures are explicit. The service does not silently switch PDF backends, OCR
modes, devices, models, or vector sources.

## Retrieval Pipeline

`POST /v1/search` is synchronous. The high-level stages are:

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
11. Rerank the ColBERT-ranked candidates with the Qwen3 yes/no reranker.
12. Return public top-K results and raw diagnostics for every stage.

Dense tie-breaking is deterministic by `unitId` ascending. Public result scores
are final reranker scores, while dense, BM25, RRF, and ColBERT scores remain
visible in `raw`.

## Admission And Backpressure

Ingest and search have separate config-backed maximum in-flight counts.
Admission uses immediate permit acquisition. Saturated endpoints return
`503 Service Unavailable` rather than waiting in a hidden queue.

`/v1/health`, `/v1/limits`, and protected admin endpoints do not consume
ingest/search admission permits.

## Readiness And Logging

`/v1/health` reports top-level readiness and component diagnostics.
Readiness-critical components are:

- `inference`: accelerator, model artifacts, model loading, and startup smoke.
- `storage_cache`: SQLite validation and active dense-cache load.

Diagnostic-only components include admission counters and logging state.

Startup prints bootstrap details to stdout, including the one-time admin token.
After file logging is initialized, operational events go to the configured log
file. Logs summarize operation status, counts, and timings; they must not store
the admin token, document contents, vector values, or oversized retrieval
internals.

## Admin Token

Each service start generates one cryptographically random admin token. The
token is printed once as `admin_shutdown_token=<token>` during bootstrap and
kept only in memory.

Admin endpoints require `Authorization: Bearer <token>`. Missing, malformed,
or invalid authorization fails explicitly and does not trigger shutdown or
version changes.

## Hard Invariants

- Normal runtime must never create, migrate, or repair SQLite schema.
- Search must use one captured active-version snapshot for the full request.
- Raw retrieval diagnostics must preserve per-stage provenance rather than
  replacing it with summaries.
- No silent fallbacks across accelerators, models, vector sources, Docling
  backends, OCR modes, or search-time ColBERT document-vector recomputation.
- Durable state and in-memory active cache updates must publish together.
- Source files are addressed by corpus-relative references; ingest request
  bodies never carry source file bytes.
- Admin tokens are startup-scoped, memory-only secrets.
- Public API strings and persisted metadata values are contracts; change them
  deliberately.

