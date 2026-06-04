# Data Store Service Specification

## 1. Purpose And Scope

A standalone Rust microservice that owns document ingestion, durable document
version storage, retrieval indexes, inference runtimes, and search.

The service exposes a data-store-specific operation protocol over HTTP. Source
document bytes do not cross the API; callers send corpus-relative source
references and search queries.

**In scope**

- Rust service process using `axum` and `tokio`.
- Operation-scoped streamed HTTP protocol.
- Document ingestion from service-owned corpus files.
- Docling conversion, unit splitting, dense embeddings, persisted ColBERT
  document vectors, SQLite storage, FTS, active-version cache, ColBERT scoring,
  and Qwen3 reranking.
- Immutable source-document versioning, active-version publish, retained-version
  listing, rollback, health, limits, and graceful shutdown.
- Runtime admin token-file handoff for local operators.

**Out of scope**

- Migrating indexes or corpora from another system.
- File listing, markdown inspection, delete, automatic rollback, inactive
  version cleanup, or corpus management operations.
- Deferred retrieval features: trigram/fuzzy retrieval, Contextual Retrieval,
  small-to-big parent retrieval, and ANN indexing.

## 2. Architecture

- **Process:** independent service binary, `data-store-service`.
- **HTTP runtime:** `axum` plus `tokio`.
- **Inference:** `candle` and `tokenizers`, with CUDA or Apple Silicon Metal
  acceleration. CPU fallback is not supported for the configured models.
- **Storage:** SQLite owns durable documents, versions, units, metadata, vector
  blobs, ColBERT document vectors, and FTS.
- **Retrieval cache:** dense retrieval uses an explicit in-memory flat vector
  cache with exact cosine similarity over active document versions.
- **Late interaction:** ColBERT scoring runs over a bounded candidate pool and
  loads persisted document token vectors from SQLite.
- **Final ranking:** Qwen3 reranker scoring produces the public result order.
- **Ownership:** the service owns corpus resolution, conversion, chunking,
  embeddings, durable storage, retrieval cache, indexes, config, and lifecycle
  controls.

## 3. Models

Models load from local `.safetensors` into accelerator memory during startup.
Startup and readiness diagnostics must make model failures explicit. The HTTP
process may still bind so health and operation errors can report why inference
is unavailable.

| Slot | Model | Key params | Formatting | Output |
|---|---|---|---|---|
| Dense | Qwen/Qwen3-Embedding-8B | 4096-d Matryoshka, 32k ctx | Query: instruct prefix; passage: raw | 1-D vector with last-token pooling |
| Late-interaction | lightonai/ColBERT-Zero | 128-d/token, about 512 ctx | `search_query:` / `search_document:` | 2-D `[num_tokens,128]` tensor, no pooling |
| Reranker | Qwen/Qwen3-Reranker-4B | 32k ctx | Chat template | yes/no token logit to score |

## 4. Storage Schema

SQLite is the durable source of truth for ingested units and their
source-document-scoped versions.

Required durable tables:

- `document_versions`: corpus-relative source path, timestamp `versionLabel`,
  document id, conversion metadata, ingest timestamps, status, and diagnostics.
  `(sourcePath, versionLabel)` identifies one immutable version of one source
  document.
- `active_document_versions`: the active `versionLabel` for each source path.
  This is the search-visible corpus map and is updated only after a new version
  is fully durable and cache-ready.
- `units`: `unitId`, `sourcePath`, `versionLabel`, `documentId`,
  `headingPath[]`, `pageNumbers[]`, sequence, token count, and `content`.
- `dense_vectors`: `unitId`, `sourcePath`, `versionLabel`, dimension, vector
  blob, vector norm, and embedding metadata.
- `colbert_document_vectors`: `unitId`, `sourcePath`, `versionLabel`, token
  count, dimension, vector blob, and embedding metadata for persisted document
  token matrices used by MaxSim.
- SQLite FTS5 table over unit `content` with enough version metadata or joins to
  restrict BM25 retrieval to a request's captured active versions.

Version rules:

- Every successful ingest creates a new immutable source-document version.
- `versionLabel` is a self-documenting timestamp scoped to the source document,
  such as `2026-06-01T21:37:22.184Z`.
- Re-ingesting a source document never overwrites or deletes older versions.
- First-time ingests remain invisible to search until publish completes.
- Re-ingests keep the previously active version searchable until the new version
  publishes.

Dense search cache:

- Store one contiguous `Vec<f32>` in row-major `[unit_count, dimension]` order.
- Store parallel arrays for `unitId`, `sourcePath`, `versionLabel`, stored
  vector norm, and deterministic sort keys.
- Cache load/update paths log duration, vector count, dimension, and memory
  footprint.
- Cache publish happens only after the new document version is fully durable and
  ready.
- Search captures the active cache/snapshot at request start and uses that same
  snapshot for the lifetime of the request.

## 5. Pipelines

### Ingestion

Ingestion blocks until the new document version is durable and, after publish,
searchable.

1. Resolve the referenced source file internally.
2. Allocate a source-document-scoped timestamp `versionLabel`.
3. Convert PDF to markdown with Docling using service-configured PDF backend,
   OCR mode, and page-batch-size options.
4. Split markdown into retrieval units with `headingPath`, `pageNumbers`, and
   content.
5. Embed Qwen3 dense vectors and ColBERT document token vectors for each
   searchable unit.
6. Write the immutable document version, units, dense vectors, ColBERT document
   vectors, vector norms, and FTS rows to SQLite in an explicit transaction.
7. Build the search cache state for the new active-version map offline.
8. Publish by atomically updating the active version for the source document and
   swapping the active search snapshot.
9. Surface conversion failures with diagnostics. No silent fallback across
   backends or OCR modes is allowed.

### Retrieval

1. Capture the current active source-document version map and active search
   cache snapshot. This captured snapshot is authoritative for the lifetime of
   the request.
2. Embed the query with Qwen3 dense.
3. Validate query vector values are finite and compute query norm. Invalid or
   zero-norm vectors fail explicitly.
4. Dense retrieval performs exact cosine similarity over the captured vector
   cache, with deterministic tie-breaking by `unitId`.
5. BM25 retrieval uses SQLite FTS5 filtered to the captured active versions.
6. RRF fuses dense and BM25 candidate lists.
7. ColBERT embeds the query, loads persisted candidate document token vectors
   for the captured versions from SQLite, and MaxSim reranks only the fused
   candidate pool.
8. Qwen3 reranker rescores the ColBERT-ranked candidate pool.
9. Return top-K in final reranker order.

## 6. Operation Protocol

The documented consumer protocol is:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Each request starts one operation. The response body is an operation-scoped
NDJSON stream. Each line is one operation event. The stream ends after a
terminal `result` or `error` event.

Request envelope:

```json
{
  "operationId": "optional-client-id",
  "operation": "search",
  "payload": {
    "query": "clear writing style rules",
    "topK": 3
  }
}
```

Event types:

- `status`: newline-worthy operation stage.
- `progress`: counted repeated work, with optional `current` and `total`.
- `result`: terminal success event with operation-specific payload.
- `error`: terminal failure event with structured error details.

All operation events include `operationId` and a monotonic per-operation
`sequence`.

The service reserves:

```http
POST /v1/operations/{operationId}/control
```

for cancellation or future mid-operation client-to-server control messages. The
first implementation may reject unsupported control messages explicitly.

## 7. Operations

### `limits`

Authentication: none.

Payload:

```json
{}
```

Result payload contains request-construction and retrieval limits:

```json
{
  "request": {
    "maxRequestBodyBytes": 16384,
    "maxIngestSourceChars": 2048,
    "maxSearchQueryChars": 4096
  },
  "retrieval": {
    "defaultTopK": 10,
    "maxTopK": 100
  }
}
```

### `health`

Authentication: none.

Payload:

```json
{}
```

Result payload reports service readiness and component diagnostics.

### `ingest`

Authentication: none.

Payload:

```json
{
  "source": "The_Elements_of_Style.pdf"
}
```

The operation must emit real server-side status/progress events for source
resolution, conversion, unit splitting, dense embedding, ColBERT embedding, and
storage publish.

Result payload:

```json
{
  "documentId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z",
  "versionLabel": "2026-06-01T21:37:22.184Z",
  "unitsIngested": 43,
  "status": "ingested"
}
```

### `search`

Authentication: none.

Payload:

```json
{
  "query": "clear writing style rules",
  "topK": 3
}
```

The operation must emit real server-side status/progress events for query
embedding, candidate retrieval, ColBERT scoring, reranking, and result assembly.

Result payload:

```json
{
  "results": [
    {
      "unitId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z:unit:000000",
      "score": 0.725617,
      "content": "Matched unit text...",
      "headingPath": ["Chapter", "Section"],
      "sourcePath": "The_Elements_of_Style.pdf",
      "pageNumbers": []
    }
  ],
  "latencyMs": 103500,
  "raw": {}
}
```

### `versions`

Authentication: bearer token required.

Payload:

```json
{}
```

Result payload lists retained source-document versions, active-version state,
checksums, model metadata, ingest status, timestamps, and conversion
diagnostics.

### `rollback`

Authentication: bearer token required.

Payload:

```json
{
  "source": "The_Elements_of_Style.pdf",
  "versionLabel": "2026-06-01T21:37:22.184Z"
}
```

Rollback validates the retained version and publishes it as the active source
document version without deleting versions, rebuilding embeddings, or changing
immutable version rows.

Result payload:

```json
{
  "sourcePath": "The_Elements_of_Style.pdf",
  "activeVersionLabel": "2026-06-01T21:37:22.184Z",
  "publishedAtMs": 1780135300000,
  "vectorCount": 43,
  "status": "rolled_back"
}
```

### `shutdown`

Authentication: bearer token required.

Payload:

```json
{}
```

Result payload:

```json
{
  "status": "shutting_down"
}
```

Accepted shutdown requests drain through the HTTP server graceful-shutdown path.

## 8. Errors

Operation failures after the stream opens must be reported as terminal `error`
events:

```json
{
  "type": "error",
  "operationId": "op-1",
  "sequence": 5,
  "stage": "docling_converting",
  "error": {
    "status": 422,
    "kind": "docling_conversion",
    "message": "failed to convert source document"
  }
}
```

Errors before the stream opens return structured HTTP error bodies with the
same `status`, `kind`, and `message` fields.

Required behavior:

- Oversized request bodies fail with `413 Payload Too Large`.
- Unknown request fields and invalid request fields fail with `400 Bad Request`.
- Missing or invalid bearer auth for protected operations fails with `401
  Unauthorized`.
- Conversion failures fail with useful diagnostics and no silent fallback.
- Inference, storage, and internal failures include enough context for an
  operator to identify the failing subsystem.
- Error responses and logs must not include bearer tokens, document contents,
  vector values, or oversized retrieval internals.

## 9. Service Lifecycle And Admin Token

- On each startup, the service generates one cryptographically random
  process-scoped admin token.
- The service keeps the token in memory and prints it once as
  `admin_shutdown_token=<token>`.
- The service config requires `[admin].token_file_path`.
- The service writes the startup token to the configured token file with
  owner-only permissions.
- Token-file creation is startup-critical.
- The service replaces stale token files from earlier runs.
- On graceful shutdown, the service removes the token file only if it still
  contains the current token.
- The token must not be stored in config, environment variables, SQLite, or the
  service log.
- Protected operations require `Authorization: Bearer <startup-token>`.
- Missing or invalid authorization fails explicitly and must not trigger
  shutdown or version changes.

Startup emits operator-visible bootstrap and startup progress to stdout before
the process is ready. Startup progress with `x/y` counters may overwrite the
current terminal line; all other startup status lines must end with a newline.

After file logging is initialized, operational service events are written to the
configured service log file for unattended/background operation.

## 10. Configuration

Service-owned config includes:

- Server bind address.
- Required request limits:
  - `server.max_request_body_bytes`
  - `server.max_ingest_source_chars`
  - `server.max_search_query_chars`
- Ingest/search admission limits.
- Logging:
  - `logging.file_path`
  - `logging.level`
- Admin token file:
  - `admin.token_file_path`
- Model paths.
- Inference device and device index.
- Docling paths and PDF defaults.
- Corpus path.
- SQLite database path.
- Cache loading policy.
- Retrieval parameters:
  - `defaultTopK`
  - `maxTopK`
  - `rrfK`
  - candidate over-fetch multiplier
  - `colbert_candidate_pool_size`
  - `minSearchUnitChars`
  - chunk sizing

`logging.file_path` is required. Absolute paths are used as-is; relative paths
resolve against the Rust service root (`service/data-store/`). The service
creates missing log parent directories before long-lived work starts and fails
before binding if the log file cannot be opened. `logging.level` is required and
controls the minimum operational event level written to the log file.

`admin.token_file_path` is required. Absolute paths are used as-is; relative
paths resolve against the Rust service root. The example config must include the
recommended explicit relative value `.data-store-admin-token`.

ColBERT reranking uses `retrieval.colbert_candidate_pool_size`, with a
recommended default of `100`. This pool is produced by dense/BM25/RRF before
ColBERT MaxSim reranking. ColBERT document token embeddings are persisted during
ingestion and loaded from SQLite during search. Search-time document-vector
recomputation is not a normal fallback path.

Service logs are human-readable structured lines with stable event fields. They
must not contain the admin token, document contents, vector values, or other
oversized retrieval internals.

## 11. Concurrency

Ingest and search each have a separate configured maximum in-flight count. When
the limit is saturated, the service emits a terminal operation `error` event
with status `503`.

The first operation-stream implementation does not need to guarantee
cancellation support, queue position, or resumable streams.

## 12. Open Validation Items

1. Dense scan performance target: benchmark around 10k, 50k, and 100k units
   before considering ANN/indexing.
