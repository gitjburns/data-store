# Data Store Service Specification

## 1. Purpose And Scope

A standalone Rust microservice that owns document ingestion, durable document
version storage, retrieval indexes, inference runtimes, and search.

The service exposes a data-store-specific operation protocol over HTTP. Source
document bytes do not cross the API; callers send corpus-relative source
references and search queries.

**In scope**

- Rust service process with `axum` and `tokio` confined to the HTTP transport
  shell.
- Operation-scoped streamed HTTP protocol.
- Document ingestion from service-owned corpus files.
- Docling conversion, unit splitting, dense embeddings, persisted ColBERT
  document vectors, SQLite storage, FTS, active-version cache, ColBERT scoring,
  and config-selected final reranking.
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
- **HTTP transport:** `axum` plus `tokio`, confined to request routing,
  response streaming, body limits, and graceful-shutdown bridging.
- **Domain execution:** synchronous operation pipelines for ingestion,
  retrieval, storage, model calls, Docling process handling, admission, and
  shutdown state.
- **Inference:** `candle` and `tokenizers` for local dense, ColBERT, and
  optional local reranker work, with CUDA or Apple Silicon Metal acceleration.
  CPU fallback is not supported for local models.
- **Storage:** SQLite owns durable documents, versions, units, metadata, vector
  blobs, ColBERT document vectors, and FTS.
- **Retrieval cache:** dense retrieval uses an explicit in-memory flat vector
  cache with exact cosine similarity over active document versions.
- **Late interaction:** ColBERT scoring runs over a bounded candidate pool and
  loads persisted document token vectors from SQLite.
- **Final ranking:** a config-selected reranker backend produces the public
  result order. Supported backends are local Candle ModernBERT and HTTP
  Cohere-compatible rerank.
- **Ownership:** the service owns corpus resolution, conversion, chunking,
  embeddings, durable storage, retrieval cache, indexes, config, and lifecycle
  controls.

## 3. Models

Local models load from `.safetensors` into accelerator memory during startup.
Startup and readiness diagnostics must make model and configured reranker
backend failures explicit. The HTTP process may still bind so health and
operation errors can report why inference is unavailable.

| Slot | Model | Key params | Formatting | Output |
|---|---|---|---|---|
| Dense | Qwen/Qwen3-Embedding-8B | 4096-d Matryoshka, 32k ctx | Query: instruct prefix; passage: raw | 1-D vector with last-token pooling |
| Late-interaction | lightonai/ColBERT-Zero | 128-d/token, about 512 ctx | `search_query:` / `search_document:` | 2-D `[num_tokens,128]` tensor, no pooling |
| Reranker | Config-selected local ModernBERT or HTTP Cohere-compatible endpoint | Backend-specific | Local sequence pair or HTTP `{model, query, documents, top_n}` | Public relevance score plus backend-dependent diagnostics |

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
- Ingesting an already-active source without `force: true` fails with
  `409 Conflict`.
- Force re-ingesting a source document never overwrites or deletes older
  versions.
- First-time ingests remain invisible to search until publish completes.
- Force re-ingests keep the previously active version searchable until the new
  version publishes.
- Failed ingest attempts must not create an active version, must not become
  search-visible, and must not require `force: true` on retry. `force: true` is
  required only when the source already has an active searchable version from a
  prior successful ingest.

Dense search cache:

- Store one contiguous `Vec<f32>` in row-major `[unit_count, dimension]` order.
- Store parallel arrays for `unitId`, `sourcePath`, `versionLabel`, stored
  vector norm, and deterministic sort keys.
- Cache load/update paths log duration, vector count, dimension, and memory
  footprint.
- Cache publish happens only after the new document version is fully durable and
  ready.
- Search captures the active cache/snapshot at request admission, before query
  embedding or later inference work, and uses that same snapshot for the
  lifetime of the request. A version published after that capture must not enter
  the already-admitted search scope.

## 5. Pipelines

### Ingestion

Ingestion blocks until the new document version is durable and, after publish,
searchable. Ingestion is atomic at the service contract boundary: it either
fully succeeds and publishes the new active version, or it fails without leaving
new successful ingested state.

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
9. If any step fails before the terminal ingest result is ready, roll back or
   remove staged durable rows and active-cache changes for that attempted
   version. A failed attempt must not leave a success-labeled retained
   `document_versions` row, units, vector rows, FTS rows, or active-version row
   that affects future duplicate checks, rollback choices, or search scope.
   Temporary conversion artifacts may be retained only as diagnostics when they
   are not treated as ingested state.
10. Surface conversion failures with diagnostics. No silent fallback across
   backends or OCR modes is allowed.

### Retrieval

1. Capture the current active source-document version map and active search
   cache snapshot at request admission, before query embedding. This captured
   snapshot is authoritative for the lifetime of the request.
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
8. The selected reranker backend rescores the configured final candidate pool.
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

### `sources`

Authentication: none.

Payload:

```json
{}
```

Result payload lists active ingested source documents:

```json
{
  "sources": [
    {
      "sourcePath": "The_Elements_of_Style.pdf",
      "activeVersionLabel": "2026-06-01T21:37:22.184Z",
      "documentId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z",
      "unitsIngested": 43,
      "status": "ingested",
      "createdAtMs": 1780135249223,
      "updatedAtMs": 1780135249223
    }
  ]
}
```

The operation must not expose retained inactive versions, absolute markdown
paths, checksums, vector metadata, or conversion diagnostics. Those remain in
the protected `versions` operation.

### `ingest`

Authentication: none.

Payload:

```json
{
  "source": "The_Elements_of_Style.pdf",
  "force": true
}
```

If `force` is absent or `false` and the resolved source already has an active
version, the operation must abort before conversion with terminal error
`status: 409`, `kind: "source_already_ingested"`, and message
`Source <source> is already ingested. Use --force to override.`

The operation must emit real server-side status/progress events for source
resolution, existing-source checking, conversion, unit splitting, dense
embedding, ColBERT embedding, and storage publish.

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
  "raw": {
    "storage": {
      "retrieval": {
        "mode": "dense_bm25_rrf_candidate_pool",
        "latencyMs": 4711,
        "queryVectorValidationLatencyMs": 0,
        "denseLatencyMs": 123,
        "bm25LatencyMs": 4201,
        "rrfFusionLatencyMs": 0,
        "candidateMaterializationLatencyMs": 365,
        "rawDiagnosticsLatencyMs": 22
      }
    },
    "reranker": {
      "mode": "modernbert_sequence_classifier",
      "scores": [
        {
          "unitId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z:unit:000000",
          "score": 0.725617,
          "rank": 1,
          "logit": 0.961434,
          "tokenCount": 512
        }
      ],
      "finalResults": [
        {
          "unitId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z:unit:000000",
          "rerankerScore": 0.725617,
          "rerankerRank": 1,
          "rerankerLogit": 0.961434,
          "rerankerTokenCount": 512
        }
      ]
    }
  }
}
```

The `raw.storage.retrieval` object must expose timing fields that break down
the `retrieving_candidates` stage into query-vector validation, dense scan,
BM25, RRF fusion, candidate materialization, and raw-diagnostics assembly.
These fields are diagnostic timing data; public search result ranking is still
the final reranker order.

The `modernbert_sequence_classifier` raw example includes local-only diagnostic
fields. For `mode: "http_rerank"`, `scores[]` omits `logit` and `tokenCount`,
and `finalResults[]` omits `rerankerLogit` and `rerankerTokenCount`. HTTP
reranker scores are the provider `relevance_score`; the service must not
synthesize logits or token counts.

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
  "status": "shutdown_complete",
  "message": "shutdown complete; service process is terminating"
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
- Duplicate ingest without `force: true` fails with `409 Conflict`.
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
- Model paths for local dense and ColBERT runtimes.
- Reranker backend configuration:
  - `models.reranker.backend`: `local` or `http`.
  - `models.reranker.path` and `models.reranker.max_tokens`, required only for
    the local ModernBERT backend.
  - `models.reranker.endpoint`, `models.reranker.model`, and
    `models.reranker.timeout_seconds`, required only for the HTTP backend.
  - `models.reranker.api_key_file_path`, optional for the HTTP backend.
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
  - `reranker_candidate_pool_size`
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

Final reranking uses `retrieval.reranker_candidate_pool_size`. The effective
pool is `max(reranker_candidate_pool_size, requested topK)`, clamped to the
available ColBERT-ranked candidates. Final public results remain limited to
`topK`.

Service logs are human-readable structured lines with stable event fields. They
must not contain the admin token, document contents, vector values, or other
oversized retrieval internals.

## 11. Concurrency

Ingest and search each have a separate configured maximum in-flight count. When
the limit is saturated, the service emits a terminal operation `error` event
with status `503`.

Separate ingest/search admission is a functional requirement, not only a
counter layout. A search admitted while an ingest is in progress must search the
already-active corpus snapshot captured at its own admission. In-progress ingest
output remains out of search scope until that ingest commits durable storage and
publishes the new active snapshot. An admitted search must not fail with `503`
solely because an ingest operation is running.

Shared accelerator and local model runtimes must be made safe for overlapping
admitted operations. The implementation may serialize individual local model
calls or small local model-call batches when required by the accelerator/runtime,
but it must not serialize whole ingest and search operations as the concurrency
mechanism. HTTP reranker network scoring must not hold the shared local
model-call gate.
For example, an ingest may yield between per-unit dense or ColBERT document
embedding calls so a search can run query embedding, ColBERT scoring, and
reranking against the captured active corpus.

Model outputs are validated at the inference boundary before storage or ranking
code consumes them. Dense vectors, ColBERT token vectors, reranker scores, and
any backend-provided reranker logits must be finite and dimensionally valid.
Non-finite model output is an inference failure with model-call diagnostics, not
a downstream storage failure.

The first operation-stream implementation does not need to guarantee
cancellation support, queue position, or resumable streams.

## 12. Open Validation Items

1. Dense scan performance target: benchmark around 10k, 50k, and 100k units
   before considering ANN/indexing.
