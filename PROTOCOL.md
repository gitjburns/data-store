# Data Store Protocol

This document is the consumer protocol reference for the standalone Data Store
service.

## Base Protocol

The service exposes one versioned operation endpoint:

```http
POST /v1/operations
```

Each request starts one operation. The response body is an operation-scoped
newline-delimited JSON stream. Each line is one operation event. The stream ends
after the server emits a terminal `result` or `error` event.

Request and event bodies use JSON. Request DTOs reject unknown fields unless a
payload object explicitly documents otherwise.

## Authentication

Public operations do not require bearer authentication.

Protected operations require:

```http
Authorization: Bearer <startup-token>
```

The startup token is printed once during service bootstrap as:

```text
admin_shutdown_token=<token>
```

When configured, the service also writes the same process-scoped token to its
runtime admin token file. The token remains the only admin authentication
mechanism. Tokens are valid only for the current process lifetime.

## Operation Request

Endpoint:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Request:

```json
{
  "operationId": "client-generated-id",
  "operation": "search",
  "payload": {
    "query": "clear writing style rules",
    "topK": 3
  }
}
```

Fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `operationId` | string | no | Client-generated correlation ID. If omitted, the service assigns one. |
| `operation` | string | yes | Operation name. |
| `payload` | object | yes | Operation-specific request body. Use `{}` for operations with no parameters. |

The `operationId` is an opaque correlation value. Consumers must not infer
ordering, timing, or operation type from it.

## Operation Events

Every response line is one JSON object with a `type` field.

Common fields:

| Field | Type | Notes |
|---|---|---|
| `type` | string | Event type: `status`, `progress`, `result`, or `error`. |
| `operationId` | string | Correlation ID for the operation. |
| `sequence` | integer | Monotonic sequence number within the operation stream, starting at 1. |
| `stage` | string | Current operation stage when applicable. |
| `message` | string | Human-readable status text when applicable. |

### Status Event

Status events mark named phases. They are newline-worthy in human interfaces.

```json
{
  "type": "status",
  "operationId": "op-1",
  "sequence": 1,
  "stage": "docling_converting",
  "message": "converting source document"
}
```

### Progress Event

Progress events report repeated counted work. Human interfaces may overwrite
the current line while `current` and `total` are present.

```json
{
  "type": "progress",
  "operationId": "op-1",
  "sequence": 8,
  "stage": "dense_embedding",
  "message": "embedding document units",
  "current": 12,
  "total": 80
}
```

### Result Event

`result` is terminal. The stream ends after this event.

```json
{
  "type": "result",
  "operationId": "op-1",
  "sequence": 17,
  "payload": {
    "status": "ingested",
    "unitsIngested": 80
  }
}
```

### Error Event

`error` is terminal. The stream ends after this event.

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

Error fields:

| Field | Type | Notes |
|---|---|---|
| `status` | integer | HTTP-equivalent status code for the failed operation. |
| `kind` | string | Stable machine-readable error kind. |
| `message` | string | Diagnostic operator-facing message. |

Consumers should display `kind`, `status`, and `message`. Consumers must not
depend on exact message text unless this protocol explicitly documents that text
as stable.

## HTTP Status Codes

Transport-level HTTP success means the operation stream was accepted and opened.
Operation failure is reported by a terminal `error` event.

Common transport statuses:

| Status | Meaning |
|---:|---|
| 200 | Operation stream opened. Read events for operation result or error. |
| 400 | Invalid operation request envelope. |
| 401 | Missing, malformed, or invalid bearer token for a protected operation. The stream is not opened. |
| 413 | Request body exceeds `request.maxRequestBodyBytes`. |
| 500 | Internal failure before an operation stream could be opened. |

Common operation error statuses:

| Status | Meaning |
|---:|---|
| 400 | Invalid payload shape, unknown field, oversized field, invalid field value, or invalid rollback target. |
| 409 | Ingest source already has an active version and the request did not set `force: true`. |
| 413 | Request body exceeds `request.maxRequestBodyBytes`. |
| 422 | Source conversion failed. |
| 500 | Configuration, inference, storage, or internal operation failure. |
| 503 | Ingest or search admission gate is saturated. |

## Control Messages

The service reserves a companion endpoint for client-to-server operation
control:

```http
POST /v1/operations/{operationId}/control
Content-Type: application/json
```

Control request:

```json
{
  "type": "cancel"
}
```

The first implementation may reject unsupported control messages explicitly.
The endpoint exists so future operations can support cancellation or
mid-operation input without changing the streaming response protocol.

## Operations

### `limits`

Authentication: none.

Payload:

```json
{}
```

Result payload:

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

Consumers should apply these limits before sending ingest or search operations.
The service still validates all requests authoritatively.

### `health`

Authentication: none.

Payload:

```json
{}
```

Result payload:

```json
{
  "service": "data-store",
  "ready": true,
  "components": [
    {
      "name": "inference",
      "ready": true,
      "details": ["readiness-critical", "device ready: metal:0"]
    },
    {
      "name": "storage_cache",
      "ready": true,
      "details": ["readiness-critical", "vectors 43"]
    }
  ]
}
```

The top-level `ready` flag depends on readiness-critical components. Diagnostic
components, such as admission counters and logging state, can be present
without controlling top-level readiness.

The inference component includes accelerator, model-artifact, model-load,
configured reranker backend, and startup smoke diagnostics. ColBERT readiness
includes a max-capacity document encoding smoke check so long-sequence
document-vector failures are reported before ingest is accepted as ready. The
configured reranker backend also performs startup smoke scoring; an unreachable
HTTP reranker endpoint makes inference unready rather than falling back to the
local backend.

### `sources`

Authentication: none.

Payload:

```json
{}
```

Result payload:

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

Source listing returns active ingested source documents only. It is the public
inventory view for callers that need to inspect which files are currently
ingested. Retained inactive versions, checksums, absolute markdown paths, vector
metadata, and conversion diagnostics remain part of the protected `versions`
operation.

### `ingest`

Authentication: none.

Payload:

```json
{
  "source": "The_Elements_of_Style.pdf",
  "force": true
}
```

Payload fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `source` | string | yes | Non-empty corpus-relative source reference. Must not exceed `maxIngestSourceChars`. |
| `force` | boolean | no | Defaults to `false`. When `true`, ingest creates and publishes a new immutable version even if the source already has an active version. |

Representative event sequence:

```json
{"type":"status","operationId":"op-1","sequence":1,"stage":"source_resolving","message":"resolving source reference"}
{"type":"status","operationId":"op-1","sequence":2,"stage":"existing_source_checking","message":"checking existing source version"}
{"type":"status","operationId":"op-1","sequence":3,"stage":"docling_converting","message":"converting source document"}
{"type":"progress","operationId":"op-1","sequence":4,"stage":"docling_converting","message":"converting source document","current":42,"total":100}
{"type":"status","operationId":"op-1","sequence":5,"stage":"unit_splitting","message":"splitting document into retrieval units"}
{"type":"progress","operationId":"op-1","sequence":6,"stage":"unit_splitting","message":"retrieval units ready","current":43,"total":43}
{"type":"status","operationId":"op-1","sequence":7,"stage":"dense_embedding","message":"embedding document units"}
{"type":"progress","operationId":"op-1","sequence":8,"stage":"dense_embedding","message":"embedding document units","current":12,"total":43}
{"type":"status","operationId":"op-1","sequence":9,"stage":"colbert_embedding","message":"embedding ColBERT document vectors"}
{"type":"progress","operationId":"op-1","sequence":10,"stage":"colbert_embedding","message":"embedding ColBERT document vectors","current":12,"total":43}
{"type":"status","operationId":"op-1","sequence":11,"stage":"storage_publishing","message":"publishing document version"}
{"type":"progress","operationId":"op-1","sequence":12,"stage":"storage_publishing","message":"persisting units and vectors","current":12,"total":43}
{"type":"progress","operationId":"op-1","sequence":13,"stage":"storage_publishing","message":"publishing active search snapshot","current":1,"total":1}
```

Result payload:

```json
{
  "documentId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z",
  "versionLabel": "2026-06-01T21:37:22.184Z",
  "unitsIngested": 43,
  "status": "ingested"
}
```

Ingest semantics:

- Every successful ingest creates a new immutable source-document version.
- If the resolved source already has an active version and `force` is absent or
  `false`, the service aborts before conversion with a terminal error:
  `status: 409`, `kind: "source_already_ingested"`, and stable message
  `Source <source> is already ingested. Use --force to override.`
- If `force` is `true`, the service permits re-ingest and publishes the new
  immutable version after the normal storage/cache publish boundary.
- The new version becomes search-visible only after storage and cache publish
  complete.
- Force re-ingesting a source does not delete older versions.
- Existing active versions remain searchable until the new version publishes.
- Conversion options are service-configured; callers cannot override Docling
  backend, OCR mode, page batch size, or document timeout per request.
- The service emits Docling percentage progress when Docling reports it, unit
  counts after splitting, dense and ColBERT embedding counts, and storage
  publish checkpoints.

### `search`

Authentication: none.

Payload:

```json
{
  "query": "clear writing style rules",
  "topK": 3
}
```

Payload fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `query` | string | yes | Non-empty search text. Must not exceed `maxSearchQueryChars`. |
| `topK` | integer | no | Result count. Defaults to `retrieval.defaultTopK`. Must be between `1` and `retrieval.maxTopK`. |

Representative event sequence:

```json
{"type":"status","operationId":"op-2","sequence":1,"stage":"embedding_query","message":"embedding search query"}
{"type":"status","operationId":"op-2","sequence":2,"stage":"retrieving_candidates","message":"retrieving candidate units"}
{"type":"progress","operationId":"op-2","sequence":3,"stage":"colbert_scoring","message":"scoring ColBERT candidates","current":12,"total":80}
{"type":"progress","operationId":"op-2","sequence":4,"stage":"reranking","message":"reranking candidates","current":12,"total":40}
```

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
    "search": {
      "mode": "dense_bm25_rrf_colbert_reranker",
      "topK": 3
    },
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
  }
}
```

Search semantics:

- The service captures the active document-version snapshot at request
  admission.
- The same snapshot is used for dense scan, BM25 filtering, candidate
  materialization, persisted ColBERT vector loading, reranking, and raw
  diagnostics.
- Public ranking is final reranker order.
- `raw` preserves stage diagnostics for dense retrieval, BM25, RRF, ColBERT,
  reranker, final-result provenance, latency, cache metadata, and active
  versions.

Retrieval raw diagnostics itemize the `retrieving_candidates` operation stage:

- `raw.retrieval.latencyMs` is the server-side candidate-pool duration.
- `queryVectorValidationLatencyMs` is dense query-vector validation time.
- `denseLatencyMs` is exact dense scan time over the active vector cache.
- `bm25LatencyMs` is SQLite FTS5 BM25 query time.
- `rrfFusionLatencyMs` is dense/BM25 Reciprocal Rank Fusion time.
- `candidateMaterializationLatencyMs` is the time to load fused unit metadata,
  content, and persisted ColBERT document vectors.
- `rawDiagnosticsLatencyMs` is raw retrieval diagnostics assembly time.

Reranker raw diagnostics are backend-dependent:

- `raw.reranker.mode` is `modernbert_sequence_classifier` for the local
  ModernBERT backend and `http_rerank` for an HTTP Cohere-compatible backend.
- `raw.reranker.scores[]` always includes `unitId`, `score`, and `rank`.
  `logit` and `tokenCount` appear only when the backend provides them.
- `raw.reranker.finalResults[]` always includes `unitId`, `rerankerScore`, and
  `rerankerRank` plus earlier-stage provenance. `rerankerLogit` and
  `rerankerTokenCount` appear only when the backend provides them.
- HTTP reranker scores use the provider `relevance_score` as the public score;
  the service does not synthesize logits or token counts.

Consumers should treat `raw` as diagnostic data. Its top-level stage objects and
documented mode strings are stable operational signals, but individual
diagnostic fields may grow as observability improves.

### `versions`

Authentication: bearer token required.

Payload:

```json
{}
```

Result payload:

```json
{
  "sources": [
    {
      "sourcePath": "The_Elements_of_Style.pdf",
      "activeVersionLabel": "2026-06-01T21:37:22.184Z",
      "versions": [
        {
          "versionLabel": "2026-06-01T21:37:22.184Z",
          "documentId": "the-elements-of-style-pdf__2026-06-01T21-37-22-184Z",
          "isActive": true,
          "sourceSha256": "hex digest or null",
          "markdownPath": "/absolute/path/to/generated.md",
          "markdownSha256": "hex digest or null",
          "pdfBackend": "docling_parse",
          "ocrMode": "auto",
          "pageBatchSize": 10,
          "unitsIngested": 43,
          "status": "ingested",
          "diagnostics": {},
          "createdAtMs": 1780135249223,
          "updatedAtMs": 1780135249223,
          "denseVectorMetadata": [
            {
              "modelPath": "/absolute/path/to/qwen3-embedding-8b",
              "modelDimension": 4096,
              "pooling": "last_token",
              "format": "f32_le",
              "vectorCount": 43
            }
          ],
          "colbertVectorMetadata": [
            {
              "modelPath": "/absolute/path/to/colbert-zero",
              "modelDimension": 128,
              "format": "f32_le",
              "vectorCount": 43
            }
          ]
        }
      ]
    }
  ]
}
```

Version listing is diagnostic and administrative. It can expose absolute paths
inside the service environment.

### `rollback`

Authentication: bearer token required.

Payload:

```json
{
  "source": "The_Elements_of_Style.pdf",
  "versionLabel": "2026-06-01T21:37:22.184Z"
}
```

Payload fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `source` | string | yes | Corpus-relative source path matching a retained document version. Must be non-empty, must not contain leading or trailing whitespace, and must not exceed `maxIngestSourceChars`. |
| `versionLabel` | string | yes | Retained version label. Must not be empty or padded with whitespace. |

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

Rollback semantics:

- The target retained version must already exist.
- The service validates retained dense vectors before publishing.
- Rollback updates `active_document_versions` and swaps the active dense cache.
- Rollback does not delete versions, rebuild embeddings, alter immutable
  version rows, or perform automatic cleanup.

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

The `shutdown_complete` result is the server-authored terminal confirmation
emitted as the final operation result before process termination. If shutdown
cannot be requested, the operation emits a terminal `error` event with the
reason. Accepted shutdown drains through the HTTP server graceful-shutdown path.

## Concurrency Behavior

Ingest and search each have a separate configured maximum in-flight count. When
the limit is saturated, the service emits a terminal `error` event with status
`503`.

Consumers should retry saturated operations after active work completes. The
first implementation does not guarantee cancellation support, queue position, or
resumable streams.

## Source Reference Rules

Ingest source references are corpus-relative. The service rejects empty,
absolute, or parent-traversing references and resolves accepted references
inside the configured corpus root.

Source bytes are never sent in the request body.
