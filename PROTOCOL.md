# Data Store Protocol

This document is the HTTP protocol reference for consumers of the standalone
Data Store service.

## Base Protocol

The service exposes JSON HTTP endpoints. Versioned ingestion and retrieval
endpoints live under `/v1`. Admin lifecycle and version-management endpoints
live under `/admin`.

Consumers should fetch `GET /v1/limits` before their first ingest or search
request and use those limits when constructing requests.

All request bodies and responses shown here use JSON. Request DTOs reject
unknown fields.

## Authentication

Versioned `/v1` endpoints do not use bearer authentication.

Admin endpoints require:

```http
Authorization: Bearer <startup-token>
```

The startup token is printed once during service bootstrap as:

```text
admin_shutdown_token=<token>
```

Tokens are valid only for the current process lifetime.

## Errors

Errors are explicit JSON responses with HTTP status codes.

Common statuses:

| Status | Meaning |
|---:|---|
| 400 | Invalid JSON shape, unknown field, oversized field, invalid field value, or invalid rollback target |
| 401 | Missing, malformed, or invalid admin bearer token |
| 413 | Request body exceeds `request.maxRequestBodyBytes` |
| 500 | Configuration, inference, storage, conversion, or internal operation failure |
| 503 | Ingest or search admission gate is saturated |

Error message bodies are diagnostic and intended for operators and developers.
Consumers must not depend on exact error text unless a field is explicitly
documented in this protocol.

## Limits

### `GET /v1/limits`

Returns request-construction and retrieval limits.

Response:

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

Consumers should apply these limits before sending ingest or search requests.
The service still validates all requests authoritatively.

## Health

### `GET /v1/health`

Returns service readiness and component diagnostics.

Response:

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

## Ingest

### `POST /v1/ingest`

Synchronously ingests one corpus-relative source file. The request carries only
a source reference; source file bytes do not cross the HTTP API.

Request:

```json
{
  "source": "The_Elements_of_Style.pdf"
}
```

Fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `source` | string | yes | Non-empty corpus-relative source reference. Must not exceed `maxIngestSourceChars`. |

Response:

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
- The new version becomes search-visible only after storage and cache publish
  complete.
- Re-ingesting a source does not delete older versions.
- Existing active versions remain searchable until the new version publishes.
- Conversion options are service-configured; callers cannot override Docling
  backend, OCR mode, or page batch size per request.

## Search

### `POST /v1/search`

Synchronously searches active document versions.

Request:

```json
{
  "query": "clear writing style rules",
  "topK": 3
}
```

Fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `query` | string | yes | Non-empty search text. Must not exceed `maxSearchQueryChars`. |
| `topK` | integer | no | Result count. Defaults to `retrieval.defaultTopK`. Must be between `1` and `retrieval.maxTopK`. |

Response:

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
      "topK": 3,
      "admission": {
        "inFlight": 1,
        "maxInFlight": 1
      },
      "embeddingLatencyMs": 120,
      "colbertLatencyMs": 20700,
      "rerankerLatencyMs": 73500,
      "latencyMs": 103500
    },
    "storage": {
      "retrieval": {
        "mode": "dense_bm25_rrf_candidate_pool",
        "activeVersions": [
          {
            "sourcePath": "The_Elements_of_Style.pdf",
            "versionLabel": "2026-06-01T21:37:22.184Z"
          }
        ]
      }
    },
    "colbert": {
      "mode": "persisted_candidate_pool_maxsim",
      "documentVectorSource": "sqlite"
    },
    "reranker": {
      "mode": "qwen3_yes_no_candidate_rerank"
    }
  }
}
```

Result fields:

| Field | Type | Notes |
|---|---|---|
| `unitId` | string | Stable identifier for the matched retrieval unit version. |
| `score` | number | Final Qwen3 reranker yes-probability score. |
| `content` | string | Retrieval unit content. |
| `headingPath` | string array | Parsed markdown heading hierarchy for the unit. |
| `sourcePath` | string | Corpus-relative source path. |
| `pageNumbers` | integer array | Page numbers when explicit page markers were available during splitting. |

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

Consumers should treat `raw` as diagnostic data. Its top-level stage objects and
documented mode strings are stable operational signals, but individual
diagnostic fields may grow as observability improves.

## Admin Shutdown

### `POST /admin/shutdown`

Requests graceful service shutdown.

Headers:

```http
Authorization: Bearer <startup-token>
```

Response:

```json
{
  "status": "shutting_down"
}
```

Accepted shutdown drains through the HTTP server graceful-shutdown path.

## Document Version Listing

### `GET /admin/document-versions`

Lists retained source-document versions and active-version state.

Headers:

```http
Authorization: Bearer <startup-token>
```

Response:

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

## Document Version Rollback

### `POST /admin/document-versions/rollback`

Publishes an already-retained source-document version as active.

Headers:

```http
Authorization: Bearer <startup-token>
Content-Type: application/json
```

Request:

```json
{
  "source": "The_Elements_of_Style.pdf",
  "versionLabel": "2026-06-01T21:37:22.184Z"
}
```

Fields:

| Field | Type | Required | Notes |
|---|---|---:|---|
| `source` | string | yes | Corpus-relative source path matching a retained document version. Must be non-empty, must not contain leading or trailing whitespace, and must not exceed `maxIngestSourceChars`. |
| `versionLabel` | string | yes | Retained version label. Must not be empty or padded with whitespace. |

Response:

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

## Concurrency Behavior

Ingest and search are synchronous. Each endpoint has a separate configured
maximum in-flight count. When the limit is saturated, the service returns
`503 Service Unavailable` immediately.

Consumers should retry saturated requests after active work completes. The
service does not provide queue position, cancellation, or async job polling in
the current protocol.

## Source Reference Rules

Ingest source references are corpus-relative. The service rejects empty,
absolute, or parent-traversing references and resolves accepted references
inside the configured corpus root.

Source bytes are never sent in the request body.

## Compatibility Notes

The `/v1` protocol is the versioned ingestion and retrieval API. Admin endpoints
are intentionally outside `/v1` because they control process lifecycle and
retained-version state rather than document ingestion or search semantics.

Consumers should rely on documented request/response field names, status
strings, auth behavior, and version/snapshot semantics. Additional diagnostic
fields can be added to `raw`, health details, and admin diagnostics without
changing the core protocol.
