# PROTOCOL.md — HTTP Contract of Record

This document is the authoritative HTTP wire contract for the data-store service.
It describes every route in the live router, the exact request and response body
shapes (with the field names as they appear on the wire), the status codes, and
the error body. Where a name on the wire differs from its Rust field name, the
wire name is the one given here.

For narrative and rationale, see `SPEC-SERVER.md` and `SPEC-CLIENT.md`; this file
is the contract they defer to for wire truth. `README.md` points here as the
contract of record.

There is no NDJSON, no streaming, and no chunked operation feed anywhere in this
API. Every response is a single JSON body. Long-running admin work is modeled as
an asynchronous **Operation** you poll (see [Operations](#operations)).

---

## Table of contents

- [Base and content type](#base-and-content-type)
- [Authentication](#authentication)
- [Error body](#error-body)
  - [Error kinds and status codes](#error-kinds-and-status-codes)
- [Route summary](#route-summary)
- [Public routes](#public-routes)
  - [`GET /v1/health`](#get-v1health)
  - [`POST /query`](#post-query)
  - [`GET /units/{unitId}`](#get-unitsunitid)
  - [`GET /units/{unitId}/relationships`](#get-unitsunitidrelationships)
  - [`GET /sources/{sourceId}`](#get-sourcessourceid)
  - [`GET /sync/status`](#get-syncstatus)
- [Protected routes](#protected-routes)
  - [`POST /sources`](#post-sources)
  - [`POST /sources/{sourceId}/parses`](#post-sourcessourceidparses)
  - [`POST /sources/{sourceId}/parses/{parseId}/activate`](#post-sourcessourceidparsesparseidactivate)
  - [`POST /parses/{parseId}/accept`](#post-parsesparseidaccept)
  - [`POST /parses/{parseId}/discard`](#post-parsesparseiddiscard)
  - [`POST /snapshots`](#post-snapshots)
  - [`POST /restore`](#post-restore)
  - [`POST /shutdown`](#post-shutdown)
  - [`GET /parses`](#get-parses)
  - [`GET /operations/{operationId}`](#get-operationsoperationid)
  - [`GET /annotations/vocabulary`](#get-annotationsvocabulary)
- [Operations](#operations)
  - [Polling model](#polling-model)
  - [`succeeded` is not a parse verdict](#succeeded-is-not-a-parse-verdict)

---

## Base and content type

All request and response bodies are `application/json`. Request bodies for the
routes that accept one must be valid JSON; the JSON envelopes reject unknown
fields (`deny_unknown_fields`) — see the note under each route.

There is a global request-body size limit. A body over that limit is rejected
with HTTP `413` (`payload_too_large`) when the handler decodes the body. On
protected routes the bearer token is checked **before** the body is decoded, so
an oversized request without valid credentials gets `401`, not `413`.

---

## Authentication

Routes split into **public** (no credentials) and **protected** (bearer token
required). The split is fixed by the router and is listed in the
[route summary](#route-summary) below.

The token is the **admin token**. It is generated fresh at service startup and
written to an owner-only token file at the path configured under
`admin.token_file_path`. Read the token from that file to authenticate.

Protected routes require an `Authorization` header carrying the token as a bearer
credential:

```
Authorization: Bearer <admin-token>
```

Notes on how the header is parsed (all failures below return `401`
`unauthorized`):

- The header name is `authorization` (case-insensitive per HTTP).
- The value must begin with the literal prefix `Bearer ` (the word `Bearer`
  followed by a single space).
- The token must be non-empty and must not have leading or trailing whitespace.
- A missing header, a non-UTF-8 header, a missing/mismatched `Bearer ` prefix, a
  malformed token, or a token that does not match the admin token all yield
  `401`.

Authorization is checked **first inside each protected handler** — it is not a
router-wide middleware layer. Every protected handler validates the token before
doing any other work, including query-parameter validation, so an unauthorized
request never touches storage or enqueues work. Public routes never check for a
token; sending one to a public route has no effect.

The token value is never logged and never appears in any error message.

---

## Error body

Every error response, on every route, uses this exact envelope:

```json
{
  "error": {
    "status": 400,
    "kind": "bad_request",
    "message": "queryText must not be empty"
  }
}
```

- `status` (`u16`): the numeric HTTP status, identical to the HTTP status line.
- `kind` (string, `snake_case`): a stable machine-readable error class. Branch on
  this, not on `message`.
- `message` (string): human-readable detail. Free text; do not parse it.

The HTTP status code of the response always equals the `status` field.

### Error kinds and status codes

The full set of error kinds and their HTTP statuses, as emitted by the service:

| `kind`                          | HTTP status | Notes |
|---------------------------------|-------------|-------|
| `bad_request`                   | 400 | Malformed request, failed validation, or an unknown/unsupported field. |
| `source_resolution`             | 400 | Source-coordinate resolution failure. |
| `unauthorized`                  | 401 | Missing/malformed bearer token or token mismatch (protected routes). |
| `payload_too_large`             | 413 | Request body exceeds the global size limit. |
| `docling_conversion`            | 422 | Document conversion failure during ingest/parse. |
| `not_found`                     | 404 | Addressed resource absent (or, for unit reads, not served — see below). |
| `service_unavailable`           | 503 | Capacity/admission saturation or a subsystem temporarily unavailable. |
| `cutover_barrier_active`        | 503 | Query rejected because the targeted source is mid-cutover. **Retryable** — the barrier holds only for the last milliseconds of a cutover; retry the request. |
| `config_read`                   | 500 | |
| `config_parse`                  | 500 | |
| `invalid_config`                | 500 | |
| `invalid_cli`                   | 500 | |
| `inference_init`                | 500 | |
| `docling_unavailable`           | 500 | |
| `internal_io`                   | 500 | |
| `unit_splitting`                | 500 | |
| `storage_init`                  | 500 | |
| `storage_operation`             | 500 | |
| `annotation_producer`           | 500 | External annotation-model call/parse failure. |
| `snapshot_verification_failed`  | **500** | Snapshot integrity gate failed. Note: 500, not 503. |
| `restore_failed`                | **500** | Restore path failed. Note: 500, not 503. |

`snapshot_verification_failed` and `restore_failed` are lifecycle-integrity
failures and are reported as `500`, not `503`. Only `cutover_barrier_active`
(and general `service_unavailable`) are retryable.

---

## Route summary

| Method | Path | Auth | Body → Response |
|--------|------|------|-----------------|
| GET  | `/v1/health`                                    | public    | — → `HealthResponse` |
| POST | `/query`                                         | public    | `QueryRequest` → `{ evidencePack }` |
| GET  | `/units/{unitId}`                                | public    | — → `ContentUnit` |
| GET  | `/units/{unitId}/relationships`                  | public    | — → `{ relationships }` |
| GET  | `/sources/{sourceId}`                            | public    | — → `SourceObject` |
| GET  | `/sync/status`                                   | public    | — → `SyncStatusResponse` |
| POST | `/sources`                                       | protected | `IngestRequest` → `202 { operationId }` |
| POST | `/sources/{sourceId}/parses`                     | protected | `IngestRequest` → `202 { operationId }` |
| POST | `/sources/{sourceId}/parses/{parseId}/activate`  | protected | — → `202 { operationId }` |
| POST | `/parses/{parseId}/accept`                       | protected | — → `202 { operationId }` |
| POST | `/parses/{parseId}/discard`                      | protected | — → `202 { operationId }` |
| POST | `/snapshots`                                     | protected | `SnapshotRequest` → `202 { operationId }` |
| POST | `/restore`                                       | protected | `RestoreRequest` → `202 { operationId }` |
| POST | `/shutdown`                                       | protected | — → `202` (no body) |
| GET  | `/parses?status=held`                            | protected | — → `{ parses }` |
| GET  | `/operations/{operationId}`                      | protected | — → `Operation` |
| GET  | `/annotations/vocabulary?annotationType=…&scope=…` | protected | — → `EntityVocabularyResponse` \| `RelationVocabularyResponse` |

---

## Public routes

### `GET /v1/health`

Service readiness and startup diagnostics. Public.

**Response `200`** — `HealthResponse` (field names are `snake_case` on the wire):

```json
{
  "service": "data-store",
  "ready": true,
  "components": [
    {
      "name": "inference",
      "ready": true,
      "details": ["..."],
      "counts": []
    },
    {
      "name": "sync",
      "ready": true,
      "details": ["..."],
      "counts": []
    },
    {
      "name": "fabric",
      "ready": true,
      "details": ["..."],
      "counts": [
        { "label": "held", "source_system": "s3", "value": 2, "as_of": "2026-07-17T12:00:00Z" }
      ]
    }
  ]
}
```

- `service` (string).
- `ready` (bool): top-level service readiness.
- `components` (array of component objects):
  - `name` (string).
  - `ready` (bool).
  - `details` (array of strings): free-form diagnostic lines.
  - `counts` (array): typed diagnostic counters. Empty array for components that
    publish none — the `inference` and `sync` components' counts are always
    empty; per-source-system counts (labels `held`, `serving_stale`,
    `access_lost`, `stuck_building`, `unparseable_mime`, `verification_halted`)
    are published by the `fabric` component.
    - `label` (string): what is being counted (e.g. `"held"`).
    - `source_system` (string, **omitted when absent**): scopes a fabric count to
      one source-system; omitted for corpus-aggregate counts.
    - `value` (`u64`).
    - `as_of` (string): the RFC3339 timestamp / cycle marker the count was
      measured at. Every count carries one.

Readiness (`ready`) is determined by the `inference` and `sync` components. The
other components (`logging`, `fabric`, `annotation`, `search_admission`) are
diagnostic-only and do not gate top-level readiness. `GET /v1/health` is the single aggregation
surface for corpus/fabric counts.

```bash
curl -s http://localhost:PORT/v1/health
```

---

### `POST /query`

Run one synchronous retrieval + assembly query and return the assembled
EvidencePack. Public.

**Request body** — `QueryRequest` (`camelCase`, `deny_unknown_fields`):

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `queryText` | string | **yes** | The natural-language query. |
| `callerContext` | any JSON | no | Opaque passthrough; accepted and validated as well-formed JSON, not interpreted or persisted in the MVP. |
| `constraints` | object | no | Scope constraints (see below). |
| `retrievalPolicy` | object | no | Per-request retrieval overrides (see below). |
| `evidencePolicy` | object | no | Per-request output toggles (see below). |
| `debug` | bool | no | When `true`, attach raw per-stage diagnostics to the response. |

`constraints` (`deny_unknown_fields`):

| Field | Type | Meaning |
|-------|------|---------|
| `sourceIds` | array of string | Restrict retrieval to these source ids. |
| `governanceDomains` | array of string | Restrict to these governance domains. |

Both are optional; both empty (or `constraints` omitted) means all-sources scope.
When **both** are present they intersect: retrieval is confined to sources that
are in `sourceIds` **and** have a current location in one of the
`governanceDomains` (a source-set scope carrying both predicates). An empty
intersection yields an empty result, not an error.

`retrievalPolicy` (`deny_unknown_fields`):

| Field | Type | Meaning |
|-------|------|---------|
| `maxFinalEvidenceUnits` | `u32` | Final evidence-unit count. When present must be `1..=max_top_k` of the active retrieval profile; when absent, the profile default is used. |

`evidencePolicy` (`deny_unknown_fields`):

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `includeSourceLocators` | bool | `true` | Include per-unit source locators. |
| `includeRelationships` | bool | `false` | Include traversed relationships in the pack. |
| `includeAnnotations` | bool | `false` | Include semantic annotations for selected units. |

**Deliberately unsupported fields.** The `QueryRequest` envelope and every nested
object use `deny_unknown_fields`. The following §24.3 spec fields are
intentionally **not** implemented in the MVP query surface, and a request that
names any of them is rejected as an unknown field with `400` `bad_request`:

```
contentTypes, timeRange, metadataFilters, sourceSystems,
freshness, channels, rerank, includeContradictions, includeFreshnessMetadata
```

These can be added additively later without breaking existing callers; their
absence is a deliberate MVP narrowing, not a gap to work around.

**Validation rejections** (all `400` `bad_request`):

- `queryText` empty after trimming whitespace → `"queryText must not be empty"`.
- `queryText` longer than the configured character cap →
  `"queryText exceeds maximum length of N characters"`.
- `maxFinalEvidenceUnits` outside `1..=max_top_k` →
  `"maxFinalEvidenceUnits must be between 1 and N"`.

Under capacity saturation the query is rejected with `503`
(`service_unavailable`). If the targeted source is mid-cutover the query is
rejected with `503` `cutover_barrier_active` (retryable).

**Response `200`** — `{ "evidencePack": EvidencePack }`, plus `diagnostics` only
when the request set `debug: true`:

```json
{
  "evidencePack": {
    "queryId": "...",
    "queryText": "...",
    "evidenceUnits": [
      {
        "unitId": "...",
        "sourceId": "...",
        "parseId": "...",
        "contentType": "...",
        "body": { },
        "textProjection": "...",
        "locators": [ ],
        "score": 0.87,
        "reasons": [ ]
      }
    ],
    "relationships": [ ],
    "annotations": [ ],
    "assemblyTrace": { },
    "createdAt": "2026-07-17T12:00:00Z"
  }
}
```

`EvidencePack` top-level fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `queryId` | string | always (per-query correlation handle, **not** an audit-record id) |
| `queryText` | string | always (echoed) |
| `evidenceUnits` | array of `EvidenceUnit` | always |
| `relationships` | array of `UnitRelationship` (see [`GET /units/{unitId}/relationships`](#get-unitsunitidrelationships)) | **omitted** unless `includeRelationships` was set |
| `annotations` | array | **omitted** unless `includeAnnotations` was set |
| `assemblyTrace` | object | always |
| `createdAt` | string | always |

`EvidenceUnit` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `unitId` | string | always |
| `sourceId` | string | always |
| `parseId` | string | always |
| `contentType` | string | always |
| `body` | JSON | always |
| `textProjection` | string | omitted when the unit has no text projection |
| `locators` | array | omitted unless `includeSourceLocators` was set and the unit has any |
| `score` | number | omitted for non-anchor units |
| `reasons` | array of string | omitted when absent |

`body` is typed per `contentType` and `locators` entries are the Locator union
— see the [`ContentUnit`](#get-unitsunitid) table for the wire values and the
owning model files.

`assemblyTrace` — `ContextAssemblyTrace` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `assemblyPolicyId` | string | always |
| `assemblyPolicyVersion` | string | always |
| `assemblyPolicyHash` | string | always |
| `inputHitIds` | array of string | always — input hit ids in rank order |
| `appliedRules` | array of `AppliedAssemblyRule` | always |
| `selectedUnitIds` | array of string | always — selected unit ids in pack order |
| `rejectedHitIds` | array of string | omitted when absent — hits rejected before selection |
| `rejectedUnitIds` | array of string | omitted when absent — units dropped after resolution |
| `budget` | object | always — the budget in force (below) |

`AppliedAssemblyRule` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `ruleId` | string | always |
| `anchorUnitId` | string | omitted when absent (unit-grained anchor) |
| `anchorHitId` | string | omitted when absent (hit-grained anchor) |
| `addedUnitIds` | array of string | always |
| `reason` | string enum | always — `anchor` \| `required_completion` \| `structural_context` \| `explicit_reference` \| `local_continuity` |

`budget` fields (`camelCase`): `maxEvidenceUnits` (`u32`, always), `maxTokens`
(`u32`, always), `maxExpansionDepth` (`u32`, always), `maxReferencedUnits`
(`u32`, omitted when absent).

**`diagnostics`** (`camelCase`) — present **only** when the request set
`debug: true`. Per the code, this is the raw per-stage retrieval diagnostics: a
serializable projection of the pipeline's in-memory stage outputs.

| Field | Type | Meaning |
|-------|------|---------|
| `fusedPool` | array of `RetrievalHit` | The fused dense+lexical+graph candidate pool the rerankers scored over. |
| `maxsim` | array | ColBERT MaxSim scores over the fused pool, best-first. Entries: `unitId` (string), `score` (number), `rank` (integer). |
| `reranked` | array | Final reranker scores over the MaxSim top-N, best-first. Entries: `unitId` (string), `score` (number), `rank` (integer), `logit` (number, omitted when absent), `tokenCount` (integer, omitted when absent). |
| `latencies` | object | Per-stage wall-clock latencies in milliseconds (all `u64`, always): `openTransactionMs`, `captureMs`, `queryEmbedMs`, `denseLexicalFusionMs`, `graphMs`, `maxsimMs`, `rerankMs`, `assemblyMs`, `snapshotHeldMs`. |

`RetrievalHit` entries (`camelCase`): `hitType` (`chunk` \| `content_unit` \|
`semantic_annotation` \| `retrieval_projection`, always), `hitId` (string,
always), `sourceId` (string, always), `parseId` (string, always), `unitIds`
(array of string, always), `channel` (`dense` \| `lexical` \| `graph`, always),
`score` (number, always), `rank` (integer, omitted when absent),
`matchedProjectionId` (string, omitted when absent), `matchedAnnotationId`
(string, omitted when absent), `explanation` (string, omitted when absent).

**Recorded deviation:** the §34.1 spec-literal response is "the EvidencePack plus
the `queryExecutionRecordId`". `queryExecutionRecordId` is **omitted** here — the
QueryExecutionRecord (QER) audit tier is deferred post-MVP and no QER is written,
so there is no id to return. `queryId` on the pack is a correlation handle only,
not a QER id. The field can be added additively when the QER tier lands.

```bash
curl -s http://localhost:PORT/query \
  -H 'Content-Type: application/json' \
  -d '{"queryText":"how does cutover work","retrievalPolicy":{"maxFinalEvidenceUnits":10}}'
```

---

### `GET /units/{unitId}`

Serve one canonical ContentUnit. Public.

A unit is served **only** when the parse derived from its parse-scoped id is the
**active** parse of the owning source (§14). A unit whose parse is not active
(e.g. a held or superseded parse) is not queryable and returns `404`
`not_found` — indistinguishable from a unit that does not exist at all. This is
by design: a non-active parse's units must not be observable through the read
API.

**Response `200`** — the `ContentUnit` object (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `id` | string | always |
| `sourceId` | string | always |
| `parseId` | string | always |
| `contentType` | string enum | always — see values below |
| `bodyHash` | string | always |
| `textHash` | string | omitted when absent |
| `structureHash` | string | omitted when absent |
| `primaryParentId` | string | omitted when absent — convenience field; the canonical structure is the `UnitRelationship` graph |
| `sequenceIndex` | `u64` | omitted when absent — convenience field, as above |
| `locators` | array of Locator | omitted when absent |
| `body` | JSON | always — typed per `contentType` |
| `createdAt` | string | always |
| `deletedAt` | string | omitted when absent |

`contentType` wire values (`snake_case`, closed set):

```
page, text_section, text_block, table, table_row, table_cell,
figure, caption, image_region, code_block
```

`body` is typed per `contentType`; the typed bodies serialize as their model
structs in `src/model/body.rs`. `locators` entries are the closed Locator
union, discriminated by a `kind` field with these wire values (`snake_case`;
per-kind payload fields live in `src/model/locator.rs`):

```
page_bbox, char_range, byte_range, time_range,
dom_path, xml_path, table_cell, repo_path
```

**Errors:** `404` `not_found` (`"no active unit <unitId>"`) when the unit is
absent or its parse is not active.

```bash
curl -s http://localhost:PORT/units/PARSE_ID:unit:3
```

---

### `GET /units/{unitId}/relationships`

Structural edges touching one unit, subject to the same §14 active-parse gate as
the unit read. Public.

**Query parameters:**

| Param | Values | Default | Meaning |
|-------|--------|---------|---------|
| `direction` | `out`, `in` | both | `out` = edges from this unit; `in` = edges to this unit; absent = both. |
| `relationshipType` | string | any | Restrict to one relationship-type wire name (e.g. `contains`). |

An invalid `direction` value is rejected `400` `bad_request`
(`"direction must be 'out' or 'in'; got ..."`).

**Response `200`** — `{ "relationships": [ UnitRelationship, ... ] }`, ordered by
`sequenceIndex` then id.

`UnitRelationship` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `id` | string | always |
| `sourceId` | string | always |
| `parseId` | string | always |
| `fromUnitId` | string | always |
| `toUnitId` | string | always |
| `relationshipType` | string enum | always — see values below |
| `relationshipRole` | string | omitted when absent |
| `sequenceIndex` | `u64` | omitted when absent |
| `confidence` | number | omitted when absent |
| `provenance` | object | omitted when absent — Provenance (producer identity/lineage; fields in `src/model/provenance.rs`) |
| `createdAt` | string | always |
| `deletedAt` | string | omitted when absent |

`relationshipType` wire values (`snake_case`, closed set):

```
contains, physically_contains, logically_contains, precedes, follows,
appears_on, caption_of, has_caption, references, continues_on, derived_from
```

**Errors:** `404` `not_found` (`"no active unit <unitId>"`) when the anchor unit
is absent or not active.

```bash
curl -s 'http://localhost:PORT/units/PARSE_ID:unit:3/relationships?direction=out&relationshipType=contains'
```

---

### `GET /sources/{sourceId}`

Return one source object with its full location set and freshness. Public.

**Response `200`** — the `SourceObject` (includes its `locations`). Fields
(`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `id` | string | always |
| `activeParseId` | string | omitted when absent — absent while no parse has activated or after deactivation |
| `mimeType` | string | always |
| `sizeBytes` | `u64` | omitted when absent |
| `sourceHash` | string | always |
| `storageUri` | string | always |
| `locations` | array of `SourceLocation` | always |
| `eventTime` | string | omitted when absent |
| `ingestTime` | string | always |
| `createdAt` | string | always |
| `deactivatedAt` | string | omitted when absent — set when zero `current` locations remain |

`SourceLocation` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `id` | string | always |
| `sourceSystem` | string | always |
| `nativeUri` | string | always |
| `nativeId` | string | omitted when absent |
| `governanceDomain` | string | always |
| `firstSeenAt` | string | always |
| `lastSeenAt` | string | always |
| `status` | string enum | always — `current` \| `deleted` \| `access_lost` (`access_lost` is not deletion) |
| `deletionEvidence` | object | omitted when absent — see below |
| `metadata` | object | omitted when absent — per-location descriptive metadata |

`deletionEvidence` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `signal` | string enum | always — `explicit_delete_event` \| `absent_from_complete_enumeration` \| `source_reported_gone` |
| `observedAt` | string | always |
| `acquisitionRecordId` | string | always |
| `detail` | string | omitted when absent |

**Errors:** `404` `not_found` (`"no source <sourceId>"`) when the source does not
exist.

```bash
curl -s http://localhost:PORT/sources/SOURCE_ID
```

---

### `GET /sync/status`

The last-published sync-scheduler health snapshot (§9.5–§9.6). Public.

This route serves the published `SyncHealth` snapshot **only**. It does not carry
a fabric-counts projection — corpus/fabric counts live on `GET /v1/health`, the
single aggregation surface.

**Response `200`** — `SyncStatusResponse` (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `fabricReady` | bool | always |
| `detail` | string | **omitted when absent** — why the subsystem is not ready, or the most recent cycle-level error |
| `pending` | `u64` | always — queue backlog: pending |
| `inFlight` | `u64` | always — queue backlog: in flight |
| `failed` | `u64` | always — queue backlog: failed |
| `coalescedTotal` | `u64` | always — later detections coalesced into queued rows |
| `cadenceMs` | `u64` | **omitted when absent** — current effective detection cadence (ms) |
| `lastSuccessAt` | string | **omitted when absent** — RFC3339 timestamp of the last fully successful cycle |

```json
{
  "fabricReady": true,
  "pending": 0,
  "inFlight": 0,
  "failed": 0,
  "coalescedTotal": 0,
  "cadenceMs": 5000,
  "lastSuccessAt": "2026-07-17T12:00:00Z"
}
```

```bash
curl -s http://localhost:PORT/sync/status
```

---

## Protected routes

All routes in this section require the bearer token (see
[Authentication](#authentication)). The seven mutating admin routes below each
return `202 Accepted` with `{ "operationId": "..." }` and run the actual work
**asynchronously**. Poll the returned id at
[`GET /operations/{operationId}`](#get-operationsoperationid); see
[Operations](#operations) for the polling model and the important
`succeeded`-is-not-a-verdict caveat.

The `202` acceptance body is the same for all seven (`camelCase`):

```json
{ "operationId": "op_..." }
```

### `POST /sources`

Ingest a source (queue-coupled). Writes a `pending` Operation
(`operationType: source_ingest`, target `source`), enqueues the coordinate with
the operation id, and returns `202`. The scheduler drain owns the
running → terminal lifecycle. The created Operation's `targetObjectType` is
`source` and its `targetObjectId` is the request's `nativeUri` — not a `src_`
id.

**Request body** — `IngestRequest` (`camelCase`, `deny_unknown_fields`):

| Field | Type | Meaning |
|-------|------|---------|
| `sourceSystem` | string | The connector/source-system id. |
| `nativeUri` | string | The connector-scoped native URI of the content. |

Before the Operation is written, `nativeUri` passes a lexical containment
prescreen: it must be an absolute path lexically under the configured corpus
root, with no parent-traversal components — otherwise `400`
`source_resolution`, because no scan enumeration could ever stage such a
URI. The prescreen is deliberately lexical only (no filesystem I/O, no
existence check — the file may legitimately land before the next scan); a
URI that passes but names no corpus file still fails asynchronously, as the
Operation, at the next scan cycle.

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/sources \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sourceSystem":"filesystem","nativeUri":"/absolute/corpus/path/doc.pdf"}'
```

---

### `POST /sources/{sourceId}/parses`

Force a re-parse of a source (queue-coupled). Same body as `POST /sources`.
Writes a `pending` Operation (`operationType: parser_execution`, target
`source`, `targetObjectId` = the path `sourceId`) and enqueues the body
coordinate.

Before the Operation is written, the body coordinate is validated against the
path source (for Operation-target integrity):

- If the path `sourceId` does not exist → `404` `not_found`
  (`"no source <sourceId>"`).
- If `(sourceSystem, nativeUri)` is not a **current** location of that source →
  `400` `bad_request`
  (`"coordinate does not name a current location of source <sourceId>"`).

**Request body** — `IngestRequest` (see above).

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/sources/SOURCE_ID/parses \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sourceSystem":"filesystem","nativeUri":"/absolute/corpus/path/doc.pdf"}'
```

---

### `POST /sources/{sourceId}/parses/{parseId}/activate`

Force-activate the addressed parse (detached async task). Writes a `pending`
Operation (`operationType: parse_activation`, target `parse`,
`targetObjectId` = `parseId`) and returns `202`; activation, predecessor
supersession, and held-supersession cleanup run in the detached task.

**Request body** — none.

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/sources/SOURCE_ID/parses/PARSE_ID/activate \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `POST /parses/{parseId}/accept`

Accept (activate) a held parse (detached async task). Writes a `pending`
Operation (`operationType: parse_activation`, target `parse`) and returns `202`.

**Request body** — none.

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/parses/PARSE_ID/accept \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `POST /parses/{parseId}/discard`

Discard a held parse (detached async task). Writes a `pending` Operation
(`operationType: parse_discard`, target `parse`) and returns `202`; the run is
moved to `archiving` and its cleanup driven in the detached task.

`parse_discard` is a recorded **additive** extension of the §34.6 operation-type
set (the spec defines the held-parse discard disposition but omits its operation
type).

**Request body** — none.

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/parses/PARSE_ID/discard \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `POST /snapshots`

Mint a corpus-wide snapshot (detached async task). Writes a `pending` Operation
(`operationType: snapshot_creation`, target `corpus`, `targetObjectId` =
`corpus`) and returns `202`.

**Request body** — `SnapshotRequest` (`camelCase`, `deny_unknown_fields`); all
fields optional:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `incident` | bool | `false` | `true` mints an incident snapshot; otherwise a manual snapshot. |
| `createdBy` | string | — | Optional operator attribution. |
| `notes` | string | — | Optional free-text notes. |

An empty body `{}` is valid (mints a manual snapshot).

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/snapshots \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"incident":true,"createdBy":"jake","notes":"pre-migration"}'
```

---

### `POST /restore`

Rollback-as-restore (detached async task): restore a source's archived parse from
its snapshot, then reactivate the source. Writes a `pending` Operation
(`operationType: restore`, target `source`, `targetObjectId` = the request's
`sourceId`) and returns `202`.

**Request body** — `RestoreRequest` (`camelCase`, `deny_unknown_fields`);
both required:

| Field | Type | Meaning |
|-------|------|---------|
| `sourceId` | string | The source to restore/reactivate. |
| `parseId` | string | The archived parse to restore. |

A failure on the restore path surfaces on the polled Operation as `failed` with
error kind `restore_failed` (`500`).

**Response `202`** — `{ operationId }`.

```bash
curl -s -X POST http://localhost:PORT/restore \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sourceId":"SOURCE_ID","parseId":"PARSE_ID"}'
```

---

### `POST /shutdown`

Signal the service to shut down. Protected. This is a **control action, not an
Operation** — it returns an immediate confirmation and signals the shutdown
latch; **no Operation row is written** and there is nothing to poll. (This route
is a recorded extra-spec additive; the §34.6 operation-type set has no shutdown
value, consistent with it not being tracked async work.)

**Request body** — none.

**Response `202`** — **no body**.

```bash
curl -s -i -X POST http://localhost:PORT/shutdown \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `GET /parses`

List held parses awaiting disposition (§13.4). Protected.

**Query parameters:**

| Param | Values | Meaning |
|-------|--------|---------|
| `status` | `held` | **Required.** Only `held` is accepted. |

Any other `status` value is rejected `400` `bad_request`
(`"GET /parses supports only status=held; got status=..."`). A request that
omits `status` entirely is likewise rejected with a `400` JSON
[error envelope](#error-body); authorization is checked first, so both
validation failures occur only after the bearer token is verified.

**Response `200`** — `{ "parses": [ ParseRun, ... ] }`.

`ParseRun` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `id` | string | always |
| `sourceId` | string | always |
| `parserName` | string | always |
| `parserVersion` | string | always |
| `parserConfigHash` | string | always |
| `capabilityProfileHash` | string | always |
| `status` | string enum | always — `building` \| `ready` \| `active` \| `archiving` \| `archived` \| `failed` |
| `heldReason` | string enum | omitted when absent — only value: `conformance_regression`. A held parse is `status: "ready"` with this set. |
| `conformanceReport` | object | omitted when absent — see below |
| `startedAt` | string | omitted when absent |
| `completedAt` | string | omitted when absent |
| `activatedAt` | string | omitted when absent |
| `archivedAt` | string | omitted when absent |
| `artifactBundleUri` | string | omitted when absent |
| `artifactBundleHash` | string | omitted when absent |
| `parserRawOutputUri` | string | omitted when absent |
| `warnings` | array of `ParseWarning` | omitted when absent |
| `metrics` | object | omitted when absent — `ParseMetrics` |
| `createdAt` | string | always |
| `error` | string | omitted when absent |

`ParseWarning` entries (`camelCase`): `code` (string, always), `message`
(string, always), `severity` (`info` \| `warning` \| `error`, always),
`locator` (a Locator, omitted when absent — kinds under
[`GET /units/{unitId}`](#get-unitsunitid)). `metrics` (`ParseMetrics`) is all
optional `u64` counts, each omitted when absent: `unitCount`,
`relationshipCount`, `pageCount`, `tableCount`, `figureCount`,
`ocrRegionCount`, `annotationCount`, `projectionCount`.

`conformanceReport` — `ConformanceReport` fields (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `parseId` | string | always |
| `unitTypeCounts` | object (string → `u64`) | always |
| `relationshipTypeCounts` | object (string → `u64`) | always |
| `locatorCoverage` | number | always |
| `captionPairingRate` | number | omitted when absent |
| `tableDecompositionRate` | number | omitted when absent |
| `dimensions` | object (string → number) | always — the metrics compared by the activation dominance rule |
| `measuredAt` | string | always |
| `reportHash` | string | always |

This is the disposition surface: after a mutating admin Operation reports
`succeeded`, consult this listing (or the individual parse run) to learn the
domain outcome — see [`succeeded` is not a parse verdict](#succeeded-is-not-a-parse-verdict).

```bash
curl -s 'http://localhost:PORT/parses?status=held' \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `GET /operations/{operationId}`

Read one Operation row by id (§34.6). Protected.

**Response `200`** — `Operation` (`camelCase`):

| Field | Type | Presence | Meaning |
|-------|------|----------|---------|
| `id` | string | always | The `op_` handle. |
| `operationType` | enum | always | See values below. |
| `status` | enum | always | `pending` \| `running` \| `succeeded` \| `failed`. |
| `targetObjectType` | string | always | Kind of object acted on (e.g. `source`, `parse`, `corpus`). |
| `targetObjectId` | string | always | Id of the acted-on object. |
| `startedAt` | string | **omitted** while `pending` | Set on transition to `running`. |
| `completedAt` | string | **omitted** until terminal | Set on `succeeded`/`failed`. |
| `error` | string | **omitted** unless `failed` | Bounded failure detail. |
| `createdAt` | string | always | When the `pending` row was inserted. |

`operationType` values (wire names, `snake_case`):

```
acquisition, parser_execution, parse_build, parse_import_validation,
parse_activation, projection_build, snapshot_creation, restore,
drill, source_ingest, parse_discard
```

`parse_discard` is an additive extension of the spec's closed set (see
[`POST /parses/{parseId}/discard`](#post-parsesparseiddiscard)).

`status` values (wire names, `snake_case`): `pending`, `running`, `succeeded`,
`failed`.

**Errors:** `404` `not_found` (`"no operation <operationId>"`) when the id is
unknown.

```bash
curl -s http://localhost:PORT/operations/op_ABC123 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### `GET /annotations/vocabulary`

Inspect the annotation vocabulary observed in `semantic_annotations`, grouped
for review. Protected. This is the read surface an operator uses to author the
operator policy documents from the corpus's own observed vocabulary.

**Query parameters:**

| Param | Values | Default | Meaning |
|-------|--------|---------|---------|
| `annotationType` | `entity`, `relation` | — | **Required.** Which vocabulary to return. |
| `scope` | `active`, `all` | `active` | `active` reads only active-parse rows; `all` additionally includes non-active-parse rows. |

**400 behavior** (both `400` `bad_request`, checked **after** the bearer token):

- Missing or unrecognized `annotationType` →
  `"GET /annotations/vocabulary requires annotationType=entity|relation; got ..."`.
- Unrecognized `scope` →
  `"GET /annotations/vocabulary scope must be active|all; got ..."`.

Both responses share a common frame of counters. The reader is bounded: it reads
at most `MAX_ROWS_READ = 200_000` rows and forms at most `MAX_GROUPS = 50_000`
groups; `truncated` is `true` when either cap was hit. Rows whose body is the
empty-marker `[]` (the "no annotations here" convention) are counted into
`skippedMarkerCount` and never grouped; rows with an otherwise-unparseable body
are counted into `malformedRowCount` and never grouped.

**Response `200` (entity)** — `EntityVocabularyResponse` (`camelCase`):

| Field | Type | Presence |
|-------|------|----------|
| `annotationType` | string | always — `entity` |
| `scope` | string | always — echoes the effective scope |
| `groups` | array of entity group | always — sorted by `normalizedName` ascending |
| `skippedMarkerCount` | `u64` | always — empty-`[]` marker rows skipped |
| `malformedRowCount` | `u64` | always — unparseable non-marker rows skipped |
| `truncated` | bool | always — a row or group cap was hit |
| `rowsRead` | `u64` | always — total rows read (≤ `MAX_ROWS_READ`) |
| `groupCount` | `u64` | always — number of groups returned |

Entity group fields (`camelCase`):

| Field | Type | Meaning |
|-------|------|---------|
| `normalizedName` | string | The shared normalized-name group key. |
| `rawForms` | array of `{ rawForm, count }` | Distinct raw name strings with their occurrence counts. |
| `entityTypes` | array of string | Distinct `entityType` values seen in the group. |
| `sourceCount` | `u64` | Count of distinct source ids the name appears in. |
| `modelCounts` | array of `{ modelName, count }` | Per-model occurrence counts (`modelName` is `(unknown)` when absent). |
| `totalCount` | `u64` | Total occurrences across all raw forms. |

**Response `200` (relation)** — `RelationVocabularyResponse` (`camelCase`):
same top-level frame (`annotationType` = `relation`, `scope`,
`skippedMarkerCount`, `malformedRowCount`, `truncated`, `rowsRead`,
`groupCount`), with `groups` sorted by `predicate` ascending. Relation group
fields (`camelCase`):

| Field | Type | Meaning |
|-------|------|---------|
| `predicate` | string | The relation predicate group key (verbatim). |
| `totalCount` | `u64` | Total occurrences of this predicate. |
| `sourceCount` | `u64` | Count of distinct source ids. |
| `modelCounts` | array of `{ modelName, count }` | Per-model occurrence counts. |

```bash
curl -s 'http://localhost:PORT/annotations/vocabulary?annotationType=entity&scope=active' \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

## Operations

### Polling model

Every mutating admin route ([`POST /sources`](#post-sources),
[`POST /sources/{sourceId}/parses`](#post-sourcessourceidparses),
[`POST /sources/{sourceId}/parses/{parseId}/activate`](#post-sourcessourceidparsesparseidactivate),
[`POST /parses/{parseId}/accept`](#post-parsesparseidaccept),
[`POST /parses/{parseId}/discard`](#post-parsesparseiddiscard),
[`POST /snapshots`](#post-snapshots),
[`POST /restore`](#post-restore)) returns `202 { "operationId": "op_..." }`
**before** the work completes. The work runs asynchronously.

To observe progress and outcome, poll
[`GET /operations/{operationId}`](#get-operationsoperationid). The `status` walks
`pending` → `running` → terminal (`succeeded` or `failed`). On `failed`, the
`error` field carries the failure detail; the corresponding error kind (e.g.
`restore_failed`, `snapshot_verification_failed`, `docling_conversion`) is what a
synchronous call would have returned.

There is **no streaming**: poll the single-JSON Operation row. (`POST /shutdown`
is the one protected mutating route that is **not** an Operation — it returns
`202` with no body and no id to poll.)

### `succeeded` is not a parse verdict

An Operation reaching `succeeded` means the **pipeline lifecycle completed** — it
does **not** encode the domain verdict of the parse. A re-parse can complete its
Operation successfully and still leave the parse **held** for disposition, or
produce an identical-identity outcome, etc.

To learn the domain outcome of a parse/ingest Operation, consult the parse run
itself — for held parses, use [`GET /parses?status=held`](#get-parses), or
inspect the individual parse run. Do not treat `status: "succeeded"` as "the
parse was activated".
