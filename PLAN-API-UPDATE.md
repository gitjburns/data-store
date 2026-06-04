# API Update Work-In-Progress Plan

## Purpose

This file is a short onboarding note for the operation-stream API migration.
Read it before the normal data-store onboarding files so the current
documentation inconsistencies are understood as expected work-in-progress
state, not as new discoveries.

The standalone Rust data-store service previously had an implemented
route-specific HTTP API and route-specific CLI client. The current specs and
plans target a universal streamed operation protocol:

```http
POST /v1/operations
Accept: application/x-ndjson
Content-Type: application/json
```

Each operation response is an operation-scoped NDJSON stream with `status`,
`progress`, `result`, and `error` events.

## Expected Inconsistencies

When reading the existing service documents, expect mixed API descriptions:

- `PROTOCOL.md`, `SPEC-SERVER.md`, `SPEC-CLIENT.md`, `PLAN-SERVER.md`, and
  `PLAN-CLIENT.md` describe the intended `/v1/operations` protocol.
- `README.md`, parts of `ARCHITECTURE.md`, startup output, and the current Rust
  implementation still describe or preserve route-specific endpoints such as
  `/v1/health`, `/v1/limits`, `/v1/ingest`, `/v1/search`, and `/admin/...`.

This mismatch is the known migration target. Do not pause merely because these
documents disagree. Treat the operation-stream protocol as the intended future
contract unless the user explicitly changes direction.

## Current Status

Server Operation Foundation is complete. The server exposes
`POST /v1/operations`, streams operation-scoped NDJSON events, preserves the
route-specific endpoints during migration, keeps protected operation auth on
the existing startup bearer token, and includes the reserved
`POST /v1/operations/{operationId}/control` endpoint.

Client Operation Migration is complete. The `data-store` CLI now sends all
service commands through `POST /v1/operations`, parses operation-scoped NDJSON
events, renders `status` and counted `progress` events, deserializes terminal
`result.payload` values into the existing human renderers, reports terminal
operation errors with operation/stage/status/kind/message, reports transport
and stream errors with method/URL/cause-chain context, reads the startup admin
token immediately before protected operations, and maps unspecified bind
addresses to loopback connection URLs.

Server Real Progress Instrumentation is complete. Operation-stream ingest now
emits status boundaries for source resolution, Docling conversion, unit
splitting, dense embedding, ColBERT embedding, and storage publish, with
counted per-unit progress for dense and ColBERT document embedding.
Operation-stream search now emits status boundaries for query embedding,
candidate retrieval, ColBERT scoring, reranking, and result assembly, with
counted per-candidate progress for ColBERT scoring and reranker scoring.
Route-specific compatibility endpoints continue to use the shared pipelines
without operation-stream progress output.

Documentation Alignment And Verification is complete. `README.md` and
`ARCHITECTURE.md` now present `POST /v1/operations` as the documented consumer
API, describe streamed operation status/progress behavior, and keep retained
route-specific endpoints framed as migration compatibility routes.

Verified:

```bash
cargo fmt --check
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual runtime verification remains pending because it depends on local config,
model artifacts, Docling, and a running service.

The next recommended scope is manual runtime verification against a running
service.

## Recommended Implementation Split

Do not try to complete the full server and client migration in one session.
Use this split.

### 1. Server Operation Foundation

Status: complete.

Goal: make the server expose the operation-stream protocol while preserving the
current route-specific handlers during migration.

Included:

- Add operation request and event DTOs.
- Add structured operation error payloads with `status`, `kind`, and `message`.
- Add `POST /v1/operations`.
- Dispatch operations by name: `health`, `limits`, `ingest`, `search`,
  `versions`, `rollback`, and `shutdown`.
- Apply bearer-token authentication only for protected operations:
  `versions`, `rollback`, and `shutdown`.
- Stream NDJSON events with monotonic per-operation sequence numbers.
- Emit terminal `result` events for success.
- Emit terminal `error` events for failures after the stream opens.
- Keep pre-stream failures as structured HTTP errors.
- Add the reserved `POST /v1/operations/{operationId}/control` endpoint.
- Emit minimal real status events, but do not attempt full counted progress yet.

Estimated effort: 14k-22k tokens.

Confidence after inspection: 90%.

### 2. Client Operation Migration

Status: complete.

Goal: migrate the `data-store` CLI from route-specific HTTP calls to
`POST /v1/operations`.

Included:

- Replace route helpers with one operation-stream helper.
- Send `Accept: application/x-ndjson`.
- Add bearer auth only for protected operations.
- Read the token file immediately before each protected operation.
- Parse NDJSON event streams line by line.
- Require a terminal `result` or `error` event.
- Deserialize `result.payload` into existing response DTOs and renderers.
- Render `status` events as normal lines.
- Render counted `progress` events by overwriting the current line.
- Render operation errors with operation, stage, status, kind, and message.
- Improve transport and stream errors with method, URL, and cause chain.
- Map unspecified bind addresses such as `0.0.0.0:<port>` and `[::]:<port>` to
  loopback connection URLs.

Estimated effort: 12k-20k tokens.

Confidence after inspection: 92%.

### 3. Server Real Progress Instrumentation

Status: complete.

Goal: add meaningful counted progress events for long operations after the
protocol and client are working end-to-end.

Included:

- Ingest status for source resolution, Docling conversion, unit splitting,
  dense embedding, ColBERT embedding, and storage publish.
- Ingest counted progress for dense embedding per unit and ColBERT embedding
  per unit.
- Search status for query embedding, candidate retrieval, ColBERT scoring,
  reranker scoring, and result assembly.
- Search counted progress for ColBERT candidate scoring and reranker scoring.
- Add callbacks and local progress emission at real pipeline boundaries.
- Do not invent fake percentages.

Estimated additional effort: 10k-18k tokens.

Confidence after implementation: 100%.

### 4. Documentation Alignment And Verification

Status: complete.

Goal: update operator docs only after the implemented behavior is clear.

Included:

- Update README and architecture text to reflect the operation-stream contract.
- Keep any temporary compatibility route behavior clearly described if retained.
- Ran the Rust verification commands from `service/data-store/`:

```bash
cargo fmt --check
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual runtime verification remains separate because it depends on local
config, models, Docling, and a running service.

## Process Notes

Before editing in a future session, read `HANDOFF-API-UPDATE.md` last. It
contains the detailed code inspection results from the prior session and is
intended to avoid repeating source exploration.

The repo rule still applies: ask before every file write, and get separate
explicit approval for any config change.
