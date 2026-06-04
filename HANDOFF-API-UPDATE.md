# API Update Handoff

## Purpose

This file captures the current handoff state for the data-store
operation-stream API migration. Read this after `PLAN-API-UPDATE.md` during
onboarding.

The older route-specific code inspection notes were intentionally removed from
this handoff because they became misleading after the server operation
foundation and client operation migration were completed. Treat
`POST /v1/operations` as the current migration contract.

## Current State

Server Operation Foundation is complete.

- The server exposes `POST /v1/operations`.
- Operation responses are operation-scoped NDJSON streams.
- Stream events include `status`, `progress`, `result`, and `error`.
- Operation events carry monotonic per-operation sequence numbers.
- Protected operations use the existing startup bearer token.
- Protected operation names are `versions`, `rollback`, and `shutdown`.
- Route-specific endpoints are still preserved during migration.
- The reserved `POST /v1/operations/{operationId}/control` endpoint exists.

Client Operation Migration is complete.

- The `data-store` CLI sends service commands through `POST /v1/operations`.
- It sends `Accept: application/x-ndjson`.
- It reads the configured admin token file immediately before protected
  operations.
- It parses NDJSON event streams line by line.
- It requires a terminal `result` or `error` event.
- It deserializes `result.payload` into the existing human-readable renderers.
- It renders `status` events as normal lines.
- It renders counted `progress` events by overwriting the current line.
- It renders operation errors with operation, stage, status, kind, and message.
- It reports transport and stream errors with method, URL, and cause-chain
  context.
- It maps unspecified bind addresses such as `0.0.0.0:<port>` and
  `[::]:<port>` to loopback connection URLs.

Server Real Progress Instrumentation is complete.

- Operation-stream ingest emits status boundaries for source resolution,
  Docling conversion, unit splitting, dense embedding, ColBERT embedding, and
  storage publish.
- Operation-stream ingest emits counted per-unit progress for dense document
  embedding and ColBERT document embedding.
- Operation-stream search emits status boundaries for query embedding,
  candidate retrieval, ColBERT scoring, reranker scoring, and result assembly.
- Operation-stream search emits counted per-candidate progress for ColBERT
  scoring and reranker scoring.
- `service/data-store/src/inference/colbert.rs` and
  `service/data-store/src/inference/reranker.rs` now expose progress-aware
  scoring methods while keeping the existing no-progress methods as route
  compatibility wrappers.
- Route-specific compatibility endpoints continue to use the shared ingest and
  search pipelines without operation-stream progress output.

Documentation Alignment And Verification is complete.

- `service/data-store/README.md` now documents `POST /v1/operations` as the
  consumer API, includes operation-stream curl examples, describes streamed
  status/progress behavior, and keeps retained route-specific endpoints as
  migration compatibility routes.
- `service/data-store/ARCHITECTURE.md` now describes the operation-stream
  protocol as the documented API contract and frames route-specific `/v1/...`
  and `/admin/...` endpoints as compatibility routes.

## Verified

The following commands passed from `service/data-store/` after documentation
alignment:

```bash
cargo fmt --check
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual runtime verification remains pending because it depends on local config,
model artifacts, Docling, and a running service.

## Next Recommended Scope

Manual runtime verification against a running service.

Goal: verify the operation-stream protocol and CLI behavior with local config,
model artifacts, Docling, and a running service.

Recommended work:

- Start the service with a config that includes `admin.token_file_path`.
- Confirm startup writes the token file and reports readiness.
- Run the `data-store` CLI and verify `help`, `health`, `limits`, `ingest`,
  `search`, `versions`, `rollback` when a retained version is available, and
  `shutdown`.
- Confirm long operations stream real status/progress events.
- Confirm operation errors include status, kind, message, operation, and stage.
- Confirm protected operations do not print or log the bearer token.

## Useful Current Boundaries

`PROTOCOL.md`, `SPEC-SERVER.md`, and `SPEC-CLIENT.md` already describe the
intended operation-stream contract and should be treated as the target docs
during alignment.

`README.md` and `ARCHITECTURE.md` now describe `POST /v1/operations` as the
primary operator API and retain route-specific endpoints only as compatibility
routes.

The compatibility endpoints remain implemented in `service/data-store/src/http.rs`.
Do not remove or de-emphasize them in code unless a separate implementation
scope is approved.

`PLAN-SERVER.md` and `PLAN-CLIENT.md` mark documentation alignment complete.

## Verification For Next Rust Changes

Run from `service/data-store/`:

```bash
cargo fmt
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual runtime verification remains separate and requires operator-controlled
local config, model artifacts, Docling, and a running service.

## Process Notes

Ask before every file write. Configuration and build-file changes require
separate explicit approval.
