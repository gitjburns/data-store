# Data Store Service Implementation Plan

## Purpose

Track implementation work for the standalone Data Store service under
`service/data-store/`.

This file is a current plan and status document. It intentionally avoids
long-form implementation logs. Detailed protocol and service requirements live
in:

- `SPEC-SERVER.md`
- `PROTOCOL.md`
- `ARCHITECTURE.md`

## Fixed Decisions

- The service is a standalone Rust application at `service/data-store/`.
- HTTP framework: Axum on Tokio.
- Inference framework: Candle with local model artifacts.
- Accelerator targets: CUDA and Apple Silicon Metal.
- CPU inference is not supported.
- There is no automatic device fallback. A requested unavailable backend must
  fail readiness clearly.
- Errors must be explicit and diagnostic. No silent fallback paths.
- SQLite is the durable source of truth for documents, retrieval units,
  metadata, dense vector blobs, ColBERT document vectors, vector norms, and FTS
  rows.
- Dense retrieval uses an explicit in-memory flat vector cache with exact cosine
  similarity, finite-value validation, stored norms, and deterministic
  tie-breaking.
- Ingested source documents are versioned immutably.
- Search captures the active source-document version set at request admission
  and uses that captured set for the lifetime of the request.
- Old source-document versions are retained for rollback support. Cleanup of
  inactive versions is out of scope.
- LanceDB is not part of the first storage implementation. Reconsider ANN or a
  dedicated vector index only after flat-scan benchmarks show a real need.
- ColBERT MaxSim runs over a bounded candidate pool rather than an all-corpus
  multi-vector index.
- The startup admin token remains the only admin authentication mechanism.
- The runtime admin token file is a local credential handoff, not a second auth
  path.

## Current Status

The service has an implemented standalone runtime with ingestion, retrieval,
storage, version management, admin shutdown, runtime token-file handoff, and the
interactive `data-store` client.

The server spec and protocol have since been revised around a universal
operation-stream contract. The next scope of work is implementing that contract
in the service and migrating the CLI to consume it.

## Completed Capability Areas

### Service Runtime

- Standalone Rust service binary: `data-store-service`.
- Axum/Tokio HTTP runtime.
- Config-backed server, logging, admin token file, inference, Docling, storage,
  and retrieval settings.
- Foreground/background startup handoff.
- Startup progress reporting for long inference initialization.
- File logging with stable structured event fields.

### Accelerator And Model Loading

- Feature-gated Metal and CUDA support.
- No CPU fallback for configured inference models.
- Local model artifact validation for config, tokenizer, and safetensor files.
- Dense Qwen3 embedding runtime with query/passage formatting and last-token
  pooling.
- ColBERT runtime with tokenizer contract validation, ModernBERT encoder,
  projection path, and MaxSim scoring.
- Qwen3 reranker runtime with yes/no token logit scoring.

### Ingestion

- Corpus-relative source resolution with traversal and absolute-path rejection.
- Docling PDF-to-markdown conversion boundary.
- Deterministic unit splitting with heading path, page numbers when available,
  sequence, token count, and content.
- Dense passage embedding per unit.
- Persisted ColBERT document token vectors per unit.
- Durable SQLite ingest transaction.
- Active-version publish after storage and cache readiness.

### Retrieval

- Active source-document version snapshot captured at request admission.
- Dense exact cosine scan over active dense cache.
- SQLite FTS5 BM25 retrieval over active versions.
- RRF fusion of dense and BM25 candidates.
- ColBERT query embedding and MaxSim scoring over the bounded candidate pool.
- Qwen3 reranker final scoring and public result ordering.
- Raw diagnostic payloads for retrieval observability.

### Storage And Versioning

- SQLite schema setup and runtime validation.
- Durable immutable `document_versions`.
- Active source-document version map.
- Durable units, dense vectors, ColBERT document vectors, and FTS rows.
- Active dense cache loading and publish.
- Retained-version listing.
- Rollback to an already-retained document version.

### Operations And Limits

- Health/readiness diagnostics.
- Request-construction and retrieval limits.
- Configured maximum request body size.
- Configured maximum ingest source and search query lengths.
- Fail-fast ingest and search admission gates.
- Graceful admin shutdown.
- Runtime admin token-file write, stale replacement, and graceful cleanup.

### CLI Client

- Separate client binary: `data-store`.
- REPL with readline-style editing and local history.
- Config-based service address and token-file discovery.
- Human-readable rendering for health, limits, ingest, search, versions,
  rollback, and shutdown.

## Verification Policy

The service does not currently use an automated test suite. Standard
verification from `service/data-store/` is:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Use `cargo check --bin data-store` when the client binary changes.

Manual runtime verification is required for service startup, model loading,
ingest, search, admin operations, and shutdown behavior.

## Open Validation Items

1. Dense scan performance target: benchmark around 10k, 50k, and 100k units
   before considering ANN/indexing.
2. CUDA verification remains environment-dependent and should be performed on a
   CUDA host before relying on CUDA deployment behavior.
3. The universal operation-stream protocol must be implemented and verified once
   its implementation phase is approved.

## Next Scope: Operation-Stream API Contract

Implement the data-store operation protocol documented in `PROTOCOL.md` and
`SPEC-SERVER.md`.

### Goals

- Make `POST /v1/operations` the documented consumer API for all operations.
- Return operation-scoped `application/x-ndjson` response streams.
- Emit real-time `status`, `progress`, `result`, and `error` events.
- Use structured operation errors with `status`, `kind`, and `message`.
- Keep protected operations on the existing startup bearer token.
- Improve client-visible transport, stream, and service-error diagnostics.
- Migrate the `data-store` CLI to the same protocol used by all consumers.

### Server Work

1. Add operation request and event DTOs.
2. Add a structured error payload conversion for operation terminal errors and
   pre-stream HTTP failures.
3. Add `POST /v1/operations`.
4. Dispatch operations by name:
   - `health`
   - `limits`
   - `ingest`
   - `search`
   - `versions`
   - `rollback`
   - `shutdown`
5. Validate bearer auth only for protected operations:
   - `versions`
   - `rollback`
   - `shutdown`
6. Stream every accepted operation as NDJSON with monotonic per-operation
   sequence numbers.
7. Emit terminal `result` events for successful operations.
8. Emit terminal `error` events for operation failures after the stream opens.
9. Preserve structured HTTP errors for failures before the stream opens.
10. Add `POST /v1/operations/{operationId}/control` as an explicit reserved
    control endpoint. The first implementation may reject unsupported control
    messages.

### Progress Instrumentation

Add real server-side status/progress events without fake percentages.

Ingest:

- source resolution
- Docling conversion
- unit splitting
- dense embedding per unit
- ColBERT embedding per unit
- storage publish

Search:

- admission
- query embedding
- candidate retrieval
- ColBERT scoring
- reranker scoring
- result assembly

Short operations:

- `health`, `limits`, `versions`, `rollback`, and `shutdown` should emit at
  least start/completion status events where useful.

### CLI Work

1. Replace route-specific helpers with one operation-stream helper.
2. Send `POST /v1/operations` with `Accept: application/x-ndjson`.
3. Add bearer auth only for protected operations.
4. Read the admin token file immediately before protected operations.
5. Parse NDJSON event streams line by line.
6. Render status events as newline-terminated lines.
7. Render counted progress events by overwriting the current line.
8. Finalize active progress lines before printing status, result, or error.
9. Deserialize terminal result payloads into the existing human renderers.
10. Render terminal operation errors with operation, stage, status, kind, and
    message.
11. Print method, URL, and cause chain for transport and stream failures.
12. Map unspecified bind addresses such as `0.0.0.0:<port>` and `[::]:<port>`
    to loopback connection URLs.

### Verification

Run from `service/data-store/`:

```bash
cargo fmt
cargo check
cargo check --bin data-store
cargo check --features metal
```

Manual verification:

1. Start the service with a config that includes `admin.token_file_path`.
2. Confirm startup writes the token file and reports readiness.
3. Run the client and verify:
   - `help`
   - `health`
   - `limits`
   - `ingest <source>`
   - `search <query> [topK]`
   - `versions`
   - `rollback <source> <versionLabel>` when a retained version is available
   - `shutdown`
4. Confirm long operations stream real status/progress events.
5. Confirm counted progress overwrites the current line.
6. Confirm normal status lines end with newlines.
7. Confirm operation errors include status, kind, message, operation, and stage.
8. Confirm transport failures include method, URL, and cause chain.
9. Confirm protected operations do not print or log the bearer token.

## Session Procedure

Before implementation work:

1. Read `SPEC-SERVER.md`, `PROTOCOL.md`, `ARCHITECTURE.md`, and this plan.
2. Confirm the intended phase or change with the user.
3. Ask for approval before editing files.

During implementation:

1. Keep changes scoped to the approved phase.
2. Preserve user changes in the worktree.
3. Avoid destructive git operations.
4. Keep docs concise and current-state oriented.

After implementation:

1. Run the appropriate verification commands.
2. Report what changed and what was verified.
3. Note any manual runtime verification that was not performed.
