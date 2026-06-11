# No-Async Rewrite Plan

## Status

Planning document for replacing the standalone Data Store service's async
server/runtime plumbing with synchronous threading and blocking I/O.

Phase 1 is complete (2026-06-11). Phases 2-5 have not started.

Per the Note below, work is sequenced for development efficiency rather than
for keeping intermediate builds functional. Interim phase acceptance is
`cargo fmt`, `cargo check`, and `cargo check --features metal`; runtime
behavior verification is deferred to Phases 4-5.

## Goal

Remove Axum/Tokio async runtime usage from `service/data-store/` while
preserving the existing service behavior, documented HTTP API, operation-stream
NDJSON contract, admin protections, startup/background lifecycle, and
diagnostic guarantees.

The target is no async in the service. Normal OS threads, synchronous channels,
blocking sockets, blocking subprocess I/O, and synchronous SQLite/model calls
are preferred.

## Rationale

The service workload is bounded, local, and blocking by nature:

- Docling conversion is a long-running external process.
- Model inference is synchronous local accelerator work.
- SQLite is synchronous.
- Dense/BM25/RRF retrieval and result assembly are synchronous CPU/storage work.
- Ingest/search concurrency is already bounded by fail-fast admission gates.
- Operator diagnostics depend on explicit lifecycle boundaries, not high socket
  concurrency.

Async complexity has already leaked into synchronous domain work. One concrete
failure was `OperationEmitter::progress_blocking` using Tokio `blocking_send`
from inside an async operation task, causing ingest to panic during synchronous
storage publish.

## Note

The app will remain offline during all phases of development. We should sequence the updates in an order that makes development efficient rather than what will keep the app functional between phases.

## Current Async Inventory

### Dependencies

Current async/runtime dependencies in `Cargo.toml`:

- `axum`
- `tokio` with `full` features
- `tokio-stream`
- `tower-http`

Current blocking client dependency:

- `reqwest` with `blocking` and `json` features

The CLI client already uses blocking HTTP and does not require an async rewrite.

### `src/http.rs`

Async responsibilities:

- Axum router construction and handler signatures.
- Route extractors and response conversion.
- Tokio `mpsc` operation-stream channels.
- `ReceiverStream` response bodies.
- `tokio::spawn` operation-stream tasks and join watcher tasks.
- Async `OperationEmitter` methods.
- Async helper functions for status/progress/terminal event delivery.
- `tokio::task::block_in_place` around synchronous model-scoring progress loops.
- Docling progress forwarding through async select loops.

Synchronous/domain responsibilities that can stay conceptually intact:

- Request validation.
- Authorization.
- Operation name parsing.
- Operation ID allocation.
- Ingest pipeline stages after Docling.
- Search pipeline stages.
- Version listing.
- Rollback.
- Shutdown response construction.
- Result shaping.
- Logging event names and diagnostic fields.

### `src/docling.rs`

Async responsibilities:

- Tokio process launching and child waiting.
- Async stdout/stderr pipe readers.
- Tokio timeout around child wait.
- Tokio spawned tasks for pipe readers.
- Tokio `mpsc` progress delivery.

Synchronous responsibilities that can stay conceptually intact:

- Option resolution.
- Output directory allocation.
- Argument construction.
- Markdown artifact discovery.
- Markdown normalization.
- Progress-line parsing.
- Diagnostic truncation.
- Exit-status formatting.

### `src/main.rs`

Async responsibilities:

- Tokio runtime creation.
- Tokio TCP listener bind.
- Axum server lifecycle.
- Async graceful shutdown future.

Synchronous responsibilities that can stay conceptually intact:

- CLI option parsing.
- Config loading and validation.
- Storage setup mode.
- Dense smoke mode.
- Background parent/child startup handoff.
- Startup reporter.
- Admin token file publish/cleanup.
- Inference startup progress reporting.
- Storage/cache initialization.
- Startup readiness logs.

### `src/state.rs`

Async/runtime responsibilities:

- Tokio semaphore for fail-fast admission.
- Tokio oneshot for shutdown signaling.
- Tokio capacity-1 semaphore (`model_call_gate`) with async acquisition
  serializing accelerator-backed model calls. (Omitted from the original
  inventory; discovered and converted during Phase 1.)

Synchronous responsibilities that can stay conceptually intact:

- Shared application state.
- Runtime accessors.
- Admin token authorization.
- Health response construction.
- Constant-time token comparison.

### `src/error.rs`

Async/framework responsibilities:

- Axum `IntoResponse` implementation and Axum `StatusCode` type.

Synchronous/domain responsibilities that can stay conceptually intact:

- Error enum.
- Error kind labels.
- Operation error detail construction.
- HTTP status mapping as numeric or transport-neutral status values.

### `src/bin/data-store.rs`

Current client behavior is already synchronous:

- `reqwest::blocking::Client`.
- Blocking NDJSON stream reading.
- Blocking health probe after ambiguous stream loss.
- Synchronous readline loop.

Expected rewrite impact is minimal unless the HTTP client dependency is changed
as part of dependency cleanup.

## Target Architecture

### Transport

Use a synchronous HTTP server with blocking request handlers. A request handler
owns one request from decoding through response completion.

Candidate dependency:

- `tiny_http`, pending confirmation that it can satisfy streaming response
  behavior cleanly.

Selection criteria:

- Blocking request acceptance and response writing.
- Header access for authorization.
- Request body size limiting without unbounded reads.
- JSON response support through explicit serialization.
- NDJSON streaming support for `POST /v1/operations`.
- Per-request write errors observable at the handler boundary.
- Clean shutdown from a protected admin operation.
- No async runtime dependency.

### Operation Execution

Run operation work synchronously on OS threads.

For `POST /v1/operations`, keep backend execution decoupled from client writes:

1. Handler validates request setup and authorization.
2. Handler creates a bounded `std::sync::mpsc::sync_channel` for operation
   events.
3. Handler spawns an operation thread.
4. Operation thread executes synchronously and sends NDJSON-ready events.
5. Handler writes events to the response stream until terminal result/error,
   stream failure, or channel close.
6. Join/panic outcome is logged durably.

Nonterminal progress/status delivery remains reporting-only. Client stream
failure must not be reported as backend execution failure.

Terminal result/error delivery remains a distinct boundary:

- Backend result ready.
- Terminal event write success/failure.
- Operation thread finish or panic.

### Docling

Use `std::process::Command` and normal reader threads:

- Spawn Docling with piped stdout/stderr.
- Spawn one reader thread per pipe.
- Parse progress from stderr and send through a synchronous progress channel.
- Wait for child using `try_wait` loop plus sleep until timeout.
- On timeout, kill then wait.
- Join reader threads and log normal completion, returned errors, and panics.

### Admission

Replace Tokio semaphore with a synchronous fail-fast gate. Options:

- Mutex-protected in-flight counter with RAII permit decrement on drop.
- Atomic counter with compare-exchange loop and RAII decrement on drop.

Recommendation: atomic counter plus RAII permit. It is simple, does not block,
and matches existing non-queueing admission semantics.

### Shutdown

Replace Tokio oneshot with synchronous shutdown state:

- Shared atomic shutdown flag for accept loop termination.
- Condvar or channel if the server implementation needs a blocking wakeup.
- Protected shutdown operation sets the signal after preparing the terminal
  `shutdown_complete` result.

The shutdown confirmation remains the operation stream terminal result, not an
inferred process state.

## Phased Implementation Plan

### Phase 1: Make State And Errors Transport-Neutral

Status: Complete (2026-06-11).

Implementation notes:

- Admission gates use an atomic in-flight counter with RAII permit decrement
  on drop (the recommended option).
- `model_call_gate`, missing from the original `state.rs` inventory, was also
  converted: it is now a synchronous `Mutex<bool>` + `Condvar` exclusive gate
  with a synchronous `acquire_model_call_gate`. The five async call sites in
  `http.rs` call it blocking; interim blocking of Tokio worker threads is
  accepted because the service is offline, and the async callers are removed
  in Phase 3.
- Shutdown uses a `ShutdownSignal` (`Mutex<bool>` + `Condvar` with
  `request()`/blocking `wait()`). `main.rs` keeps a temporary
  `tokio::task::spawn_blocking` adapter for Axum graceful shutdown, removed
  with the server in Phase 4.
- `ApiError::status_code()` became transport-neutral `status_u16()`; the
  `IntoResponse` rendering and `ErrorBody` moved into `http.rs`. One
  mechanical rename landed in `src/storage.rs`, which was not in the expected
  file list.
- The runtime acceptance items below (admission counts, live 503, shutdown
  confirmation) were deferred per the offline-development decision; compile
  acceptance (`cargo fmt`, `cargo check`, `cargo check --features metal`)
  passed.

Scope:

- Remove Tokio semaphore from `state.rs`.
- Replace admission gates with a synchronous RAII permit implementation.
- Replace Tokio oneshot shutdown sender with a transport-neutral shutdown
  signal object.
- Split Axum response rendering out of `ApiError`.
- Keep Axum/Tokio server temporarily compiling through adapter code.

Expected files:

- `src/state.rs`
- `src/error.rs`
- `src/http.rs`
- `src/main.rs`

Acceptance:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- Health reports admission counts correctly.
- Saturated ingest/search still returns 503 without queueing.
- Protected shutdown still produces the documented confirmation.

### Phase 2: Convert Docling To Synchronous Process Handling

Scope:

- Change `convert_source_to_markdown` and internal Docling helpers to
  synchronous functions.
- Replace Tokio process and async pipe readers with `std::process::Command`
  and reader threads.
- Replace Tokio progress channel with `std::sync::mpsc`.
- Preserve Docling logs and bounded diagnostics.
- Prototype `tiny_http` NDJSON streaming, body limiting, and shutdown wakeup
  with a throwaway example to settle the Phase 4 server-crate decision early
  (see Open Decision).

Expected files:

- `src/docling.rs`
- `src/http.rs`

Acceptance:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- Successful ingest still streams Docling progress when Docling emits it.
- Missing Docling executable logs spawn failure with local context.
- Docling timeout logs timeout, kill/wait result, bounded stdout/stderr, and
  terminal operation error.

### Phase 3: Convert Operation Pipelines To Synchronous Functions

Scope:

- Make `execute_ingest`, `execute_search`, `execute_operation`, and terminal
  result emission synchronous.
- Make `OperationEmitter` synchronous.
- Remove async progress helper functions.
- Remove `tokio::task::block_in_place`.
- Keep operation events and log event names stable.

Expected files:

- `src/http.rs`

Acceptance:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- Ingest publishes storage and active cache without async runtime involvement.
- Search completes dense/BM25/RRF/ColBERT/reranker pipeline synchronously.
- Operation stream result/error delivery remains separately logged from backend
  execution.

### Phase 4: Replace Axum/Tokio Server

Scope:

- Choose and add a synchronous HTTP server crate.
- Replace Axum router and handlers with synchronous route dispatch.
- Implement JSON body limit handling.
- Implement route-specific JSON responses.
- Implement `POST /v1/operations` NDJSON streaming.
- Implement protected admin routes.
- Replace Tokio listener/server lifecycle in `main.rs`.
- Remove `axum`, `tokio`, `tokio-stream`, and `tower-http` dependencies once
  unused.

Expected files:

- `Cargo.toml`
- `Cargo.lock`
- `src/http.rs`
- `src/main.rs`
- `src/error.rs`
- `src/state.rs`

Acceptance:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- `/v1/health` and `/v1/limits` compatibility routes work.
- `POST /v1/operations` streams status/progress/result/error NDJSON.
- Route-specific ingest/search/admin compatibility routes still work during
  migration.
- Protected operations require the startup token.
- Graceful shutdown removes the current admin token file when appropriate.

### Phase 5: Diagnostic Parity And Documentation

Scope:

- Compare logs before and after rewrite against `DIAGNOSTICS.md`.
- Update service docs to remove Axum/Tokio references.
- Update runbooks only after behavior is verified.

Expected files:

- `README.md`
- `ARCHITECTURE.md`
- `DIAGNOSTICS.md` if standards need transport-neutral wording
- Possibly `SPEC-SERVER.md` and `PROTOCOL.md` if any transport wording changes

Required verification scenarios:

- Startup fatal config error.
- Startup model/inference failure when safely forceable.
- Source resolution failure.
- Docling executable unavailable.
- Docling timeout or nonzero exit.
- Failed search request validation.
- Client stream loss before terminal event.
- Terminal result delivery failure after backend success when forceable.
- Failed rollback request.
- Graceful shutdown.

## Risks And Mitigations

### HTTP Streaming Semantics

Risk: selected synchronous HTTP crate may buffer responses or make streaming
awkward.

Mitigation: verify NDJSON flush behavior with a small prototype before Phase 4.

### Client Write Backpressure

Risk: slow clients can block response writer threads.

Mitigation: keep bounded operation-event channels, log write latency/failure,
and preserve admission limits. Consider write timeouts if the selected server
supports them.

### Shutdown Wakeup

Risk: a blocking accept loop may not wake immediately on shutdown.

Mitigation: choose a server API with explicit shutdown support, or use a
listener timeout/flag loop with durable shutdown logs.

### Panic Visibility

Risk: replacing Tokio join handles could lose panic diagnostics.

Mitigation: every spawned operation or reader thread must be joined by an owner
that logs normal completion, returned error, or panic.

### Dependency Churn

Risk: removing Axum/Tokio changes error/response types and may affect API
shape.

Mitigation: keep DTOs unchanged and serialize responses explicitly. Treat public
JSON field names and status codes as contracts.

## Non-Goals

- No API redesign.
- No schema changes.
- No runtime migrations.
- No fallback inference device behavior.
- No hidden operation queue.
- No change to retrieval ranking semantics.
- No frontend or Node backend integration work.

## Open Decision

Select the synchronous HTTP server crate before Phase 4.

First investigation is scheduled into Phase 2 scope: prototype `tiny_http`
response streaming and shutdown behavior using a temporary throwaway example
inside the service during an approved implementation session. The prototype
requires runtime verification (flush behavior is not provable by compile
checks), so it needs explicit approval to add the dependency and to run the
prototype process.
