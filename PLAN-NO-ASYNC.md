# No-Async Rewrite Plan

## Status

Planning document for replacing the standalone Data Store service's async
server/runtime plumbing with synchronous threading and blocking I/O.

Phase 1 is complete (2026-06-11). Phase 2 is complete (2026-06-11): the
`http.rs` ingest caller restructure, the `docling.rs` synchronous rewrite, and
the `tiny_http` prototype are done. The prototype failed the streaming-flush
criterion, so `tiny_http` was excluded from the Phase 4 server selection.
Fallback ladder step 2 research is complete (2026-06-11): `oxhttp` and `astra`
were both excluded, and the Open Decision is resolved by user decision via
fallback step 3 — no crate swap; Axum/Tokio is retained as a confined
transport shell (see the revised Goal and the Open Decision resolution).
Phase 3 is complete (2026-06-11). Phase 4 is complete (2026-06-11). Phase 5 is
the next scheduled work item.

Per the Note below, work is sequenced for development efficiency rather than
for keeping intermediate builds functional. Interim phase acceptance is
`cargo fmt`, `cargo check`, and `cargo check --features metal`; runtime
behavior verification is deferred to Phases 4-5.

## Goal

Confine Axum/Tokio async runtime usage in `service/data-store/` to a thin
transport shell (`main.rs` plus the transport layer of `http.rs`) while
preserving the existing service behavior, documented HTTP API, operation-stream
NDJSON contract, admin protections, startup/background lifecycle, and
diagnostic guarantees.

The target is zero async in domain logic. Operation pipelines, storage, model
calls, Docling process handling, and admission/shutdown state use normal OS
threads, synchronous channels, blocking subprocess I/O, and synchronous
SQLite/model calls. Async may exist only inside the transport shell, and
domain code must never name a `tokio`/`axum` type.

Revision (2026-06-11): the original goal was full Axum/Tokio removal. After
fallback ladder steps 1-2 eliminated every candidate synchronous server crate
(see Open Decision), the user decided to retain the proven Axum/Tokio
transport and enforce containment instead. This preserves the rewrite's
motivation — no async leaking into domain logic — while keeping the verified
streaming, shutdown, and body-limit behavior of the existing transport.

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

Revision (2026-06-11): superseded by the Open Decision resolution. The
transport remains Axum/Tokio as a confined shell; no synchronous server crate
is selected. The candidate text and selection criteria below are retained as
the record the candidates were evaluated against.

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
  `tokio::task::spawn_blocking` adapter for Axum graceful shutdown, replaced
  by the final shell-owned bridge in Phase 4.
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

Status: Complete (2026-06-11). The `docling.rs` synchronous rewrite and the
`tiny_http` prototype are done. The prototype verdict is FAIL; see the Open
Decision section.

Implementation notes (`http.rs` caller restructure):

- The `http.rs` ingest Docling call site no longer uses `tokio::pin!` +
  `tokio::select!`. Conversion runs as an independent task with cloned
  `DoclingConfig`/`index_root` ownership; the caller forwards progress
  in a receive-until-disconnect loop, then joins the task. Channel disconnect
  is the completion signal, so this lands on the target architecture's shape.
  The follow-up swap to `std::thread::spawn`, `std::sync::mpsc`, and
  `JoinHandle::join` landed with the `docling.rs` rewrite below.
- The progress sender is moved unconditionally (`then_some`) so the caller
  never retains a sender when there is no emitter; a retained sender would
  keep the channel open and stall the receive loop.
- `drain_docling_progress` was removed; draining is inherent in the receive
  loop, including after delivery failure, so bounded progress sends cannot
  block conversion.

Implementation notes (`docling.rs` synchronous rewrite, 2026-06-11):

- `docling.rs` is fully synchronous and tokio-free: `std::process::Command`
  spawn, one `std::thread::spawn` reader thread per pipe, `SyncSender`
  progress delivery, a `try_wait` + `thread::sleep` poll loop, and
  kill-then-wait on timeout.
- The post-100 feedback path calls `inspect_docling_activity` directly; the
  `tokio::task::spawn_blocking` wrapper and its join-error log path were
  removed.
- `process_id` is plain `u32` end to end (std `Child::id()` is infallible,
  unlike tokio's `Option<u32>`); the dead `missing_process_id` skip path was
  removed. Log values now render as `123` rather than `Some(123)`; the
  Phase 5 diagnostic-parity comparison must account for that formatting
  change.
- Thread join failures (reader threads and the `http.rs` conversion join) log
  their existing event names with `is_panic = true` plus a bounded
  `panic_message` extracted by a shared `pub(crate) panic_payload_message`
  helper in `docling.rs`; `is_cancelled` was dropped because std thread joins
  fail only on panic. All other log event names and fields are unchanged.
- The `http.rs` blocking `recv()`/`join()` calls inside still-async
  `execute_ingest` are accepted interim Tokio-worker blocking per the offline
  note; Phase 3 removes the async callers.
- Compile acceptance (`cargo fmt`, `cargo check`,
  `cargo check --features metal`) passed for both sessions; runtime
  acceptance remains deferred per the offline-development decision.

Implementation notes (`tiny_http` prototype, 2026-06-11):

- `tiny_http = "0.12.0"` was added under `[dev-dependencies]` (version
  confirmed against docs.rs/crates.io in the approved session), and
  `examples/tiny_http_prototype.rs` was added as a throwaway harness. The
  harness runs a tiny_http server and an in-process `reqwest::blocking`
  client in one process and prints a PASS/FAIL verdict per Phase 4 selection
  criterion. Streaming flush is measured by timestamping NDJSON line arrivals
  against a 300ms server-side emit delay.
- Prototype run results (`cargo run --example tiny_http_prototype`):
  - NDJSON streaming flush: FAIL. Five lines emitted 300ms apart arrived as
    one terminal burst; inter-arrival gaps were sub-microsecond.
  - Request body limiting: PASS (declared-length 413 rejection plus bounded
    `take` read).
  - Authorization header access: PASS (200 with token, 401 without).
  - Write-error observability: FAIL, but contaminated by the buffering
    failure: the client could not disconnect mid-stream because no bytes
    arrived until the body completed, so `respond()` finished successfully
    before the disconnect. Not independent evidence either way.
  - Shutdown wakeup via `Server::unblock()`: PASS (accept loop exited in
    under 1ms).
- Failure analysis (unverified from crate source; observed behavior is
  consistent): tiny_http selects chunked encoding correctly for
  `data_length: None`, but pipes the body reader through a
  `chunked_transfer` encoder that buffers internally and flushes only when
  full or at EOF. `with_chunked_threshold` controls chunked-vs-Content-Length
  selection only; there is no per-event flush API.
- Decision: `tiny_http` is excluded from the Phase 4 server selection. The
  harness and the dev-dependency are retained so the same harness can verify
  the next candidate crate; the dev-dependency is removed when the Phase 4
  server crate is selected.
- Compile acceptance (`cargo fmt`, `cargo check`,
  `cargo check --features metal`) passed; the prototype run was the approved
  runtime verification for this item.

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

Status: Complete (2026-06-11).

Implementation notes:

- `execute_ingest`, `execute_search`, `run_operation_stream`,
  `execute_operation`, operation progress helpers, and terminal result emission
  are synchronous.
- Operation-stream work is spawned with `std::thread::spawn`; a separate
  standard-thread join watcher logs normal completion or panic with a bounded
  panic message. The previous `tokio::spawn` operation task and Tokio join
  watcher were removed.
- `OperationEmitter` is synchronous. Nonterminal status/progress delivery uses
  `try_send` so reporting does not block backend work. Terminal result/error
  delivery uses `blocking_send` from the OS operation thread so terminal
  delivery remains a distinct logged boundary. Axum/Tokio channel ownership
  remains confined to the transport layer of `http.rs`.
- `tokio::task::block_in_place` was removed from ColBERT and reranker progress
  paths; model progress callbacks now call the synchronous emitter directly.
- The previous `progress_blocking` compatibility method was removed; storage,
  ColBERT, and reranker progress use the same synchronous progress path.
- Compile acceptance (`cargo fmt`, `cargo check`,
  `cargo check --features metal`) passed. A scan confirmed no
  operation-pipeline `.await`, no `tokio::spawn`, and no `block_in_place` in
  `src/http.rs`; remaining async functions are Axum route handlers.

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

### Phase 4: Confine Async Runtime To Transport Shell

Rescoped (2026-06-11) from "Replace Axum/Tokio Server" per the Open Decision
resolution.

Status: Complete (2026-06-11).

Implementation notes:

- `OperationEmitter` no longer names Tokio channel types directly. The
  transport-owned `OperationStreamSender` wraps the Axum/Tokio response-body
  channel and preserves the previous delivery behavior: nonterminal reporting
  uses nonblocking delivery, terminal result/error delivery uses blocking
  delivery, and delivery outcomes remain separately logged from backend
  execution outcomes.
- `main.rs` replaced the temporary Phase 1 `tokio::task::spawn_blocking`
  graceful-shutdown adapter with a shell-owned bridge: a standard thread waits
  on the blocking `ShutdownSignal`, logs its lifecycle, and wakes Axum graceful
  shutdown through a Tokio oneshot confined to the transport shell.
- `tokio` features were trimmed from `full` to `rt-multi-thread`, `net`, and
  `sync`.
- The stale `state.rs` comment referring to future async call-site cleanup was
  removed.
- Compile acceptance (`cargo fmt`, `cargo check`,
  `cargo check --features metal`) passed. Containment scans confirmed no
  `spawn_blocking`, `tokio::spawn`, or `block_in_place`, and no `tokio`/`axum`
  references outside `src/main.rs` and `src/http.rs`.
- Live HTTP route, operation-stream, protected-admin, and token-file shutdown
  checks were not run in this session because the service was not started; they
  remain Phase 5 runtime verification inputs.

Scope:

- Restrict `tokio`, `axum`, `tokio-stream`, and `tower-http` usage to
  `main.rs` and the transport layer of `http.rs`; domain code must not name
  async types.
- Trim `tokio` features from `full` to the features the shell actually uses.
- Keep route dispatch, JSON body limit handling, route-specific JSON
  responses, `POST /v1/operations` NDJSON streaming, and protected admin
  routes on the existing Axum implementation.
- Replace the temporary Phase 1 `tokio::task::spawn_blocking` graceful
  shutdown adapter in `main.rs` with the final shell-owned bridge.
- Add boundary documentation at the shell: module-level comments stating what
  the containment boundary is, why it exists (the `progress_blocking` panic
  class: synchronous domain work running inside async tasks), and that
  crossing it is a rule violation, not a style preference.
- Done early (2026-06-11): the `tiny_http` dev-dependency and
  `examples/tiny_http_prototype.rs` are removed; the harness existed solely
  to select a replacement server crate, and no crate will be selected.
  `Cargo.lock` was pruned via approved `cargo check`, which passed. The
  example file was deleted by the user.

Expected files:

- `Cargo.toml`
- `Cargo.lock`
- `src/http.rs`
- `src/main.rs`
- `src/state.rs` (comment cleanup only)

Acceptance:

- `cargo fmt`
- `cargo check`
- `cargo check --features metal`
- `tokio`/`axum` references outside `main.rs` and the `http.rs` transport
  layer: none (verifiable with `rg`).
- `/v1/health` and `/v1/limits` compatibility routes work.
- `POST /v1/operations` streams status/progress/result/error NDJSON.
- Route-specific ingest/search/admin compatibility routes still work during
  migration.
- Protected operations require the startup token.
- Graceful shutdown removes the current admin token file when appropriate.

### Phase 5: Diagnostic Parity And Documentation

Status: Complete for the no-async cleanup scope (2026-06-11). Functional
runtime verification is intentionally deferred because the service is still in
the middle of the SERVERv2 refactor and is not expected to be runnable.

The Phase 5 diagnostics analysis was performed against `DIAGNOSTICS.md` after
Phase 4 completed. The specific diagnostic parity failures found in that
analysis have been addressed for the no-async cleanup scope.

Analysis result:

- Overall diagnostics evaluation for static/code-reviewable Phase 5 items:
  PASS.
- Startup, parent/child handoff, operation streams, Docling process handling,
  spawned operation threads, source resolution, model calls, search pipeline,
  shutdown, and CLI ambiguous stream loss have substantial diagnostic coverage.
- Functional runtime verification was not run by user direction because
  SERVERv2 is mid-refactor. The earlier attempted startup exposed local
  `config.toml` drift (`models.reranker.backend` is missing), but that is not
  part of this no-async cleanup completion.

Specific failures addressed:

- Storage ingest transaction abort visibility is complete for the reviewed
  paths. `StorageRuntime::ingest_document` now logs
  `storage.ingest_transaction.aborting` before post-begin/pre-commit returns,
  including source path, version label, document ID, phase, unit/vector counts,
  publish timestamp when available, error, and `durable_commit_completed =
  false`.
- Active-version rollback publish abort visibility is complete for the reviewed
  path. `StorageRuntime::publish_source_version_with_vectors` now logs
  `storage.active_version_publish.aborting` before the active-version publish
  transaction drops without commit.
- Reranker per-candidate service logs no longer include the per-candidate
  `logit` or public `score` on `model_call.completed`. API raw diagnostics and
  scoring behavior remain unchanged.
- Documentation now reflects the final Phase 4 transport decision:
  Axum/Tokio is retained as a confined transport shell in `src/main.rs` and the
  transport layer of `src/http.rs`; operation pipelines, storage, model calls,
  Docling process handling, admission/shutdown state, and CLI operation use
  synchronous domain code.

Specific Phase 5 completion:

1. Added explicit transaction-abort logs in `src/storage.rs` for
   `StorageRuntime::ingest_document`.
2. Added explicit transaction-abort logs in `src/storage.rs` for
   `StorageRuntime::publish_source_version_with_vectors`.
3. Removed per-candidate reranker model outputs from service logs in
   `src/inference/reranker.rs`.
4. Ran compile verification after the code changes:
   - `cargo fmt`
   - `cargo check`
   - `cargo check --features metal`
5. Updated `README.md`, `ARCHITECTURE.md`, and `SPEC-SERVER.md` for the final
   confined Axum/Tokio transport-shell architecture. `DIAGNOSTICS.md` already
   used transport-neutral wording and did not require changes. `PROTOCOL.md`
   did not contain stale transport wording.

Files changed:

- `src/storage.rs`
- `src/inference/reranker.rs`
- `README.md`
- `ARCHITECTURE.md`
- `SPEC-SERVER.md`
- `PLAN-NO-ASYNC.md`

Runtime verification deferred:

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

Prototype verdict (2026-06-11): `tiny_http` 0.12.0 FAILED the streaming-flush
criterion (full-body buffering of chunked NDJSON; see Phase 2 implementation
notes) and is excluded. The fallback ladder below is now active; step 2 is the
next scheduled work item. `examples/tiny_http_prototype.rs` is retained as the
candidate-verification harness.

Fallback ladder if `tiny_http` fails the prototype (decided 2026-06-11):

1. Hand-rolling an HTTP layer on `std::net::TcpListener` is excluded by user
   decision and is not a fallback.
2. Research alternative synchronous server crates in an approved session.
   Candidates from unverified training knowledge: `oxhttp`, `astra`. Notes to
   verify: `rouille` wraps `tiny_http` and inherits its transport behavior;
   `astra` reuses hyper protocol internals, which needs checking against the
   no-async-runtime goal. Any candidate must pass the same prototype harness
   before selection.
3. If no synchronous crate satisfies the selection criteria, pause and
   reassess the plan with the user — most plausibly confining the async
   runtime to a thin transport shell while all domain work behind it stays
   synchronous, preserving the rewrite's motivation (no async leaking into
   domain logic) even if not its letter.

Step 2 research findings (2026-06-11, crates.io/docs.rs/repository
inspection):

- `oxhttp` 0.3.2: zero async dependencies (`http` + `httparse` plus optional
  TLS), blocking handlers, and `Body::from_read` chunked streaming. Excluded:
  no shutdown mechanism at all (`ListeningServer` exposes only `join()`; the
  accept loop has no break condition), the socket is wrapped in a `BufWriter`
  with no per-chunk `flush()` visible in `server.rs` (encoder internals
  unverified; same failure shape as `tiny_http`), no built-in request body
  limiting plus unbounded drain of leftover request bodies, write errors
  reported via `eprintln!` rather than at a handler-observable boundary, and
  the docs describe the server as a work in progress for use behind a reverse
  proxy.
- `astra` 0.4.0: blocking handlers over hyper protocol internals driven by a
  private mio event loop on a background thread — an embedded async runtime
  in all but name, failing the no-async-runtime letter. Also: no documented
  stop/shutdown API, no built-in body limiting, plausible but unverified
  per-chunk flush, and modest adoption (~16k downloads, last release
  2024-11). Excluded without prototyping once the step 3 resolution below
  made the question moot.

Resolution (2026-06-11, user decision): fallback ladder step 3 is invoked. No
synchronous server crate is selected. The service retains Axum/Tokio as a
confined transport shell with all domain work synchronous on OS threads; see
the revised Goal and the rescoped Phase 4. Rationale: every evaluated
synchronous crate fails at least one selection criterion on paper or in the
harness, while the existing Axum transport has verified streaming flush,
graceful shutdown, and body-limit behavior in this service today. Containment
preserves the rewrite's motivation (no async in domain logic) and is
enforceable by review: `tokio`/`axum` references outside the shell must be
zero.
