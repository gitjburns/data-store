# Atomic Ingest And Concurrent Search Remediation Plan

## Purpose

This handoff plan covers two related data-store service bugs:

1. Searches can fail during an in-progress ingest because search and ingest use
   the same Candle/Metal inference runtimes concurrently without an explicit
   model-execution safety boundary.
2. Ingest is not fully atomic at the service contract boundary because durable
   document/version rows can commit before active-version publish completes.

The target service behavior is now explicit in `SPEC-SERVER.md`:

- Search must work while ingest is in progress.
- In-progress ingest output remains out of search scope until publish.
- Search captures the active snapshot at request admission, before query
  embedding.
- Failed ingest attempts must not become active, must not become search-visible,
  and must not require `force: true` on retry.
- `force: true` is required only when the source already has an active
  searchable version from a prior successful ingest.
- Model outputs must be validated at the inference boundary.

## Current Status

Phase 1 and Phase 2 are complete as of 2026-06-06.

Implemented work:

Phase 1:

- `AppState` now owns a shared model-call gate that logs wait, acquire, release,
  and acquisition failure boundaries.
- Ingest dense passage embedding and ColBERT document embedding are gated per
  unit, so ingest can yield between model calls.
- Search captures the active storage snapshot immediately after search
  admission and before dense query embedding.
- Search dense query embedding, ColBERT persisted-candidate scoring, and
  ModernBERT reranker scoring are gated as individual model-backed stages.
- `inference/dense.rs` now validates dense output dimension, finite values, and
  finite nonzero norm before logging model-call completion or returning vectors
  to callers.
- Storage candidate-pool construction now consumes the captured search snapshot
  instead of locking and cloning the active cache internally.

Phase 2:

- `storage.rs` now publishes ingest storage atomically at the service contract
  boundary.
- Ingest validates dense and ColBERT vectors before opening the SQLite
  transaction.
- Ingest writes `document_versions`, `units`, `dense_vectors`,
  `colbert_document_vectors`, `units_fts`, and `active_document_versions` in
  one SQLite transaction.
- Ingest prepares the replacement dense-cache snapshot before commit, holds the
  cache lock through commit, and swaps the in-memory cache immediately after
  commit.
- The commit-to-cache-swap window has no fallible stream/progress callback.
- The previous ingest-only publish wrapper was removed; rollback still uses the
  retained-version publish helper.
- Storage logs now cover ingest publish timestamp allocation, cache lock,
  cache preparation, active-row write, commit, cache swap, and publish
  completion/failure boundaries.

Verification completed for Phase 1 and Phase 2:

```bash
cargo fmt --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml --features metal
```

Manual runtime checks have not been run yet because starting/stopping service
processes requires separate approval.

Remaining work is manual runtime validation of the concurrent search and atomic
ingest behaviors, with user approval before starting or stopping service
processes.

## Current Findings

The original observed search failure was:

```text
dense vector search-query contains non-finite values
```

The service log showed search query embedding completed and then storage rejected
the returned 4096-value query vector as non-finite. An ingest was running at the
same time. This points to unsafe overlapping model execution, not a search-scope
or SQLite issue.

There was also an ingest failure where storage rejected a dense passage vector
as non-finite. Phase 1 moved dense finite-value validation into the inference
boundary before dense model-call completion is logged.

Search visibility was mostly implemented correctly before Phase 1: storage
captured an active dense-cache snapshot and BM25 filtered to active versions.
Phase 1 moved snapshot capture to request admission, before query embedding.

Before Phase 2, ingest durability was not fully atomic. The implementation
wrote document version, unit, vector, and FTS rows in one SQLite transaction,
committed that transaction, then published the active version/cache in a
separate step. A failure after the first commit but before publish could leave
inactive retained state for a failed attempt. Phase 2 corrected this by moving
the active-version row into the ingest transaction and preparing the cache swap
before commit.

## Target Invariants

- Search and ingest have independent operation-level admission.
- A running ingest must not cause an otherwise valid search to fail with `503`.
- Search may wait briefly for a currently running model call, but not for the
  entire ingest operation.
- Ingest may yield between per-unit dense and ColBERT document embedding calls
  so admitted searches can run.
- A search uses one active snapshot for the full request.
- Any version published after a search captures its snapshot must not enter that
  search's scope.
- A failed ingest attempt leaves no active version and no success-labeled
  retained ingest state.
- Retrying a failed ingest for a source with no prior active version must not
  require `force: true`.
- Force re-ingest keeps the previous active version searchable until the new
  version fully publishes.
- Client stream delivery failure after a completed publish is reporting failure
  only; the ingest remains successful.

## Implementation Plan

### 1. Capture Search Snapshot At Admission - Completed In Phase 1

Move active search snapshot capture to immediately after search validation and
search admission, before dense query embedding.

Likely work:

- Add a storage method that captures and returns the active dense cache plus
  active version map without requiring a query vector.
- Change `execute_search` to capture this snapshot before calling
  `inference.dense.embed_query_vector`.
- Change candidate-pool construction to accept the captured snapshot rather than
  locking and cloning the cache internally.
- Ensure dense retrieval, BM25 filtering, candidate materialization, ColBERT
  provenance, and raw diagnostics all use the captured snapshot.
- Preserve existing service-log lifecycle boundaries and add missing logs for
  request-admission snapshot capture.

### 2. Add Shared Model Execution Safety - Completed In Phase 1

Add an explicit model-execution boundary that protects shared accelerator/model
runtimes without serializing whole operations.

Likely work:

- Add a shared inference/model-call gate to application state or inference
  runtime ownership.
- Use the gate around individual model calls or small batches:
  - ingest dense passage embedding per unit;
  - ingest ColBERT document embedding per unit;
  - search dense query embedding;
  - search ColBERT query/scoring work;
  - search reranker scoring.
- Do not hold the gate during Docling conversion, unit splitting, SQLite work,
  dense scan, BM25, RRF, candidate assembly, or result materialization.
- Log model-gate acquisition, release, wait duration, operation ID, model role,
  and call purpose at useful boundaries without logging document content,
  prompts, vectors, or tokens.
- Avoid a hidden queue for operation admission. The model gate may serialize
  model execution internally, but ingest/search operation admission remains
  separate.

Phase 1 uses an unbounded model-call wait because the spec allows model-call
waiting but does not currently define a timeout. Revisit only if runtime checks
show operator-visible wait behavior needs an explicit bound.

### 3. Validate Dense Output At The Inference Boundary - Completed In Phase 1

Make dense embedding validation part of `inference/dense.rs`, before the model
call is logged as completed and before vectors are returned to callers.

Likely work:

- Validate returned vector dimension equals configured dense dimension.
- Validate every value is finite.
- Validate norm is finite and greater than zero.
- Return an inference error with model role, call purpose, input kind, token
  count, expected dimension, actual dimension, and elapsed time.
- Update successful model-call logs to happen only after validation succeeds.
- Keep vector values out of logs.
- Audit ColBERT and reranker paths so non-finite token vectors, logits, and
  scores are also reported at inference/ranking boundaries rather than storage.

### 4. Make Ingest Storage Publish Atomic - Completed In Phase 2

Restructure ingest persistence so an ingest does not leave success-labeled
durable rows unless the active publish also succeeds.

Target approach:

- Validate all dense and ColBERT vectors before opening the SQLite transaction.
- Prepare the replacement dense-cache snapshot before committing durable state.
- Use one SQLite transaction for:
  - `document_versions`;
  - `units`;
  - `dense_vectors`;
  - `colbert_document_vectors`;
  - `units_fts`;
  - `active_document_versions`.
- Commit only after all durable rows for the new active version and active map
  are ready.
- Swap the in-memory active dense cache immediately after commit and before the
  terminal ingest result is considered ready.
- If cache swap can fail after commit, either make that impossible by preparing
  all fallible cache work before commit, or add a clear recovery/rollback design
  for keeping durable active map and memory cache consistent.

Important constraint: do not add runtime schema migrations. If schema changes
are needed, stop and propose an explicit setup/migration script instead.

Phase 2 implementation notes:

- No schema changes or runtime migrations were added.
- The ingest path now writes active-version publication in the same SQLite
  transaction as the successful document/version rows.
- All fallible dense-cache preparation is done before commit.
- The in-memory active dense cache is swapped immediately after commit while
  the cache lock is still held.
- A failed pre-commit publish/cache preparation path rolls back the whole
  ingest transaction.

### 5. Preserve Retry And Force Semantics - Completed In Phase 2

Make duplicate checks depend only on active searchable versions from prior
successful ingests.

Required behavior:

- Failed first-time ingest retry: no `force: true` required.
- Failed force re-ingest retry: `force: true` is still required because the
  previous active successful version remains active.
- Successful ingest where the CLI disconnected before receiving the terminal
  result: `force: true` is required on re-ingest, because backend success and
  publish are authoritative.
- Retained failed attempts must not appear as rollback targets or successful
  document versions.

### 6. Diagnostics

Maintain the diagnostics standard from `DIAGNOSTICS-ONBOARDING.md`.

Required log coverage:

- search admission;
- search snapshot capture start/success/failure;
- model gate wait/acquire/release/failure where useful;
- model call start/success/failure after output validation;
- ingest storage transaction begin, phases, commit attempt, commit success or
  failure;
- active publish/cache preparation and cache swap;
- operation terminal result/error readiness and delivery outcome.

Logs must not include admin tokens, document contents, prompts, vectors, token
dumps, or unbounded process output.

## Verification Plan

Required build checks:

```bash
cargo fmt
cargo check
cargo check --features metal
```

Manual runtime checks, with user approval before starting/stopping service
processes:

Checks 1-3 validate Phase 1 behavior. Checks 4-6 validate Phase 2 behavior
after ingest publish atomicity is implemented.

1. Start a long ingest and run search while ingest is in progress.
   - Search should complete against the previously active corpus.
   - In-progress document should not appear in results.
   - Search should not return non-finite vector errors.
2. During that ingest, verify search snapshot logs occur before dense query
   embedding logs.
3. Let the ingest complete.
   - The new document should become searchable only after publish.
4. Force an ingest failure before publish.
   - Retry without `--force` should be accepted when no prior active version
     exists for that source.
   - No active version or success-labeled retained version should remain from
     the failed attempt.
5. Force re-ingest a source with an existing active version.
   - Without `--force`, request should fail with `409`.
   - With `--force`, previous active version should remain searchable until the
     replacement publishes.
6. Disconnect the CLI after a publish-ready operation completes but before the
   terminal result is delivered, if practical.
   - Backend success should remain authoritative.

## Expected Files To Inspect First

- `service/data-store/SPEC-SERVER.md`
- `service/data-store/ARCHITECTURE.md`
- `service/data-store/DIAGNOSTICS-ONBOARDING.md`
- `service/data-store/src/http.rs`
- `service/data-store/src/state.rs`
- `service/data-store/src/storage.rs`
- `service/data-store/src/inference/dense.rs`
- `service/data-store/src/inference/colbert.rs`
- `service/data-store/src/inference/reranker.rs`

## Approval Reminder

Before implementation, present the concrete code-edit plan and wait for explicit
approval. Configuration changes, schema changes, runtime migrations, or service
start/stop actions require separate explicit approval.
