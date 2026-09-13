# SPEC — Decoupling the Ingestion Pipeline

Status: proposal. Not approved for implementation.

This document proposes splitting the scheduler's serial per-entry chain into
independently scheduled stages that hand work off through durable state. It
records the problem, the findings behind it, the proposed shape, why that shape
solves the problem, and the alternatives considered.

Scope: the acquisition → parse → projection → activation chain owned by
`src/scheduler.rs`. Out of scope: the annotation worker, the projection
publication worker for annotations, query execution, and any parser.

## 1. Problem statement

One scheduler thread runs one cycle at a time. A cycle scans the corpus, then
drains the queue by running, for each entry and inline: acquisition import,
parse, the content-derived projection build, snapshots, the activation gate,
predecessor cleanup, queue completion, and bundle removal (`run_cycle`,
`dispatch_parse_chain`, `gate_ready_parse`).

Three kinds of work with different costs and different resource needs are
therefore serialized behind one another:

- CPU-bound parsing (minutes for a large book).
- Network-bound embedding of every chunk, section window, and unit through the
  dense and ColBERT backends (tens of minutes to hours for a large book).
- Millisecond-scale SQLite writes that need the single writer lock.

Consequences:

- Detection stops while any source is parsing or embedding. A new or changed
  file is not even observed until the current entry finishes, so measured
  freshness (`SyncHealth.last_success_at`) lags by the duration of the slowest
  source in the queue.
- Activation of parses that are already fully built waits behind unrelated
  sources still embedding.
- The projection build holds the hot-plane writer lock across the entire
  embedding of a source (`build_content_derived_projections` opens one
  IMMEDIATE transaction and every builder runs on it). Every other writer,
  including the annotation worker and the annotation projection worker, is
  locked out for that duration.
- Throughput equals the sum of per-source times. Nothing runs in parallel
  across sources, even where the backends are remote HTTP services that could
  serve several sources at once.

## 2. Findings

Read from the code as built.

### 2.1 The chain is serial by construction, not by accident

`run_cycle` claims every pending entry, then loops. Each iteration calls
`acquisition::import_staged_bundle`, then `dispatch_parse_chain`, then
`complete`, then removes the acquisition bundle. `dispatch_parse_chain` runs
`parse_chain_prefix` (route, no-retry guard, containment, identity check,
worker, parse import) and then `gate_ready_parse` (projection build,
pre-activation snapshot, gate, post-activation snapshot, predecessor cleanup,
held-candidate cleanup, parser bundle removal). No step yields.

### 2.2 The writer lock is held across embedding, and other workers already pay for it

`build_content_derived_projections` begins one IMMEDIATE write transaction and
runs chunk, lexical, dense, section-dense, multivector, and derived-view
builders on it. The dense builder's own documentation (`src/projections/dense.rs`)
records that the scheduler's writer lock is held across the HTTP fan-out as a
pre-existing property "accepted pending the banked structural fix (embed
before opening the transaction, lock only for the commit)".

The annotation worker (`src/annotations/worker.rs`) documents a contention
policy that exists only because of this hold: write boundaries are classified
as pre-paid (defer quietly on `SQLITE_BUSY`, end the cycle early) or post-paid
(wait for the lock so a paid producer result is not lost). `hot_plane` grew a
contention-aware `begin_write_transaction` sibling for the same reason.

### 2.3 The durable intermediate states already exist

Decoupling does not require inventing recovery states. The chain already
persists its progress at every boundary and already recovers from a crash at
each one:

- **Acquired, not parsed.** The acquisition import is durable
  (`source_objects`, `acquisition_records`) and the queue row stays `in_flight`
  with its staging bundle retained until `complete`. A crash replays the entry
  idempotently (dedup by `source_hash`).
- **Parsed, not projected or gated.** The importer commits `parse_runs` at
  `ready`. The no-retry guard's `GateExisting` arm adopts a `ready`, un-held
  run without re-parsing. The annotation dry-run mode depends on exactly this:
  it truncates the chain after import and leaves rows for the next normal
  start to adopt.
- **Projected, not gated.** Projection freshness is recorded per parse and
  per type in `retrieval_projections` envelopes. Each builder deletes its
  type for the parse and rebuilds, so a re-run replaces rather than
  accumulates. `verify_activation_prerequisites` refuses to activate unless
  all five content-derived types are fresh.
- **Gated, not completed.** The queue row is deleted and the staging bundle
  removed only after the gate returns; a crash before that replays through
  `GateExisting`.

### 2.4 The repository already has the worker pattern the scheduler lacks

The annotation worker and the annotation projection worker are each one
`std::thread` that, every cycle, discovers its work from the hot plane alone,
holds a maintenance permit for the cycle, runs external calls with no
transaction open, commits results in short transactions, and treats any
`building` row visible at discovery as a crash orphan to adopt. The
projection publication worker archives embedding batches to the artifact
store before opening its publication transaction. Nothing about that pattern
is specific to annotations.

### 2.5 Single-thread assumptions that a change must revisit

- `claim_pending` reclaims every `in_flight` row on every cycle because "any
  observed `in_flight` row is stale wreckage". With work in progress on other
  threads, an `in_flight` row may be live.
- `evaluate_no_retry_guard` treats a visible `building` parse run as stale
  wreckage (`DispatchOverStaleBuilding`) for the same reason.
- `sweep_orphan_parse_temp_dirs` at thread start assumes workers run inline
  on the scheduler thread.
- `SyncCycleStats` and `SyncHealth.last_cycle` describe one serial pass.
- Queue-coupled Operations are marked `running` at parse dispatch and
  `succeeded` in `complete`, both on the scheduler thread.

### 2.6 What concurrency can and cannot buy

- Remote HTTP dense, ColBERT, and reranker backends can serve several sources
  concurrently; the dense builder already fans out within one source.
- Local accelerator backends serialize on the process-global model gate
  regardless of thread count. Cross-source concurrency does not help them;
  removing the lock hold still does.
- Parsing is CPU-bound and parallelizes across sources.
- SQLite admits one writer. More threads do not add write throughput; they
  only make it essential that every transaction stays short.

## 3. Proposed solution

Split the chain into four stages. Each stage discovers its work from durable
state, holds a maintenance permit per unit of work, and hands off by committing
a durable boundary. No stage keeps in-flight state that a crash would need to
reconcile beyond what the current chain already reconciles.

### 3.1 Stages

| Stage | Owner | Work | Durable exit |
| --- | --- | --- | --- |
| Detect and acquire | scheduler thread | scan, stage, enqueue, import acquisition bundles, enumeration deletions, reappearance | `source_objects` row, queue row `in_flight`, acquisition bundle retained |
| Parse | parse pool (bounded thread count) | route, no-retry guard, containment, identity check, worker, parse import | `parse_runs` at `ready` or `failed`; parser bundle retained |
| Project | projection stage worker (bounded in-flight sources) | chunk, lexical, dense, section-dense, multivector, derived view | all five types `fresh` for the parse |
| Gate | gate worker (serial) | pre-activation snapshot, `gate_and_activate`, post-activation snapshot, predecessor and held cleanup, queue completion, bundle removal, Operation terminal state | queue row deleted; bundles removed |

The scheduler thread keeps only the first stage plus deletion propagation and
reappearance, which must follow a complete enumeration. Its cycle time becomes
the scan time plus acquisition imports, so detection cadence measures
detection.

### 3.2 Work discovery

Each stage derives its worklist from existing tables. No new durable state is
introduced; the queue row and its `operation_id` link survive until the gate
stage completes the entry.

- **Parse** claims queue rows that are `in_flight`, whose acquisition import
  has succeeded, and that are not held by an in-process stage. The existing
  `parse_chain_prefix` runs unchanged, including the guard, containment, and
  identity check.
- **Project** selects `parse_runs` at `ready` with `held_reason` NULL whose
  five content-derived types are not all fresh, not already active, and not
  held by an in-process stage.
- **Gate** selects `parse_runs` at `ready` with `held_reason` NULL whose five
  types are all fresh and that are not the source's active parse.

In-process exclusivity: one process-local registry keyed by `source_id` marks
a source as owned by a stage. A row is never claimed by two stages or two
workers at once. The registry is memory only. After a restart it is empty and
every durable state re-enters discovery exactly as the current crash-replay
arms handle it today.

### 3.3 Projection build without the lock hold

The projection stage builds in three steps:

1. One short transaction: delete-for-parse and rebuild chunk and lexical
   (SQL-only builders), commit.
2. No transaction open: read the chunks and units, run dense, section-dense,
   and multivector embedding through the configured backends, collecting
   vectors and section artifacts in memory or in the artifact store.
3. One short transaction: delete-for-parse and insert the dense, section-dense,
   multivector, and derived-view rows and envelopes, commit.

Chunking is deterministic from units and the sealed chunker config, so the
chunks read in step 2 are the chunks step 3's envelopes describe. A crash
between steps leaves some types fresh and others not; discovery selects the
parse again and the delete-then-rebuild per type restores a consistent set.
The gate still refuses any parse whose five types are not all fresh, so the
"never a torn set" guarantee moves from one long transaction to the freshness
check that already enforces it.

Cross-source concurrency in step 2 is bounded by a configured in-flight source
count. Local backends acquire the model gate as today; HTTP backends do not.

### 3.4 Gate stage

The gate stage runs the current `gate_ready_parse` body from the pre-activation
snapshot onward, then `complete`, then bundle removal, serially. Serial is
sufficient: every step is short except snapshot archival, and the per-source
cutover barrier is unchanged.

### 3.5 Contracts that change

- **Queue reclamation.** `claim_pending` stops treating `in_flight` as
  wreckage. Live ownership is the in-process registry; at startup the
  registry is empty, so every `in_flight` row is reclaimable, which is the
  current behavior restricted to the case where it is true.
- **No-retry guard.** `DispatchOverStaleBuilding` applies only when the
  `building` run is not owned in-process. At startup no run is owned, so the
  current behavior holds.
- **Operations.** `mark_running` at parse claim; `mark_succeeded` at gate
  completion; `mark_failed` at whichever stage records the failure. The
  running → terminal guard and the "success only after the queue row is gone"
  rule are unchanged.
- **Health.** `SyncHealth` reports detection cycle statistics only. Parse,
  project, and gate publish their own backlog and last-outcome counts into
  separate slots, as the annotation and projection workers do. `--health`,
  `--monitor`, and PROTOCOL.md gain per-stage fields; existing fields keep
  their meaning where the meaning survives and are removed where it does not.
- **Cadence.** Backpressure counts pending plus in-flight as today. Cycle
  time no longer includes parse or embedding time.
- **Maintenance.** Each stage holds a worker permit per unit of work. Rebuild
  and failure-clear drain all stages through the existing gate; the projection
  stage adopts the annotation worker's cancellation watch so a rebuild does
  not wait out an embedding call.
- **Staging sweep.** The temp-workspace sweep runs at startup before any
  stage starts, which is the same invariant with the same justification.
- **Dry-run mode.** Unchanged. It runs `parse_chain_prefix` inline and
  truncates before projection, exactly as now.

### 3.6 Configuration

Two settings, both under `[workers]`, both requiring explicit approval before
implementation: the parse pool size and the projection stage's in-flight
source count. Neither enters parser or embedding identity.

### 3.7 Why this solves the problem

- Detection is never behind parsing or embedding, because the scheduler
  thread no longer runs either. Freshness measures what it claims to measure.
- The writer lock is held only for short transactions. The annotation
  worker's contention policy becomes unnecessary and can be retired later.
- Sources proceed independently. A finished parse activates without waiting
  for an unrelated source, and several sources embed concurrently against
  remote backends.
- Crash recovery is the current recovery, generalized. Every hand-off is a
  state the chain already persists and already re-enters after a crash.
- The trust boundaries are untouched: workers still stage untrusted bundles,
  the importer is still the only canonical writer of parse state, activation
  still runs under the per-source barrier, and no async is introduced.

## 4. Alternatives considered

### 4.1 Move embedding off the lock, keep the chain serial

Restructure `build_content_derived_projections` as in §3.3 but leave it on
the scheduler thread.

- Solves: the writer-lock hold and the annotation worker's contention.
- Does not solve: detection blocking, activation waiting behind other
  sources, or cross-source throughput.
- Tradeoff: smallest change. Reasonable as the first increment of §3, not as
  the end state.

### 4.2 Child process per parse, chain otherwise unchanged

Run parser workers in child processes with a timeout, as MuPDF does today.

- Solves: parser crash and memory isolation; a kill switch for pathological
  input.
- Does not solve: anything in §1. The scheduler still waits on the child.
- Tradeoff: fits naturally inside §3's parse pool; on its own it is
  isolation, not concurrency.

### 4.3 Several scheduler threads, each running the full chain

Run N copies of the current per-entry chain.

- Solves: cross-source parallelism for parsing.
- Makes worse: N long writer-lock holds contending with each other and with
  every other worker; N sources' worth of embedding memory; no way to balance
  stages independently.
- Tradeoff: least restructuring, worst contention. Rejected.

### 4.4 Durable stage markers on the queue row

Add a `stage` column to `sync_queue` advanced at each hand-off, instead of
deriving stage from `parse_runs` and projection freshness.

- Solves: simpler discovery queries.
- Costs: a schema change (setup script, never runtime migration) and a second
  copy of state that `parse_runs` and the envelopes already hold, which can
  drift from them.
- Tradeoff: rejected in favor of derivation, which keeps one source of truth
  per fact. Revisit only if derivation queries prove too slow at scale.

### 4.5 Async pipeline on the Tokio runtime

Model stages as tasks with channels.

- Rejected: PRINCIPLES.md confines async to HTTP boundaries and requires
  SQLite work to stay synchronous. The stages are synchronous OS-thread work
  with external calls, which is what the existing workers already are.

## 5. Recommended sequencing

1. §3.3 alone: projection build in three steps on the scheduler thread
   (alternative §4.1 as the first increment). Removes the lock hold with no
   change to discovery, health, or Operations.
2. Projection stage worker and gate worker: the scheduler thread stops at the
   parse import. Health and Operation contract changes land here.
3. Parse pool: parallel parsing with per-source exclusivity, optionally with
   child-process isolation.

Each step leaves the system in a consistent, fully recoverable state and can
be shipped without the next.
