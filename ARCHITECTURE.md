# Data Store Architecture

This document describes the internals of the Data Store service — the canonical
content-graph retrieval fabric — for developers and operators working on the
service itself. It describes the system as built in code. The API contract of
record is `PROTOCOL.md`; the server and client surface specs are
`SPEC-SERVER.md` and `SPEC-CLIENT.md`. Terminology (EvidencePack, ContentUnit,
UnitRelationship, RetrievalChannel, ResolvedScope, Operation) matches the
canonical spec.

## 1. System shape

The service is an **autonomous pipeline** wrapped in a **thin async HTTP
transport shell**. Nothing routine is operator-driven: a background scheduler
detects source changes, acquires bytes, parses them into canonical units, builds
retrieval projections, and activates the result — on its own adaptive cadence. A
dedicated annotation worker then enriches each newly activated parse. The HTTP
surface exists to answer queries, expose diagnostics, and accept rare operator
overrides (re-parse, activate/accept/discard, snapshot, restore, shutdown).

Operational logs use the existing `tracing` backend and `util::LogContext`.
Owners attach known targets, triggers, and IDs; contexts travel explicitly with
blocking/thread closures and instrumented futures. Process-local request/call IDs
supplement canonical IDs without creating records or changing HTTP contracts.
Stage outcomes distinguish received, validated, staged, committed, and published
state. `DIAGNOSTICS.md` defines the fields, measurements, and level policy.

### Architectural invariant: async is confined to HTTP boundaries

Async/`tokio` is confined to HTTP serving and annotator HTTP I/O. Every piece of
lifecycle machinery is synchronous OS-thread work over `rusqlite`:

- the acquisition/parse/gate scheduler (`src/scheduler.rs`) runs on one
  `std::thread`;
- the annotation worker (`src/annotations/worker.rs`) runs on its own
  `std::thread`;
- the importer, activation, snapshotting, projection builders, and the query
  pipeline are all synchronous functions.

The annotator's HTTP client also uses async request futures internally so rebuild
and shutdown can cancel network waits. Its synchronous producer interface owns
a shared single-worker Tokio runtime for HTTP I/O; no SQLite work runs there.

HTTP handlers that touch the pipeline do so inside `spawn_blocking`: the query
handler and every administrative Operation run as detached blocking tasks. SQLite
is never accessed from an async context. This is a standing repository rule, not
an incidental choice — new lifecycle machinery must keep this shape.

```
   ┌────────────────────────────────────────────────────────────┐
   │  async HTTP transport shell (axum/tokio)  — src/http.rs     │
   │  routing · bearer auth · request limits · spawn_blocking    │
   └───────────────┬───────────────────────────┬────────────────┘
                   │ (blocking task)            │ (blocking task)
                   ▼                            ▼
        synchronous query pipeline    synchronous admin Operations
        src/query/execute.rs          activate/accept/discard/snapshot/restore
                   │                            │
                   ▼                            ▼
   ┌────────────────────────────────────────────────────────────┐
   │  autonomous synchronous pipeline (OS threads, rusqlite)     │
   │                                                            │
   │  scheduler thread ──▶ detect ▶ acquire ▶ parse ▶ project   │
   │                       ▶ gate/activate                      │
   │  annotation worker thread ──▶ entity/relation/summary      │
   │                       ▶ graph + summary projections        │
   └────────────────────────────────────────────────────────────┘
```

### 1.1 Admin Operation execution model

Administrative work is recorded as **§34.6 Operation rows** (`operations`
table, `src/operations.rs`) with **status-guarded transitions**: a row is
inserted `pending` (`created_at` set; `started_at`/`completed_at`/`error`
NULL), moves `pending → running` (stamping `started_at`), and terminates
`running → succeeded` or `running → failed` (stamping `completed_at`, plus a
bounded `error`). Each transition's UPDATE matches only the expected prior
status, so an out-of-order transition fails loudly instead of silently
corrupting the record. Clients observe progress by polling
`GET /operations/{operationId}` — there is no streamed progress anywhere in
the API.

Two execution shapes exist (`src/http.rs`):

- **Detached tasks** (activate/accept/discard/snapshot/restore). The handler
  authorizes, awaits `insert_pending` (itself a `spawn_blocking` call), and
  returns **202 Accepted immediately** — the work has not completed when the
  response leaves. A detached, **un-awaited** `spawn_blocking` closure then
  runs: `mark_running` → the domain call inside `catch_unwind` →
  `mark_succeeded` on `Ok`, `mark_failed` (bounded detail) on `Err` **or
  panic**. A panic is converted to a durable `failed` record in-closure; it
  never unwinds out of the detached task leaving a stuck `running` row.

- **Queue-coupled operations** (`POST /sources` ingest, and force re-parse via
  `POST /sources/{sourceId}/parses`). The handler writes ONLY the `pending`
  Operation and enqueues through `enqueue_coalesced` with the `operation_id`
  threaded onto the `sync_queue` row. The **scheduler drain owns the full
  running → terminal lifecycle**: `mark_running` at drain dispatch;
  `complete` reads the `operation_id`, deletes the queue row, then runs
  `mark_succeeded` — the queue unit of work is finished at that point. A
  pipeline fault parks the queue row `failed` and drives the Operation to
  `failed` (ensuring `running` first, so a pre-dispatch failure still reaches
  a terminal record). Force re-parse additionally carries a **prescreen
  override**: queue rows with `operation_id IS NOT NULL` are subtracted from
  the connector's unchanged-prescreen so unchanged content is force-staged —
  a parser rollout over unchanged content would otherwise be defeated by the
  (mtime, size) prescreen.

**Operation-succeeded ≠ parse-outcome.** An Operation records that the
requested unit of work ran to completion and left a durable domain record; the
**domain verdict lives in the parse run**. A parse whose bundle fails the
§13.1 hard gates produces a durable `failed` `parse_runs` row while its
queue-coupled Operation still **succeeds** — outcomes are not faults. Only a
fault of the canonical machinery itself (SQL, artifact store, staging
filesystem) fails the Operation.

`POST /shutdown` is a **control action, not an Operation**: it authorizes,
signals `request_shutdown`, and returns 202 with no Operation row (extra-spec,
recorded additive).

### 1.2 Rebuild maintenance

Normal service startup holds the same storage admission gate while serving HTTP
for `server.startup_delay_seconds` (nonnegative integer seconds; `0` skips the
wait). Inference and policy-file loading precede HTTP serving; ordinary corpus
initialization, staging cleanup, policy registration, cache loading, and workers
wait until afterward. Startup logging and admin-token publication remain active.
Health, Operation polling, rebuild-all, and shutdown bypass the startup hold;
other storage requests return `503` and readiness stays false. Deferred corpus
initialization runs synchronously behind a blocking task. Rebuild-all ends the
countdown immediately; successful clearing allows startup initialization and
worker handoff to proceed without any remaining delay. Failed rebuilds keep
storage paused. Shutdown cancels the wait; setup-storage and annotation dry-run
modes do not use it.

`POST /rebuild-all` closes a shared admission gate and signals annotation
cancellation. The annotation worker stops dispatch, cancels outstanding HTTP
waits, discards unfinished results, and rolls back uncommitted writes. Its lease
remains held until all producer threads and local writes stop. Other admitted
HTTP/admin work drains, and the scheduler parks at a cycle boundary. Only then
does the service persist the pending `rebuild_all` Operation and return `202`. Draining
and detached clearing run synchronously behind `spawn_blocking`. Client timeout
or disconnect does not cancel the work. Health, Operation polling, and shutdown
remain available.

Cancellation is a control outcome, not a provider failure or retry-budget charge.
The watch signal resets only after clearing succeeds and the storage generation
advances. `DIAGNOSTICS.md` documents the cancellation fields and terminal events.

With storage users drained, the operation clears application data while
preserving schema and its own Operation, deletes the artifact and staging trees,
re-registers loaded policies, and resets in-memory caches, worker bookkeeping,
cadence, and health counts. Operation success marks clearing completion and
release of rebuild maintenance. During startup, the rebuild ends the countdown
and supplies registered policies and empty caches; worker handoff proceeds
without another wait. Resuming the pipeline rebuilds the corpus, including
all embeddings and annotations; queries see the progressively rebuilt corpus.

The rebuild Operation is also the durable recovery marker. A clearing failure
keeps storage paused; after durable acceptance, shutdown before maintenance release leaves
failed or interrupted work. Normal startup detects these rebuilds and requires
an explicit retry; annotation dry-run mode refuses them. This protects the boundary between
transactional database clearing and filesystem deletion.

## 2. Storage planes (D1)

Two physical planes live under `{index_root}/fabric/`.

### 2.1 Hot plane — the relational SQLite database

`{index_root}/fabric/fabric.sqlite3` (`src/hot_plane.rs`). Rows are envelopes and
pipeline state; heavy payloads are referenced by URI + hash into the artifact
store. Policy, all as code constants and never operator-tunable:

- `journal_mode=WAL`, set at setup and **validated fatally at startup** — a
  missing or non-WAL database points at setup and is never repaired at runtime.
- `synchronous=FULL` on write connections (a lost-but-served record is a
  durability breach).
- Read paths open `SQLITE_OPEN_READ_ONLY` (`open_read`); write paths use
  `open_write`.
- `foreign_keys=ON` applied per connection (not in the DDL).
- `busy_timeout` and statement deadlines are code constants
  (`BUSY_TIMEOUT_MS = 5_000`, `STATEMENT_DEADLINE_MS = 5_000`), not config.
- Its own `PRAGMA user_version`, starting at **1**.
- A fresh connection per operation; writes go through the shared IMMEDIATE
  transaction helpers (`begin_write_transaction`/`commit_transaction`/
  `abort_transaction`), reads through `begin_read_transaction`.

Runtime never creates or migrates schema. The schema arrives only via the
explicit `--setup-storage` path (`setup_fabric_storage`) applying
`sql/fabric/schema.sql`; schema changes are new setup scripts run deliberately.

**Fabric tables** (`sql/fabric/schema.sql`):

```
source_objects              content identity + active_parse_id pointer (§10, §14)
source_locations            where each object was seen
acquisition_records         provenance of each acquisition
sync_queue                  durable detection queue (+ nullable operation_id)
parse_runs                  one row per parse attempt; status + held_reason
content_units               canonical typed ContentUnits (parse-scoped)
unit_relationships          structural UnitRelationships
retrieval_projections       projection envelopes (type, freshness, refs)
query_execution_records     QER metadata rows (writer deferred — see Section 9)
forensic_snapshots          snapshot metadata rows
operations                  §34.6 Operation records (status-guarded)
semantic_annotations        entity/relation/summary annotations
annotation_memo             §21.2 producer-output memo cache
system_events               in-plane event log
chunk_projections           chunk grain for retrieval targeting
chunk_dense_vectors         dense embeddings per chunk
unit_multivector_projections ColBERT token matrices per unit
graph_entity_mentions       normalized entity → unit_ids (D9 entry)
graph_entity_edges          normalized name-pair relation edges, relation_type
                            (predicate) stored normalized too (D9 traversal)
policy_versions             append-only system-assigned registry of operator
                            policy-document content hashes (Section 3.2)
```

### 2.2 Artifact store — content-addressed filesystem tree

`{index_root}/fabric/artifacts/sha256/<first-2-hex>/<full-64-hex-hash>`
(`src/artifact_store.rs`). Write-once and immutable: a same-hash rewrite is a
no-op; the same hash with conflicting bytes is an error. Writes are crash-safe —
bytes go to a uniquely named temp file in the destination shard directory and
are published with an atomic same-directory rename, so a crash leaves at worst an
orphan temp file, never a partial blob at a hashed path. Raw sources, canonical
parse bundles, projection payloads, and (when the audit tier lands) full QERs
live here, referenced from the hot plane by `ArtifactRef` (`uri` + `hash` +
`size_bytes`).

### 2.3 Event log

`system_events` is an in-plane table, written inside the owning operation's
transaction/boundary discipline.

```
{index_root}/fabric/
├── fabric.sqlite3                     hot plane (WAL, synchronous=FULL, user_version=1)
└── artifacts/sha256/<2hex>/<hash>     content-addressed, write-once, temp+rename
```

## 3. The autonomous cycle

The scheduler (`src/scheduler.rs`) drives one `std::thread` scan/drain loop at a
**knob-free adaptive cadence**. Detection coalesces into a **durable sync queue**
(`sync_queue`): at most one pending change per `source_key`
(`"{source_system}:{native_uri}"`), advanced by `enqueue_coalesced`; a new
detection is what re-pends a failed row. Cadence is an internal EMA with no
operator knob — it backs off multiplicatively after change-free cycles, when the
queue will not drain, and after failed cycles, and tightens when work appears.
The shared `MAX_BACKOFF_MS` in `src/util.rs` caps scheduler waits, including the
scan-duration floor, and dense HTTP retry backoff at 60 seconds. Annotation
retries use independent configured timing (Section 3.1). The ceiling bounds
sleep, not work duration or request timeouts; cadence logs report delay changes.

```
detect ──▶ acquire ──▶ parse ──▶ build projections ──▶ gate / activate
(coalesce   (importer   (mime      (chunk·lexical·        (single active parse
 into        owns        routing +   dense·multivector·     per source; dominance;
 sync_queue) canonical   §12/§13.1   derived-view, one tx)  hold path)
             writes)     hard gates)
```

- **Detect.** A scan enqueues detections via `enqueue_coalesced`; autonomous
  detections carry no Operation, while an HTTP-triggered enqueue threads an
  `operation_id` through the queue row (COALESCE keeps whichever is set).

- **Acquire.** The importer (`src/acquisition.rs`) **owns every canonical
  write**. It dedups by content hash and writes the **raw bytes to the artifact
  store first**, deliberately outside/before the SQL transaction
  (`import_validated_bundle`: `store.put_bytes` then a single write tx). Objects
  are keyed by immutable content identity; locations record where each was seen.

- **Parse.** `dispatch_parse_chain` routes by the stored authoritative MIME type
  to a worker (`src/parse/pdf_worker.rs`, `src/parse/text_worker.rs`); an
  unroutable type is warn-only. Workers are untrusted producers outside the hot
  retrieval trust boundary (§12.1): they emit a **staged candidate bundle**, and
  `src/parse/importer.rs` performs digest verification and the **§13.1 hard
  gates** (id assignment, local-ref integrity, capability profile,
  body/content-type mapping, resource limits) before writing canonical
  `content_units` / `unit_relationships`. The canonical parse **bundle is written
  to the artifact store before the ready transaction commits**. A rejected bundle
  or gate breach becomes a durable failed `parse_runs` row (`Ok` with a failed
  status); `Err` is reserved for faults of the canonical side itself.

  Before staging candidates, both workers run versioned Rust cleanup
  (`src/parse/cleanup.rs`): conservative prose repair, explicit furniture removal,
  and geometry-supported PDF paragraph reflow. Original locators survive merges;
  reading-order links are rebuilt before canonical hashing. Original extraction,
  pre-cleanup candidates, and the cleanup report are archived through the existing
  artifact store and `parser_raw_output_uri`, including verified failed parses.
  Cleanup changes parser identity; existing sources receive it through reparse.

  Worker dispatch is guarded three ways, in order (`src/scheduler.rs`):

  1. **§13.5 no-blind-retry guard** (`evaluate_no_retry_guard` →
     `NoRetryGuardDecision`). A prior FAILED parse run of the same (source,
     parser identity, parser configuration) tuple suppresses dispatch — a
     source object is 1:1 with its content hash (§10 dedup), so identical
     bytes through an identical parser fail (or succeed) identically. Only
     new content (a new `sourceHash`, hence a new source object) or a new
     parser identity/configuration re-parses. A crash-orphaned READY run is
     not re-parsed either: the `GateExisting` replay arm rebuilds its
     content-derived projections and gates the existing run. This determinism
     rule is why the operator force re-parse override (Section 1.1) exists.
  2. **Corpus containment.** Both routes resolve the worker's input through
     the `crate::source` containment authority (`src/source.rs`): traversal
     components are rejected, and the canonicalized, symlink-resolved path is
     verified to lie inside the corpus root — a corrupted or foreign queue
     row cannot point a worker outside the corpus.
  3. **Content-identity check (§10 rule 1).** The live corpus bytes are
     re-hashed against the staged acquisition's `source_hash` immediately
     before the worker runs; a mismatch skips dispatch (self-healing — the
     changed bytes are re-detected, re-staged, and re-parsed under their own
     new SourceObject by the next scan), so changed bytes can never bind
     old-hash identity to new content.

  The PDF worker's parse engine is an external **Docling CLI child process**
  (`src/docling.rs`): spawned as a `Command`/`Child`, waited on by a poll
  loop with a per-document timeout, monitored for live process activity
  (`src/docling_activity.rs`), its stdout/stderr captured as bounded
  diagnostics — all on the scheduler thread's synchronous dispatch. The
  `[docling]` options are **identity-bearing parser configuration** folded
  into `parserConfigHash`, so a Docling option change is a NEW parser
  identity: the §13.5 no-retry tuple no longer matches, and unchanged bytes
  legitimately re-parse into a net-new canonical graph.

- **Build projections.** Between import and gate, `build_content_derived_projections`
  builds the content-derived projections in **one transaction**
  (`build_projection_transaction`): `chunk::build_chunks` →
  `lexical::build_lexical_index` → `dense::build_dense_vectors` →
  `section_dense::build_section_dense` →
  `multivector::build_multivectors` → `view::build_derived_view`. Each carries a
  `retrieval_projections` envelope with a freshness status.

  The fine chunker (`src/projections/chunk.rs`) measures normalized targeting
  text with a per-build ColBERT tokenizer copy, disabling truncation and padding
  while retaining special tokens. Oversized words split at measured UTF-8
  boundaries; their final suffix can join following words. Every retained chunk
  is recounted and checked against 512 tokens before persistence. The
  400-character minimum applies to normalized fragments, including split
  remainders. The chunker name, version, and limits determine `chunkerConfigHash`.

  Section windows retain the existing passage vectors and add heading-prefixed
  canonical text, capped at 2,048 tokens measured with the local ColBERT
  tokenizer. Unsectioned text uses explicit document-scoped windows. The
  self-contained artifact records exact text fragments, canonical membership,
  model/tokenizer identity, window-policy hash, and vectors. Its `dense_vector`
  envelope uses `index_name = section_dense_v1`; the passage envelope retains
  a null index name. Both must be fresh before activation. No schema migration
  is involved; existing corpora require the explicit rebuild-all operation.

- **Gate / activate.** `activation::gate_and_activate` enforces a **single active
  parse per source** with dominance gating. A non-dominant but valid parse takes
  the **hold path** (`ready` with a `held_reason`) rather than activating.
  Superseded held candidates are threaded out for post-barrier cleanup (see
  Section 4).

**Staging lifecycle.** Acquisition and parse both work through staging
workspaces under the index root — `{index_root}/fabric/staging/acquisition`
(`acquisition_staging_root`) and `{index_root}/fabric/staging/parse`
(`parse_staging_root`); staging is never canonical storage. At startup, before
the first cycle can start workers, `sweep_orphan_parse_temp_dirs` removes
crash-orphaned `.tmp` parse workspaces (safe because workers run inline on the
single scheduler thread, so any temp directory visible at thread start is crash
leftover). A consumed bundle is deleted only AFTER its entry's whole unit of
work completes (import → parse chain → queue completion), so a crash replay
finds it intact; a FAILED parse bundle is deliberately retained on disk for
inspection (§12.2).

After activation, the annotation worker takes over.

### 3.1 Post-activation annotation build

The annotation worker (`src/annotations/worker.rs`) is one dedicated
`std::thread`. It discovers work for active parses using the sealed policy's
`post_activation_types` (`src/annotations/policy.rs`).

**Excerpts and stages.** `producer::build_invocation_plan` and
`excerpt::split_text` partition each evidence-bearing unit without dropping
text. Each invocation contains one fragment bounded by `max_input_chars`.
Splitting prefers paragraph, sentence, then whitespace boundaries, with a
Unicode-character cut when necessary. Provenance `inputRefs[].textRange`
records start/end Unicode scalar offsets (end-exclusive) and an exact UTF-8
text hash. Section ownership groups discovery; it does not enlarge requests.

`src/annotations/stages.rs` defines the prompts and schemas;
`src/annotations/chains.rs` runs one goal per call:

- Entity candidate names → accepted types or explicit rejections.
- Source statements → relationships per selected statement → supporting
  quotations in bounded candidate batches.
- One summary per excerpt.

Later requests use the source excerpt and prior outputs from that chain.
Intermediate candidate arrays are also bounded by the configured excerpt cap.
Shape checks validate required fields, source substrings, name mappings, and
receipt indexes. Relations retain `evidenceQuotes`. Semantic verification is
not implemented.

Entity typing requires both `entities` (`name`, `entityType`) and `rejected`
(`name`, `reason`) arrays. Their combined names must match the supplied candidate
multiset exactly. Only accepted items become annotations; rejected names and
reasons remain in the call transcript. Empty names skip typing, and all-rejected
output uses existing successful empty coverage. Rejection is not a retryable
failure. Missing/excess names, empty required text, and an accepted
`NOT_AN_ENTITY` type are malformed outputs.

A completed chain commits its output set and any memo entry in one worker
transaction. Empty results record fresh coverage without a memo entry.
Intermediate results are not checkpointed; a failed or interrupted chain
restarts as a whole.

**Completion and reuse.** Shared `KeyMaterial` in `src/annotations/memo.rs`
contains source-unit content hashes, exact fragment ranges/text hashes, and
annotation type:

- `content_key_hash` additionally includes target unit IDs, identifying
  completion at each canonical source location without producer identity.
  `store::fresh_content_key_hashes_for_parse` supplies completed coverage;
  failed/building rows without a fresh sibling are reopenable.
- `memoization_key_hash` excludes target unit IDs and includes producer identity,
  allowing equal source content at different locations to reuse output with
  their own annotation references. Producer identity covers ordered stage
  prompts/schemas, model/endpoint, excerpt cap, and generation controls.
  Completion stamps the running producer's memo key onto its annotation rows.

The worker flushes a pending duplicate memo key before preparing another request
for it, so the next lookup can reuse the committed result. Legacy whole-group
annotations remain intact; their keys cannot satisfy new fragment coverage.

After every required key in the current excerpt plan has fresh output, and the
parse is still active, the worker builds summary then graph projections in one
transaction. Legacy failed rows outside that plan do not block publication.
Derived names, entity types, and predicates use `normalize_entity_name`;
stored annotation strings retain their producer output.

**Retries.** Two counters in `AnnotationRetryState` track failed chain attempts
per annotation and process run. Malformed outputs use
`annotation_max_retries` and the fixed `annotation_retry_interval_seconds`.
Execution failures (call/protocol failures, token-limit termination, and internal
producer faults) use `execution_max_retries`; their delay doubles from
`execution_retry_initial_delay_seconds` to `execution_retry_max_delay_seconds`.
Annotation intervals have no ceiling; neither path uses `MAX_BACKOFF_MS`.

A category is exhausted when its failure count exceeds its retry allowance.
Zero permits the initial attempt only. Exhaustion skips the annotation, emits
`annotation_worker.retry_exhausted`, and contributes to the `exhausted` health
count. Cancellation and scheduling deferrals consume neither allowance.
Counters and timers reset on restart or rebuild; they are not persisted.

Temperature starts at zero and scales as
`min(invalid_outputs / annotation_max_retries, 1.0)`, with a zero-limit guard.
Execution failures do not advance it. Actual temperature is recorded in call
logs and completed provenance. Retry settings enter application configuration
identity, not producer memo identity.

`RetryDelay` tracks monotonic eligibility per annotation. Ineligible work is
skipped; the next cycle wakes for the earliest encountered retry or ordinary
discovery interval, whichever is sooner. A call failure ends the cycle after
the current wave's results are recorded. Failure logs retain source errors,
both counters and limits, the delay, remaining wait, and next action. See
README's annotation settings for shipped values.

The worker loads its client inside the thread. A client-load failure parks it
for the run; annotation health is diagnostic-only and never gates readiness.

### 3.2 Operator policy documents and the auto-versioning registry

Two **operator-editable policy documents** (`src/policy.rs`) sit beside the
sealed code documents of Section 6, but their identity is content-hashed rather
than build-sealed. `[policies]` config holds their **paths only**
(`entity_match_file_path`, `annotator_naming_file_path`, `src/config.rs`,
`deny_unknown_fields`); the documents themselves are strict TOML
(`deny_unknown_fields` on every struct):

- **`policies/entity-match.toml`** — the graph-entry fuzzy-match ruleset
  (`EntityMatchPolicy`: `acronym.{enabled,min_name_tokens}`,
  `token_prefix.{enabled,min_token_len}`, `max_fuzzy_candidates`) consumed by
  the query graph channel (Section 6). Shipped neutral: both fuzzy classes
  disabled.
- **`policies/annotator-naming.toml`** — retained as a required, validated and
  versioned document, but not applied to the single-goal annotation prompts.
  Its hash remains in application identity; edits do not change producer memo
  identity. An empty rule list is valid.

Both are **loaded once at startup and fatal on invalid** (`src/main.rs`); each
is content-hashed over its **parsed canonical serialization**
(`canonical::canonical_sha256_hex_of`), so comment/whitespace edits do not
change identity. Editing a document requires a **service restart** (config is
startup-only). Both content hashes fold into `ApplicationIdentity`
(`entity_match_policy_hash`, `annotator_naming_policy_hash`, Section 7); the
config-hash projection carries their **paths only**.

Versioning is **system-assigned** through the append-only `policy_versions`
table (`policy_id`, `version`, `content_hash`, `observed_at`; INSERT-only by
convention). `register_policy_version` reads the latest row per policy: an
unchanged content hash writes nothing; a new or changed hash appends
`latest.version + 1` and mints a `policy.changed` SystemEvent in the same
transaction. A **revert to previously seen content still advances** the counter
(it records change events, not distinct contents). Registration runs only when
the fabric plane is ready at startup; a **plane-missing start defers**
registration to the next valid-plane start.

## 4. Cutover discipline

`CutoverRegistry` (`src/state.rs`) hands out **one barrier per source** so
distinct sources never contend. §31.1 invariants, held across a single
read-decide-swap:

- The barrier covers the active-parse pointer swap **plus its paired in-memory
  publish**, nothing else. It is a few bounded SQLite statements —
  milliseconds; non-disruptiveness rests on brevity and per-source scope.
- The **dense-cache publish happens under the held barrier**: activation commits
  the durable pointer write, then `publish_dense_cache` loads the newly active
  parse's dense plane and **evicts the predecessor's**, all before the guard
  drops. A load failure returns `Err` — a searchable active parse with no loaded
  dense plane is a broken publish. The durable active-pointer write and its
  paired in-memory dense publish form **one publish**, and the held barrier
  guarantees that publish never interleaves with another publish of the same
  source (`src/activation.rs`).
- Queries targeting a source whose barrier is **active** are rejected with a
  retryable 503 (`cutover_barrier_active`), via the post-capture
  `reject_if_active` probe in the query pipeline.
- Queries **already in flight** execute entirely against their **captured
  pre-cutover WAL snapshot** — one per-query read-only transaction opened as the
  pipeline's first act (see Section 6). The dense planes they scored against are
  in-memory `Arc` clones captured inside that snapshot, so a mid-query cutover
  cannot mutate them.

### Held-candidate cleanup (Ruling 1 / §31.2)

Superseded held candidates that will never activate are cleaned via a third
`SupersededCleanupMode::HeldSupersession` arm that gates over **each candidate's
own pre-activation snapshot**, completing `archiving → archived` with a
`parse.archived` event. `supersede_other_held` returns the superseded ids;
`gate_and_activate`/`hold_candidate`/`accept_held_parse`/`discard_held_parse`
thread them out; the scheduler and the C10a admin call sites clean them
post-barrier, mirroring the predecessor arm. Gate failure halts/retains (no
auto-retry). The verification and deletion mechanics behind this cleanup live
in Section 5.

## 5. Lifecycle forensics: snapshots, deletion, restore

The unifying invariant: **all three exits from `archiving` pass a verified
snapshot gate before any hot-row deletion, and rollback is restore-from-store,
never recompute.** Nothing in the forensics path re-parses, re-embeds, or
re-scores.

### 5.1 Forensic snapshots (`src/snapshot.rs`)

Snapshots are minted at every lifecycle transition that requires one:
`pre_activation_snapshot` (before activating a ParseRun),
`post_activation_snapshot` (after a cutover), `pre_deactivation_snapshot`
(before deactivating a source), plus `request_snapshot`, which mints **only**
`manual` or `incident` snapshots (the lifecycle types are scheduler-triggered,
never requestable).

Snapshot creation is two-phase, artifact-store-first:

1. **Archive, then seal.** Every hot-only plane is archived and every
   already-archived artifact referenced into a **§30.4 manifest**; every
   referenced blob is in the write-once store **before** the manifest seals,
   and the **self-hashed manifest is written LAST** — a manifest never
   references bytes that are not in the store. No SQL is touched in this
   phase.
2. **Header row.** Only then does one write transaction insert the
   `forensic_snapshots` metadata row carrying `manifest_uri`/`manifest_hash`
   (the `manifestHash` that verification later checks against).

Every mint carries the **application identity** (`src/identity.rs`,
`ApplicationIdentity::capture`): system version, spec version, compiled build
features, and the aggregate configuration hash, captured ONCE at startup and
threaded explicitly to every snapshot-minting site (never a global). The header
row stamps the system and spec versions, and the full identity is archived into
the manifest's runtime artifacts (`src/snapshot.rs`) — the audit provenance
that pins the code-and-config half of a replay environment.

Every snapshot stamps the MVP `ReplayProfile`: `evidenceReplayMode =
bit_exact`; retrieval and generation replay = `not_supported` (the
probe/tolerance machinery is post-MVP — see Section 9).

### 5.2 Verification (`src/snapshot/verify.rs`)

- `verify_mechanical` — the manifest's self-hash plus a **re-hash of every
  blob-backed reference**. Runs on **every** snapshot.
- `verify_deletion_gate` — mechanical verification **plus** deterministic
  index-rebuild verification over the subject parse: the chunk plane is
  compared on its deterministic columns, dense and multivector blobs are
  **decoded and compared**, and the graph plane is **re-derived from the
  archived annotations** and compared; the lexical index is verified
  transitively through the chunk plane. The rebuild check re-imports archived
  bytes and re-derives from archived rows — it **never re-embeds**.

The verifier returns only a verdict; the caller owns the consequences.

### 5.3 Archive-verify-delete (`src/restore.rs::complete_superseded_parse`)

Every superseded parse leaves `archiving` through one of three
`SupersededCleanupMode` arms, each locating its **gating snapshot** by the
subject identity `(subject_source_id, subject_parse_id, snapshot_type)` — the
gate reuses the lifecycle snapshot the scheduler already minted and **never
re-takes one**:

```
mode                      gating snapshot                     archived completion
─────────────────────     ─────────────────────────────────   ───────────────────
ActivationSupersession    predecessor's post_activation       yes (archiving→archived)
Deactivation              source's pre_deactivation           no
HeldSupersession          the held candidate's OWN            yes (archiving→archived)
                          pre_activation
```

A **failed gate halts before any write transaction**: no deletion, the
superseded state is retained, there is no auto-retry, and the verification
error propagates. On a pass, the hot rows of the superseded parse are deleted
in one transaction in **derived-before-source order** — FTS5 lexical rows
(scoped through the chunk subselect), graph mentions and edges, dense and
multivector vectors, chunk projections, semantic annotations, unit
relationships, content units, and the projection envelopes. Two row classes
deliberately survive: **`parse_runs` rows are never deleted** (the durable
lifecycle record), and **`annotation_memo` is never touched** — the memo is
keyed on content × producer identity, not on a parse, so producer output
survives supersession.

### 5.4 Rollback-as-restore (`src/restore.rs::restore_source_from_snapshot`)

Restore **re-imports** a source's canonical rows and projection payloads from
its ForensicSnapshot's archived artifacts, **preserving IDs**; vectors are
byte-reproduced from the archived blobs. The non-archived planes — the FTS5
lexical index and the graph tables — are **deterministically rebuilt** from
the restored rows. The dense-cache publish happens **under the held per-source
barrier**, exactly as at activation (Section 4). A hard module invariant: no
model call anywhere in the restore/verify path — a grep for
`embed|InferenceRuntime|score_|docling` over `restore.rs` must match no code
symbols (its only hits are the comments stating this invariant).

Section payloads are explicitly pinned in snapshot manifests. Verification checks
their exact canonical fragment coverage and complete live/archive envelope sets.
Restore requires the target's passage and section representations before writing;
pre-feature snapshots are incompatible. Valid section vectors are read from the
archived artifact and published with the passage plane without inference.

### 5.5 Deletion lifecycle (`src/deletion.rs`)

Deletion propagation is **evidence-based** and runs post-drain in the
scheduler cycle:

- **Deactivation (§11.3).** When a source's LAST `current` location is gone
  (deletion evidence recorded by acquisition), the scheduler mints the
  `pre_deactivation` snapshot, then under the source's cutover barrier sets
  `source_objects.deactivated_at`, evicts the dense plane (`evict_parse`),
  and records `source.deactivated`. `active_parse_id` is deliberately left
  intact — deactivation is a **reversible flag-set**. It is the
  `deactivated_at` flag, NOT location status, that removes a source from
  All-scope queries.
- **Access lost (§11.2) is NOT deletion.** When enumeration loses sight of a
  whole scope, locations become `access_lost`: the document presumably still
  exists; observation was lost. **Serving continues** — no deactivation, no
  barrier — and the freshness clock stops simply because `last_seen_at`
  stops advancing. Access loss never feeds deactivation.
- **Reappearance (§11.4).** A deactivated source regaining a `current`
  location is **restore plus flag-clear** (`deactivated_at = NULL`, guarded
  against double-clear), never a re-activation and never a re-embedding.

## 6. The query pipeline

`execute_query` (`src/query/execute.rs`) is a synchronous function run inside a
`spawn_blocking` task, entered only after the admission permit is held
(Section 7).

### DP1 — one read snapshot, opened first

The pipeline's **first act** is to open one read-only connection and begin one
DEFERRED read transaction (`begin_read_transaction`). The scope-filtered active
`(source_id → parse_id)` set is captured **inside** that transaction, so scope
capture and every subsequent read share a single pinned WAL snapshot. The
passed `ResolvedScope` / `&[CapturedParse]` **is** the scope mechanism (§6, §38):
there is no separate scope enforcement pass. Each captured parse carries its
dense plane as an `Arc<DensePlane>` clone taken under the same snapshot.
The plane contains passage and section vectors together. Startup reloads active
planes before starting workers; a scoped parse missing either representation
causes an explicit rebuild-required query error.

```
open read-only transaction → capture scoped active parses → cutover-barrier probe
       ↓
dense passages + section nominations + lexical chunks → canonical units ┐
graph entity matches + one semantic hop ─┴→ rank fusion (100 units)
       ↓
ColBERT MaxSim (persisted unit matrices)
       ↓
bounded same-section passages → final passage reranker → requested result count
       ↓
citations + full canonical constituents → { results, evidencePack, diagnostics? }
```

### Stages

- **Channels** (`src/query/channels.rs`). The **dense** and **lexical** channels
  generate chunk-grained candidates and resolve chunk → unit, excluding units
  explicitly marked as headers or footers. The **graph** channel (D9) performs
  entity-name matching of query text against stored entity-annotation names and
  one semantic relational hop over
  `graph_entity_mentions`/`graph_entity_edges`, tiered deterministically (multi-entity units,
  then direct mentions, then one-hop related). No LLM call is made in the query
  path. Each channel supplies up to 100 candidates independently of the requested
  result count. **RRF across all three channels** deduplicates canonical units
  into a 100-unit pool before ColBERT scoring.

  Dense retrieval uses the same query vector to shortlist 20 section windows.
  Each nominates up to five eligible units by their best fine-chunk cosine,
  strictly within that window's canonical membership. Units lacking fine vectors
  contribute context but cannot be nominated. Direct and section-guided lists
  merge with equal-weight RRF, deduplicate, and cap at 100 before contributing
  one dense ranking to outer fusion. Result provenance retains both routes and
  their section headings.

  Entry matching is **policy-threaded** (the D9 amendment). `graph_channel`
  receives the `EntityMatchPolicy` (Section 3.2), threaded from startup through
  `execute_query` → `run_pipeline_body`. An always-on **exact** class is joined,
  when a class is enabled, by **acronym** (a single query token equals the
  first-letter acronym of a stored name whose token count is at least
  `min_name_tokens`) and **token_prefix** (each query token of at least
  `min_token_len` chars is a leading prefix of the correspondingly positioned
  stored-name token) classes, capped at `max_fuzzy_candidates`. Candidate order
  is rank-only and deterministic: tier, then match class (`Exact` < `Acronym` <
  `TokenPrefix`), then matched-name character length descending, then name
  ascending, then unitId then parseId ascending. **When both fuzzy classes are disabled (the shipped default) the
  path is byte-identical to the prior exact-only behavior:**
  `entity_names_for_parse` (`src/projections/graph.rs`, the per-parse name
  enumeration) is **never called**, so the class order component is constant.
  These knobs live in the entity-match policy document, deliberately **not** the
  `RetrievalProfile`.
- **MaxSim** (`src/query/rerank.rs`). ColBERT MaxSim **re-scores the already-fused
  pool** from persisted C6e matrices; the loader decodes stored blobs and never
  re-embeds. This is a rerank/scoring stage, not candidate generation — see the
  deferred `multi_vector` channel in Section 9.
- **Passages** (`src/query/passages.rs`). Starting from MaxSim-ranked units,
  construct same-section passages in canonical reading order, bounded to 512
  ColBERT tokens and 64 contributing units. Merge overlapping passages when they
  fit; preserve structured-content boundaries. Oversized single-unit prefixes
  carry `truncated: true`, while raw evidence retains the full canonical body.
- **Reranker** (`src/query/rerank.rs`). Scores up to 30 passages, or the requested
  count if larger (maximum 100), with their section headings via the
  config-selected backend. Model-call gating is **caller-side and
  backend-aware**: the shared model-call gate is acquired only when a local
  accelerator-backed model is invoked; a remote HTTP reranker is not gated behind
  the local runtime lock.
- **Final results and assembly** (`src/query/passages.rs`, `src/assembly/`).
  Apply the requested passage count (default 10, maximum 100), resolve source
  locations and physical PDF page references, and retain exactly the selected
  canonical constituents in `evidencePack`. AssemblyPolicy v2 adds no neighbors
  or containers; raw safety-limit failures are errors. Citation and evidence
  reads share the query transaction, and every pack includes an assembly trace.

### Sealed policy documents

Three compile-sealed policy documents capture retrieval, evidence-retention,
and annotation requirements. Each has a stable version and self-hash over its
canonical serialization, excluding the hash field, and an `active_*()` accessor.

- The **`RetrievalProfile`** (`src/query/profile.rs`, `active_profile` /
  `seal_mvp_profile`, version 3) supplies the RRF
  fusion constant (`rrf_k = 60`), per-channel and MaxSim candidate pool sizes
  (`100`), section shortlist (`20`) and nominations per window (`5`), the reranker
  passage pool size (`30`, raised to the requested count up to `100`), and the D9 graph hop budget (`1`). These are
  deliberately NOT config keys: the D3 ruling moved
  every retrieval knob out of operational config into this hashed document,
  because a mutable config surface cannot guarantee a stable, auditable
  retrieval identity.
- The **`AssemblyPolicy`** (`src/assembly/policy.rs`) records selected-passage
  retention and raw safety ceilings — the Assembly stage above.
- The **§21.4 required-annotation-set policy** (`src/annotations/policy.rs`,
  `RequiredAnnotationSetPolicy`) rules which annotation types must be fresh
  BEFORE activation (empty in the MVP document: nothing blocks activation)
  and which build AFTER activation with visible freshness; the annotation
  worker's discovery reads it (Section 3.1).

The JSON response contains ranked `results` and `evidencePack`, plus `diagnostics`
only when requested with `debug: true` (`POST /query`).
`queryExecutionRecordId` is **omitted** — a recorded narrowing pending the QER
audit tier (Section 9), addable additively. `QueryStageLatencies` records
per-stage timings, including passage construction and the duration the WAL read
snapshot was held. Diagnostics also retain `channelHits`, `fusedPool`, `maxsim`,
`passageCandidates`, and `reranked` stage outputs.

## 7. Health and admission internals

Health is assembled entirely **in memory** via a publish-into-slot pattern
(`src/state.rs`, `AppState::health()`): each owning thread writes its own
`Arc<Mutex<…>>` slot every cycle, and `health()` only reads slots — it opens no
database connections. Slots are poison-recovered on read.

- The **scheduler** publishes `SyncHealth` (queue depths, cycle stats, cadence,
  freshness) and a separate `FabricHealth` slot of per-cycle, per-source-system
  fabric counts (held, serving-stale, stuck-`building`, access-lost,
  unparseable-mime, verification-halted). Each count carries an **as-of** label
  so an operator never reads a count without knowing when it was taken.
- The **annotation worker** publishes `AnnotationHealth`: parked state and counts
  from the last completed cycle, including exhausted work. This slot is not
  live per-call progress; call starts and outcomes are recorded in the service log.
  `GET /sync/status` reads only the scheduler's slot.
- **Readiness = {inference, sync}** —
  `inference_component.ready && sync_component.ready`. The fabric and annotation
  counts are **diagnostic-only** and NEVER gate readiness; a degraded diagnostic
  must not make a running service look down.
- `AdmissionGate` (`search_admission`) enforces a **single in-flight search**
  (`MAX_IN_FLIGHT_SEARCH = 1`, a code constant, not operator-tunable). The
  `/query` handler acquires the permit **first**, before `spawn_blocking`;
  over-capacity fails fast. `AdmissionGate::snapshot` feeds a diagnostic-only
  `search_admission` health component.
- The **inference** ready details are **per-backend**. The dense line carries
  `dense backend: http|local` plus that backend's facts — for HTTP the endpoint,
  model, dimension, timeout, key-file **path**, and the startup smoke
  vector-count/norm (never the key contents); for local the runtime's own
  detail. The artifacts line reads `remote HTTP backend` (in place of a local
  artifact detail) whenever a backend is HTTP
  (`src/inference/artifacts.rs`, `src/inference/dense_backend.rs`).
- **Identity capture** (`src/identity.rs`) records the dense backend kind and
  its per-backend facts — local `path`/`max_tokens`, or HTTP
  `endpoint`/`model`/`timeout_seconds`/`api_key_file_path` — with credential
  files captured as their resolved **PATH only**, never contents. It also folds
  the two operator policy-document content hashes into `ApplicationIdentity`
  (`entity_match_policy_hash`, `annotator_naming_policy_hash`, Section 3.2),
  captured once at startup alongside the config hash; the config-hash projection
  itself carries the two document **paths only**.

## 8. Model runtime

`InferenceRuntime` (`src/inference/mod.rs`) is initialized once at startup and
holds the three selected retrieval backends and an optional local accelerator:

- **Dense embedding** (`src/inference/dense_backend.rs`). An enum, not a trait:
  `DenseEmbeddingBackend::Local(DenseEmbeddingRuntime)` (the in-process Qwen3
  Candle runtime, `src/inference/dense.rs`, `qwen3.rs`) or
  `DenseEmbeddingBackend::Http(HttpDenseClient)` (an OpenAI-compatible
  `/v1/embeddings` client). The backend is config-selected via
  `[models.dense].backend` and the variants are **exclusive — there is no
  cross-backend fallback**. Both surface `embed_query_vector` (query time, with
  the shared retrieval instruction prefix) and a passage surface (the Local
  runtime `embed_passage_vector`, batch-1; the Http client the batched
  `embed_passage_vectors`). The HTTP path restores input order by the response's
  per-entry `index`, validates every returned vector (dimension against the
  configured width, all values finite, finite nonzero norm), and L2-normalizes
  client-side so both backends preserve the unit-norm invariant.
- **ColBERT late-interaction** (`src/inference/colbert_backend.rs`).
  `ColbertBackend::Local` loads ColBERT-Zero through Candle;
  `ColbertBackend::Http` sends document and query token IDs to vLLM's `/pooling`
  endpoint with `task = token_embed`. Both use the same local formatting and
  tokenization contract. The HTTP backend loads a matching `tokenizer.json` but
  no local model weights, validates indexed 128-dimensional token matrices, and
  normalizes their rows. Document matrices are persisted in
  `unit_multivector_projections`. With the HTTP backend, queries embed only the
  query and perform MaxSim against stored matrices on the CPU; startup checks
  batched remote embedding and CPU scoring. Backend selection is exclusive,
  with no fallback or implicit HTTP retry.
- **Reranker** (`src/inference/reranker_backend.rs`). An enum, not a trait:
  `RerankerBackend::Local` (ModernBERT sequence classifier on the local
  accelerator) or `RerankerBackend::Http` (Cohere-compatible remote client).
  Exactly one instance exists and the variants are **exclusive — there is no
  cross-backend fallback**.

**Accelerator initialization is conditional.** If any retrieval backend is local,
`[inference]` selects its CUDA or Metal device and the binary must include the
matching Cargo feature; local model inference has no CPU fallback. If dense,
ColBERT, and reranker backends are all HTTP, startup skips accelerator
initialization. The required `[inference]` settings are then unused; local
tokenization and remote-ColBERT MaxSim need only the CPU.

**Model-call gate.** One process-global `ExclusiveGate` serializes access to
the shared accelerator-backed runtimes. Acquisition discipline is
**caller-side and backend-aware**: callers acquire the gate (per model role,
via `acquire_model_call_gate_on`) only around a live **local** model call, for
the duration of that work. For all three retrieval backends, the acquiring site
guards acquisition behind `uses_local_model_gate()`, which is `true` only for
the Local variant. On the **local** dense path the scheduler's projection build
and the query embedding
(`src/scheduler.rs`, `src/query/execute.rs`) each take the dense-role permit
around local dense work; local ColBERT batches and query scoring use their own
permits. All **HTTP** backends and CPU MaxSim acquire no accelerator permit.
The gate must never be held across an HTTP round-trip. The predicate acquires
nothing itself.

**HTTP dense backend** (`src/inference/dense_backend.rs`). When
`[models.dense].backend = http` the endpoint/model/timeout come from config and
an optional bearer key is loaded once at startup from an **owner-only**
`api_key_file_path` (the `.data-store-dense-api-key` convention, permission-
checked exactly like the reranker/annotator keys and never logged). Startup does
**no** local artifact validation or model load for this backend — it runs a
smoke round-trip through the configured endpoint (`dense_http_smoke_embedding` →
`dense_http_smoke_ready` progress stages) that exercises the full receive path
(dimension, finiteness, nonzero norm), so a misconfigured or unreachable
endpoint fails startup rather than surfacing per-request. Requests carry a
**bounded 429-only retry**: `DENSE_HTTP_RETRY_LIMIT = 3` retries on HTTP 429
only (every other status and every transport failure keeps the fail-immediately
policy), backoff `2/4/8s`, each retry logged `model_call.http_retry` at WARN, and
the retry count folded into the terminal `retried_attempts` field on the
completed/failed logs.

**Concurrent HTTP dispatch, serial SQLite writes.** Two sites fan HTTP calls out
on scoped OS threads while keeping **every SQLite write serial on the owning
thread** (rusqlite `Transaction`/`Connection` is not `Sync`, and the atomicity
contract requires one writer). The dense builder (`src/projections/dense.rs`)
packs `DENSE_HTTP_BATCH_SIZE = 32` passage windows and dispatches up to
`DENSE_HTTP_CONCURRENT_REQUESTS = 8` at a time; all windows join, then vectors
persist serially in chunk order, byte-identical to the local path. Honest
caveat, documented in `dense.rs`: because the builder runs on the **caller's**
transaction, the scheduler's `projection_build` writer lock is held **across the
HTTP fan-out** — a pre-existing take-the-caller's-tx property, accepted pending
the banked structural fix (embed before opening the transaction, lock only for
the commit). The annotation worker (`src/annotations/worker.rs`) fans out
up to `ANNOTATOR_CONCURRENT_CALLS = 32` independent producer chains per wave.
Dependent stage calls within each chain are sequential, with no nested fan-out.
The prepare/dispatch/commit split commits each chain's result serially; the pre-paid /
post-paid deferral ruling keeps its writes off the hot writer lock during the
fan-out. Rebuild/shutdown cancellation joins the outstanding calls, discards
unfinished results, and rolls back uncommitted writes before releasing admission.

**Annotator** (`src/annotations/llm_client.rs`). A synchronous producer interface
over cancellable async OpenAI-compatible HTTP. Each stage requests thinking,
strict JSON-schema output, and a complete response (`stream: false`) under the
configured call timeout. The output-token allowance includes reasoning.
Acceptance requires one choice at index zero, `finish_reason = stop`, and
nonempty message content before stage parsing.
One owned Tokio runtime services HTTP I/O; lifecycle and SQLite work
remain on synchronous worker threads. A maintenance watch cancels the complete
send/body wait on rebuild or shutdown. Dropping the request reports local
cancellation with `remote_outcome = unknown`, not proof that remote inference
stopped. There is one external endpoint, no fallback, and no local model-call
gate. It is **not readiness-critical**: a client load
failure (e.g. a bad key file) parks the annotation worker instead of failing
startup (Section 3.1).

Response diagnostics retain measured answer/reasoning character counts, received
bytes, provider response ID, finish reason, and provider token usage when supplied.
Call starts and terminal outcomes remain in the service log; no generation-
progress events are emitted.

`src/annotations/transcript.rs` owns the separate append-only `logs/annotator.log`
under the config directory. Each call owns a mutable transcript buffer separately
from its tracing context. The terminal result consumes that buffer and appends
REQUEST, RESPONSE, and RESULT as one contiguous group. Client clones share the
writer lock, held only for the group's write and flush. Both service and dry-run
clients filter a display copy of the request, omitting `response_format` and
`stream` while retaining temperature. Complete response bodies remain available
for protocol parsing; the transcript prints only content, reasoning, and the four
provider token counts specified in `DIAGNOSTICS.md`. Missing values remain
unavailable. Display filtering does not change requests or validation.
No chunk records are emitted.
Unfinished groups can be lost on process termination; call-start diagnostics
remain in the service log. Cancellation or a
failed receive can leave no complete body, which is reported explicitly.
A call context spans HTTP and structural validation;
the transcript's RESULT distinguishes success, failure, and cancellation from
database persistence. Open/write failures are reported in the service log and do
not change annotation outcomes.

## 9. Recorded architecture-level deviations

These are deliberate MVP narrowings, recorded in code and in the plan. The
Post-MVP Horizon (plan §5) holds the seams already built for each.

- **QER audit tier deferred.** No §24/§28 QueryExecutionRecord or QueryPlan model
  types exist in `src/model/`. The per-query `planHash`/`QueryPlan` and the QER
  writer are deferred; `queryExecutionRecordId` is omitted from the `/query`
  response (commented at the handler). The delivered system answers evidence
  questions at serve time only; retrospective per-query reconstruction is not yet
  available. Replay capability today is what each snapshot's `ReplayProfile`
  declares (Section 5.1): evidence replay `bit_exact`, retrieval and generation
  replay `not_supported`; scheduled restore drills with evidence replay over
  sampled QERs land with this tier (until then, restore is exercised through
  the lifecycle paths in Section 5). Seams already built: the
  `query_execution_records` metadata table,
  `qer_` IDs, per-source boundary timestamps, the hashed RetrievalProfile, and
  the `ContextAssemblyTrace` embedded in every EvidencePack.

- **`multi_vector` channel deferred.** An exhaustive MaxSim candidate-generation
  scan was measured infeasible on the target corpus/hardware. ColBERT MaxSim is
  therefore retained only as a **rerank/scoring stage over the already-fused
  pool**, not as a candidate-generation channel. The C6e token matrices are still
  built and persisted (`unit_multivector_projections`), so the future channel is
  an index-push path plus a channel client with no re-embedding.

- **Guarantee-4 gap (accepted).** The annotation producers make external
  chat-completions calls before the audit tier that would record external model
  calls exists. This is an accepted, recorded gap; the external-model-call record
  lands with the QER audit tier.

Cross-references: `PROTOCOL.md` is the API contract of record; `SPEC-SERVER.md`
and `SPEC-CLIENT.md` define the server and client surfaces; the canonical spec
(`canonical_content_graph_retrieval_fabric_v_0_3.md`) is the normative source for
EvidencePack, ContentUnit, UnitRelationship, RetrievalChannel, ResolvedScope, and
Operation.
