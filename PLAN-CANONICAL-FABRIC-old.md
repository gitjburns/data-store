# PLAN: Canonical Content Graph and Retrieval Fabric

Reference specification: `canonical_content_graph_retrieval_fabric_v_0_3.md`
(all `§` references below point into that document).

## Current Status

- 2026-07-05: Plan approved in structure and written. No phases started.

## 1. Target and Ground Rules

### 1.1 Target

The end state is the full v0.3 specification. The committed, step-planned
phases below cover the spec's own MVP scope (§36). Tiers the spec explicitly
defers (semantic annotations, graph/multi-vector/learned-sparse channels,
verified recompute, annotation memoization, compliance erasure) appear in
§4 "Post-MVP Horizon" as named phases with their reserved seams and entry
points, without step-level detail. They receive their own planning passes
when reached.

### 1.2 Transition strategy (decided 2026-07-05)

Rebuild as primary path. Nothing depends on the current service staying
operational or on already-ingested data surviving:

- The existing SQLite database contents (schema v4 in `sql/schema.sql`) are
  discarded, not migrated. No data-migration scripts.
- No API/CLI compatibility shims. Surfaces may break between phases until
  re-baselined.
- The existing corpus is re-acquired through the new acquisition layer
  (filesystem connector over the corpus root), giving documents genuine
  acquisition provenance instead of synthetic backfill.

### 1.3 Repository process rules that continue to govern

- Runtime never creates or migrates schema. All schema arrives via explicit
  operator-run setup (the `--setup-storage` pattern); schema changes during
  this programme are new setup scripts, run deliberately.
- Mandatory verification after every Rust change: `cargo fmt`,
  `cargo check` (plus `cargo check --features metal` when inference paths
  are touched), `cargo clippy`. No automated tests unless explicitly
  re-enabled. Runtime verification (starting the service, running
  acquisition/parse/search cycles) requires explicit user approval per
  phase; unverified behavior is reported as residual risk.
- Async stays confined to the HTTP transport shell. All new lifecycle
  machinery (scheduler, importer, activation, snapshotting) is synchronous
  OS-thread work. SQLite access stays synchronous `rusqlite`.
- Every config shape change requires explicit approval and a matching
  `config.example.toml` update.
- `README.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `SPEC-SERVER.md`,
  `SPEC-CLIENT.md` describe surfaces this programme replaces. Each phase
  that changes a documented surface includes the corresponding doc update
  as an approval item at phase time.

### 1.4 Module disposition (current code → end state)

| Current module | Disposition |
| --- | --- |
| `src/inference/` (dense, colbert, reranker, reranker_backend, device, artifacts, tensor_ops) | Retained. Model runtimes become projection/ranking producers. ColBERT subject to D4. |
| `src/docling.rs`, `src/docling_activity.rs` | Retained as the engine inside the PDF parser worker, behind the §12 parser boundary. Output mode subject to D6. |
| `src/units.rs` | Repurposed. Splitting logic moves out of the canonical path and becomes the chunk-projection builder (§23); canonical units become typed ContentUnits (§15). |
| `src/storage.rs` | Largely replaced. New hot-plane schema (P1–P5); dense in-memory cache survives conceptually as the dense-projection index. |
| `src/http.rs` | Transport shell retained; operation dispatch and API surface reworked (P5/P7, D2). |
| `src/state.rs` | Admission/model gates retained; extended for scheduler and cutover barriers. Admission knobs subject to D3. |
| `src/config.rs`, `config.example.toml` | Reworked per §35 (D3). |
| `src/error.rs`, `src/logging.rs`, `src/source.rs`, `src/types.rs` | Evolve in place. |
| `src/main.rs` | Retained (startup/daemonization/token handoff); gains scheduler lifecycle startup. |
| `src/bin/data-store.rs` (CLI) | Reworked against the new API surface (P7). |
| `sql/schema.sql` | Replaced by new hot-plane schema files, phase by phase. |

## 2. Phases

Each phase lists: goal, deliverables, spec sections, existing code touched,
and verification. A phase is not started until its open decisions
(§3) are resolved and its plan step is approved in-session.

### P0 — Foundations: canonical serialization, hashing, IDs, artifact store

Goal: the mechanical substrate every later phase depends on.

Deliverables:
- Canonical serialization module (new `src/canonical.rs`): UTF-8, NFC
  string normalization, lexicographically sorted JSON keys, canonical
  number rules, RFC3339 UTC `Z` timestamps, omitted-vs-null semantics,
  SHA-256 hashing (§16.1–16.2). Used by every content-derived hash
  (`sourceHash`, `bodyHash`, `planHash`, `manifestHash`, `reportHash`, …).
- Canonical ID scheme (new `src/ids.rs`): typed-prefix time-ordered IDs
  (`src_`, `parse_`, `acq_`, `qer_`, `snap_`) and parse-scoped
  deterministic ContentUnit/UnitRelationship IDs (§16.4).
- Content-addressed artifact store (new `src/artifact_store.rs`):
  write-once hash-keyed blobs, JSON/JSONL bundle writer/reader, manifest
  hashing over canonical lines joined by LF (§16.3, §30.1, §32). Layout
  per D1.
- Hot-plane schema versioning approach for the new database (successor of
  `--setup-storage`), so P1+ can add tables via explicit setup scripts.

Spec: §16, §30.1, §32. Depends on: D1.
Existing code: none replaced; additive modules.
Verification: cargo checks. Hash/serialization correctness has no
automated-test coverage under current policy; residual risk is recorded
and re-raised if the user wants controlled verification binaries.

### P1 — Acquisition layer and content-based identity

Goal: autonomous acquisition into SourceObjects with provenance.

Deliverables:
- New hot-plane tables: `source_objects`, `source_locations`,
  `acquisition_records`, `sync_queue` (setup script).
- Staged acquisition-bundle contract: raw bytes + manifest (byte hash,
  native identifiers, native version/etag/mtime, timestamps, connector
  identity + config hash, governance domain) in a staging area; core
  validates, computes `sourceHash`, dedups content, maintains locations
  losslessly, writes AcquisitionRecords (success and failure), emits
  events (§9.1–9.2, §10).
- ConnectorCapabilityProfile records (§9.3).
- Filesystem connector (`full_scan`, complete enumeration ⇒ qualifies for
  §11.1 deletion evidence) over the configured corpus root.
- Durable sync queue with latest-state coalescing (at most one pending
  change per source) and visible pending/in-flight/failed/lag state
  (§9.4).
- Adaptive knob-free scheduler thread: cadence from observed churn,
  pipeline backpressure, and source pushback; every adaptation logged with
  cause; cadence/backlog/drain/coalescing surfaced in health (§9.5–9.6).
  Boundary-timestamp capture (observed/acquired/parsed/activated) begins
  here and feeds P5 freshness records.
- DeletionEvidence recording (`absent_from_complete_enumeration`,
  `source_reported_gone`); propagation itself lands in P6 (§11.1).
- P1b (deferred sub-phase): one API-based incremental connector, after D5.

Spec: §9, §10, §11.1. Depends on: D1, D3 (config keys), D5 (P1b only).
Existing code: `src/source.rs` absorbed into the filesystem connector;
`src/state.rs` gains scheduler lifecycle; `src/main.rs` starts/stops it.
Verification: cargo checks; live acquisition run requires approval.

### P2 — Parser boundary and typed canonical content model

Goal: typed ContentUnit graphs replace markdown-as-truth.

Deliverables:
- Canonical model types (new `src/model/`): ContentUnit with typed bodies
  for the MVP content types (page, text_section, text_block, table,
  table_cell, figure, caption), Locators (page_bbox, char_range at
  minimum), UnitRelationships for the MVP relationship set (contains,
  logically_contains, physically_contains, precedes, appears_on,
  caption_of, has_caption, references, continues_on), Provenance
  (including the §20 memoization fields as inert schema) (§15, §17–§20,
  §36).
- ParseRun records and states (building/ready/active/archiving/archived/
  failed, `heldReason`) (§12).
- Parser worker contract: staged parser output bundles (manifest, typed
  candidate JSONL, warnings, metrics, bounded logs) — untrusted until
  imported (§12.1–12.2).
- Core importer: validation (typed-body mapping §15.2, locator/
  relationship/provenance rules, resource limits, capability-profile
  conformance), canonical ID assignment, canonical parse artifact bundle
  written to the artifact store (§12.3).
- ParserCapabilityProfile records and ConformanceReport measurement
  (unit/relationship type counts, locatorCoverage, captionPairingRate,
  tableDecompositionRate, extensible `dimensions`) (§12.4–12.5).
- PDF parser worker wrapping the existing Docling machinery, emitting
  typed candidate units (output mode per D6); plain-text parser worker.
- New hot-plane tables for parse runs, content units, relationships
  (setup script).

Spec: §12, §15, §17–§20. Depends on: D6.
Existing code: `src/docling.rs`/`docling_activity.rs` wrapped;
`src/units.rs` splitting removed from this path (reused in P4).
Verification: cargo checks; live parse runs require approval.

### P3 — Activation lifecycle

Goal: unattended, threshold-free activation with honest failure states.

Deliverables:
- Binary structural invariants as the only hard gate at import (§13.1).
- Changed-content auto-activation (§13.2); unchanged-content conformance
  dominance rule with held parses (`heldReason = conformance_regression`,
  at most one held candidate per source, newer supersedes) (§13.3–13.4).
- Parse-failure disposition: keep serving last valid version, durable
  failure records, no blind retry of deterministic failures, coalesced
  pending state (§13.5).
- Per-source cutover barrier: atomic `activeParseId` pointer swap;
  queries during the barrier rejected retryably; in-flight queries run on
  their captured snapshot (§13.6, §31.1). Generalizes the existing
  ingest-publish cache-swap discipline in `src/storage.rs`.
- Active-parse-only invariant enforced at every query path (§14).
- Held/stale counts in health; activation/hold/failure events logged.

Spec: §13, §14, §31.1. Depends on: P2.
Existing code: replaces `active_document_versions` publish logic.
Verification: cargo checks; live activation cycles require approval.

### P4 — Retrieval fabric rework

Goal: projections, deterministic planning, and scoped candidate
generation over active parses.

Deliverables:
- RetrievalProjection envelope + typed payloads; builders for the MVP set:
  `lexical_document` (FTS5), `chunk` (reusing `src/units.rs` splitting as
  the chunker, with chunker identity/config hash in ChunkPayload),
  `dense_vector` (existing dense runtime), `derived_view`; `summary` per
  D7 (§22, §23, §36).
- Projection freshness states and rebuild-from-canonical-state paths;
  projection payload archival into the artifact store for snapshots
  (§8.3, §22).
- In-memory dense cache re-keyed to active parses/projections (successor
  of `DenseVectorCache`).
- RetrievalProfile (versioned, hashed, visible) and deterministic
  QueryPlanner producing QueryPlan with normative `planHash` coverage
  (§24.2).
- Scope model: `governanceDomain` on locations (P1), ResolvedScope,
  scope-predicate enforcement at candidate generation in every channel,
  never post-filtering; opaque `callerContext` passthrough (§6, §24.3).
- RetrievalHit model; RRF as named fusion strategy; final reranker
  integration (local/HTTP backends retained); ColBERT stage per D4;
  RetrievalTrace/RankingTrace capture for P5 (§24.4, §28.2–28.3).

Spec: §6, §22–§24. Depends on: P3, D4, D7.
Existing code: `src/storage.rs` search-candidate machinery rebuilt;
`src/inference/` reused as-is.
Verification: cargo checks (incl. `--features metal`); live search
requires approval.

### P5 — Assembly, EvidencePack, QueryExecutionRecords

Goal: every query yields deterministic evidence and an immutable record.

Deliverables:
- AssemblyPolicy as versioned, hashed, externally visible policy
  documents; generic operators (include_anchor, include_parent_container,
  include_heading_path, include_caption_pair, include_explicit_references,
  include_continuation_chain, include_text_neighbors); budgets;
  relationship-type dependency checking against active-parse conformance
  reports with loud unmet-dependency warnings (§25).
- Deterministic EvidencePack construction: canonical ContentUnits only,
  chunks resolved to units before assembly, ContextAssemblyTrace
  explaining every non-anchor inclusion (§23.3, §26–§27).
- QueryExecutionRecord: embedded full EvidencePack, resolved plan/scope,
  retrieval/ranking/assembly traces, freshness record (per-source-system
  lag, pending changes, access-lost), `retrievalReplayMode:
  "record_replay"`, written before or atomically with the response;
  compressed QER archive in the artifact store plus hot metadata row
  (§28, §29.2 Guarantee 1).
- Query endpoint returns EvidencePack + `queryExecutionRecordId` (§34.1;
  surface finalized under D2).

Spec: §25–§28, §29.2. Depends on: P4.
Verification: cargo checks; live query + QER inspection requires approval.

### P6 — Snapshots, verification, deletion propagation, rollback-as-restore

Goal: the forensic and lifecycle guarantees.

Deliverables:
- ForensicSnapshot content-addressed manifests over the artifact store;
  required contents per §30.2; snapshot triggers wired into activation,
  deactivation, deletion, policy/profile change, deployment, schedule,
  manual (§30.3–30.4, §30.6).
- Verification tiers: mechanical (every snapshot), deletion gate
  (mechanical + deterministic index-rebuild check), scheduled restore
  drills (isolated restore + Guarantee 2 evidence replay over sampled
  QERs). Verification failure halts only the affected source's lifecycle,
  retains superseded state, surfaces in health (§30.5, §29.2 Guarantee 2).
- Superseded-state lifecycle: archive-verify-delete after cutover, no
  grace window, failure-gated (§31.2).
- Deletion propagation (location-scoped, snapshot-before-deactivation),
  access-lost handling, reappearance-as-restore (§11.2–11.4).
- Rollback = restore-from-store through normal activation (§31.3),
  replacing the current `rollback` operation's semantics.
- External model call records (HTTP reranker request/response capture per
  retention policy) satisfying Guarantee 4 (§29.2, §29.5).

Spec: §11, §29–§31. Depends on: P5.
Verification: cargo checks; drills/restores require approval.

### P7 — Events, API surface, freshness/health, CLI

Goal: the operational shell around the autonomous pipeline.

Deliverables:
- SystemEvent log (full §33 vocabulary) as durable, audit-facing state.
- Operation records + polling for asynchronous administrative actions
  (§34.6).
- Final API surface per D2: query, administrative ingest/parse overrides,
  held-parse disposition (list/accept/discard), inspection (units,
  relationships, sources with locations+freshness, QERs, sync status),
  snapshots/restore (§34). Protected operations continue using the
  startup-scoped admin token model.
- Health: readiness plus held counts, serving-stale counts, per-source
  cadence/backlog/lag, access-lost sources (§9.5–9.6, §13.4–13.5, §11.2).
- CLI client (`src/bin/data-store.rs`) reworked against the new surface.
- Documentation re-baseline: `README.md`, `ARCHITECTURE.md`,
  `PROTOCOL.md`, `SPEC-SERVER.md`, `SPEC-CLIENT.md` rewritten to describe
  the new system.

Spec: §33–§34, plus health/freshness threads from §9, §11, §13.
Depends on: D2; earlier phases.
Verification: cargo checks; end-to-end operation requires approval.

### Acceptance traceability (§37 → phases)

| §37 criteria | Phase |
| --- | --- |
| 1–5 (connectors, acquisition records, content identity, adaptive cadence, measured freshness) | P1 (freshness per-query: P5) |
| 6–9 (single active parse, net-new graphs, gating, failure disposition) | P2–P3 |
| 10 (deletion lifecycle) | P1 (evidence) + P6 (propagation) |
| 11 (typed units, relationships; annotations deferred) | P2 (+post-MVP) |
| 12–13 (projections, evidence-only packs, visible assembly policy) | P4–P5 |
| 14–16 (QER, replay guarantees, graded claims) | P5–P6 |
| 17–18 (snapshots, rollback-as-restore) | P6 |
| 19 (scope enforcement, callerContext) | P4–P5 |
| 20 (config = external facts only) | D3, enforced from P1 onward |

## 3. Open Design Decisions

Each is resolved with the user before the phase that depends on it.

- **D1 — Physical storage mapping** (before P0). Recommendation to be
  proposed: new SQLite database as the hot plane (new schema files under
  `sql/`), filesystem content-addressed artifact store under
  `[storage].index_root`, event log as a hot-plane table. §32 makes
  technology non-normative; the data contract is what matters.
- **D2 — API surface transition** (before P5's query endpoint; finalized
  in P7). Spec §34 defines plain request/response + operation polling;
  the current service streams NDJSON over `POST /v1/operations`.
  Reconcile: adopt §34 paths, and decide whether streamed progress
  survives as a transport detail on long-running administrative
  operations.
- **D3 — §35 vs current configuration** (before P1). Current config
  carries internal-guess knobs the spec prohibits (e.g.
  `[server].max_in_flight_ingest`/`max_in_flight_search`, retrieval
  candidate-pool sizes). Decide key-by-key: remove and adapt from
  observed signals, or justify as external fact / versioned policy
  document (e.g. assembly budgets reflect model context limits and belong
  in AssemblyPolicy, not config). Any config change requires its own
  explicit approval.
- **D4 — ColBERT disposition** (before P4). ColBERT persisted token
  vectors are a `multi_vector` projection, which §36 lists as
  forward-compatible, not MVP. Keep the existing ColBERT MaxSim stage
  (implementing `multi_vector` early) or drop it from the MVP pipeline
  and reintroduce post-MVP.
- **D5 — Second connector target** (before P1b). Which external
  API-based source system gets the incremental-detection connector.
- **D6 — Docling typed-output mode** (before P2). Typed bodies (tables,
  figures, captions, pages, bboxes) cannot be recovered from
  markdown-only output (§12.2 declares Markdown-only insufficient).
  Investigate Docling's structured JSON/DocTags export as the parser
  worker's source of typed candidate units.
- **D7 — Summary projection producer** (before P4). §36 recommends
  `summary` projections in the MVP, but a producer (model, provenance,
  cost) must be chosen — or summary projections deferred post-MVP.

## 4. Post-MVP Horizon

Named tiers, their reserved seams (already built by the phases above), and
entry points. No step-level detail; each gets its own planning pass.

- **SemanticAnnotations** (§21): parse-scoped annotation records with
  provenance and `freshnessStatus`; required-annotation-set policy
  (§21.4) as a versioned policy document. Seam: Provenance and bundle
  formats from P2 already carry the fields; hot-plane table and builders
  are additive.
- **Annotation memoization** (§20, §21.1–21.3): memoization key/honesty
  fields are inert schema from P2; implementation adds a content-hash
  cache and eligibility declarations.
- **Additional retrieval channels** (§22, §24): `learned_sparse_vector`,
  `multi_vector` (or ColBERT re-entry if D4 drops it),
  `graph_projection`, `temporal_projection`. Seam: RetrievalProjection
  envelope, channel-scoped planner, and per-channel traces from P4.
- **Verified recompute — Guarantee 3** (§29.4): probe query sets,
  measured per-channel tolerances, numeric-environment capture. Seam:
  `ReplayProfile.channelReplayModes`/`declaredTolerances` in snapshot
  manifests from P6.
- **Entitlement layer** (§6): resolves callers to allowed source sets and
  intersects into ResolvedScope. Seam: callerContext passthrough,
  governanceDomain tagging, scope-at-candidate-generation from P1/P4/P5.
- **Compliance-driven erasure** (§11.5): designed, audited purge
  operation over immutable stores. Deliberately not improvised.
- **Reference-style QERs** (§28): mechanical derivation from embedded
  records if volume demands; never the reverse.
- **Per-domain index partitioning** (§6 caveat): remedy for corpus-global
  lexical statistics if scoped-score shadowing becomes material.
