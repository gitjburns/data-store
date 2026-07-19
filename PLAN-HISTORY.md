# PLAN-CANONICAL-FABRIC — Completed Session History

Verbatim relocation (2026-07-18, user-approved) of the completed Current
Status entries from `PLAN-CANONICAL-FABRIC.md`: the 2026-07-05 first entry
through the 2026-07-18 CPc-attempt-2 / NaN-sanitize-fix / CPb-verification
session. Nothing was summarized or edited in the move; date-cited
cross-references elsewhere in the plan ("see the YYYY-MM-DD entry") resolve
here. The plan file retains the latest session entry, the Handoff, and its
§1–§5 sections. This file is an audit record, not part of the default
onboarding read list — read it only when a dated entry must be consulted.

Second-stage relocation (2026-07-18, user-approved): the completed planning
corpus moved verbatim into the "Retired planning sections" part below —
§1.5 pinned contracts, §1.6 recon findings, §2 module disposition, the §3
dependency spine, completed cluster/package texts C1–C10e with their C7/C9/
C10/CP fact bases, CPa/CPb/CPd, the §37 acceptance-traceability table, and
the §4 full decision texts. The plan file now retains condensed rulings
indexes in §4 pointing here by grep anchor; nothing was summarized or
edited in the move.

## Completed Current Status entries

- 2026-07-05: Plan approved in structure and written (serial P0–P7 phase
  form). No phases started.
- 2026-07-06: Restructured into a workflow-execution work-package graph,
  grounded in an eight-cluster read-only code recon. Supersedes the P0–P7
  phase structure; decision identifiers D1–D7 are unchanged. No packages
  started.
- 2026-07-07: C1 complete (C1a, C1b, C1c), implemented as a serial agent
  workflow and verified by an adversarially-confirmed review workflow.
  - C1a: six `execute_*` pipelines moved from `src/http.rs` (now ~1300-line
    transport shell) into `src/operations/{ingest,search,shutdown,sources,
    versions,rollback}.rs`; `leaf_benchmark_stage` shared in
    `operations/mod.rs` (used by ingest and search, contra recon); emitter
    machinery untouched, `pub(crate)` widened only for the emitter surface.
  - C1b: pure primitives extracted to `src/primitives/{bm25,codec,fusion,
    hash,latency,time,validate}.rs`; shared data types stayed in
    `storage.rs` as `pub(crate)`; `RetrievalStageLatencies`
    (`primitives/latency.rs`) replaced the string-keyed
    `retrieval_substage_ms` contract; raw search JSON payload unchanged.
  - C1c: `panic_payload_message`, `truncate_diagnostic_text`,
    `MAX_DIAGNOSTIC_CHARS` unified in `src/util.rs`; docling/device
    duplicates deleted. Approved behavior delta: device.rs panic fallback
    string is now "unknown panic payload" and gains the 16k truncation
    bound. `src/bin/colbert-diagnostic.rs` gained `#[path = "../util.rs"]
    mod util;` (required for `--features metal` compile of that bin).
  - Decision (2026-07-07): no eager `ApiError` batch-add (would force
    dead-code allows); variants land per-cluster on first construction
    (C1c package text amended above).
  - Verification: 33 baseline facts confirmed intact; 2 minor comment
    findings confirmed and fixed in `http.rs` (boundary comment named the
    private `OperationStreamSender` instead of the emitter surface;
    `OPERATION_STREAM_CHANNEL_CAPACITY` and `OperationEmitter` doc
    comments added). Pre-existing, untouched: search pipeline logs
    `reranker_candidate_limit` inconsistently (started/failed use the
    effective pool size, completed and raw `candidateLimit` use `top_k`);
    candidate cleanup when C7c replaces that code.
  - All checks green: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`, zero warnings.
  - Next: resolve D1 (physical storage mapping), then C2.
- 2026-07-07 (later): D1 and D3 resolved (see §4 for the full resolutions).
  C2 complete (C2a–C2g including C2f), implemented as a four-wave workflow
  (C2a∥C2b → C2c∥C2d → C2e → C2g) plus in-session C2f, verified by a
  five-dimension adversarially-confirmed review workflow.
  - New substrate modules: `src/canonical.rs` (§16 canonical JSON/JSONL
    serialization + hashing; rejects post-NFC key collisions; JSONL rule:
    canonical lines LF-joined, no trailing LF, written bytes = hashed
    bytes), `src/ids.rs` (§16.4; prefixes `src_ parse_ acq_ qer_ snap_
    evt_` via one `new_prefixed_id`, 14-digit epoch-ms + 10-byte hex;
    deterministic `<parseId>:unit|rel:NNNNNN`, 6-digit width is persisted
    contract), `src/artifact_store.rs` (write-once content-addressed store
    at `{index_root}/fabric/artifacts/sha256/<2-hex>/<hash>`, temp+atomic
    rename, re-hash on read), `src/model/` (all 19 spec types transcribed
    field-for-field, verified against §9–§33; `content_type_body_matches`
    is the §13.1 hard-gate hook for C4b), `sql/fabric/schema.sql` +
    `src/hot_plane.rs` (ten tables, `user_version=1`, WAL+synchronous=FULL,
    `busy_timeout`/deadline constants, read-only open flags, validator
    battery; fresh setup builds at temp path then atomic-renames so a
    crashed setup is recoverable), `src/events.rs` (SystemEvent appender on
    the caller's connection so events commit atomically with the state
    they record).
  - `--setup-storage` now sets up BOTH planes: legacy schema first, then
    the fabric hot plane (`src/main.rs`). Legacy path retires at C10e.
  - C2f config rework landed per D3: `deny_unknown_fields` on all 13
    config structs; required `[client]` section (server-validated,
    client-owned); the two `serde(default)` pool-size violations removed;
    `CARGO_MANIFEST_DIR`/`service_root()` fully replaced by config-file-
    parent resolution (`ServiceConfig::config_root()`, threaded through
    logging/main/state/reranker; CLI uses `config_parent_dir` for token
    and history paths). `config.example.toml` comments updated.
  - Verification: 75 facts intact (incl. full field-by-field model spec
    fidelity); 8 findings confirmed and fixed (atomic fresh setup, db_path
    in connection-policy errors, `ArtifactRef` deny_unknown_fields,
    artifact-store start-boundary logs, schema.sql CHECK-convention
    comment scoped with `sync_queue.state` named as §9.4-prose-defined
    pending the C3 queue type, `ids.rs` doc lists `evt_`); 4 refuted as
    documented deviations.
  - New dependency: `unicode-normalization` (approved 2026-07-07).
  - Dead-code convention in force: substrate modules carry module-level
    `#![allow(dead_code)]` with a consuming-cluster comment; `hot_plane`
    uses item-scoped allows on `open_read`/`open_write`/
    `statement_deadline`/`STATEMENT_DEADLINE_MS` only. Remove each allow
    when its consumer lands (C3 onward).
  - All checks green: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`, zero warnings.
- 2026-07-10: Handoff action item closed — local `config.toml` gained the
  three C2f-required keys (`[client].operation_timeout_seconds = 3600`,
  `[retrieval].colbert_candidate_pool_size = 100`,
  `[retrieval].reranker_candidate_pool_size = 10`), matching
  `config.example.toml` placement and comments (approved 2026-07-10).
  D6 resolved (see §4 for the full resolution). Next: present the C3∥C4
  cluster plans per the §1.4 cluster cycle.
- 2026-07-10 (later): C3 complete (C3a, C3b, C3c), implemented as a
  two-phase workflow (C3a∥C3b → C3c) plus main-loop integration, verified
  by a five-dimension adversarially-confirmed review workflow (23 findings
  confirmed and fixed, 6 refuted).
  - C3a `src/connectors/filesystem.rs`: full-scan connector; never follows
    symlinks; (mtime, size) prescreen against importer-supplied known
    state; atomic temp→rename staging. Shared staged-bundle contract in
    `src/connectors/mod.rs`: manifest + `ScanError` (SourceSide vs
    Internal) + bundle dir = `{index_root}/fabric/staging/acquisition/
    <sha256(native_uri)>/` (replace-on-coalesce by construction).
  - C3b `src/acquisition.rs`: importer owns all canonical acquisition
    writes; malformed bundles are recorded failed AcquisitionRecords
    (never Err); raw bytes → artifact store before the SQL transaction;
    dedup by hash; location new/refresh/rebind (rebind resets
    first_seen_at per §10 rules 3/7); enumeration deletions are
    scope-filtered to the enumerated root; scope-level failed
    AcquisitionRecord for whole-scan source-side failures; `record_
    enumeration` anchors DeletionEvidence.
  - C3c `src/scheduler.rs` + `src/model/sync.rs`: coalescing durable queue
    (claim reclaims stale in_flight rows; failed rows terminal until a new
    detection); knob-free adaptive cadence (EMA inter-change estimate,
    growth constants in code, NO ceiling per §9.5/§38, every adaptation
    logged with cause; backpressure edges are durable events);
    panic-caught thread publishing not-ready health. `src/state.rs`:
    `ShutdownSignal::wait_timeout`, `SyncHealth`, readiness-critical
    `sync` health component. `src/main.rs`: fabric pre-check for truthful
    startup handoff (`sync=` token added to the ready line), scheduler
    spawn/join with explicit shutdown request before join.
  - Shared-helper consolidation (verification-driven): IMMEDIATE
    transaction lifecycle helpers in `hot_plane.rs` (namespace-
    parameterized, commit-attempt logged); `parse_utc_timestamp_ms` +
    `utc_now` in `primitives/time.rs`; `truncate_persisted_detail` (500)
    in `util.rs`; `loc_`/`syncq_` mints in `ids.rs`.
  - Deviation from the cluster plan: the `src/source.rs` path-safety lift
    was unnecessary and not performed — the connector enumerates the
    corpus root directly and never follows symlinks; `source.rs` and the
    legacy ingest path are untouched.
  - Open decision: wall-clock statement-deadline enforcement needs
    rusqlite's `hooks` feature (Cargo.toml change, user approval);
    `busy_timeout` 5s is the only statement bound meanwhile. Seam
    commented at `acquisition.rs` open_bounded_* and `hot_plane.rs`.
  - C3 pipeline boundary: detect → acquire → import; parse dispatch of
    imported sources is C5 integration.
  - NOT runtime-verified: no scan/import cycle has run; the fabric DB does
    not exist until `--setup-storage` is run deliberately (it now gates
    top-level readiness). All checks green: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`, zero warnings.
  - Next: C4 (parsing) per the approved cluster plan.
- 2026-07-10 (session end): C4 IMPLEMENTED but NOT COMPLETE — the
  five-dimension adversarially-confirmed verification finished (21
  confirmed findings, 5 refuted) but the fixes are NOT yet applied; two
  of them are canonical-data-contract decisions awaiting the user
  (resolved next session — see the 2026-07-10 next-session entry below).
  All cargo checks green at this tree state (`cargo
  fmt`, `cargo check`, `cargo check --features metal`, `cargo clippy`,
  zero warnings). Implementation was a two-phase workflow (C4a contract →
  C4b∥C4c∥C4d) plus main-loop integration.
  - New modules: `src/parse/bundle.rs` (§12.2 staged output-bundle
    contract: schema-versioned manifest with per-file digests over every
    file except itself, `BundleWriter` temp→atomic-rename promotion,
    `read_bundle` two-way digest verification, `BundleReadError`
    ContractViolation-vs-Internal split; staging root
    `{index_root}/fabric/staging/parse/bundle-<ms>-<seq>`);
    `src/parse/importer.rs` (§13.1 hard gates each with a distinct
    error; resource limits incl. `MAX_CANDIDATE_UNITS = 999_999` coupled
    to the six-digit ID width; canonical ID assignment + local-ref
    remap; §12.3 canonical bundle to the artifact store BEFORE the ready
    transaction; `parse_runs` building→ready/failed with events;
    recorded-failure vs canonical-`Err` split — run row deliberately
    stays `building` on a canonical fault; unattributable bundles
    (unreadable manifest / unknown source_id) are `BadRequest`, no row
    possible); `src/parse/conformance.rs` (§12.5 pure measurement; extra
    `relationship_coverage` dimension; optional rates absent when
    unmeasurable — C5a dominance must handle differing dimension sets);
    `src/parse/pdf_worker.rs` (DoclingDocument JSON → typed candidates
    per D6; heading-stack sectioning; tables+cells; caption pairing via
    edges; page_bbox + char_range locators; parser identity
    "docling_pdf"/"1", config hash over the effective Docling options);
    `src/parse/text_worker.rs` ("plain_text"/"1", blank-line paragraph
    split, char-offset `char_range` locators, `precedes` chain).
  - `src/docling.rs`: ADDITIVE `convert_source_to_document_json`
    (`--to json`, artifact discovery generalized by extension, raw
    un-normalized JSON, `output_dir_override` for worker workspaces);
    legacy markdown path preserved (verification refuted the one claimed
    observable delta). Authorized behavioral fix on BOTH paths: progress
    delivery is now `try_send` (drop-on-full, stop-on-disconnect) — the
    old blocking send could fail a whole conversion or deadlock the
    stderr pipe when the consumer died.
  - Main-loop consolidations landed with C4:
    `canonical::canonical_sha256_hex_without_field` (shared self-hash
    pattern), `UnitRelationshipType::wire_name` (moved onto the model),
    ids.rs C4 allows removed, model/mod.rs re-export split updated.
  - All `src/parse/*` modules carry `#![allow(dead_code)]` "Consumed
    from C5 onward"; C5 owns dispatch (mime routing to workers), staging
    lifecycle/cleanup of consumed and `.tmp` bundles, and surfacing
    stuck-`building` runs in health.
- 2026-07-10 (next session): C4 COMPLETE. The handoff resume point is
  closed (and removed from the Handoff section below): both open
  decisions ruled and the full 21-finding verification fix pass applied
  in-session.
  - Decision 1 ruled (Option B): workers never populate body-embedded unit
    references. `emit_caption` sets `captionForUnitIds` to `None`
    (spec-legal absent optional, same policy as `headerRefs`); the
    caption_of/has_caption edges are the sole authoritative pairing.
    Policy documented at `assign_canonical_rows` (importer.rs): typed
    bodies are copied verbatim and body-embedded refs are NEVER remapped,
    keeping `bodyHash` purely content-derived (§16.1) and §21.2
    memoization keys viable.
  - Decision 2 ruled (Option 1): `page_bbox` locators pack
    `[l, b, r, t]` (PDF llx,lly,urx,ury), satisfying the documented
    `[x0, y0, x1, y1]` contract under Docling's BOTTOMLEFT origin.
  - Behavioral fixes landed: `emit_section` gates the global
    section-stack pop AND push on `inherited_parent.is_none()`
    (container-scoped headings no longer corrupt the heading trail —
    the D6 #/texts/38 wrong-parent bug); `seal_report` and the canonical
    bundle manifest hash both route through
    `canonical::canonical_sha256_hex_without_field` (unchecked-remove
    flaws gone); `ParserOutputManifest`/`FileDigest` derive
    `PartialEq/Eq` and the importer records a failed parse when the
    verified manifest differs from the claims read;
    `import_parser_bundle` split into wrapper + `run_attributed_import`
    so EVERY canonical-side Err after the building insert emits one
    terminal `parse.import_faulted` (run id, bundle dir, elapsed, error);
    unreferenced texts/tables/pictures/groups self_refs warn as
    `docling_unreferenced_item` after traversal instead of dropping
    silently.
  - Hygiene fixes landed: `PROFILE_HASH_JSON_KEY` defined once
    `pub(crate)` in `canonical.rs` (three copies deleted); docling.rs
    uses `primitives::current_time_ms`; `caption_targets` handed to
    `build_relationships` via `mem::take`; recorded PDF parse failure
    logs at `warn!` with the cluster rationale comment; Docling
    `docling.process.*` lifecycle logs carry `output_format`, threaded
    through new `Copy` context structs `DoclingLaunch` (also keeps
    `run_docling` under the clippy argument limit) and
    `DoclingProcessContext`; `expected_markdown_exists`/
    `expected_markdown_path` renamed to `expected_artifact_*`
    (docling_activity.rs + the feedback log); `TableHeaderSpan` moved to
    the wired re-export list; furniture comments corrected (D6 furniture
    root is EMPTY, traversal is defensive); `MAX_CANDIDATE_UNITS`
    comment distinguishes count (999,999) from max index (999,998), cap
    1,000,000 without widening the six-digit contract;
    source_object.json omission rationale extended (§10 rule 1
    immutability, C9 first-class refs) and the C6/post-MVP wording
    split; doc comments added to `ResolvedDoclingOptions`,
    `DoclingConversionResult`, `DoclingProgressUpdate`.
  - The five refuted findings were left untouched per the handoff.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`. Still nothing
    runtime-verified (no `--setup-storage`, no scan/parse cycle).
  - Next: C5 cluster (activation lifecycle) per the handoff and the
    §1.4 cluster cycle; no open decisions block it.
- 2026-07-10 (offline-development restructure): remaining scope
  reoptimized for end-state-only development (user-ruled and approved
  2026-07-10): the app does not need to be functional at any point until
  the programme completes, so between-phase operability is dropped as a
  planning constraint. Plan changes: new cluster CR (early legacy
  retirement — legacy operation pipelines, `storage.rs`, `sql/schema.sql`,
  dual-plane setup, legacy-only config keys) precedes C5, dissolving the
  legacy rows of the §1.5 pinned-contract table and the don't-break-legacy
  constraint on every remaining cluster; per-cluster health/operability
  wiring defers to C10b; documentation re-baseline consolidates at C10d;
  runtime verification consolidates into new package C10f (commissioning);
  C5 gains explicit package C5c (parse dispatch + staging lifecycle);
  `units.rs` deletion moves to C6b post-harvest. C5 ruling recorded:
  activation gates on canonical state only, §13.6 projection prerequisite
  as a commented seam extended at C6. Cargo battery and
  adversarially-confirmed verification workflows unchanged. No code
  changed by the restructure itself. Next: present the CR cluster plan.
- 2026-07-11 (MVP rescope): QER audit tier deferred post-MVP; semantic
  annotations, multi-vector, and graph retrieval pulled forward into MVP
  scope (user-ruled and approved 2026-07-11, after workload confirmation
  that production queries are both single-fact passage lookup and
  relational/entity-centric). Deferring the audit tier is a recorded
  deviation from the spec's committed guarantees: §29.2 Guarantees 1/2/4
  and acceptance criteria 14–16 defer with it. Plan changes: C8c (QER
  writer), per-query QueryPlan/planHash persistence, C9b scheduled
  restore drills, and C9e external-call records move to §5 as the named
  "QER audit tier"; the learned-sparse channel defers with it; C7a's
  per-query planner is replaced by a static versioned hashed
  RetrievalProfile; new cluster CA (semantic annotations + memoization,
  §20–§21) lands between C5 and C6; C6 gains multi-vector (D4 resolved:
  keep ColBERT) and graph projection builders; C7b gains `multi_vector`
  and `graph` channels; the §21.4 required-annotation-set policy ships
  with MVP content "nothing blocks activation" (annotations build
  post-activation with visible freshness). D7 dissolved into new
  decision D8 (annotation producers and MVP annotation-type set, before
  CA); new decision D9 (graph channel query-time semantics, before the
  graph packages). Known accepted risk, raised and accepted at approval:
  annotation producers may make external model calls before the deferred
  audit tier exists to record them under Guarantee 4. No code changed by
  the rescope itself. Next: present the CR cluster plan (CR and C5 are
  unaffected by the rescope).
- 2026-07-11 (later): CR complete (CRa, CRb, CRc), implemented as
  main-loop file deletions plus a two-stage agent workflow (CRa → CRb)
  with in-session main-loop wiring/config edits, verified by a
  five-dimension adversarially-confirmed review workflow (13 findings
  confirmed collapsing to 6 unique fixes, all applied; 8 refuted as
  pre-existing or plan-deferred).
  - CRa: `src/operations/` (all 7 files) deleted; `src/http.rs` trimmed
    1310→768 lines to the retained transport shell (router = GET
    /v1/health + control skeleton; OperationEmitter/stream sender/auth/
    body limits frozen under "consumed at C10a" allows); `src/types.rs`
    321→110 lines (operation/limits DTOs and validation impls deleted;
    health types live; `OperationRequest`/`OperationEvent`/
    `OperationBenchmarks`/`BenchmarkStage` frozen substrate). `error.rs`:
    `SourceAlreadyIngested` removed; `SourceResolution`/`UnitSplitting`
    allowed (their constructors are retained substrate);
    `DoclingConversion` kept (constructed by `docling.rs`).
  - CRb: `src/storage.rs` (4,469 lines) and `sql/schema.sql` deleted;
    `--setup-storage` builds the fabric plane only. Shared types
    relocated into their consuming primitives modules: `DenseMatch`/
    `Bm25Match`/`FusedMatch` → `primitives/fusion.rs`, `Bm25Queries` →
    `primitives/bm25.rs`, `UnitColbertDocumentVector` →
    `primitives/codec.rs`, `StoredDenseVector`/
    `StoredColbertDocumentVector`/`storage_operation_error` →
    `primitives/validate.rs`; unused `primitives/mod.rs` re-exports
    trimmed (convention: fabric consumers import via direct submodule
    paths). `units.rs` parameterized (`min_search_unit_chars`,
    `max_unit_tokens` as arguments; `RetrievalConfig` import gone) and
    module-allowed as C6b harvest material. `state.rs`: storage slot/
    getter, both admission gates, and the `storage_cache` + `admission`
    health components removed; readiness = inference && sync;
    `AdmissionGate`/`AdmissionPermit`/`AdmissionSnapshot` retained under
    "consumed at C8d" allows; model-gate and admin-token/shutdown
    methods allowed naming C6c/C7 and C10a. `main.rs`: `mod operations`/
    `mod storage` removed, `StorageRuntime` init and all
    `storage_cache=` startup tokens removed, ready line now
    `ready=… inference=true sync=…`.
  - CRc: `[retrieval]` section (the entire `RetrievalConfig` struct) and
    `[server].max_in_flight_ingest`/`.max_in_flight_search` removed from
    `config.rs`, `config.example.toml`, and `config.toml` together; both
    retrieval cross-validations and the orphaned
    `acknowledge_non_negative` helper pruned.
  - Banked values (durable record): `default_top_k=10`, `max_top_k=100`,
    `rrf_k=60`, `candidate_overfetch_multiplier=3`,
    `colbert_candidate_pool_size=100`, `reranker_candidate_pool_size=10`
    → C7a RetrievalProfile; `min_search_unit_chars=400`,
    `max_unit_tokens=512` → C6b chunker config; `max_in_flight_search=1`
    → C8d code constant; `max_in_flight_ingest` superseded by the
    adaptive scheduler.
  - Retained-substrate allow sweep (zero-warnings bar): scoped allows
    with future-consumer comments in `source.rs` (C5c), the `docling.rs`
    markdown path (pending the C6d derived-view decision),
    `primitives/{fusion→C7b, bm25→C6a/C7b, codec/validate→C6c/C6e,
    latency→C7}`, and `inference/{colbert→C6e/C7c, dense→C6c/C7b,
    reranker_backend→C7c}` plus the `inference/mod.rs` re-export.
  - Verification fixes applied: stale hardcoded `storage_ready=true`
    removed from the `service.listening` log event (flagged
    independently by five dimensions); stale `config.rs` server/
    `setup_storage` doc comments; `latency.rs` present-tense contract
    wording; bare allow on `OperationEvent::Progress` given its C10a
    comment; the fabric pre-check comment now documents the brief window
    where the printed `ready=true` precedes the scheduler's first
    sync-slot publish. Notable refuted-as-expected: the CLI still
    targets the deleted `POST /v1/operations` (compiles untouched; dead
    at runtime until C10a/C10c per §1.2).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`. Nothing
    runtime-verified (unchanged; first runs at C10f).
  - Next: C5 (activation lifecycle) per the §1.4 cluster cycle; the C5a
    dominance-comparison question in the Handoff awaits ruling at C5
    planning.
- 2026-07-11 (session end): C5 complete (C5a, C5b, C5c), implemented as a
  three-stage serial agent workflow (C5b → C5a → C5c; serialized so each
  stage compiles against the previous stage's real API) plus main-loop
  integration, verified by a five-dimension adversarially-confirmed review
  workflow (10 confirmed findings collapsing to 9 unique — 6 fixed in the
  fix pass, 2 fixed under in-session rulings, 1 recorded residual; 8
  refuted).
  - Dominance ruling (user-approved 2026-07-11, closes the banked C5a
    question): union comparison over `ConformanceReport.dimensions`,
    absence-conservative — present-in-active/absent-in-candidate compares
    as worse (hold); present-in-candidate/absent-in-active does not block;
    absent from both is equal. Dominance is scoped to the `dimensions` map
    per the model contract; unit/relationship type counts never gate.
  - C5b `src/state.rs`: `CutoverRegistry` — lazy per-source barrier map
    generalizing the private `ExclusiveGate` (log identity parameterized;
    model-gate logging byte-identical; poison-recovery invariant preserved
    everywhere); blocking `acquire` → Drop-releasing guard with
    wait_ms/held_ms logs; peek-only `reject_if_active` +
    `CutoverBarrierActive` retryable rejection (allows → C7/C8). One
    registry per process, constructed in `main.rs` and passed to the
    scheduler; AppState wiring deferred to the C7/C8 query side.
  - C5a `src/activation.rs`: `gate_and_activate` — pre-barrier source_id
    lookup (barrier keying only), barrier held across the whole
    read-decide-swap, one IMMEDIATE transaction (namespace "activation");
    §13.2 auto-activation, §13.3 dominance per the ruling, hold path
    persisting per-dimension deltas in the parse.held payload; activate
    path: predecessor active→archiving (C9 completes to archived),
    candidate→active (held_reason cleared atomically), the
    `active_parse_id` pointer write, held-candidate supersession
    (parse.hold_superseded), parse.activated — all events atomic with
    their transitions, all updates status-guarded with exact-one-row
    checks. `accept_held_parse` (barrier + parse.accepted) and
    `discard_held_parse` (no barrier — no pointer swap; race-free under
    IMMEDIATE serialization, verification-confirmed) allowed → C10a.
    §13.6/§21.4 prerequisite seam is `verify_activation_prerequisites`
    (C6 extends; CAd reads it).
  - C5c `src/scheduler.rs` + `src/parse/*`: drain-loop parse chain —
    authoritative `source_objects.mime_type` routing (mime constants
    single-sourced in `acquisition.rs`), §13.5 rule-5 no-blind-retry
    guard keyed on (source, parser identity, config hash) with
    GateExisting crash recovery and dispatch-past-stale-`building`,
    worker → `import_parser_bundle` → gate → activate; recorded outcomes
    (no-parser mime, guard skip, recorded parse failure, held, activated)
    complete the queue entry, canonical faults park it failed; startup
    `.tmp` sweep (writer-shared naming constants in `bundle.rs`); all
    `src/parse/*` module allows removed. `scheduler::start` grew
    `DoclingConfig` + `Arc<CutoverRegistry>` parameters.
  - Post-verification ruled fixes (both user-approved 2026-07-11):
    (1) consumed acquisition-bundle deletion moved from
    `import_validated_bundle` into the scheduler — drain deletes only
    after `complete()` succeeds, the direct-import arm after its import —
    so a crash replay re-imports the still-present bundle idempotently
    instead of parking the entry failed with a spurious UNKNOWN_CLAIM
    "malformed" AcquisitionRecord; (2) pre-worker content-identity check —
    the resolved live file is hashed (`canonical::sha256_hex_bytes`, the
    same derivation as acquisition) against the run's `source_hash`
    before any worker runs; mismatch warn-logs
    (scheduler.parse_dispatch.content_changed) and skips, self-healing
    via re-detection. The structural alternative (parsing the acquired
    bytes themselves) was deliberately deferred by ruling as a
    worker-input design change.
  - Fix-pass items (verification-confirmed): shared
    `resolve_contained_source` extracted in `source.rs` (single
    canonicalized symlink-safe containment authority;
    `resolve_source_reference` is now a thin PDF wrapper; the text route
    previously relied on a lexical-only strip); `entry()` payload helper
    consolidated into `crate::events` (three private copies deleted);
    sweep start-boundary log + elapsed_ms; three stale comments corrected
    (importer header stuck-`building` surfacing → C10b, pdf_worker
    progress-consumer parenthetical → open D2, main.rs registry comment →
    singular accept disposition); `SyncCycleStats.failures` doc records
    the dual-count semantics (a recorded parse failure counts in both
    `imported` and `failures`).
  - Residual risks recorded: direct (queue-bypassing) imports of
    transiently-unreadable-manifest bundles never enter the parse chain
    (pre-existing C3 recovery path, warn-logged, vanishing same-thread
    trigger; ruled 2026-07-11: record, promote to a fix at C10f if
    observed); an instant remains between the content-identity check and
    the worker's own read; crash-orphaned ready runs whose queue entry
    already completed have no dispatch path until a new detection
    (drain-invoked dispatch only, per approved scope).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`. Nothing
    runtime-verified (unchanged; first runs at C10f).
  - Next: resolve D8 (annotation producers + MVP annotation-type set),
    then present the CA cluster plan per the §1.4 cluster cycle.
- 2026-07-13: CA complete (CAa, CAb, CAc, CAd), implemented as a
  three-stage agent workflow (CAa → CAb∥CAd-policy → CAc+worker) plus
  main-loop config/wiring, verified by the five-dimension
  adversarially-confirmed review workflow (7 findings confirmed
  collapsing to 6 unique, all fixed; 2 refuted). First cluster run under
  the §1.4 model-assignment rule (added this session, permanent): all
  subagents on Opus, Fable 5 main-loop only.
  - D8 resolved (see §4). Two pre-plan rulings (user-approved
    2026-07-13): annotation builds run on a dedicated discovery-based
    worker thread, NOT inline in the scheduler drain (LLM latency must
    never stall detect→acquire→parse→activate); producer granularity is
    entity+relation per section group, summary per document, with the
    binding identity: invocation input unit set = annotation
    targetUnitIds = memo key basis.
  - Config (approved): new `[models.annotator]` — endpoint (full
    chat-completions URL), model, timeout_seconds, optional
    api_key_file_path (resolved against config root, non-empty-validated
    when present), max_input_chars (external fact: model context budget)
    — in config.rs + config.example.toml + local config.toml; local
    points at the user's endpoint with model Qwen3.6-27B.
    `.annotator-api-key` created owner-only (placeholder key "none") and
    added to .gitignore (approved).
  - CAa `src/annotations/store.rs` + schema: `semantic_annotations` and
    `annotation_memo` tables added to `sql/fabric/schema.sql`
    (user_version stays 1 — nothing has ever run), hot_plane validator
    coverage; store mutations take the caller's Transaction and append
    `annotation.*` events atomically (annotation.requested/completed/
    failed/stale are a RECORDED ADDITIVE extension of the spec §33
    closed enum — the spec defines §21 annotations but no lifecycle
    events for them); status-guarded transitions with exact-one-row
    checks; `ann_` id mint added to ids.rs.
  - CAb `src/annotations/{llm_client,producer,entity,relation,summary}`:
    blocking reqwest chat-completions client mirroring the HTTP-reranker
    discipline (owner-only key file, bounded diagnostics, `annotator_http.*`
    logs; deliberately NO startup smoke and NOT readiness-critical);
    producer contract enforces input purity (prompt = ordered target-unit
    text only, the memoization-eligibility basis); §20 provenance with
    prompt/config hashes and ContentUnit input_refs; producer identity
    hash covers model+endpoint+prompt+max_input_chars so any change
    invalidates memo reuse; temperature 0 code constant; strict JSON
    parsing with typed bodies (entity {name,entityType}, relation
    {subject,predicate,object}, summary {text}); new
    `ApiError::AnnotationProducer` variant (batched error.rs edit).
  - CAc `src/annotations/memo.rs`: §21.2 key = canonical SHA-256 over
    ordered per-target content hashes (textHash-or-bodyHash) + type +
    producer identity hash; cache row stores one invocation's full output
    as a per-item array (body, confidence, originalAnnotationId each), so
    re-mints carry faithful per-item confidence and exact per-item
    memoizedFrom; cache deliberately survives parse archival (cross-parse
    reuse is its purpose); reuse recorded via Provenance
    memoized/memoizedFrom/memoizationKeyHash (§21.3).
  - CAd `src/annotations/{policy,worker}.rs` + activation seam + main.rs:
    versioned self-hashed `required-annotation-set` policy v1 ("nothing
    blocks activation"; entity/relation/summary post-activation), read
    generically by `verify_activation_prerequisites` (behavior unchanged
    under the empty blocking set; future non-empty sets take effect
    without a seam change); worker thread mirrors the scheduler lifecycle
    (panic-caught, ShutdownSignal, 30s code-constant idle cycle),
    stateless discovery per cycle (expected memo keys vs present rows),
    memo-first build, two-phase miss path (visible building row commit →
    producer call outside any tx → complete/fail + memo record in one
    tx); client loads inside the thread and a load failure parks the
    worker (annotations are non-critical); spawned/joined in main.rs
    beside the scheduler; takes NO cutover barrier (parse-scoped writes
    only).
  - Verification fixes (all applied): (1) HIGH — invocation-plan SQL
    referenced nonexistent `content_units.deleted_at`; every discovery
    cycle would have failed at runtime; (2) HIGH+MED — crash-orphaned
    `building` rows were counted satisfied forever and the module doc
    claimed otherwise; fixed knob-free via the structural fact that the
    single worker completes every build within its cycle, so any
    discovery-time building row is a crash orphan: `reopenable_rows_for_
    parse` (failed + orphaned-building keys with no fresh sibling) with
    orphan adoption + `annotation_worker.orphan_adopted` evidence;
    (3) annotator api_key_file_path non-empty validation (reranker
    parity); (4) stale allow removed from resolved_api_key_file_path;
    (5) ProvenanceInputRef/ProvenanceObjectType moved to the wired
    re-export block. Refuted as designed: duplicate-memo-key wedge
    (per-item memo lookup prevents it), endpoint scheme check (recorded
    choice). Main-loop pre-verification fix: memo cache originally
    persisted bare bodies (item 1's confidence/memoizedFrom would have
    stamped all re-mints); now persists the full per-item array.
  - Residual risks (C10f): no live chat-completions round-trip, no
    section grouping against real parse data, no worker cycle against a
    real database; C9 hot cleanup must include semantic_annotations rows
    (named comment in worker.rs).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`. Nothing
    runtime-verified (unchanged; first runs at C10f).
  - Next: resolve D9 (graph channel query-time semantics), then present
    the C6 cluster plan per the §1.4 cluster cycle.
- 2026-07-14: C6 complete (C6a–C6f), implemented as a
  substrate→packages→two-phase-integration agent workflow (substrate +
  adversarial substrate review → prompt drafter + adversarial prompt
  reviewer → C6a∥C6b∥C6c-1∥C6d∥C6e in a batch of 5, then C6c-2∥C6f →
  integration phase 1 → phase 2 → 3 group finders → 3 refute-by-default
  confirmers → fix agent), verified by the five-dimension
  adversarially-confirmed review workflow (finders A clean / B1 two
  recorded non-defect nits / B2 four findings → 1 confirmed and fixed,
  2 refuted, 1 informational). All subagents on Opus per the §1.4 rule.
  - D9 resolved (see §4). First cluster run under the 2026-07-14 process
    rulings (recorded in §1.4/AGENTS.md): main-loop reads limited to
    structured verdicts; ~150k per-agent coherence cap with sequential
    sub-agent decomposition when exceeded; `main.rs`/`error.rs`
    serialized through exclusive non-parallel agents; a session-harness
    artifact read exception (AGENTS.md). The substrate agent's two
    follow-ups (envelope `inputAnnotationIds` column, shared envelope
    store) and the adversarial prompt reviewer's NOT-READY (confined to
    the finder prompts, fixed by an orchestrator prompt split) both
    landed before implementation dispatch.
  - Substrate (`sql/fabric/schema.sql` + `src/projections/{mod,envelope}.rs`,
    `ids.rs`): schema gained `chunk_projections`, `chunk_text_index`
    (FTS5, validated via a new `FABRIC_VIRTUAL_TABLES` existence
    battery), `chunk_dense_vectors` (per-chunk),
    `unit_multivector_projections` (per-unit, `UNIQUE(parse_id,unit_id)`),
    `graph_entity_mentions`/`graph_entity_edges` (D9), and a nullable
    `retrieval_projections.input_annotation_ids_json` (§22
    `inputAnnotationIds`); `user_version` stays 1. Banked chunker
    constants 400/512 under `ChunkerConfig` config-hash. Envelope store
    mirrors `annotations/store.rs` (`insert_building`/`complete_fresh`/
    `mark_failed`/`mark_stale`/`mark_superseded`/`delete_for_parse`/
    `fresh_types_for_parse`/`fresh_for_active_parse`); `projection.*`
    events atomic on the caller's tx; `ProjectionSuperseded` added as a
    recorded additive §33 extension (emitted by `mark_superseded`);
    `proj_` id mint added.
  - C6a `src/projections/lexical.rs`: `build_lexical_index` over
    chunk-targeting text; `match_chunks` BM25 read for C7b; the FTS5
    match string is built only by `primitives::bm25`.
  - C6b `src/projections/chunk.rs`: `build_chunks` harvesting `units.rs`
    greedy token-cap splitting; input = ordered content units via
    `SELECT_PARSE_UNITS_SQL`; chunks carry `input_unit_ids`, never mint
    slug IDs. `units.rs` is NOT yet deleted — the named deletion
    approval remains open (harvest done).
  - C6c-1 `src/projections/dense.rs`: `build_dense_vectors` per-chunk
    under a caller-held `ModelCallPermit`; `load_dense_vectors_for_parse`
    validated loader.
  - C6c-2 `src/projections/dense_cache.rs`: `DenseCache`/`DensePlane` —
    immutable `Arc` planes, lock held only for pointer moves;
    `snapshot_for_parse` for C7 captured reads.
  - C6d `src/projections/view.rs`: `build_derived_view` from canonical
    units only → artifact store `payload_uri`; `build_summary`
    materializes CA summary annotations with a linkage-only payload and
    `input_annotation_ids`.
  - C6e `src/projections/multivector.rs` + `codec.rs` extension:
    `encode_colbert_matrix_blob` reusing `encode_f32_blob`;
    `build_multivectors` per-unit ColBERT under a held permit.
  - C6f `src/projections/graph.rs`: `normalize_entity_name`
    (whitespace-collapse + NFC + Unicode-lowercase + NFC), single source
    for C7b; mentions/edges from FRESH CA annotations only;
    `one_hop_edges` both-direction UNION over the directional indexes;
    tier RANKING deferred to C7b.
  - Integration phase 1: a `ProjectionRuntime` handle (runtimes clones,
    config dimensions, a shared gate via a new `state.rs`
    `acquire_model_call_gate_on` free fn + `model_call_gate_handle`,
    `Arc<DenseCache>`) threaded into `scheduler::start`/
    `ParseDispatchContext`; the parse chain builds
    chunk→lexical→dense→multivector→derived-view in ONE tx between
    import and gate (also on the `GateExisting` replay arm), one permit
    per model role per parse (non-overlapping), builder-failure audit
    via a separate committed tx (`record_projection_build_failure`);
    `verify_activation_prerequisites` requires the five content-derived
    types fresh (Graph deliberately excluded — post-activation), CAd
    policy read intact; `publish_dense_cache` after commit under the
    held cutover barrier with predecessor eviction (the §1.6
    publish-invariant successor); `ColbertRuntime::tokenizer()`
    accessor; the one mechanical `main.rs` spawn-site edit. The phase-1
    investigator retired at the cap (170k, exceeded the 150k bound) and
    was decomposed per the cap rule, with handoff to a fresh
    implementer.
  - Integration phase 2: an annotation-worker completion hook
    `build_annotation_derived_projections` (active-parse re-check +
    annotations-complete guard via `reopenable_rows_for_parse`;
    `delete_for_parse` then summary then graph in one tx; separate-tx
    failure audit; `CycleTotals.projection_failures`). The batched
    `error.rs` edit is a recorded no-op — zero new variants
    cluster-wide.
  - Verification fixes: (1) CONFIRMED — `accept_held_parse` missed the
    dense-cache publish; FIXED, signature gained `dense_cache`/
    `dense_dimension` and publishes under the held barrier at
    `activation.rs:407`. (2) two REFUTED — the graph indexes exist in
    `sql/fabric/schema.sql` (the finder grepped only `src/`); the
    `state.rs` comment narrowing was reduced to a reword, applied.
    (3) one informational — the `mark_superseded` caller lands at C9
    supersession completion.
  - Process breaches recorded: two agents used read-only git despite
    the prohibition; the phase-1 investigator exceeded the 150k cap
    (170k) and was decomposed per the cap rule.
  - Residual risks / banked for later clusters: nothing runtime-verified
    (unchanged; C10f); the `hot_plane` validator does not validate
    secondary indexes (recorded gap);
    `retrieval_projections.freshness_status` has no DDL CHECK
    (pre-existing C2e; the Rust-side enum is sole enforcement). C7
    seams: AppState `DenseCache` clone; C7b consumes `match_chunks`/
    `snapshot_for_parse`/`mentions_for_name`/`one_hop_edges`/
    `normalize_entity_name` plus D9 tier ordering; dead-code allows on
    the read helpers stay until C7. Deletion approvals still open:
    `units.rs` (C6b harvest done) and the `docling.rs` legacy markdown
    path (C6d renders from canonical units).
  - All checks green, zero warnings throughout: `cargo fmt`,
    `cargo check`, `cargo check --features metal`, `cargo clippy` both
    ways.
  - Next: C7 (retrieval fabric) per the plan; no decisions block it —
    D9 tier ordering is a C7b input, already resolved. D2 remains open
    before C8d.
- 2026-07-15: C7 pre-plan rulings, process rules, and a rescope
  amendment (all user-ruled/approved 2026-07-15). No code changed.
  - Three §1.4 process rules added: the end-state-only premise (§1.2)
    as a standing every-agent-prompt constraint (never reason from
    "X doesn't exist yet"; evaluate against the completed end state);
    the spec-decides-it rule (clearly spec-decided options are chosen
    without asking and recorded with their citation); the honest-option
    rule (no false choices; options that fail honest advocacy are
    excluded with a one-line reason, at both the agent and main-loop
    layers). Motivating misses: an Opus recon agent argued a design
    from code-time state ("the C9 deletion hazard doesn't exist yet"),
    invalid under §1.2 since first runtime is C10f, after C9; and it
    tabled an AppState pre-wiring option that fails honest advocacy.
  - C7 design point 1 ruled (auto-ruled under the spec-decides-it
    rule, §31.1 "in-flight queries execute entirely against their
    captured pre-cutover snapshot view"): every query executes its
    hot-plane reads inside ONE per-query read-only transaction on one
    connection, opened at admission immediately after
    `reject_if_active`; the scope-filtered active (source_id →
    parse_id) capture is read inside that transaction, so capture and
    reads are one WAL snapshot. Parse-scoping of every SQL read stays
    (the §14 filter); the snapshot is the §31.1 guarantee. Recorded
    tradeoff: a pinned WAL read snapshot blocks checkpointing past it
    for the query's duration (bounded by the C8d single-search
    admission constant); boundary comment + held-duration log
    required. C8d threads the same transaction into C8 assembly. C9
    consequence: superseded-row deletion needs NO query-lease
    machinery — open snapshots keep deleted rows visible.
  - C7 design point 2 resolved (single viable option; AppState
    pre-wiring failed honest advocacy): C7 exposes a synchronous
    `execute_query`-style pipeline function in `src/query/` taking
    explicit handles (`CutoverRegistry`, `DenseCache`,
    `InferenceRuntime`, index root, profile), dead-code-allowed
    "consumed at C8d"; zero `main.rs`/`AppState` changes at C7 beyond
    the main-loop `mod query;` line. Admission (`reject_if_active` +
    the C8d in-flight gate) is the C8d caller's job; `execute_query`
    opens the DP1 read transaction as its first act.
  - Rescope amendment (user-ruled 2026-07-15; amends the 2026-07-11
    entry): the `multi_vector` retrieval CHANNEL defers post-MVP.
    Empirical basis (legacy DB + log + this machine, measured): 120
    active documents, 65,369 units, 20.1M ColBERT tokens = 10.3 GB of
    persisted matrices; measured MaxSim 0.28 ms/candidate and 81–84 ms
    for the 100-candidate fused-pool stage; an exhaustive
    candidate-generation scan estimates 30–60 s/query on this 32 GB
    M1 Max (compute ~18 s + 10.3 GB SQLite stream/decode per query,
    uncacheable beside ~16 GB of model weights). Stateless endpoint
    offload rejected: the cost is matrix movement/holding, not FLOPs.
    Unaffected: C6e keeps building/persisting matrices (deferral is
    reversible with no re-embedding); C7c keeps ColBERT MaxSim over
    the fused pool. Named post-MVP design and the C10f overlap
    diagnostic are recorded in §5 and C10f.
  - Recon correction recorded: no spec-§24/§28 model types exist in
    `src/` (`RetrievalProfile`, `QueryPlan`, `QueryRequest`,
    `RetrievalHit`, `RetrievalChannel`, `ResolvedScope`,
    `QueryExecutionRecord`, traces — verified zero hits; only the
    `qer_` ID mint exists). §5's "seams already built: C2d QER/trace
    model types" claim was wrong and is corrected; C7 defines the
    §24 types it needs fresh.
  - C7 cluster plan presented and APPROVED in-session (2026-07-15),
    including the C7d `src/primitives/latency.rs` named deletion. §3
    "C7 — Retrieval fabric" rewritten with the approved package
    structure (C7s substrate, C7a, C7b-1/C7b-2, C7c, C7d) plus the
    recorded "C7 fact base" (recon signatures, so the implementation
    session needs no re-recon); the Handoff rewritten for C7 dispatch.
  - Next: dispatch C7 per §3 (cluster-cycle step 3) in a dedicated
    session.
- 2026-07-15 (session end): C7 complete (C7s, C7a, C7b-1, C7b-2, C7c,
  C7d), implemented per the approved §3 "C7 — Retrieval fabric" plan as
  a substrate→packages→serial-integration agent workflow (prompt
  drafter + adversarial prompt reviewer → C7s substrate → C7a ∥
  (C7b-1 → C7b-2) ∥ C7c in parallel background agents → C7d serial →
  main-loop `mod query;` + the approved `src/primitives/latency.rs`
  deletion → five-dimension verification → fix pass), verified by the
  five-dimension adversarially-confirmed review workflow. All subagents
  on Opus per §1.4. The adversarial prompt reviewer returned NOT-READY
  before dispatch (caught two gate-discipline errors in the drafted C7c
  prompt and a false DensePlane-privacy claim in C7b-1's; amendments
  applied at dispatch).
  - Four rulings recorded this session: (1) **Gate discipline
    (user-ruled 2026-07-15)** amends the DP2 handle list — the plan's
    C7c "(local acquires internally)" was factually wrong
    (`uses_local_model_gate`, reranker_backend.rs:117-125, is a
    predicate that acquires nothing); the codebase discipline is
    CALLER-SIDE per the §1.5-pinned "caller-side model-call-gate
    discipline". `execute_query`'s handle set gains
    `Arc<ExclusiveGate>`; exactly three local model calls are gated
    caller-side via `acquire_model_call_gate_on` mirroring
    scheduler.rs:1638/1661 — dense query embed (execute.rs), ColBERT
    QUERY embed inside `run_maxsim_stage` (persisted document matrices
    are decoded, never re-embedded, per §38; only the query embeds are
    gated), and the local reranker inside `run_reranker_stage`; the
    HTTP reranker branch acquires nothing and the gate is never held
    across SQL or HTTP I/O. (2) **SPEC-1 predicate (spec-decides-it
    auto-ruling, §10 rule 4)**: DomainSet scope capture requires
    `AND locations.status = 'current'`; the `!= 'deleted'` alternative
    (a §11.2 "serving continues" reading) was rejected — §11.2 governs
    source-level serving, not per-domain visibility (user may
    override). (3) **mod-skeleton resolution (under the §1.4 substrate
    rule)**: C7s pre-created all five `src/query/` submodule skeletons
    and declared all `mod` lines, so parallel packages never contended
    on mod.rs. (4) **Clippy Option A (honest-option filter,
    main-loop-ruled)**: `#[allow(clippy::too_many_arguments)]` with
    concrete-reason comment on `dense_lexical_fusion_channel` (8 args
    after the DIAG-2 query_id fix); the params-struct alternative was
    excluded as one-call-site indirection without reuse (the
    RerankStageContext precedent bundles a coherent context reused by
    two functions).
  - Plan fact-base correction: the §3 C7c line records the ColBERT
    decoder as `codec::decode_colbert_document_vector_blob` in
    `projections/codec.rs`; it lives in `primitives/codec.rs` (the C7c
    loader calls `primitives::codec::decode_colbert_document_vector_blob`).
    Corrected in place in §3 this session.
  - C7s (`src/query/{mod,model}.rs` + four skeletons): §24 types
    `RetrievalHit`/`RetrievalHitType`/`RetrievalChannel
    {Dense,Lexical,Graph}` (3-variant MVP narrowing, spec snake_case
    wire literals)/`ResolvedScope`/`ResolvedScopeKind`;
    `hot_plane::begin_read_transaction` (DEFERRED read twin of
    `begin_write_transaction`, "query" namespace logging). Resolved the
    plan's open `chunk_text_index` unknown: standalone FTS5 (NOT
    external-content; chunk_id UNINDEXED stored; MATCH returns chunk_id
    directly, no join).
  - C7a (`src/query/profile.rs`): sealed `RetrievalProfile` v1 via
    `canonical_sha256_hex_without_field` + `PROFILE_HASH_JSON_KEY`
    (OnceLock, fixed authoring timestamp), values exactly the
    CRc-banked set (default_top_k=10, max_top_k=100, rrf_k=60,
    candidate_overfetch_multiplier=3, colbert_candidate_pool_size=100
    = defaultMaxCandidatesPerChannel, reranker_candidate_pool_size=10,
    graph_hop_budget=1, channels [dense, lexical, graph],
    defaultFusionStrategy "rrf"); `resolve_scope(ScopeInput)` default
    All. BANKED OPEN ITEM for C8d: `resolve_scope`'s both-present
    precedence (source_ids wins; empty-as-absent) is a recorded choice
    to revisit when the QueryRequest/QueryConstraints envelope lands —
    intersection semantics is the honest rival.
  - C7b-1 (`src/query/channels.rs`): `CapturedParse {source_id,
    parse_id, dense_plane: Option<Arc<DensePlane>>}` as the sole scope
    surface; dense exact-cosine over DensePlane accessors; lexical via
    `match_chunks` with `primitives::bm25` match strings only (recorded
    choice: strict-AND preferred, broad-OR fallback, None = explicit
    empty result); chunk→unit resolution BEFORE fusion via
    `chunk_projections.input_unit_ids_json`; RRF via `fuse_matches`;
    `dense_lexical_fusion_channel` entry point.
  - C7b-2 (graph channel, per D9): entry by normalized n-gram (width ≤
    MAX_ENTITY_NAME_TOKENS=6 code constant) exact-match probing via
    `normalize_entity_name`; tiers 1/2/3 with the ruled within-tier
    ordering (matched-name char length desc, name asc, unitId asc) plus
    a parse_id total-order discriminator engaging only on cross-parse
    unit_id collisions (recorded, beyond the ruled tiebreak); score
    strictly rank-monotonic; `explanation` populated with tier +
    matched names; `graph_channel` entry point.
  - C7c (`src/query/rerank.rs` + `src/projections/multivector.rs`
    loader): `load_multivectors_for_units` (parse-scoped, caller's
    connection, decodes via
    `primitives::codec::decode_colbert_document_vector_blob`);
    `RerankStageContext {conn, gate, query, query_id}`;
    `run_maxsim_stage` (distinct-unit pool cap, gate under colbert
    role); `run_reranker_stage` (gate only on the Local branch);
    reranker content resolved from `content_units.body_json` per
    content_type as a verified arm-for-arm mirror of multivector.rs
    `evidence_text` with must-stay-in-step comments on both sides
    (duplication assessed and accepted — a shared helper would couple
    query/ to projections/).
  - C7d (`src/query/execute.rs`): `execute_query(_registry,
    dense_cache, inference, gate, index_root, profile,
    colbert_expected_dimension, query_id, query_text, scope) ->
    Result<QueryPipelineOutcome, ApiError>`; DP1 first act = one
    read-only connection + transaction via `begin_read_transaction`,
    scope-filtered active capture inside it, `&*tx` threaded to every
    stage; `QueryStageLatencies` (8 fields incl. snapshot_held_ms)
    replacing the deleted latency.rs; `QueryPipelineOutcome
    {fused_pool, maxsim, reranked, latencies}`;
    `query.execute.snapshot_released` held-duration log structurally
    unskippable on every exit; dense-plane-missing per-occurrence warn
    (visible degradation); `_registry` carried unused by design (DP2
    contractual handle). The DP2 handle list gained two members beyond
    the ruled set: `gate: Arc<ExclusiveGate>` (ruling 1) and
    `colbert_expected_dimension` (ColbertRuntime exposes no dimension
    accessor — the C8d caller passes `config.models.colbert.dimension`,
    mirroring the build path; recorded C8d wiring obligation).
  - Cluster-wide: zero new ApiError variants, zero config changes;
    `src/main.rs` gained only `mod query;`. The latency.rs deletion
    cleaned its `mod` line and stale comment references in
    primitives/mod.rs.
  - Verification (five-dimension adversarially-confirmed): finders
    SPEC 1 / PRINCIPLES 1 / COMMENTS 5 / DIAGNOSTICS 3 / INTEGRATION
    CLEAN across all nine seam checks = 10 findings; PRIN-1/CMT-4
    merged as duplicates → 9 refute-by-default confirmer tasks → 5
    confirmed, 4 refuted. Refuted: DIAG-1 (drop delta exactly derivable
    cross-line from min(maxsim.scored, pool_size) vs survivor count);
    DIAG-3 (dense-embed boundary owned and self-logged by the runtime
    per the DIAGNOSTICS helper-function clause); CMT-1 and CMT-5
    (provenance notes recording authorship, not false present-state
    assertions). All 5 confirmed fixed: SPEC-1 (MED) — `AND
    locations.status = 'current'` added to capture_active_by_domains +
    invariant comment (ruling 2); PRIN-1/CMT-4 (MED) — multivector.rs
    file-scoped allow + false "not yet wired" rationale removed
    (empirically no dead_code warning; build_multivectors live via
    scheduler.rs:1667); CMT-3 (LOW) — hot_plane.rs
    begin_read_transaction allow + comment removed (settled
    empirically: no warning, since rustc seeds allow(dead_code) items
    as live roots, so the query module's allow makes its callees used);
    CMT-2 (LOW) — the C7d descriptor comment moved from the rerank mod
    line to the execute mod line; DIAG-2 (LOW) — query_id threaded into
    dense_lexical_fusion_channel/graph_channel and the private channel
    helpers and added to all six query.fusion.*/query.channel.* events
    (concrete harm was defeated by max_in_flight_search=1 at end-state,
    but the every-stage-line-carries-its-correlation-id convention was
    real; triggered the Option A clippy allow, ruling 4).
  - C8d wiring obligations recorded: C8d threads `CutoverRegistry`,
    `DenseCache`, `InferenceRuntime`, the model-call gate `Arc`, the
    index root, the profile, and `config.models.colbert.dimension` into
    `execute_query`; owns admission via `reject_if_active` + the
    `max_in_flight_search=1` code constant; C8 assembly joins the
    per-query read transaction.
  - All checks green, zero warnings throughout and at final state:
    `cargo fmt`, `cargo check`, `cargo check --features metal`,
    `cargo clippy`, `cargo clippy --features metal`. Nothing
    runtime-verified (unchanged; first runs at C10f); D2 open before
    C8d; D5 deferred.
  - Next: present the C8 cluster plan per the §1.4 cluster cycle
    (resolve D2, then C8a, C8b, C8d; C8c stays deferred).
- 2026-07-15 (second session): C8 complete (C8s, C8a, C8d-1, C8b,
  C8d-2), implemented per the approved plan as prompt-drafter +
  adversarial prompt reviewer (NOT-READY: 5 surgical amendments,
  applied at dispatch) → C8s serial → C8a ∥ C8d-1 → C8b → C8d-2 →
  main-loop wiring, verified by the five-dimension
  adversarially-confirmed workflow. All subagents Opus per §1.4.
  - Pre-plan rulings (user-approved 2026-07-15): D2 resolved for the
    query surface (see §4; admin-transport residual finalizes at C10a).
    R2 barrier-probe placement — the §31.1 probe runs POST-CAPTURE
    inside the per-query read transaction over the captured in-scope
    source_ids; any active barrier aborts the whole query with a
    retryable 503 before any retrieval stage. AMENDS the DP1 recorded
    ordering ("opened at admission immediately after reject_if_active"),
    which was unimplementable for All/DomainSet scopes:
    `reject_if_active` is per-source and the in-scope set is only known
    after `capture_active_set`. A barrier engaging post-probe is
    licensed §31.1 in-flight behavior (WAL snapshot isolation).
    R3 scope intersection — both-present constraints INTERSECT (kind
    source_set with both fields populated; capture SQL enforces
    source-id membership AND a current location in the domains,
    `locations.status = 'current'`; empty intersection = explicit empty
    result), superseding the C7a source_ids-wins banked choice.
  - C8s `src/assembly/{mod,model}.rs`: §25–§27 types field-for-field
    (9 structs + closed snake_case enums, camelCase +
    deny_unknown_fields); submodule skeletons pre-declared;
    `sql/fabric/schema.sql` gained `idx_unit_relationships_parse_from`
    and `_parse_to` (`user_version` stays 1).
  - C8a `src/assembly/{policy,operators}.rs`: sealed MVP AssemblyPolicy
    v1 via the seal pattern (fixed authoring timestamp; budgets
    maxEvidenceUnits=30, maxExpansionDepth=1, maxTokens=15360 = 30×512;
    rules: anchor / parent-container+heading-path for text-bearing
    anchors / caption-pair for figure-table / continuation chain / text
    neighbors ±1 for text_block); all SEVEN §25 operators (relationship
    graph authoritative per §19 — convenience columns not substituted);
    §25.1 dependency check reads `parse_runs.conformance_report_json`
    via `active_parse_id` (the first such reader) and emits ONE
    aggregate warn per query (missing type wire name → affected-parse
    count; per-(parse,type) logging amended out at prompt review);
    `requiresRelationshipTypes` = 9 types deliberately including
    never-emitted `continues_on`/`references` (inert-visible by design).
  - C8d-1 `src/query/request.rs` + `state.rs` + `error.rs`: R6 MVP
    envelope with deny_unknown_fields (queryText bounded by
    `max_search_query_chars` — its first functional reader;
    callerContext accepted-unrecorded per the §6 reservation;
    constraints sourceIds/governanceDomains; retrievalPolicy
    maxFinalEvidenceUnits 1..=max_top_k default default_top_k;
    evidencePolicy includeSourceLocators/includeRelationships/
    includeAnnotations; debug); `ValidatedQuery` output struct with
    caps/defaults applied; AppState gained
    dense_cache/cutover_registry/search_admission
    (`MAX_IN_FLIGHT_SEARCH: u32 = 1`, D3 code constant);
    `ApiError::CutoverBarrierActive` (503, kind
    "cutover_barrier_active") — the `From<CutoverBarrierActive>` impl
    lives in `state.rs` NOT `error.rs` (colbert-diagnostic
    `#[path]`-includes error.rs without a state module; verified
    deviation).
  - C8b `src/assembly/evidence.rs`:
    `build_evidence_pack(conn, &[CapturedParseRef], &[Anchor],
    EvidenceOptions, count_tokens, ...)` → pack + ContextAssemblyTrace;
    anchors rank-ordered; R13 determinism (per-anchor rule order then
    unitId asc; first-inclusion-wins dedupe; later rules append
    reasons); §26 enforced structurally (units resolve only
    `WHERE parse_id = <captured>`); FIRST `locators_json` reader;
    relationships = operator-traversed edges when requested;
    annotations via `fresh_for_active_parse` filtered to selected
    units; content read once per unit; maxTokens via caller-supplied
    closure (CPU-only, never gated).
  - C8d-2 `src/query/{execute,profile}.rs` + `http.rs` + allow sweep:
    probe after capture before any stage (`snapshot_released`
    structurally unskippable on the new exit);
    `capture_active_by_source_ids_and_domains` (both predicates,
    status='current'); assembly stage after rerank inside the tx
    (`QueryPipelineOutcome` gained `evidence_pack`, latencies gained
    `assembly_ms`); `QueryRequestContext` bundles request-scoped
    inputs (debug deliberately handler-owned — the compiler proved a
    bundled copy dead); the request's max_final_evidence_units now
    drives the pipeline top_k (replacing `profile.default_top_k` as
    the fusion input; a default request is byte-identical to C7);
    `POST /query` (spec-literal path; /v1/health unchanged) —
    admission permit FIRST and held across `spawn_blocking` wrapping
    the synchronous pipeline (http.rs's first blocking seam),
    JoinError → logged 500, debug diagnostics via local view DTOs
    (inference score types are not Serialize); the correlation
    query_id reuses `new_query_execution_record_id()` (commented: a
    correlation handle only, no QER written this cluster);
    EvidenceOptions mapped request→assembly field-by-field (recorded
    layering choice: two same-shaped structs, request DTO vs assembly
    contract); allow sweep across src/query/, src/assembly/, and the
    named state.rs items — sole survivor:
    `ValidatedQuery.caller_context` (QER-deferred).
  - Verification (five-dimension adversarially-confirmed): finders
    SPEC/PRINCIPLES/DIAGNOSTICS clean, COMMENTS 3, INTEGRATION 1 = 4
    findings → 1 confirmed and fixed (stale state.rs CutoverRegistry
    seam comment still saying the query-side consumer "arrives at
    C7/C8"); 1 split-verdict fixed by main-loop direction
    (`query.execute.started` logged `profile.default_top_k` instead of
    the effective request top_k; both confirmer runs agreed on the
    facts, disagreed on criterion strength); 2 refuted-but-directed
    (evidence_text mirror-comment cross-references — refuted because
    the confirmer prompts lacked the finder checklists, an
    orchestrator plumbing gap, so the criterion read as invented;
    fixed as stale-comment hygiene by direction). Fix-pass
    truth-mapping CORRECTED the C8 recon fact base: the believed
    TableCell normalizedText divergence between rerank.rs and
    multivector.rs is FALSE — all FOUR `evidence_text` sites
    (rerank.rs, multivector.rs, evidence.rs, and
    `annotations/producer.rs`, previously uncounted) are arm-for-arm
    identical; all four now carry an accurate four-site
    must-stay-in-step banner.
  - Workflow mechanics recorded: one confirmer died at the
    StructuredOutput retry cap and its finding was silently dropped
    from the first run's report — recovered via workflow resume; and
    resume caching is POSITIONAL (longest unchanged prefix), so the
    confirmer after the failed one also re-ran and downgraded its
    verdict (confirmed→informational); the split was ruled by the main
    loop. Future verification scripts should pass finder checklists
    into confirmer context and return unverified findings explicitly.
  - Process breaches recorded: the C8s agent ran read-only
    `git diff --stat` (self-reported, same class as the two C6
    breaches); the C8d-2 agent finished at ~187k tokens, exceeding the
    §1.4 150k cap instead of being decomposed.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`; colbert-diagnostic compiles both
    ways. Nothing runtime-verified (unchanged; first runs at C10f):
    /query, the spawn_blocking seam, the barrier probe, the
    intersection capture SQL, and token-budget counting have never
    executed.
  - Next: C9 (lifecycle forensics) per the §1.4 cluster cycle; no open
    decisions block it (D2's admin residual is C10a's; D5 deferred).
- 2026-07-16: C9 pre-plan recon, rulings, and cluster plan approval
  (all user-ruled/approved 2026-07-16). Dispatch follows in-session.
  - Recon (three parallel read-only agents, triangulated, all Opus per
    §1.4): the C2 "all 19 spec types" claim is FALSE for §30 — no
    `ForensicSnapshot`/`ForensicSnapshotManifest`/`SnapshotArtifactRef`/
    `ReplayProfile` types exist in `src/model/` (`model/mod.rs`
    documents coverage as §7–§20, §33; same class as the corrected
    §24/§28 claim, now corrected in §5 and here); all C9 event types
    (`snapshot.*`, `drill.*`, `source.deactivated/reactivated`,
    `source.access_lost/access_restored`, `parse.archived`) are defined
    in the §33 enum with ZERO mint sites; projection payloads are
    hot-plane-only except derived views — every other plane completes
    with `payload_uri = None`, so the 2026-07-14 supplement ruling is
    entirely unimplemented and C9a builds the archival;
    `source_objects.deactivated_at` is written by no code and the
    All-scope query capture never joins locations (its SELECTs filter
    `active_parse_id IS NOT NULL AND deactivated_at IS NULL`), so
    marking locations deleted alone cannot remove a source from search;
    dense/multivector planes are model-dependent from scratch but
    byte-reproducible from archived blobs (`primitives/codec.rs`);
    chunk/lexical/graph planes are deterministic from hot rows; NO FK
    cascades exist anywhere (the only two REFERENCES clauses carry no
    ON DELETE) so cleanup is explicit per-table DELETEs; no DELETE
    exists for `content_units`/`unit_relationships`/
    `semantic_annotations`/`retrieval_projections`/`parse_runs`;
    nothing sets `archived`/`archived_at`; `annotation_memo`
    deliberately survives cleanup (schema comment); deletion runs
    synchronously post-drain in `run_cycle`, never through the queue.
  - Three rulings (user-ruled 2026-07-16):
    (1) **Snapshot artifact scope** — archive what only archival can
    preserve: `chunk_dense_vectors` + `unit_multivector_projections`
    blobs (only byte-reproducible from blobs; never re-embed),
    `semantic_annotations`, `chunk_projections` payloads, and the
    canonical active-parse rows (`content_units`/`unit_relationships`/
    `parse_runs`) are archived content-addressed; the sealed §12.3
    parse bundle and raw source bytes are referenced by their existing
    uri+hash; the FTS5 and graph planes are covered by the deletion
    gate's verified deterministic rebuild (§8.3/§30.5 rebuild
    allowance). The supplement ruling's `retrievalProjections`/
    `semanticAnnotations` manifest sections are satisfied by the
    archived payloads. Uniform all-plane blob archival rejected:
    duplicates what the gate rebuilds and verifies anyway.
    (2) **ReplayProfile at MVP** — `evidenceReplayMode: "bit_exact"`,
    `retrievalReplayMode`/`generationReplayMode: "not_supported"`.
    `record_replay` is excluded as undemonstrable until the QER tier
    lands (no recorded per-query stage outputs exist to substitute;
    §29.1). Recorded deviation note: the `record_replay` upgrade lands
    with the QER audit tier. Basis correction recorded: Guarantee 2 is
    deferred with the QER tier, so the bit_exact evidence claim rests
    on the snapshot verification tiers (mechanical re-hash every
    snapshot + deletion-gate rebuild check), not on G2.
    (3) **`scheduled` snapshot trigger deferred post-MVP** (inert enum
    variant, like `pre_deployment`): every protected state change
    already has a lifecycle trigger; no external retention/cadence
    obligation exists (user-confirmed 2026-07-16), so a cadence key
    would be a §35-prohibited internal-guess knob; restore drills (the
    one cadence-needing tier) already deferred. Small recorded
    deviation from the §30.6 trigger list, mitigated by §36 naming
    only manifests + mechanical verification for MVP.
  - Auto-rulings recorded (spec/code-decided under the spec-decides-it
    rule): `forensic_snapshots` hot-plane metadata table + manifest in
    the artifact store, mirroring the `query_execution_records`
    pattern, `user_version` stays 1 (CAa/C6/C8s precedent); C9 trigger
    set = pre/post-activation, pre-deactivation, plus a parameterized
    manual/incident fn (HTTP at C10a), `pre_deployment` inert
    (corrected 2026-07-16 at prompt review — see the correction bullet
    below); deactivation sets
    `source_objects.deactivated_at` (forced by the capture SQL;
    `active_parse_id` stays intact so §11.4 reappearance is a
    reversible flag-clear) and acquires the per-source cutover barrier
    (§31.1; third `acquire` caller after activation/accept); the
    deletion-gate rebuild check re-imports dense/multivector blobs and
    never re-embeds (§38); restore re-imports preserving IDs
    (§31.3/§11.4/ids.rs) INCLUDING annotations from archived artifacts
    — a memo-cache re-mint would create new IDs/provenance (a rebuild,
    not a restore); the memo cache still survives for future
    re-parses; app identity = `CARGO_PKG_VERSION` + `cfg!` build
    features + canonical hash of the resolved config (secret file
    PATHS only, never values) + `SPEC_VERSION = "0.3"` constant, all
    newly built in `src/identity.rs` (none exist today); superseded
    cleanup = explicit ordered per-table DELETEs incl. the
    `semantic_annotations` hard-delete, `annotation_memo` untouched;
    access-lost mechanism lands at C9c wired to the one evidence class
    the full-scan connector honestly produces (source-side
    scope-enumeration failure → `access_lost`; per-file 403
    granularity recorded as a D5/C10f residual — deferring the
    mechanism entirely was excluded, §37 criterion 10); manifest
    sub-scopes — acquisition/deletion records and the resolved sealed
    policies/profiles are serialized at snapshot time (§30.2; today
    they are hot rows / OnceLock constants), `parserOutputBundles`
    section absent at MVP (spec-optional; parser output is
    staging-only and no retention policy is invented).
  - C9 cluster plan presented and APPROVED in-session (2026-07-16):
    packages C9s (substrate) → C9a ∥ C9c → C9b → C9d (serial last),
    the named `sql/fabric/schema.sql` edit (`forensic_snapshots` table
    + `FABRIC_TABLE_CONTRACTS` entry), zero config changes, zero
    deletions. §3 "C9 — Lifecycle forensics" rewritten with the
    approved package structure plus the recorded C9 fact base; the
    Handoff updated for C9 dispatch.
  - Prompt-review correction (user-confirmed 2026-07-16): the original
    auto-ruling listed a distinct pre-superseded-deletion snapshot
    trigger; the spec's §30.3 snapshotType closed enum has NO such
    variant, and §31.2 steps 3–5 are contiguous (post-activation
    snapshot → gate → delete), as is §11.3 step 4 in the deactivation
    flow. Ruling: C9a ships NO pre-superseded-deletion trigger; the
    deletion gate verifies over the immediately-preceding lifecycle
    snapshot — `post_activation` in the activation flow,
    `pre_deactivation` in the deactivation flow — located through the
    C9s-pinned snapshot-lookup contract, never re-taken. §3 corrected
    in place. The adversarial prompt reviewer returned NOT-READY with
    six amendments, all applied at dispatch: the real
    `gate_and_activate` handle set (it opens its own connection and
    derives source_id from parse_run_id — trigger fns mirror that, no
    Connection/source_id parameters from the scheduler);
    `RestoreFailed` batched by C9b under a scoped
    `#[allow(dead_code)]` "Constructed at C9d" arm with C9d removing
    that allow at first construction (an unconstructed variant would
    trip dead_code at C9b's own battery — the C1c precedent);
    the pinned snapshot-lookup contract (C9s decides handle-passed vs
    query by (source_id, parse_id, snapshot_type) and the
    `forensic_snapshots` DDL must carry the lookup columns);
    barrier-release label fix (release is at guard drop when
    `gate_and_activate` returns, not at the commit line);
    `envelope::delete_for_parse` is TYPE-scoped, so wholesale
    per-parse `retrieval_projections` cleanup iterates every type or
    authors a parse-wide DELETE; and this trigger correction.
- 2026-07-16 (session end): C9 complete (C9s, C9a, C9b, C9c, C9d plus
  two integration passes), implemented per the approved §3 plan as
  prompt-drafter + adversarial prompt reviewer (NOT-READY, six
  amendments — see the prompt-review bullet above) → C9s serial →
  C9a ∥ C9c → identity-threading integration → C9b → C9d → final
  wiring integration → five-dimension adversarially-confirmed
  verification workflow → fix pass. All subagents Opus per §1.4.
  - Three in-flight rulings (user-ruled 2026-07-16):
    (1) **Identity threading, Option A**: `ApplicationIdentity`
    (system_version, spec_version, build_features,
    configuration_hash) is captured ONCE in `main.rs` after config
    load and threaded explicitly — main → `scheduler::start` →
    `ParseDispatchContext` → every snapshot trigger, and
    `propagate_deletions` → `deactivate_one_source` →
    `pre_deactivation_snapshot`. The C9s-pinned trigger signatures
    gained `identity: &ApplicationIdentity`. A config-derived
    OnceLock/global accessor was rejected (new hidden-dependency
    pattern vs the codebase's explicit-handle discipline). This
    closed the C9a escalation: the runtimeArtifacts application
    identity carries `configurationHash` (§30.2 complete).
    (2) **Predecessor on the activation outcome**:
    `ActivationDecision::Activated { superseded_predecessor_id:
    Option<String> }` (single surviving option — a status-only
    'archiving' lookup is ambiguous because superseded HELD
    candidates also sit in 'archiving'); `accept_held_parse` now
    returns `ActivationDecision` (uniform contract, consumer C10a).
    (3) **Deactivation-mode cleanup leaves parse status untouched**
    and emits no `parse.archived` (follows the
    deactivated_at-reversibility auto-ruling); the
    `archiving`→`archived` + `archived_at` + `parse.archived`
    completion belongs to the ActivationSupersession mode only —
    verified already-correct in C9d's implementation.
  - C9s: `src/model/snapshot.rs` (§30.3/§30.4 types field-for-field,
    full enums incl. MVP-inert variants), `src/identity.rs`
    (CARGO_PKG_VERSION; `cfg!` features incl. cuda; canonical config
    hash over a `ConfigurationIdentity` projection — secret PATHS
    only; `SPEC_VERSION = "0.3"`), `forensic_snapshots` table +
    subject partial index + `FABRIC_TABLE_CONTRACTS` entry
    (`user_version` stays 1), four skeletons with pinned signatures.
    Snapshot-lookup contract pinned: verify receives the resolved
    `&ForensicSnapshot` header; restore queries `forensic_snapshots`
    by (subject_source_id, subject_parse_id, snapshot_type);
    lifecycle snapshots always set the subject columns,
    manual/incident leave them NULL.
  - C9a `src/snapshot.rs`: manifest builder per the artifact-scope
    ruling — ARCHIVED: sourceObjects/acquisitionRecords/parseRuns/
    contentUnits/unitRelationships/semanticAnnotations/
    retrievalProjections as deterministic JSONL row projections,
    retrievalIndexes as raw dense/multivector blobs,
    assemblyPolicies/retrievalProfiles/capabilityProfiles
    (required-annotation-set)/runtimeArtifacts as JSON; REFERENCED:
    canonicalParseBundles by `parse_runs.artifact_bundle_uri/_hash`;
    EMPTY: queryExecutionRecords; ABSENT: parserOutputBundles,
    modelArtifacts; deletionRecords absent-when-none;
    parser/connector capability profiles pinned by hash-marker refs.
    Manifest self-hashed via `canonical_sha256_hex_without_field`;
    archival write-once BEFORE the row tx; triggers
    pre/post-activation + pre_deactivation + `request_snapshot`
    (manual/incident, C10a allow); first `snapshot.*` emitters;
    ids.rs `snap_` allow removed. ReplayProfile stamped
    bit_exact / not_supported / not_supported.
  - C9c `src/deletion.rs`: `propagate_deletions` (post-drain,
    complete-enumeration-gated; returns the deactivated
    (source_id, parse_id) pairs), `deactivate_one_source` (snapshot
    BEFORE barrier; barrier held only across the `deactivated_at`
    write + `evict_parse`; `source.deactivated` atomic),
    `restore_reappeared_sources` (same-hash reappearance → C9d
    restore → clear `deactivated_at` + `source.reactivated` on Ok
    only), `mark_scope_access_lost` (source-side scan failure →
    `access_lost` + clock stop, serving continues; no deletion
    inference from failed scans). `acquisition.rs` refresh branch
    gained the `source.access_restored` mint (audit-pair
    completion).
  - C9b `src/snapshot/verify.rs`: `verify_mechanical` (manifest
    self-hash + every blob-backed ref re-hashed via `get_bytes`;
    marker refs skipped under a named commented policy;
    explicit-battery style) and `verify_deletion_gate`
    (+ deterministic rebuild: dense/multivector blobs decoded via
    `primitives/codec` and compared to hot rows — never re-embed;
    chunk plane compared on its deterministic columns with banked
    ChunkerConfig-hash pinning; graph re-derived from archived
    annotations, must-stay-in-step with graph.rs; lexical covered
    transitively via its verified source rows — FTS5 internals
    undiffable, commented). Failure = `SnapshotVerificationFailed`;
    halting/retention are the caller's doc-commented duty. error.rs
    batch: `SnapshotVerificationFailed`
    (500/"snapshot_verification_failed") + `RestoreFailed`
    (500/"restore_failed"; scoped allow removed by C9d at first
    construction per the amendment).
  - C9d `src/restore.rs`: `complete_superseded_parse`
    (`SupersededCleanupMode::ActivationSupersession {
    activated_parse_id } | Deactivation`) — locates the gating
    snapshot (post_activation / pre_deactivation) via the lookup
    contract, never re-mints; a gate Err halts before any write tx
    opens; on pass, one IMMEDIATE tx: `mark_superseded` per fresh
    envelope (`projection.superseded` audited before the hot
    delete), ordered DELETEs — `chunk_text_index` FIRST via
    `chunk_id IN (SELECT …)`, then graph, dense, multivector,
    chunk_projections, retrieval_projections (parse-wide),
    semantic_annotations hard-delete, unit_relationships,
    content_units LAST; `parse_runs` never deleted;
    `annotation_memo` untouched — then (supersession mode only)
    `archiving`→`archived` + `archived_at` + `parse.archived`
    atomic (first writer of all three).
    `restore_source_from_snapshot(index_root, registry, dense_cache,
    dense_dimension, source_id, parse_id)`: `verify_mechanical` →
    one tx generic column-driven re-import preserving IDs (canonical
    rows, annotations from archived JSONL — never memo re-mints,
    envelopes, chunk rows, dense/multivector blobs byte-for-byte) →
    deterministic FTS5 + graph rebuild → post-commit under-barrier
    dense publish (mirrors activation's publish; no predecessor
    eviction by design). `parse_runs` is not re-imported (never
    deleted; stays `active` with `active_parse_id` pointing at it).
  - Scheduler wiring: `pre_activation_snapshot` before BOTH
    `gate_and_activate` arms; `post_activation_snapshot` +
    `complete_superseded_parse(ActivationSupersession)` on
    `Activated` with `Some(predecessor)`; post-drain ordering
    propagate → restore_reappeared → Deactivation-cleanup loop
    (guards same-cycle flip-flop; the deactivation/reappearance
    candidate SQLs are verified disjoint within a cycle); the
    `ScanError::SourceSide` arm binds the failed-acquisition record
    id → `mark_scope_access_lost` before returning Err.
  - Verification: SPEC finder CLEAN; PRINCIPLES 1 / COMMENTS 2 /
    DIAGNOSTICS 2 / INTEGRATION 2 = 7 findings, ALL confirmed
    (0 refuted, 0 unverified — the C8 lesson applied: finder
    checklists passed into confirmer prompts; nulls surfaced
    explicitly), collapsing to 5 unique fixes, all applied:
    verify.rs stale module allow + comment removed (flagged
    independently by three dimensions; zero warnings after removal —
    the module is fully live); identity.rs Clone-rationale +
    module-doc corrected (main keeps no copy; the deletion path is
    reached through the scheduler thread); `snapshot.completed`
    gained per-plane archived counts (MED — counts existed only in
    manifest metadata, the log-vs-artifact substitution DIAGNOSTICS
    forbids; `ArchivedCounts` returned from
    `build_and_archive_manifest`, no new queries; `snapshot.failed`
    unchanged — counts unavailable at that boundary);
    `deletion_gate_succeeded` gained per-plane compared counts
    (`RebuildCounts`; lexical stays a countless documented no-op);
    model/mod.rs — the 8 wired §30 re-exports promoted to the
    no-allow block, `ChannelReplayMode` kept deferred with its
    consumer named (post-MVP verified-recompute tier, §29.4).
  - Residual risks / banked: **superseded-HELD-candidate cleanup
    gap** — held candidates superseded into 'archiving' by
    `supersede_other_held` are not the predecessor and have no
    cleanup path (commented in activation.rs; NEEDS A RULING —
    candidate C10-planning scope); `configuration_hash` is a
    hand-picked `ConfigurationIdentity` projection (client section
    excluded) with Debug-coupling on three enum-valued fields
    (recorded fingerprint choice; extend the projection if audits
    need more); restore dense publish never evicts (by design;
    must-stay-in-step mirror of activation's publish); per-file 403
    access-lost granularity unobservable through the full-scan
    connector (D5/C10f residual); nothing runtime-verified
    (unchanged; first runs at C10f — `--setup-storage` creates the
    fabric plane incl. `forensic_snapshots` at commissioning).
  - All checks green, zero warnings throughout and at final state:
    `cargo fmt`, `cargo check`, `cargo check --features metal`,
    `cargo clippy`, `cargo clippy --features metal`.
  - Next: C10 (operational shell + commissioning) per the §1.4
    cluster cycle — present the C10 cluster plan; D2's admin
    residual finalizes at C10a; resolve the superseded-HELD cleanup
    ruling at C10 planning.
- 2026-07-16 (later, C10 planning): C10 pre-plan rulings, recon,
  pre-plan resolutions, and cluster plan approval (all
  user-ruled/approved 2026-07-16). No code changed. Dispatch follows
  in dedicated sessions.
  - Ruling 1 (closes the C9 superseded-HELD-candidate residual):
    never-activated held candidates leaving to 'archiving' — BOTH the
    `supersede_other_held` population AND `discard_held_parse`
    discards (recon confirmed discard shares the identical no-cleanup
    gap, activation.rs:646-691) — are cleaned by a new third
    `SupersededCleanupMode` (working name HeldSupersession) on
    `complete_superseded_parse`, gating over the held candidate's OWN
    `pre_activation` snapshot located by (source_id, held_parse_id,
    'pre_activation') via the existing latest-wins lookup
    (restore.rs:103-109). Basis (adversarial decision package):
    pre_activation snapshots set subject_parse_id = the candidate
    itself and archive planes whole-table (snapshot.rs:103-128,
    478-530), so the candidate's dense/multivector blobs — the only
    model-dependent state; the §12.3 bundle never carries vector
    blobs — are provably archived; running the existing
    ActivationSupersession mode instead would verify the WRONG parse.
    The mode completes archiving→archived + parse.archived (this exit
    is terminal, unlike reversible deactivation); the
    superseded/discarded id is threaded out and cleaned AFTER the
    barrier releases, mirroring the predecessor arm (§31.1 brevity,
    spec-decided); gate failure → halt/retain/health-visible (C10b),
    no auto-retry; re-anchoring the snapshot on failure is excluded
    (collides with the 2026-07-16 never-re-taken correction).
    Recorded clarification: this deliberately extends
    pre_activation-snapshot semantics to serve as the deletion gate
    for a candidate that will never activate — a documented overload,
    not a quiet one. An ungated delete was excluded (§38's "after
    verified snapshot and deletion gating" conditional;
    rebuild-not-re-embed). All three 'archiving' exits (activation
    predecessor, superseded held, discarded held) now share one
    contract: every path out of hot storage passes a verified
    snapshot gate.
  - Ruling 2 (D2 administrative residual, finalized): Operation
    records + polling ONLY (§34.6) — NO NDJSON streaming survives
    anywhere in the end-state API. Consequence: the http.rs emitter
    machinery, the types.rs OperationRequest/OperationEvent/
    OperationBenchmarks/BenchmarkStage substrate, and the CLI
    streaming/stream-render machinery are reclassified from
    "consumed at C10a" to NAMED DELETIONS (C10e / the C10c rework);
    their "consumed at C10a" comments are stale as of this ruling.
    Streamed admin progress was excluded: at end state these are rare
    operator overrides (the autonomous pipeline owns routine work), a
    poll loop keeps user-triggered work visibly alive, and the
    durable service log remains the authoritative stage record.
  - Recon: three parallel read-only agents (C10a API/transport, C10b
    health surfacing, C10c/C10e CLI + deletion inventory), all Opus
    per §1.4; facts banked into the §3 C10 fact base. Notable: NO
    Operation model type or operations table exists; no route
    performs bearer auth today (the auth substrate is
    dead-code-allowed); no inspection read helpers exist; health is
    purely in-memory (the handler opens no connections);
    docling_activity.rs is LIVE (the C4c carry ruling stands — not a
    deletion candidate); the docling.rs markdown path's "pending
    C6d" retention condition is satisfied (view.rs renders from
    canonical units only), so it is now deletable; units.rs has zero
    consumers.
  - Eight pre-plan resolutions recorded with bases (condensed in §3):
    (1) all mutating admin routes are async Operations (spec-decided,
    §34.6), executed on detached spawn_blocking tasks updating the
    Operation row; discard gets an additive `parse_discard`
    operationType (recorded additive extension, annotation.*
    precedent); (2) queue-coupled ops (POST /sources, force re-parse)
    enqueue via `enqueue_coalesced` and complete their Operation via
    the drain — `sync_queue` gains a nullable operation_id column;
    direct HTTP-thread parse-chain execution excluded (a second
    parse-chain executor); a force re-parse with unchanged parser
    identity AND content completes with a recorded identical-identity
    outcome (§13.5 determinism); (3) protected POST /shutdown
    retained (single viable: no OS signal handling exists;
    `request_shutdown` is the only graceful-shutdown path) —
    extra-spec, recorded additive; (4) protected = all mutating admin
    POSTs + GET /parses?status=held + GET /operations/{id}; public =
    POST /query, GET /v1/health, GET /units/{id}(/relationships),
    GET /sources/{id}, GET /sync/status; (5) GET /units/{id} derives
    the parse from the parse-scoped unit ID and serves only if that
    parse is active on its source (§14); (6) health extends the
    publish-into-slot pattern (scheduler publishes fabric counts per
    cycle; the annotation worker gets its own slot;
    `AdmissionGate::snapshot` consumed; every count carries an as-of
    label per the PRINCIPLES accuracy rule); readiness set UNCHANGED
    = {inference, sync}; per-source-system granularity keyed by
    source_system (one system at MVP); (7) `ApplicationIdentity` is
    cloned onto AppState (snapshots/restore from HTTP handlers need
    it; today it moves wholesale into the scheduler); (8) the CLI
    keeps the interactive REPL.
  - C10 cluster plan presented and APPROVED in-session (2026-07-16):
    packages C10s (Operation substrate) → C10r (ruling-1 cleanup) →
    C10a → C10b ∥ C10c → C10d → C10e → C10f; named schema edits
    (`operations` table + FABRIC_TABLE_CONTRACTS entry;
    `sync_queue.operation_id`; `user_version` stays 1); one named
    config-file edit (stale `operation_timeout_seconds` comment
    re-wording only — zero key changes anywhere in C10); the C10e
    named-deletion list (full list in §3). §3 "C10 — Operational
    shell + commissioning" rewritten with the approved structure plus
    the recorded C10 fact base; §4 D2 marked fully resolved; the
    Handoff rewritten for C10 dispatch.
  - Next: dispatch C10s → C10r → C10a per §3 (cluster-cycle step 3)
    in a dedicated session.
- 2026-07-16 (C10 first implementation session): C10s, C10r, C10a
  COMPLETE plus two user-ruled design fixes and an interim
  five-dimension adversarially-confirmed verification over the session
  diff (5 confirmed findings, all fixed; 4 refuted; 0 unverified —
  finder checklists passed into confirmer prompts per the C8 lesson).
  Flow: prompt drafter + adversarial prompt reviewer (READY;
  amendments A1/A2/A3 applied at dispatch) → C10s serial → C10r
  serial → C10a serial, main-loop wiring after each. All subagents
  Opus per §1.4.
  - Four rulings recorded (user-ruled 2026-07-16 this session):
    (1) Drain-side Operation completion for queue-coupled operations
    ASSIGNED TO C10R (resolution 2 fixed the mechanism, not the
    ownership; the C10s/C10a split could not own scheduler.rs).
    (2) OPTION A — prescreen override: the force re-parse route could
    never succeed as planned — the drain imports only connector-staged
    bundles and the (mtime,size) prescreen stages only changed files,
    so §34.2's parser-rollout case (unchanged content) never reached
    the parse chain. Fix at the detection layer: pending `sync_queue`
    entries with `operation_id IS NOT NULL` (the structural
    operator-request discriminator) are subtracted from the connector
    known-state before `full_scan`, force-staging them; drain
    missing-bundle policy — scan-eligible-but-unstaged → honest
    Operation failure ("absent from corpus or outside corpus root");
    enqueued-after-scan race → left re-claimable one cycle. Excluded:
    a second parse-dispatch arm (the "second parse-chain executor"
    resolution 2 rejects) and leave-as-built (the route's named
    purpose would never work).
    (3) OPTION 1 — `POST /sources/{sourceId}/parses` keeps the
    body-carried coordinate and gained validation before
    `insert_pending`: source absent → 404 NotFound; coordinate not a
    `current` location of the path source → 400 (Operation
    `targetObjectId` audit integrity). Path-only derivation rejected
    (multi-location sources force an arbitrary pick rule).
    (4) The §1.4 per-agent ~150k coherence cap is a GUIDELINE, not a
    hard limit (C10r finished ~166k, accepted).
  - C10s: `src/model/operation.rs` (§34.6 field-for-field;
    operationType = the spec 10 values + recorded additive
    `parse_discard`; status 4 values), `src/operations.rs` store
    (`insert_pending`/`mark_running`/`mark_succeeded`/`mark_failed`/
    `get`; status-guarded transitions; self-contained write connection
    per call; NO SystemEvent — recorded resolution, §33 closed enum,
    "operators poll Operations"), `operations` table + nullable
    `sync_queue.operation_id` + paired FABRIC_TABLE_CONTRACTS entries
    (`user_version` stays 1), `op_` mint. `get` returns Option (the
    handler owns 404); no CHECK on operation_type
    (forensic_snapshots.snapshot_type precedent).
  - C10r: `SupersededCleanupMode::HeldSupersession` gates over the
    candidate's OWN pre_activation snapshot (latest-wins lookup, no
    new snapshotType, no re-take) and completes archiving→archived +
    `parse.archived` (terminal); `supersede_other_held` returns the
    superseded ids; `ActivationDecision` carries `superseded_held_ids`
    on BOTH arms (a hold can supersede an older held candidate);
    both scheduler drain arms clean post-barrier under
    must-stay-in-step banners; `discard_held_parse` already moved
    ready→archiving (amendment A3 corrected the drafted prompt).
    Queue-coupled completion: `enqueue_coalesced(…, operation_id:
    Option<&str>)` persisted with `COALESCE(?4, operation_id)` so an
    autonomous re-detection coalescing in never drops an operator
    link; the drain flips pending→running at dispatch
    (crash-replay-tolerant), `complete()` marks succeeded (id read
    before its DELETE), `fail()` drives terminal failed even for
    pre-dispatch faults.
  - C10a: the full §34 surface in `http.rs`. Protected: POST /sources
    (source_ingest, queue-coupled), POST /sources/{sourceId}/parses
    (parser_execution, queue-coupled + the Option 1 validation),
    POST …/activate + /parses/{parseId}/accept (parse_activation,
    detached), /parses/{parseId}/discard (parse_discard, detached),
    /snapshots, /restore (detached), /shutdown (control action, NO
    Operation row — recorded), GET /parses?status=held,
    GET /operations/{id}. Public: GET /units/{unitId}(/relationships),
    GET /sources/{sourceId}, GET /sync/status (+ /v1/health, /query).
    First live bearer auth (`bearer_token_from_headers` +
    `authorize_admin_token` consumed). Detached-task lifecycle: the
    handler awaits `insert_pending` → 202 {operationId}; the
    un-awaited spawn_blocking closure does mark_running → domain call
    inside catch_unwind → mark_succeeded/mark_failed, panic-to-failed
    IN-closure (a JoinError on a never-awaited handle is
    unobservable), double-fault logging. Queue-coupled handlers write
    ONLY pending (+ an orphan guard failing the op if the enqueue
    itself fails). §14 gating in SQL (`active_parse_id` subselect;
    non-active parses 404 indistinguishably from absence).
    `ApiError::NotFound` (404/"not_found") batched. The accept
    handler implements the scheduler-arms model via
    `drive_activation_cleanup` (pre/post-activation snapshots,
    predecessor ActivationSupersession, HeldSupersession per
    `superseded_held_ids`); discard drives HeldSupersession
    post-return. Allow sweeps: operations.rs module allow removed,
    model/mod.rs operation+source re-exports wired. `state.rs`:
    `application_identity` + accessors. Main-loop edits: `mod
    operations;` and `AppState::new`'s 8th arg
    `application_identity.clone()` before the move into
    `scheduler::start`.
  - Interim verification (user-approved; the cluster remainder gets a
    second pass after C10e): spec and integration finders CLEAN;
    principles 2 / comments 5 / diagnostics 2 → cross-dimension merge
    → refute-by-default confirmers. 5 confirmed, all fixed:
    (1) LOW behavioral — POST /restore marked succeeded without
    clearing `deactivated_at` (restore_source_from_snapshot's
    documented caller duty); fixed via new shared `pub(crate)`
    `deletion::restore_and_reactivate_source` (restore → flag-clear →
    `source.reactivated` atomically; reason parameterized:
    "same_hash_reappearance" autonomous / "operator_rollback_restore"
    HTTP — new payload tag, recorded, no enforced vocabulary exists);
    (2) MED activation.rs stale accept/discard "no in-crate caller"
    comments + redundant allows removed; (3) LOW state.rs
    "Consumer-less since cluster CR" comments + allows removed;
    (4) LOW schema.sql sync_queue.state comment now names
    model/sync.rs `SyncQueueState`; (5) LOW all drain error logs
    gained `operation_id` (best-effort read that never aborts the
    drain). Notable refutation: a queue-coupled Operation marked
    succeeded on a recorded ParseFailed is CORRECT — outcomes are not
    faults; Operation.status is the pipeline lifecycle and the parse
    outcome lives in the parse run row. Operator-UX consequence for
    C10c/C10d: a polling client must read the parse run for the
    domain verdict.
  - Residual risks / banked: corpus-containment validation of
    operator-supplied URIs absent at the HTTP boundary (candidate
    C10f ruling); benign one-cycle `attempt_count` inflation on
    race-deferred rows; redundant-but-harmless allows remain on
    now-used items in ids.rs (`new_operation_id`), model/operation.rs
    (module allow), snapshot.rs (`request_snapshot`), acquisition.rs
    (ImportOutcome fields) — owner cleanup at the C10e sweep;
    POST /restore serves the PreDeactivation/reactivation case (the
    restore lookup contract); nothing runtime-verified (unchanged;
    first runs at C10f).
  - All checks green, zero warnings throughout and at final state:
    `cargo fmt`, `cargo check`, `cargo check --features metal`,
    `cargo clippy`, `cargo clippy --features metal`.
  - Next: dispatch C10b ∥ C10c per §3 (cluster-cycle step 3), then
    C10d → C10e → C10f; cluster-remainder verification after C10e.
- 2026-07-17: C10b and C10c COMPLETE. C10c was decomposed into
  C10c-1 → C10c-2 under the §1.4 cap rule at adversarial prompt
  review (a single-agent full surface swap of the 2,038-line CLI was
  judged likely to breach the guideline mid-rewrite, where
  decomposition is most disruptive). Flow: prompt drafter →
  adversarial prompt reviewer (NOT-READY: the drafted C10c prompt
  never named `execute_command` as the replaced dispatcher — the
  KEEP/DEAD lists alone would have left a broken build or a failed
  acceptance check; plus the per-source-keying shape requirement and
  the both-allows precision on C10b) → split → delta review
  (NOT-READY: three surgical amendments — the EvidencePack
  `src/assembly/model.rs` read pointer, held-listing `dimensions`
  precision, handoff hardening with removed-helper signatures) →
  C10b ∥ C10c-1 parallel dispatch (disjoint owned files) → main-loop
  `main.rs` wiring → C10c-2 on C10c-1's embedded handoff report. All
  subagents Opus per §1.4. The CLUSTER-REMAINDER adversarial
  verification pass still runs after C10e per the approved plan
  (this session ran prompt-level review only).
  - C10b (`state.rs`, `scheduler.rs`, `annotations/worker.rs`,
    `types.rs`; `main.rs` wiring main-loop): `HealthComponent` gained
    additive typed `counts: Vec<HealthCount>`
    (`{label, source_system?, value, as_of}`, existing plain-serde
    style — deliberately NOT the model camelCase standard); new slot
    types `FabricHealth` (per-`source_system` map of
    `FabricSourceCounts {held, serving_stale, access_lost,
    stuck_building, unparseable_mime, verification_halted}` +
    `measured_at`, resolution 6) and `AnnotationHealth`
    (parked/parked_detail/last_cycle `AnnotationCycleCounts` incl.
    new `orphans_adopted`/`measured_at`); `health()` assembles three
    new diagnostic-only components `fabric`/`annotation`/
    `search_admission`; readiness UNCHANGED {inference, sync}; the
    health handler still opens no connections (slots only). Scheduler:
    seven bounded SQL count consts + `fabric_counts` (mirrors
    `queue_depths` open_read pattern) + `publish_cycle_fabric_health`
    published each cycle beside `publish_health`; panic path clears
    the slot. Worker: `start`/`run_worker` gained the slot arg; the
    parked path publishes; `run_cycle` returns `CycleReport`;
    `CycleTotals.orphans_adopted` added at the `orphan_adopted` log
    site. Admission surfaced via `AdmissionGate::snapshot` (both
    stale allows removed; as-of literal "live"). As-of discipline:
    cycle `utc_now` stamped per publish, "not-yet-measured" before
    the first cycle. `main.rs` (main-loop): two slot constructions,
    `AppState::new` 8→10 args (fabric/annotation clones after
    `sync_health`), `scheduler::start`/`worker::start` +1 trailing
    arg each, stale worker-spawn health comment corrected.
  - Recorded C10b semantics: `unparseable_mime` counts ACTIVE source
    objects with an unroutable stored MIME — no durable failure
    marker exists for the warn-only no-parser path; switch the SELECT
    if a marker lands. `parse_runs`/`source_objects` counts are
    corpus-global, attributed to the single MVP source_system
    (documented in `fabric_counts`; over-attributes if a second
    system arrives before those tables gain a `source_system`
    column). Optional follow-up reported, not performed: projecting
    the new counts onto `GET /sync/status` (an `http.rs` surface
    decision).
  - C10c-1 (`src/bin/data-store.rs`, exclusive owner): full excision
    + minimal complete surface. New command table — ingest, reparse,
    activate, accept, discard, snapshot, restore, shutdown,
    held-parses, operation, query, unit, relationships, source,
    sync-status, health, help, exit — targeting the §34 routes
    (full verb→args→route→protection table in the session handoff,
    reproduced in C10c-1's report); transport seam `get_public`/
    `get_protected`/`post_public_json`/`run_admin_operation`/
    `poll_operation(_once)`/`send_shutdown`; 202→poll loop with
    `OPERATION_POLL_INTERVAL = 1s` code constant (rationale
    commented; per-request timeout stays
    `[client].operation_timeout_seconds`, key untouched); the
    Operation-succeeded ≠ parse-outcome rendering rule implemented
    and commented (`is_parse_producing_operation` directs the
    operator to the parse run / held-parses); per-request admin
    token reads preserved. Every NDJSON/stream-loss/benchmark/
    bm25-panic/legacy-DTO symbol removed; zero `/v1/operations`
    references remain (acceptance check green).
  - C10c-2: typed lenient response mirrors (Deserialize, NO
    deny_unknown_fields — additive server fields tolerated) +
    human-readable renderers: query (salient evidence-unit view with
    excerpted textProjection + full §18 body and §27 assemblyTrace
    passthrough via `print_labeled_json`; debug diagnostics latency
    table, ranked/reranked lines with logit/tokenCount when present,
    fusedPool counted AND dumped in full), held-parses (salient
    fields + the conformance report's `dimensions` map — the report
    has no single verdict field), unit/relationships (§14 404
    "absent OR non-active parse, indistinguishable by design"
    annotation via `annotate_unit_404`), source (location status/
    lastSeenAt prominent), sync-status; `render_health` extended for
    the C10b `counts`; `excerpt_text` (`TEXT_EXCERPT_CHARS = 800`)
    restores the removed `render_content` intent. Stage-1 transport
    layer unchanged (stage contract held).
  - Residual risks: nothing runtime-verified (unchanged; first runs
    at C10f) — the poll loop, the new command surface, and every
    renderer are compile-checked only; rich shapes (fusedPool,
    evidence body/locators, conformance report, provenance,
    deletionEvidence, metadata) are `serde_json::Value` passthroughs,
    so structural drift surfaces as raw JSON rather than a decode
    failure (recorded honesty-over-precision choice); C10c-1's report
    misattributed the in-flight C10b `main.rs` arity errors to
    "pre-existing end-state gaps" — resolved by the main-loop wiring,
    recorded here to keep the report trail honest.
  - All checks green, zero warnings at final state: `cargo fmt`,
    `cargo check`, `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`; `/v1/operations` absent from the
    CLI.
  - Next: C10d (documentation re-baseline; per-file draft approvals,
    includes the NAMED `[client]` comment-only config edit) → C10e
    (final legacy sweep, then the cluster-remainder verification
    pass) → C10f (commissioning).
- 2026-07-17 (later): C10d COMPLETE (documentation re-baseline). All seven
  docs FULLY REPLACED from live code reads (never from the legacy docs):
  README.md, ARCHITECTURE.md, PROTOCOL.md (the wire contract of record,
  now with serde-exact field tables for all referenced response models),
  SPEC-SERVER.md (1,171 lines, §1–§19, incl. ten new domain-contract
  sections: acquisition, sync queue/backpressure, parsing, activation,
  projections, annotations, query pipeline, forensics, system events,
  error model), SPEC-CLIENT.md, INSTALL.md, INTERACTIVE.md. The NAMED
  config edit landed (comment-only, both config files):
  `[client].operation_timeout_seconds` now documents per-request
  semantics — explicitly NOT a total-poll cap — superseding the planned
  "stream timeout" rewording with the stronger correction the reviews
  established.
  - Process (user-escalated 2026-07-17): six parallel Opus draft agents
    (prompt drafter + adversarial prompt reviewer, NOT-READY, amendments
    at dispatch), then — user-ruled, scoped to the C10d docs as critical
    files — Fable subagents for verification and fix passes (a recorded
    scoped exception to the §1.4 all-subagents-Opus rule). Gate, now
    precedent for doc work: independent completeness/accuracy review
    (two models for both SPECs and INSTALL+INTERACTIVE; Fable verifiers
    for README/ARCHITECTURE/PROTOCOL) → reconciled verify-before-write
    fix pass → independent POST-FIX verification; no file presented for
    approval until its post-fix verifier passed or its residue was
    verifier-prescribed edits applied verbatim. Full set user-approved
    2026-07-17.
  - Rulings this session: (1) the GET /sync/status fabric-counts
    projection (open decide-or-drop) is DROPPED — /v1/health is the
    single aggregation surface; sync/status serves the published
    SyncHealth projection only. (2) PROTOCOL.md documents ONLY the
    supported end-state surface: the /v1/operations/{id}/control stub is
    omitted entirely (mention-and-mark rejected; the stub is a named
    C10e deletion and the app is offline until the MVP completes).
  - Review findings → C10e scope additions (user-approved 2026-07-17),
    four named items: the false total-poll-bound comment
    (src/bin/data-store.rs:24-26 — it misled three doc drafts before
    being caught); the stale "stream timeout" doc comment
    (src/config.rs:87, main-loop-owned); the stale NDJSON-stream
    reference (src/model/event.rs:3-4, dies with the emitter deletion);
    and a get_parses micro-package — wrap the non-Result Query extractor
    so a missing `status` param returns the JSON error envelope AFTER
    auth (today: framework plain-text 400 before auth, the sole breach
    of the envelope-everywhere and auth-first invariants), removing
    PROTOCOL.md's three as-built edge notes in the same change.
    Recorded no-action: the control stub performs no bearer auth (it
    always rejects 400; named C10e deletion).
  - Notable documented facts (verified this session): /v1/health is the
    one snake_case response on the surface; fabric counts key by
    source_system "filesystem", not the governance domain; the CLI poll
    loop is unbounded (per-request timeout only); a terminal-failed
    Operation still exits 0 in one-shot mode; flagless `snapshot` sends
    the JSON body {}; --setup-storage creates only fabric.sqlite3 (the
    artifact tree is lazy at runtime); the service never re-reads the
    token file (in-memory constant-time compare).
  - Process breaches recorded: two agents ran read-only `git diff
    --stat` (same class as the C6/C8 breaches); the SPEC-SERVER
    expansion agent finished ~173k, over the ~150k guideline (accepted
    per the 2026-07-16 guideline ruling).
  - Residual: docs describe as-built, compile-checked behavior; nothing
    runtime-verified (unchanged; first runs at C10f).
  - Next: C10e (final legacy sweep: the approved named-deletion list
    PLUS the four additions above) → the cluster-remainder
    five-dimension verification pass over C10s–C10e → C10f.
- 2026-07-17 (C10e session): C10e COMPLETE (final legacy sweep + the
  four 2026-07-17 additions + residual allow cleanup). Flow: prompt
  drafter → adversarial prompt reviewer (NOT-READY, four amendments
  applied at dispatch — see the corrections bullet) → main-loop `rm`
  of `src/units.rs` (subagents may not delete files) → single serial
  executor → main-loop `config.rs` comment edit. All subagents Opus
  per §1.4. The CLUSTER-REMAINDER verification pass has NOT yet run
  (next step; this session ran prompt-level review only).
  - Deleted: `src/units.rs` + its `main.rs` mod line; the docling.rs
    markdown path (`DoclingConversionResult`,
    `convert_source_to_markdown`, `read_and_normalize_markdown`,
    `normalize_markdown`; `DoclingJsonConversionResult` doc comment
    rewritten to stand alone); the entire http.rs NDJSON emitter
    cluster (stream constants/statics, `OperationStreamSendStatus`,
    `OperationStreamSender`, `OperationEmitter` + impl, all
    `emit_operation_*`/`operation_*` free helpers incl. the two
    unmarked emitter-internal fns) + the
    `/v1/operations/{operationId}/control` reject stub + route + the
    now-unused imports (`Infallible`, `AtomicU64`, `Bytes`, `mpsc`,
    `OperationControlRequest`/`OperationEvent`), file-header comment
    rewritten to the transport-shell role; types.rs
    `OperationBenchmarks`/`BenchmarkStage`/`OperationRequest`/
    `OperationEvent`/`OperationControlRequest` + the dead
    `OperationErrorDetail` and `Deserialize` imports (the
    `OperationErrorDetail` TYPE stays live in error.rs/http.rs).
  - The four additions landed: get_parses envelope-after-auth — the
    `status` extractor is now `Result<Query<ParsesQuery>,
    QueryRejection>`, examined only AFTER `authorize_request`, mapped
    to `ApiError::BadRequest` preserving the rejection detail and
    logged at the validating stage; PROTOCOL.md's exactly-three
    as-built edge notes removed in the same change. Comment
    corrections: src/bin/data-store.rs poll-interval comment (the
    false "and the total time we will wait" clause removed),
    src/model/event.rs NDJSON-stream reference, and the main-loop
    `config.rs` `operation_timeout_seconds` doc comment (per-request
    semantics; the poll loop is unbounded until terminal).
  - Fact-base corrections recorded (the 2026-07-16 recon had
    drifted; all handled under the approved verify-then-prune
    discipline — anything live stays): (1) `ApiError::UnitSplitting`
    is LIVE via `projections/chunk.rs:740` (`count_tokens`, the C6b
    harvest) — retained, stale "only in units.rs" comment + allow
    removed (the adversarial prompt reviewer caught this; the
    drafted unconditional delete would have broken an unowned file);
    (2) `src/source.rs` is FULLY LIVE (scheduler containment call
    sites) — NO dead paths existed; `SourceResolution` retained with
    its false comment/allow removed; (3) `primitives/{validate,
    fusion}` are WIRED — content retained, module-level allows +
    false "remove when wired" headers removed. CLI dead mirrors were
    already absent (cleaned at C10c/d; only the comment fix
    remained).
  - Residual allow cleanup: removed on `ids.rs::new_operation_id`,
    the `model/operation.rs` module allow, and
    `snapshot.rs::request_snapshot` (all now-live; adjacent
    "remove when wired"/"no MVP caller" comments corrected). NEW
    dead item surfaced: `StoredDenseVector.unit_id` is never read —
    narrowed field-level allow with a recorded-but-unread comment
    (primitives/validate.rs).
  - BANKED RULING (open): the three `ImportOutcome` fields
    (`source_location_id`, `new_source_object`,
    `new_source_location`, acquisition.rs) marked "Consumed at C10a
    (inspection surfaces)" are read by NO consumer — the inspection
    surfaces never used them. Allows retained, comments corrected to
    recorded-but-unread pending a ruling; honest options are delete
    the fields or wire a consumer. Candidate scope: the
    cluster-remainder verification pass or C10f planning.
  - Recorded mechanical edit beyond the named file list:
    `src/projections/chunk.rs:25` one-line COMMENT-ONLY fix (stale
    mention of the deleted `DoclingConversionResult`).
  - All checks green, zero warnings after every unit and at final
    state: `cargo fmt`, `cargo check`, `cargo check --features
    metal`, `cargo clippy`, `cargo clippy --features metal`. Nothing
    runtime-verified (unchanged; first runs at C10f).
  - Next: the cluster-remainder five-dimension verification pass
    over C10s–C10e, then C10f (commissioning).
- 2026-07-17 (cluster-remainder verification): the cluster-remainder
  five-dimension adversarially-confirmed verification pass over the
  C10s–C10e diff COMPLETE (five parallel Opus finders with
  per-dimension checklists + do-not-re-flag list; finder checklists
  passed into confirmer prompts per the C8 lesson). Spec, diagnostics,
  and integration finders CLEAN (integration ran the deletion-closure
  grep sweep and the cargo battery); principles 1 + comments 1 → 2
  refute-by-default confirmers → both CONFIRMED (0 refuted, 0
  unverified), both fixed (comment/attribute-only, zero behavior
  change):
  - PRIN-1 (LOW): stale `#[allow(dead_code)]` + "remove the allow
    when wired" comment on `ids.rs::new_query_execution_record_id` —
    live at http.rs:474 as the /query correlation id; allow removed,
    doc comment now records the correlation-handle role until the QER
    tier lands (no dead_code warning after removal).
  - CMT-1 (MED): five docling.rs comment sites asserted the
    C10e-deleted markdown path as present-state machinery
    (execute_docling_conversion, DoclingProcessContext.output_format,
    build_docling_args, convert_source_to_document_json docs), plus
    one adjacent same-class site (DoclingJsonConversionResult
    .json_text "Unlike the markdown path" contrast) — all six
    rewritten to describe the single JSON entry point; error-mapping
    and no-shell-interpolation invariants preserved.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`. Nothing runtime-verified
    (unchanged; first runs at C10f).
  - Next: C10f commissioning. Open rulings banked for C10f planning:
    the three recorded-but-unread `ImportOutcome` fields (delete or
    wire a consumer) and corpus-containment validation of
    operator-supplied URIs.
- 2026-07-17 (C10f pre-planning rulings): both banked C10f-planning
  rulings RESOLVED, implemented main-loop in-session (small edits, no
  subagents), and verified by the cargo battery (user-ruled/approved
  2026-07-17).
  - Ruling A — `ImportOutcome` fields: DELETE. The three
    recorded-but-unread fields (`source_location_id`,
    `new_source_object`, `new_source_location`), their allows, and
    retention comments removed from `ImportOutcome` (acquisition.rs)
    and both construction sites (the `AcquisitionRecord` model rows
    keep `source_location_id` — persisted §9.2 state). Wire-a-consumer
    was excluded under the honest-option rule: the C10a inspection
    surfaces read the hot plane (source of truth), and every fact
    stays durable in the `acquisition.import_succeeded` boundary log,
    the acquisition_records row, and the source.ingested /
    source.location_added events. `ImportLinkage` untouched (still
    feeds the log); trivially reversible if a post-MVP surface wants
    the values in-memory.
  - Ruling B — operator-URI corpus containment: lexical HTTP-boundary
    prescreen (Option B). `corpus_relative_source` moved verbatim from
    scheduler.rs to src/source.rs (now pub; the single mapping rule,
    shared by parse dispatch and the prescreen); new
    `source::prescreen_operator_native_uri` = `corpus_relative_source`
    + `validate_relative_source` on the remainder — LEXICAL only, no
    filesystem I/O, no existence check (a file may legitimately land
    before the next scan); `post_sources` (http.rs) runs it after body
    decode, before `insert_pending`, rejecting 400 `source_resolution`
    at the `coordinate_validating` stage. Basis: the drain's Option A
    missing-bundle failure is honest but its latency is unbounded (the
    knob-free cadence has no ceiling on an idle corpus); a URI
    lexically incapable of ever being staged should not mint a pending
    Operation. The drain missing-bundle policy and the parse-dispatch
    containment authority remain authoritative (commented at all three
    sites). Existence-at-submit excluded (TOCTOU + semantics change).
    Scope: POST /sources only — the re-parse coordinate is already
    DB-validated as a current location.
  - PROTOCOL.md updated (separately approved): POST /sources documents
    the prescreen (400 `source_resolution`, lexical-only rationale,
    async Operation failure for passing-but-absent URIs); both admin
    curl examples corrected from impossible `s3://` coordinates to
    filesystem-style absolute paths (the only two live-doc `s3://`
    occurrences).
  - Onboarding doc findings RECORDED, not fixed (separate approval
    items): AGENTS.md's "Operation API and event changes" rule still
    names the deleted `POST /v1/operations` + NDJSON event fields, and
    DIAGNOSTICS-ONBOARDING.md retains an "Operation stream delivery"
    boundary section and Required Context table row — both stale since
    the D2 ruling / C10e sweep (the C10d re-baseline covered the seven
    operator docs, not these two process files).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`. Nothing runtime-verified
    (unchanged; first runs at C10f).
  - Next: present the C10f commissioning plan (run sequence per §3;
    every run individually user-approved).
- 2026-07-17 (C10f commissioning session 1): commissioning runs R0–R3
  plus a graceful shutdown COMPLETE — the programme's FIRST runtime
  execution. Run-sequence plan (R0–R9) approved in-session; corpus
  ruling: staged subset (user replaced the out-of-repo `sources`
  symlink with an in-repo `sources/` dir — 12 PDFs + 2 txt, 107 MB).
  Cluster CP (ingestion performance) planned and APPROVED this session
  (see §3); implementation deferred to a new session.
  - R0: first fabric release build (`cargo build --release --features
    metal`, clean, both binaries; prior target/release binaries were
    June 21 legacy artifacts). All three configured model dirs
    verified present.
  - R1: `--setup-storage` created `index/fabric/fabric.sqlite3` —
    WAL, `user_version=1`, all 19 fabric tables + FTS5 shadow tables
    verified; artifacts/ tree correctly absent (lazy).
  - R2: first startup attempt failed bind — the LEGACY service
    (running since June 21, PID 34387) still held 8091; user-approved
    SIGTERM kill (legacy plane is reference-only). Retry: full clean
    startup — bind-first, token handoff + 0600 token file, inference
    init with per-model smokes (dense 36 layers, colbert 22,
    reranker 22), sync validated, `ready=true`; /v1/health + CLI
    `--health` verified (typed counts, as-of labels, snake_case
    exception).
  - R3: first autonomous cycle COMPLETE with zero pipeline failures:
    enumerated=14 staged=14 imported=14 failures=0, cycle
    elapsed_ms=13,780,182 (~3 h 50 m); all 14 sources parsed,
    embedded, gated, ACTIVATED; pre/post-activation snapshots and
    supersession cleanup ran; deletion/reappearance scans 0/0;
    cadence initialized. Graceful shutdown via CLI `--shutdown`
    (202) exercised the R7 control path; `service.stopped` clean.
  - Annotator diagnosis (two stacked root causes; 12+ calls failed
    `status none` at exactly the 120 s whole-request timeout):
    (1) Little Snitch silently DROPPED outbound from the in-place
    rebuilt binary (rule keyed to the old code hash); user allowed
    it — OPERATIONAL NOTE: recurs at every rebuild unless the rule
    is path-scoped. (2) Still failing after the allow: the endpoint
    serves Qwen3.6-27B in THINKING mode and producers send no
    output-token bound, so generation exceeds 120 s (engine busy,
    client times out). USER RULING 2026-07-17: the shared endpoint
    stays thinking-enabled; this client must opt out per-request →
    package CPb.
  - Measured first-cycle cost model (truthful `model_call` logs,
    tonight-only verified): dense passage embedding 149.6 min = 65%
    (5,120 calls, avg 1,753 ms, batch 1); ColBERT document embedding
    30.2 min = 13% (34,092 calls, avg 53 ms); Docling + import +
    snapshots + gate + misc ≈ 50 min = 22%.
  - Recon (two parallel read-only Opus agents per §1.4) banked into
    the §3 CP fact base: qwen3 `forward_hidden` is batch-shape-
    capable (mask/RoPE broadcast; NO padding mask anywhere;
    last-token pooling assumes unpadded input); the ColBERT runtime
    is structurally batch-1 (rank-2 tensors, hard B≠1 rejects,
    bidirectional + local sliding-window masks, per-head Metal-safe
    2D matmuls); model-call gate discipline is caller-side
    everywhere (scheduler holds ONE permit per role per parse across
    the whole builder loop); both builders read inputs up front and
    encode/insert per item (blob formats cannot change under
    batching); per-item `model_call` logging is runtime-side with
    `text_count` hardcoded 1.
  - FINDING WITHDRAWN (recorded for honesty): the in-session
    "elapsed_ms lies (1–2 ms vs 1.8 s)" timing finding was a
    main-loop log-extraction artifact — `cut -c1-300` truncated
    `elapsed_ms=1720` to `elapsed_ms=1`. Instrumentation verified
    correct (`as_millis`, span encloses the synchronizing readback
    in all three runtimes). No timing fix package exists.
  - Banked findings for later rulings/fixes: (a) graceful shutdown
    waited ~2 h 20 m — the scheduler checks the shutdown signal per
    CYCLE, not per drain entry; needs a ruling (bound latency at one
    entry?); (b) producer requests carry no output-token bound and
    the response `content: String` is null-intolerant
    (llm_client.rs) — robustness gap, partially mitigated by CPb;
    (c) the Docling activity monitor logs 2 INFO lines/sec per
    conversion — log-noise review candidate.
  - Rulings recorded: kill legacy service (2026-07-17); smaller
    dense model (option C) REJECTED by user for now; parse/embed
    overlap (option B) DEFERRED to a post-CPc discussion with real
    numbers; ColBERT batching DEFERRED data-gated (large rework of a
    13% stage — decide after CPc).
  - Current machine state: service STOPPED cleanly; fabric plane at
    `index/fabric/` populated with 14 active sources (annotations
    all parked failed, self-healing once CPb lands); admin token
    file present; sources/ is the staged corpus.
  - Next: implement CPa+CPb in a NEW session per §1.4, then the CPc
    benchmark re-ingestion run, then discussion B + the ColBERT
    batching decision; C10f runs R4–R9 resume after CPc (R4 gains
    the graph channel once annotations build).
- 2026-07-17 (CP implementation session): CPa and CPb COMPLETE,
  implemented per the approved §3 CP cluster plan as prompt drafter +
  adversarial prompt reviewer (READY; three precision notes applied at
  dispatch) → CPa ∥ CPb parallel implementation agents (disjoint owned
  files) → main-loop cargo battery → five-dimension
  adversarially-confirmed verification (finder checklists passed into
  confirmer prompts per the C8 lesson) → fix pass. All subagents Opus
  per §1.4.
  - CPa (`src/inference/dense.rs`, `src/projections/dense.rs`;
    `qwen3.rs` untouched): new `embed_passage_vectors(&[&str]) ->
    Result<Vec<Vec<f32>>, ApiError>` — per-text `tokenize_truncated` +
    empty-token guard, right-pad to batch max (pad id 0,
    value-independent under causal attention, commented), flattened
    rows → `[B, seq]` tensor, ONE `forward_hidden`, per-row pooling
    narrowed at each row's TRUE length − 1 (never `seq_len − 1`),
    per-row L2 normalize, rows in input order; private
    `forward_passage_batch` carries the mandatory
    causal-right-padding-soundness comment;
    `validate_batch_vectors`/`batch_output_validation_error` attribute
    failures by row index, no vector values logged. Builder:
    `DENSE_EMBED_BATCH = 16` code constant (engineering-fact comment,
    NOT a §35 knob); `build_all_chunks` embeds per 16-chunk window;
    per-item `validate_vector` → `encode_vector_blob` → INSERT
    unchanged (byte-compatible with commissioned rows); chunk-id
    failure attribution preserved; a batch-forward error fails the
    whole build (current semantics). Logging: one
    `model_call.started/completed|failed` pair per batch with real
    `text_count` and summed `token_count` (+ vector_dimension/
    expected_dimension/elapsed_ms). Startup smoke
    `startup_smoke_batch_consistency`: `SMOKE_BATCH` (3
    different-length texts) embedded batched AND singly, per-row
    cosine ≥ `SMOKE_BATCH_COSINE_MIN = 0.999` else
    `ApiError::InferenceInit`; wired into `load_with_progress` as the
    additive `dense_smoke_batch` step (batched-forward wrongness
    becomes a startup-seconds failure; BF16 reduction-order drift
    recorded acceptable, no cross-build byte contract). Gate
    discipline untouched (scheduler's single per-parse permit; the
    runtime acquires nothing).
  - CPb (`src/annotations/llm_client.rs`): `ChatCompletionRequest`
    gained constant `chat_template_kwargs: ChatTemplateKwargs {
    enable_thinking: false }` (typed Serialize struct mirroring
    `ChatMessage`), single construction site in `send_and_parse`;
    vLLM-extension-coupling comment (OpenAI-compatible servers ignore
    unknown fields; suppresses thinking for these calls only; shared
    endpoint stays thinking-enabled — user ruling 2026-07-17). Banked
    out-of-scope findings untouched (no output-token bound; `content`
    stays null-intolerant).
  - Recorded implementation deviations (in-scope judgments):
    flattened-Vec padding representation; the 0.999 cosine tolerance;
    a separate batch validator (no per-row token_count after pooling).
  - Verification: plan-conformance, PRINCIPLES, and integration
    finders CLEAN (pooling arithmetic, byte-compatibility, gate
    contract, scheduler/loader/query seams, unowned files all
    verified); comments 1 + diagnostics 1 → 2 refute-by-default
    confirmers → 1 CONFIRMED and fixed (HIGH — `embed_text`'s doc
    claimed to be "the single boundary" applying
    truncation/pooling/normalization, false beside the batch path;
    reworded to name the two must-stay-in-step paths), 1 REFUTED
    (batched events omit the singular events' `text_chars`: the CP
    plan's logging bullet is the governing spec for the new ADDITIVE
    event and names `text_count` + total `token_count` only; the
    non-narrowing rule targets replacements of authoritative records,
    not additive siblings).
  - Process notes: CPb's in-agent cargo battery ran mid-CPa-edit and
    saw transient dense.rs compile errors (parallel-dispatch race;
    resolved when CPa landed — final main-loop battery green); the
    status entry was main-loop-drafted, a disclosed deviation from
    the §1.4 status-drafting-as-subagent-work item.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`. The batch path and the thinking
    opt-out are compile-verified only — first live execution is CPc
    (if genuine BF16 drift exceeds 0.999 on this hardware, the smoke
    false-fails at startup with the cosine in the log; a loosening
    decision, never silent bad data).
  - Next: the CPc benchmark re-ingestion run (named `index/fabric/`
    deletion approved with the CP plan; release rebuild; USER
    re-allows the new binary in Little Snitch; `--setup-storage`;
    full cycle over the same 14-file corpus; before/after metric
    comparison — doubles as the first live annotation run), then
    discussion B + the ColBERT batching decision with CPc's numbers;
    C10f runs R4–R9 resume after CPc.
- 2026-07-18 (CPc attempt session): CPc ABORTED mid-first-cycle
  (user-ruled kill) after the CPa batch smoke exposed a Metal
  correctness defect and live metrics showed dense batching is a net
  REGRESSION on this hardware. Session outcomes: the candle Metal
  stride defect diagnosed and permanently fixed; CPa REVERTED
  (user-approved); F16 compute dtype evaluated test-first and
  REJECTED by validation (user-ruled); new package CPd (ColBERT
  batched document embedding) benchmark-gated IN and APPROVED for a
  new session (§3). All work was main-loop (diagnostics, revert,
  small approved edits) — a disclosed §1.4 deviation, including this
  status entry (CPa/CPb-session precedent).
  - CPc run record: `index/fabric/` deleted per the approved named
    deletion; release rebuild; `--setup-storage` verified (WAL,
    `user_version=1`, 19 tables + FTS5 shadow set); FIRST start
    FAILED at the new `dense_smoke_batch` gate — batched row 1 vs
    single cosine −0.0205 (garbage, not drift). The smoke did
    exactly its designed job: a startup-seconds failure instead of a
    corrupted corpus discovered hours later.
  - Diagnosis (via the approved scratch bin
    `src/bin/dense-batch-diagnostic.rs`): model-level probes — B=1
    through the batch path exact; EVERY B≥2 batch garbage on rows
    ≥1 (including three identical unpadded rows → NaN), row 0 always
    correct, results nondeterministic across identical runs.
    Op-level Metal-vs-CPU probes — every individual batched op
    exact; the minimal reproducer is `Tensor::cat` over narrowed
    kv-head views (`qwen3.rs::repeat_kv_heads`): candle 0.10.2
    returns a stride-PERMUTED view (shape [3,16,13,128], stride
    [1664,4992,128,1] — buffer ordered [heads,batch,seq,dim]); the
    CPU matmul REJECTS that layout (`MatMulUnexpectedStriding`
    "non-contiguous rhs") while Metal silently accepts it and
    miscomputes every batch row past the first (rows 1/2
    max-abs-diff 14.4/12.7 vs exact with `.contiguous()`). At B=1
    the layout is degenerate-equivalent to contiguous — why the
    singular path and ALL commissioned data were always correct.
  - FIX LANDED (user-approved; stays permanently): `repeat_kv_heads`
    materializes the cat result via `.contiguous()` with the
    invariant comment; value-neutral under any future candle (a
    no-op if cats become contiguous). Verified end-to-end: all
    model-level probes then exact (cos_self 1.000000 on every row of
    every batch shape, deterministic across runs) and a rebuilt
    service passed all startup smokes to `ready=true`. Upstream
    status UNVERIFIED (no web access used; unknown whether newer
    candle fixes it). `colbert.rs`/`reranker.rs` share the
    narrow+cat pattern but are batch-1 by construction — benign,
    unchanged.
  - Regression verdict (live cycle, 17 batches, vs the legacy-era
    singular logs): batched 16-text calls ran 35–50 s ≈ 8.58 ms per
    TRUE token vs baseline 5.11 (1,486 ms avg at 291 avg tokens);
    per PADDED token 4.93 vs 5.11 — only ~4% raw kernel gain (the
    8B forward already saturates this GPU at batch 1) while
    right-padding inflated useful work ~1.7×. Length-sorting's
    ceiling is that same ~4%: NO batching win exists for the 8B
    dense model on this hardware. User ruled: kill the run
    (graceful shutdown checks the signal per CYCLE, so
    `POST /shutdown` would have completed the remaining ~3 h first;
    SIGTERM kill user-approved, PID 96460).
  - CPa REVERTED (user-approved): `build_all_chunks` back to the
    singular per-chunk loop (a rationale comment at the site records
    the measurements); `embed_passage_vectors`/
    `forward_passage_batch`/batch validators/`SMOKE_BATCH` + the
    batch smoke deleted; the load split restored to one
    `load_with_progress`. KEPT: the `.contiguous()` fix, and the
    diagnostic bin rewritten as a permanent harness (candle-upgrade
    reproducer probes + Metal throughput benchmarks, no model load
    by default; flag-gated real-model dtype validation).
  - F16 evaluated test-first (user-approved) and REJECTED
    (user-ruled): the Metal layer-shape benchmark measured F16
    17–25% faster than BF16 on the dominant matmuls (24.97 →
    20.03–21.29 ms/layer-linears; F32 27.84) — the ≥15% gate passed
    — but the real-model cross-dtype validation
    (`--validate-dense-dtypes`, sequential 16 GB loads) produced
    NON-FINITE activations under F16 at the first passage smoke:
    Qwen-family outlier activations overflow F16's exponent range
    (e.g. RmsNorm's square). BF16 retained. The F32-norm salvage
    variant was excluded (a permanent per-document availability
    cliff — deterministic dense-build failure on overflow-prone
    documents — bought for ~20% of one stage). Verdict recorded at
    `DENSE_COMPUTE_DTYPE` (dense.rs). Kept scaffolding:
    `Qwen3Model::load_with_progress` compute-dtype parameter,
    `DenseEmbeddingRuntime::load_with_dtype_for_validation`,
    `InferenceRuntime::initialize_dense_for_dtype_validation`, and
    the bin's part 4 — the re-test for any future model/dtype
    change.
  - CPd gated IN (benchmark 2.43× ≥ the 2× gate) and APPROVED as
    scoped in §3: ColBERT batched DOCUMENT embedding. Measured:
    single per-doc/per-head mode 40.4 ms of layer-work for 16 docs
    (the synthetic composite validates against reality — it projects
    56 ms/doc vs the measured 53 ms/call); the FLATTENED batched
    formulation (rank-2 linears over B·S tokens, rank-3 attention
    over B·heads, `.contiguous()` discipline) 16.6 ms = 2.43×; the
    naive rank-4/`broadcast_matmul` formulation measured 0.68× —
    SLOWER than today's code — and is excluded by measurement, not
    preference. Padding under BIDIRECTIONAL attention requires true
    key masking composed with the alternating global/local
    sliding-window masks — the named risk; a batch-consistency
    startup smoke (dense-smoke precedent) is mandatory scope.
  - Also recorded: an HTTP dense-embedding backend (reranker-pattern
    Local/Http split) named as the largest structural lever for
    cold-ingest cost, not scoped; parse/embed overlap (discussion B)
    still deferred pending CPc numbers; the smaller dense model
    remains excluded by the 2026-07-17 ruling.
  - Machine state: service STOPPED (killed mid-cycle);
    `index/fabric/` holds a PARTIAL aborted cycle (early sources
    activated, no annotations) — it must be deleted before the CPc
    re-run and that deletion needs FRESH approval (the prior named
    approval was consumed by this attempt).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`.
  - Next: dispatch CPd per §3 in a NEW session (§1.4 ceremony:
    prompt drafter → adversarial prompt reviewer → implementation
    agent → cargo battery → five-dimension adversarially-confirmed
    verification → fix pass; ~150–200k; ~85% confidence with the
    design-note-before-edit requirement), then the CPc re-ingest
    (re-approve the `index/fabric/` deletion; benchmarks CPd + the
    reverted-dense baseline + the first live annotation run), then
    discussion B and C10f runs R4–R9.
- 2026-07-18 (CPd implementation session): CPd COMPLETE —
  ColBERT batched DOCUMENT embedding implemented and verified per
  the approved §3 scope as prompt drafter → adversarial prompt
  reviewer (NOT-READY; 4 amendments applied at dispatch — 2
  MUST-FIX: the batched entry-point cross-file contract was
  unnamed, same defect class as the prior missing-dispatcher, and
  the parallel-additive vs rank-generalize decision across the
  six-method forward chain was unspecified; 2 SHOULD-FIX: batch
  model_call field semantics and the smoke's runtime-loaded
  longest-text sizing with a documented fallback) → single Opus
  implementation agent (two mandatory pre-edit design notes: mask
  composition; forward-path shape plan) → main-loop cargo battery
  → five-dimension adversarially-confirmed verification (finder
  checklists into confirmer prompts per the C8 lesson) → fix pass
  → a focused refute-by-default confirmation of the SPEC-1 fix →
  final battery.
  - colbert.rs (owned): additive public entry `embed_documents(&[
    (&str, &str)]) -> Result<Vec<ColbertDocumentEmbedding>,
    ApiError>` (input order = return order, per-unit true-length
    matrices); PARALLEL-ADDITIVE batched methods with the singular
    paths untouched — `ColbertInputPath::forward_batched`,
    `ColbertAttentionPrimitive::forward_batched`,
    `attention_output_by_head_batched`, `flatten_heads`,
    `add_key_padding_mask`, `split_attention_projection_batched`,
    `ColbertLayerPrimitive::forward_batched`,
    `ColbertEncoderRuntime::encode_batched`; `ColbertProjection::
    project` reused unchanged (already rank-2 over rows);
    `apply_local_attention_mask` refactored to delegate to a shared
    `local_attention_mask` builder (singular caller
    behavior-preserved); new `key_padding_mask` and (fix pass)
    `row_validity_mask`; free fns `batched_document_matrices`,
    `build_batch_consistency_smoke_texts`,
    `run_colbert_batch_consistency_smoke`,
    `assert_token_matrices_agree`. Flattened formulation only:
    rank-2 linears over `(B·S, 768)`, rank-3/per-slab attention
    over `(B·heads, S, 64)`, `.contiguous()` after every
    stride-permuting reshape with candle-Metal defect citations; no
    rank-4/`broadcast_matmul` (grep-verified). Recon sharpening:
    GLOBAL layers previously carried NO mask, so the key-padding
    mask is INTRODUCED there and composed additively with the
    positional window mask on Local layers, independent of the
    `local_attention >= seq_len` early return. Batched model_call
    logging: ONE started/completed|failed pair per window with
    `text_count` = batch size and summed `document_chars` +
    `token_count`, no single `unit_id`.
  - multivector.rs (owned): `COLBERT_DOCUMENT_BATCH_WINDOW = 16`
    code constant (engineering-fact comment; gate evidence 2.43× at
    16 docs × seq 128 F32); `build_rows` length-sorts internally
    (byte-length proxy; `batched_document_matrices` re-sorts by true
    token length) and packs 16-unit windows through
    `embed_documents`; per-unit validate→encode→INSERT unchanged
    (persisted blob byte-compatible; per-unit unit-id attribution
    preserved; a batch-forward error still fails the whole build);
    `SELECT_PARSE_UNITS_SQL` untouched; gate discipline untouched
    (caller-held permit; the runtime acquires nothing).
  - Mandatory startup smoke `colbert_smoke_batch_consistency` wired
    before `colbert_smoke_ready`: 3 fixed distinct-length texts
    (longest sized past the runtime-loaded `local_attention`;
    `crosses_local_boundary` surfaced in the readiness log when
    infeasible), batched vs singular per-token-vector cosine gate
    `SMOKE_BATCH_COSINE_FLOOR = 0.9999`, failure =
    `ApiError::InferenceInit`.
  - Verification: five dimensions → spec 1, principles 1, comments
    3, diagnostics 2, integration CLEAN = 7 findings, ALL 7
    CONFIRMED by refute-by-default confirmers (0 refuted, 0
    unverified), all fixed:
    - SPEC-1 (HIGH, behavioral): the NaN-safety argument held within
      one layer but FAILED across layers — a padded query row fully
      masked on a Local layer (`padded_len > local_attention` leaves
      rows outside every real key's window) goes NaN at softmax;
      `encode_batched` looped layers with no sanitization, so the
      NaN row fed the next layer's qkv, its K/V rows went NaN,
      q·kᵀ made that key COLUMN NaN for every REAL row of the same
      document, NaN + −inf defeated the additive masks, and the
      key-axis softmax sum spread NaN across real rows. The
      mandatory smoke deterministically constructs the trigger
      (would have failed startup at commissioning); builder windows
      mixing a short unit with a `>local_attention` unit fail the
      same way. FIX: `row_validity_mask` (`(B·S, 1)`, packed-row
      order, 1.0 real / 0.0 padded) broadcast_mul onto hidden states
      after EVERY layer inside `encode_batched`'s loop (final_norm
      input sanitized); the false "no cross-token reduction
      downstream" comment rewritten to state the cross-layer hazard
      and per-layer confinement. Adversarially confirmed adequate on
      8 criteria — mask built from the same truncated token lengths
      in the same packed row order (predicate mirrors the key mask,
      no off-by-one); `qkv_proj` is `linear_no_bias`, so zeroed
      padded rows produce EXACTLY-ZERO K/V whose score columns stay
      −inf-masked; real rows bit-exact (multiply by literal 1.0f32;
      attention consumes the sanitized layer INPUT's K/V); zero rows
      finite through the norms (eps before sqrt); sanitization is
      batched-only (singular `encode` untouched); smoke geometry
      passes in principle.
    - PRINCIPLES-1 (LOW): inert `#[allow(clippy::
      too_many_arguments)]` on the 7-parameter
      `batched_document_matrices` deleted (the threshold fires only
      above 7; the unannotated 7-param sibling proved it).
    - COMMENTS-1 (MED): five stale multivector.rs comment sites
      still naming singular `embed_document` as the builder's
      producer/gated call (module doc, producer line, gate
      paragraph, `build_multivectors` doc, the "embeds PER UNIT" SQL
      comment) corrected to the batched `embed_documents` flow.
    - COMMENTS-2 (LOW): colbert.rs consumer claims corrected — the
      singular `embed_document`'s only caller is the
      colbert-diagnostic bin (plus its byte-compatibility-reference
      role); the query path uses `encode_projected_query` and the
      smoke's reference side calls the singular primitives directly;
      the false "consumed at C6e/C7c" note amended (C6e's producer
      is now `embed_documents`).
    - COMMENTS-3 (MED): candle-Metal strided-view defect citations
      added at three uncited `.contiguous()` sites
      (`add_key_padding_mask`'s stride-0 `broadcast_as`,
      `batched_document_matrices`' narrow-based true-length
      extraction, `flatten_heads`).
    - DIAGNOSTICS-1 (MED): batched tokenization failures were
      unattributable (whole build failed with no identifier narrower
      than the parse — a regression vs the singular path's
      `unit_id`); the tokenize loop now wraps errors with the
      window-local input index, mirroring the extraction-error
      convention.
    - DIAGNOSTICS-2 (LOW): smoke reference-side forward/encode
      failures now carry a per-text label
      ("batch-consistency smoke text {index}"); the label-less
      `projection.project` call is `map_err`-wrapped with the text
      index.
  - Process notes: the five finders and seven confirmers ran on the
    session-default model (Fable), NOT Opus — a disclosed deviation
    from the §1.4 all-subagents-Opus rule (workflow-harness model
    inheritance); the standalone agents (prompt drafter, adversarial
    reviewer, implementer, fix agent, SPEC-1 fix confirmer) were all
    Opus. This status entry was drafted by a subagent per §1.4,
    closing the prior two sessions' disclosed main-loop-drafting
    deviation.
  - All checks green, zero warnings throughout and at final state:
    `cargo fmt`, `cargo check`, `cargo check --features metal`,
    `cargo clippy`, `cargo clippy --features metal`.
  - Runtime status: NOTHING runtime-verified — the batched path and
    the smoke are compile-verified only; first live execution is the
    CPc re-ingest, where the smoke gates startup (its geometry is
    exactly the SPEC-1 trigger, so a wrong fix is a startup-seconds
    failure, never corrupt data). The `target/release` binaries now
    PREDATE CPd (stale; CPc's procedure already includes the release
    rebuild).
  - Residual risks (banked for CPc/C10f): local-boundary smoke
    coverage is config-dependent (`crosses_local_boundary=false`
    surfaces when the loaded `local_attention >= document_max_tokens`);
    the builder pre-sort uses byte length as a proxy (inner re-sort
    by true token length; correctness unaffected, packing efficiency
    only); Metal stride correctness stays runtime-guarded by the
    smoke; the per-layer broadcast_mul cost is negligible vs the
    matmuls.
  - Next: the CPc benchmark re-ingest (FRESH user approval required
    for the `index/fabric/` deletion — the plane still holds the
    partial aborted 2026-07-18 cycle; release rebuild;
    `--setup-storage`; full cycle over the same 14-file corpus;
    doubles as the first live annotation run / CPb execution), then
    discussion B (parse/embed overlap) with CPc's numbers, then C10f
    runs R4–R9.
- 2026-07-18 (CPc attempt 2: NaN-sanitize fix + CPb verification):
  CPc attempt 2 ran setup through a PARTIAL cycle (user-ruled stop
  once CPb was verified, "not longer than necessary"); the CPd
  batch-consistency smoke caught a REAL formulation defect at first
  startup, diagnosed and fixed in-session; CPb is VERIFIED PASS.
  All work was main-loop (approved small edits + live-run
  operations) — a disclosed §1.4 deviation, including this status
  entry (CPc-attempt-session precedent).
  - Run procedure: `index/fabric/` deleted (the fresh approval
    consumed); release rebuild; `--setup-storage` verified (WAL,
    `user_version=1`, 19 tables + FTS5 shadow set); FIRST startup
    FAILED at `colbert_smoke_batch_consistency`: "text 0 token 0
    cosine NaN below floor 0.9999" — the SHORT smoke text (packed
    row 2, the most-padded row) had REAL tokens NaN. The smoke again
    did exactly its designed job, before any corpus data was touched.
  - Diagnosis (approved harness extension: new flag-gated Part 1b
    `--probe-batched-sanitize` in dense-batch-diagnostic.rs — NaN*0
    semantics probe, finite stride-0 broadcast_mul probe, and a
    two-layer propagation chain under both sanitizer mechanisms;
    kept permanent): H3 CONFIRMED — IEEE 754 `NaN * 0.0 = NaN`, so
    the SPEC-1 `broadcast_mul` sanitize can NEVER clear a
    fully-masked-softmax NaN row; the surviving NaN enters the next
    layer's bias-free K projection and the key-axis softmax spreads
    it across every real row of the same document (chain probe: real
    rows 8/8 non-finite under broadcast_mul, 0/0/0 under where_cond,
    IDENTICALLY on Metal and CPU). H1 REFUTED — the sanitize op is
    bit-exact on finite data (max_abs_diff 0.000000). A formulation
    defect on every backend, not a Metal defect; the CPd
    verification's "zeroed rows → exactly-zero K/V" criterion was
    sound only GIVEN zeroed rows — its premise was false under NaN.
  - FIX LANDED (approved): `encode_batched`'s per-layer sanitize is
    now a `where_cond` SELECT — the (B·S, 1) validity mask broadcast
    to (B·S, hidden), materialized `.contiguous()` (stride-0
    `broadcast_as`, the candle-Metal defect class), cast U8, built
    ONCE before the loop with a zeros template, selecting after
    EVERY layer; the false sanitize comments rewritten to record the
    NaN*0 hazard (`encode_batched` + `row_validity_mask`).
    Smoke-validated live at the next startup
    (crosses_local_boundary=true, local_attention=128, 357 ms) →
    `ready=true`.
  - CPc partial-cycle results (log era 2026-07-19T00:02–01:18Z,
    ~76 min; stopped by user ruling; SIGTERM user-authorized —
    graceful shutdown still waits out the full cycle):
    - CPd INTERIM verdict (112 windows / 1,777 docs vs the R3
      fabric-era singular logs, length-bucketed): batched is 4.4×
      faster at ~16 tokens (8.3 vs 36.6 ms/doc), 2.3× at ~46, 1.3×
      at ~90; SLOWER above ~130 tokens (102.1 vs 89.0 ms/doc at
      ~179) and 1.8× slower at the 512 cap (338.1 vs 188.0 ms/doc).
      Crossover ≈130 tokens. R3-mix-weighted projection: ~25% stage
      reduction (30.2 → ~22 min), far short of the 2.43× gate
      benchmark (the synthetic measured seq-128 layer work; the
      real long-unit regime pays quadratic attention over the padded
      (B·heads, S, S) score tensor plus mask/scrub overheads the
      singular per-head path never builds). PROPOSED for ruling
      (not implemented): a length-threshold HYBRID — batch only
      windows of short units (threshold ≈128 tokens, to be set from
      full-corpus data), singular path above — projects ~17 min on
      the same arithmetic; it only routes between two
      already-validated paths, so adoption needs no re-benchmark run
      for correctness.
    - CPb VERIFIED PASS (the first live annotation run): after the
      network unblock, annotator calls completed in 2.5–7 s typical
      (avg 24.6 s over the first 9, pulled up by one 103.4 s call);
      43 entity annotations reached `fresh`, 6 memo rows persisted,
      and orphan adoption was observed working live. The 103.4 s
      call demonstrates the banked no-output-token-bound residual in
      production (120 s timeout headroom nearly exhausted); the
      llm_client robustness gaps stay banked. Relation/summary
      producers, per-source annotation completeness, and the
      summary/graph projections remain UNTESTED (starved behind
      projection builds this run; C10f scope).
    - FINDING (handoff correction): the Little Snitch re-flag DOES
      recur per rebuild — the 2026-07-18 "no per-rebuild action
      needed" line was wrong. Evidence: first annotator calls failed
      status-none at exactly 120 s while curl reached the endpoint
      in 7 ms. User re-whitelisted 2026-07-18 with additional scope
      expected to persist across rebuilds — unverifiable until the
      next rebuild.
    - FINDING (new, runtime-only): annotation-worker WRITE-LOCK
      STARVATION. The parse chain's single projection-build
      transaction holds the SQLite writer lock for a source's entire
      dense embed (tens of minutes+), so the worker's `build_open`
      fails "database is locked" (5 s busy_timeout) every 30 s cycle
      (ERROR-pair log noise), and a successful producer call whose
      completion tx then hits the lock LOSES its output (row stays
      `building` → orphan-adopted → producer re-invoked). Correct
      and self-healing but wasteful; NEEDS A RULING (per-plane
      projection commits vs quiet worker deferral while the
      scheduler holds the lock vs accept-as-is).
  - Machine state: service STOPPED (user-authorized SIGTERM,
    2026-07-19T01:18Z). `index/fabric/` holds partial aborted
    cycle #2 (4 sources active, partial entity annotations) —
    delete + `--setup-storage` again before the next run; the
    deletion needs FRESH approval. `target/release` binaries are
    CURRENT (they include the where_cond fix; no rebuild pending).
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`.
  - Next: rule on the CPd length-threshold hybrid and the
    lock-starvation finding, then the CPc full re-ingest (fresh
    `index/fabric/` deletion approval; benchmarks the ruled ColBERT
    configuration against the reverted-dense baseline and completes
    the annotation-chain first run), then discussion B (parse/embed
    overlap), then C10f runs R4–R9.

## Retired planning sections (relocated verbatim 2026-07-18, second stage)

Sections appear in their original plan-file order: §1.5, §1.6, §2, the §3
spine, clusters C1–C9 (with the C7/C9 fact bases), the C10 intro +
C10s–C10e + C10 fact base, CPa/CPb/CPd + CP fact base, the §37
acceptance-traceability table, and the §4 full decision texts D1–D9. Grep
the section heading (e.g. `### C7 — Retrieval fabric`, `D6 — Docling
typed-output mode`) or package name to locate a ruling or fact base.

### 1.5 Pinned contracts (rewritten 2026-07-10)

Most of the 2026-07-06 pinned-contract table protected legacy consumers
and dissolves when CR retires them. Still frozen — cross-module shapes
parallel agents must not change unilaterally:

| Contract | Consumers today | Cutover |
| --- | --- | --- |
| `types::OperationEvent`/`OperationBenchmarks`/`BenchmarkStage` + emitter semantics (sequence from 1; nonterminal `try_send` capacity 16, drop-on-full, stop-on-closed; terminal `blocking_send`) | `http.rs` transport machinery, CLI | C10a |
| `ApiError` variant set and its three exhaustive matches | all src files | per-cluster batched edit (process rule, permanent) |
| Inference inbound API (`embed_passage_vector`/`embed_query_vector`, `embed_document`/`score_persisted_candidates`, `score_candidates`, `uses_local_model_gate`) and the caller-side model-call-gate discipline in `state.rs` | fabric consumers from C6c/C7 | retained; not cut over |

Dissolved at CR (their legacy consumers are deleted):
`docling::DoclingConversionResult`; `units::RetrievalUnit`/
`units::build_document_id` (`units.rs` stays as dead source material for
the C6b harvest); the implicit ColBERT flattened-blob layout (the legacy
`colbert_document_vectors` plane is discarded; C6 defines the fabric
projection codec per D4); the CLI-side DTO pairing rule (the CLI is
self-contained, keeps compiling untouched, and is reworked at C10c). The
string-keyed benchmark contract was already replaced at C1b.

### 1.6 Recon findings that bind the rebuild (2026-07-06)

Facts about the current code the new design must consciously address:

- `storage.rs` connection policy: fresh read-write `Connection::open` per
  operation with only `foreign_keys=ON` — no WAL, no `busy_timeout`, no
  read-only open flags, no statement timeouts. The new hot plane defines an
  explicit connection/deadline policy (D1 scope); `PRINCIPLES.md` requires
  bounded queries and read-only paths, which the current code does not
  fully satisfy.
- `config.rs` has no `#[serde(deny_unknown_fields)]`; unknown keys and the
  entire stray `[client]` table are silently ignored, contrary to
  `PRINCIPLES.md` config policy. The fabric config (C2f) adopts
  `deny_unknown_fields`; ownership of `[client]` is resolved under D3.
- `service_root()` bakes compile-time `CARGO_MANIFEST_DIR` into the binary
  for resolving `logging.file_path`, `admin.token_file_path`, and reranker
  key paths (the CLI does the same for token/history/config). Breaks for
  relocated binaries; D3 decides the replacement resolution rule.
- The ingest publish invariant: `ingest_document` holds the dense-cache
  Mutex across the SQLite commit so the durable active-row write and the
  in-memory swap cannot interleave with another publish. The per-source
  cutover barrier (C5) must preserve this atomicity under its new shape.
- Sync/async boundary: operation pipelines run on `std::thread` and touch
  tokio only through `OperationStreamSender` (`http.rs:24-27` states the
  invariant). All new pipelines keep this shape.
- No OS signal handling exists (shutdown only via the admin operation), and
  no operation cancellation exists (the control endpoint is a stub). The
  autonomous lifecycle (scheduler, parse workers) needs a real
  stop/cancellation design at C3c/C4.
- The startup parent/child protocol is stringly typed
  (`data-store startup fatal=`, `http=bind_failed` substring matching) and
  every fatal path must call `admin_token_file.cleanup_if_current()`; new
  startup stages must follow both rules.
- `docling.rs` is markdown-only today: `--to md` is hard-coded and no
  JSON/DocTags path exists anywhere in the file (D6 investigation target).
- `docling_activity.rs` is freestanding but macOS-only, and its `sample`
  path is dead code at the current call site; carry or drop is decided at
  C4c.

Restructure note (2026-07-10): under end-state-only development these
facts stop being preservation constraints and remain design context. The
`storage.rs` connection-policy and config gaps are already superseded by
the fabric substrate (D1, C2f done); the ingest publish invariant becomes
a pattern C5b reimplements after CRb deletes `ingest_document`, not code
to preserve; the sync/async boundary rule and the stringly-typed startup
protocol remain binding (transport shell and `main.rs` are retained
substrate); the stop/cancellation design landed for the scheduler at C3c,
with parse-worker cancellation owned by C5c dispatch.

## 2. Module Disposition (current code → end state)

Restructure note (2026-07-10): legacy retirements previously spread across
the programme (or scheduled at C10e) now happen at cluster CR; rows below
were updated where a module's timing or survival changed.

| Current module | Disposition |
| --- | --- |
| `src/inference/` | Retained as-is; projection/ranking producers. Files partition cleanly (dense+qwen3, colbert, reranker+backend, artifacts+device+tensor_ops); `mod.rs` is the only contended file. ColBERT retained per the D4 resolution (C6e/C7c, 2026-07-11). |
| `src/docling.rs` | Retained as the engine inside the PDF parser worker behind the §12 boundary (C4c). Args/timeout/progress machinery reusable verbatim; output mode subject to D6. |
| `src/docling_activity.rs` | Freestanding, macOS-only; keep-or-drop decided at C4c. |
| `src/units.rs` | Loses its consumers (legacy ingest, `storage.rs`) at CR; kept compiling as dead source material for the C6b chunk-builder harvest, then deleted at C6b (named deletion). |
| `src/source.rs` | Retained fabric substrate: `ResolvedSource` (source.rs:9) and the path-safety core are consumed by `docling.rs` and the C4 PDF worker. The planned C3a lift was not needed (see the C3 status deviation); legacy-ingest-only policy surface is trimmed at CR package time if orphaned. |
| `src/storage.rs` | Deleted at CRb. Reusable primitives already extracted (C1b); the fabric schema-validator battery was regenerated at C2e; the shared data types C1b left here (still consumed by `src/primitives/`) relocate at CRb. |
| `src/http.rs` | Split (C1a, done). Legacy operation dispatch trimmed at CRa; the transport machinery (router skeleton, auth, NDJSON emitter, sequencing, limits) is retained dead-code substrate until C10a rebuilds the §34 surface. |
| `src/state.rs` | Retained: `ShutdownSignal`, `AdmissionGate`, `ExclusiveGate` (with its poison-recovery invariant), `constant_time_eq`. `ExclusiveGate` generalizes to a keyed per-source registry for cutover barriers (C5b). Admission knobs subject to D3. |
| `src/config.rs`, `config.example.toml` | Reworked per §35 (D3; C2f done); legacy-only keys removed at CRc as their readers die. Main-loop-owned. |
| `src/error.rs`, `src/logging.rs`, `src/types.rs` | Evolve in place; `error.rs` single-owner batched edits; `types.rs` legacy DTOs deleted at CRa, emitter/benchmark types retained until C10a. |
| `src/main.rs` | Retained (daemonization, startup reporter, token handoff — ~700 spec-agnostic lines); gained scheduler lifecycle (C3); trimmed of legacy runtime wiring at CRa/CRb; `mod` declarations main-loop-owned. |
| `src/bin/data-store.rs` | Reworked against the new API (C10c). Fully self-contained; ~600 lines of generic stream/transport/rendering machinery reusable; command table and DTOs replaced. |
| `src/bin/colbert-diagnostic.rs` | Retained; acts as a compile-time guard on `config.rs`/`error.rs`/`inference` dependencies. |
| `sql/schema.sql` | Replaced by `sql/fabric/` (C2e, done); legacy file deleted at CRb (named deletion). |

Dependency spine:

```text
C0 decisions ──► C2 contracts ──► C3 acquisition ──┐
      │               │                            ├─► CR legacy retirement ─► C5 activation ─► CA annotations ─► C6 projections ─► C7 retrieval ─► C8 assembly/evidence ─► C9 lifecycle forensics ─► C10 shell + commissioning
      │               └─────────► C4 parsing ──────┘
      └─► C1 seam cuts (no decision or contract dependency; may start first)
```

CR has no decision dependencies of its own; it is sequenced first among
the remaining clusters (2026-07-10 restructure) so every later cluster
works free of legacy-coexistence constraints.

Each package: goal — owned files — spec §§ — checks — mode.

### C1 — Seam cuts (behavior-preserving; enables parallel ownership)

- **C1a HTTP transport split.** Move `execute_*` operation pipelines out of
  `src/http.rs` into `src/operations/` (one file per operation, legacy
  forms), leaving the transport shell (router, `post_operation` setup,
  `OperationEmitter`, auth, limits, terminal emission) in `http.rs`.
  Owned: `src/http.rs`, `src/operations/*` (new). §: none (mechanical).
  Checks: cargo checks pass; no behavior change; emitter semantics
  untouched. Mode: agent-serial.
- **C1b Storage primitive extraction.** Extract into `src/primitives/`:
  f32 blob codecs + vector/norm/ColBERT validators, BM25 query builder +
  stopwords, RRF fusion + tie-break, `sha256_hex`, UTC-ms timestamp
  formatting. Replace the string-keyed `retrieval_substage_ms` benchmark
  contract with a typed latencies struct shared by `storage.rs` and
  `http.rs`. Owned: `src/primitives/*` (new), `src/storage.rs`,
  benchmark-assembly sites in `src/operations/`. §16 (hash groundwork).
  Checks: cargo checks; search/ingest behavior unchanged; no remaining
  string-keyed latency lookups. Mode: agent-serial (touches storage +
  operations together by design).
- **C1c Shared util extraction.** Move `panic_payload_message` to
  `src/util.rs` (currently in `docling.rs`, used by `http.rs`, duplicated
  in `inference/device.rs`). `ApiError` variants for the programme's new
  failure domains (acquisition, parse, activation, projection, assembly,
  query-record, snapshot) are NOT batch-added here (decided 2026-07-07:
  unused variants would force `dead_code` allows); each cluster adds its
  variants in that cluster's single-owner batched `error.rs` edit when the
  first constructor lands, per the §1.5 pinned-contract rule. This list
  remains the naming pre-agreement. Owned: `src/util.rs` (new),
  `src/docling.rs`, `src/inference/device.rs`, plus import updates in
  `src/http.rs` and `src/operations/ingest.rs`. Checks: cargo checks
  including `--features metal`; `colbert-diagnostic` still compiles.
  Mode: agent-serial.

### C2 — Contracts and substrate (after D1; D3 for C2f)

- **C2a Canonical serialization + hashing** (`src/canonical.rs`): UTF-8,
  NFC normalization, sorted-key JSON, canonical numbers, RFC3339 UTC `Z`,
  omitted-vs-null, SHA-256; JSONL record-set hashing over LF-joined
  canonical lines. §16.1–16.3. Checks: cargo checks; every content-derived
  hash routed through this module (grep: no ad-hoc `Sha256` use outside it
  except legacy `storage.rs` until cutover). Mode: agent-parallel.
- **C2b Canonical ID scheme** (`src/ids.rs`): typed-prefix time-ordered IDs
  (`src_`, `parse_`, `acq_`, `qer_`, `snap_`); parse-scoped deterministic
  ContentUnit/UnitRelationship IDs (parseId + type discriminator +
  sequence). §16.4. Checks: cargo checks; IDs opaque, core-assigned only.
  Mode: agent-parallel.
- **C2c Artifact store** (`src/artifact_store.rs`): write-once hash-keyed
  blobs, JSON/JSONL bundle writer/reader, manifest hashing; layout per D1.
  §16.3, §30.1, §32. Depends on C2a. Checks: cargo checks; write-once
  enforced (second write of same hash is a no-op, conflicting content is
  an error). Mode: agent-parallel.
- **C2d Fabric model types** (`src/model/`): SourceObject/SourceLocation,
  AcquisitionRecord, ConnectorCapabilityProfile, ParseRun +
  warnings/metrics, ContentUnit + all MVP typed bodies, Locators,
  UnitRelationship, Provenance (memoization fields inert),
  ParserCapabilityProfile, ConformanceReport, DeletionEvidence,
  SystemEvent. §7, §9.2–9.3, §10–§12, §15, §17–§20, §33. Depends on
  C2a/C2b conventions. Checks: cargo checks; field names/optionality match
  spec schemas; every public item doc-commented. Mode: agent-serial (one
  coherent type owner).
- **C2e Hot-plane schema + setup** (`sql/fabric/*.sql`, setup-storage
  successor wiring): tables for source objects/locations, acquisition
  records, sync queue, parse runs, content units, relationships,
  projection metadata, QER metadata, event log; schema-version validation
  regenerated for the new contract. §32. Depends on C2d, D1. Checks: cargo
  checks; runtime never creates schema; validator matches DDL. Mode:
  agent-serial; `main.rs` wiring main-loop.
- **C2f Fabric config + example** (`src/config.rs`,
  `config.example.toml`): new sections per D3 resolution;
  `deny_unknown_fields`; service-root resolution rule per D3. §35. Mode:
  main-loop (each config diff individually approved).
- **C2g Event log writer** (`src/events.rs`): durable SystemEvent append
  over the C2e table; vocabulary grows additively per cluster. §33.
  Checks: cargo checks; events written inside the owning operation's
  boundary logging discipline. Mode: agent-parallel.

### C3 — Acquisition (after C2; D3 resolved; runs parallel with C4)

- **C3a Filesystem connector** (`src/connectors/filesystem.rs` + lifted
  path-safety core): full-scan enumeration over the corpus root, staged
  acquisition bundles (bytes + manifest: byte hash, native identifiers,
  mtime, timestamps, connector identity + config hash, governance domain);
  complete-enumeration capability ⇒ qualifies for §11.1 deletion evidence.
  §9.1, §9.3. Checks: cargo checks; connector writes staging only, never
  canonical stores. Mode: agent-parallel.
- **C3b Acquisition importer** (`src/acquisition.rs`): bundle validation,
  `sourceHash` computation, content dedup, lossless location maintenance
  (rename = location transition), AcquisitionRecords for success and
  failure, DeletionEvidence recording (`absent_from_complete_enumeration`,
  `source_reported_gone`), events. §9.2, §10, §11.1. Checks: cargo checks;
  one SourceObject per hash invariant; failed acquisition leaves a durable
  record; boundary logs per DIAGNOSTICS standard. Mode: agent-parallel.
- **C3c Sync queue + adaptive scheduler** (`src/scheduler.rs`,
  `src/state.rs`, `src/main.rs` lifecycle): durable queue with
  latest-state coalescing (at most one pending change per source), visible
  pending/in-flight/failed/lag state; knob-free cadence from observed
  churn, backpressure, and source pushback, every adaptation logged with
  cause; boundary timestamps (observed/acquired/parsed/activated) captured
  for health-surfaced freshness now and per-query freshness records when
  the deferred QER audit tier lands (2026-07-11 rescope); scheduler
  thread joins the
  `ShutdownSignal` latch (same pattern as `wait_for_shutdown_signal`),
  with an explicit stop design. §9.4–9.6. Checks: cargo checks; no cadence
  or backlog config knobs exist; health exposes cadence/backlog/drain/
  coalescing. Mode: agent-serial (AppState coupling), `main.rs` wiring
  main-loop.

### C4 — Parsing (after C2; D6 resolved; runs parallel with C3)

- **C4a Parser worker contract** (`src/parse/bundle.rs`): staged output
  bundle layout (manifest with file hashes, typed candidate JSONL,
  warnings, metrics, bounded logs), untrusted until imported. §12.1–12.2.
  Checks: cargo checks; bundle manifest hash-covers every file. Mode:
  agent-parallel.
- **C4b Core importer + conformance** (`src/parse/importer.rs`,
  `src/parse/conformance.rs`): validation (typed-body mapping §15.2,
  locator/relationship/provenance rules, resource limits,
  capability-profile conformance), canonical ID assignment, canonical
  parse bundle written to the artifact store, ConformanceReport
  measurement (type counts, locatorCoverage, captionPairingRate,
  tableDecompositionRate, extensible dimensions). §12.3–12.5, §13.1.
  Checks: cargo checks; every §13.1 invariant enforced with a distinct
  error; importer is the only writer of canonical parse state. Mode:
  agent-parallel.
- **C4c PDF parser worker** (`src/parse/pdf_worker.rs` wrapping
  `docling.rs`): typed candidate units per D6 output mode; progress
  channel decoupled so a dead consumer does not kill a parse; serializable
  failure diagnostics; workspace = staged bundle root; decide
  `docling_activity.rs` carry-or-drop. §12.1–12.2. Checks: cargo checks;
  worker cannot write canonical stores; timeout/kill behavior preserved.
  Mode: agent-parallel.
- **C4d Plain-text parser worker** (`src/parse/text_worker.rs`). §36.
  Checks: cargo checks. Mode: agent-parallel.

### CR — Legacy retirement (added 2026-07-10; immediately before C5)

Rationale: with end-state-only development ruled (§1.2), maintaining the
legacy service through the programme is pure cost. Retiring it now
dissolves the legacy rows of the §1.5 pinned-contract table, the
dual-plane `--setup-storage`, and the don't-break-legacy constraint on
every remaining cluster. Every deletion below is an explicit, named
approval item in the CR cluster plan. The CLI binary is untouched (fully
self-contained; it keeps compiling and is reworked at C10c). Retained
substrate stays compiling under the existing dead-code-allow convention
until its fabric consumer lands.

- **CRa Legacy operation pipelines + transport trim.** Delete
  `src/operations/{ingest,search,sources,versions,rollback,shutdown}.rs`
  (legacy forms) and trim the `http.rs` dispatch to the retained
  transport machinery (router skeleton, `OperationEmitter`, auth, body
  limits — C10a's substrate); delete `types.rs` DTOs whose only consumers
  are the deleted pipelines (the `OperationEvent`/benchmark/emitter types
  are retained, frozen until C10a). Owned: `src/operations/*` (deletions),
  `src/http.rs`, `src/types.rs`; `src/main.rs` route wiring main-loop.
  Checks: cargo battery; `colbert-diagnostic` still compiles. Mode:
  agent-serial; `main.rs` edits main-loop.
- **CRb Legacy storage plane.** Delete `src/storage.rs` (StorageRuntime,
  legacy dense cache, legacy schema-validator battery) and
  `sql/schema.sql`; `--setup-storage` builds the fabric plane only.
  Relocate the shared data types C1b left in `storage.rs` that
  `src/primitives/{validate,fusion,codec,bm25}.rs` still consume (into
  `primitives` or `model`, decided at package time). `units.rs` loses its
  consumers and stays as dead source material for the C6b harvest.
  Owned: `src/storage.rs`, `sql/schema.sql` (deletions),
  `src/primitives/*`, `src/units.rs` (allow only); `src/main.rs` setup/
  init wiring and `src/state.rs` storage slot main-loop. Checks: cargo
  battery; no remaining reference to the legacy plane. Mode: agent-serial;
  `main.rs`/`state.rs` edits main-loop.
- **CRc Config cleanup (main-loop).** Remove legacy-only keys whose
  readers die with CRa/CRb, per the D3 removal list: `default_top_k`,
  `max_top_k`, `rrf_k`, `candidate_overfetch_multiplier`,
  `colbert_candidate_pool_size`, `reranker_candidate_pool_size` (current
  values banked in the CR cluster plan for the C7a RetrievalProfile);
  `min_search_unit_chars`, `max_unit_tokens` (banked for the C6b chunker
  config); `max_in_flight_ingest` (superseded by the adaptive scheduler);
  `max_in_flight_search` (becomes a code constant per D3). `[client]` and
  all external-fact keys stay. Each config diff individually approved;
  `config.example.toml` and local `config.toml` updated together. Mode:
  main-loop.

### C5 — Activation lifecycle (after CR)

Ruling recorded 2026-07-10: activation gates on canonical state only.
§13.6's projection/index prerequisite is written as an explicit commented
seam in the activation readiness check and extended at C6 when projection
builders exist. Nothing can observe a projection-less active parse: no
fabric query path exists until C7, and the app is not operational during
the programme (§1.2). The §21.4 required-annotation-set policy reads the
same activation seam at CAd, with MVP policy content "nothing blocks
activation" (2026-07-11 rescope).

- **C5a Gating + disposition** (`src/activation.rs`): changed-content
  auto-activation; unchanged-content conformance dominance with held
  parses (`heldReason = conformance_regression`, one held candidate per
  source, newer supersedes); parse-failure disposition (serve last valid,
  durable failure records, no blind retry, coalesced pending); explicit
  accept/discard operations (internal functions now; HTTP exposure is
  C10a per D2). Dominance comparison over differing conformance dimension
  sets RULED 2026-07-11 (implemented; see Current Status): union
  comparison, absence-conservative — a dimension present in the active
  report but absent from the candidate compares as worse (hold); present
  in the candidate but absent from the active does not block; absent from
  both is equal. §13.2–13.5. Checks: cargo checks; no absolute quality
  threshold exists anywhere in the activation path. Mode: agent-serial.
- **C5b Cutover barrier + active-parse invariant** (`src/state.rs` keyed
  `ExclusiveGate` registry, activation pointer swap): atomic
  `activeParseId` swap behind a per-source barrier; retryable-rejection
  and captured-snapshot semantics built to spec now, with their
  query-side consumers arriving at C7/C8. Reimplements the publish
  atomicity pattern (the durable pointer write and the in-memory publish
  must not interleave with another publish) that legacy `ingest_document`
  embodied before CR deleted it, and preserves the `ExclusiveGate`
  poison-recovery invariant. §13.6, §14, §31.1. Checks: cargo checks; no
  query path reads non-active parse state. Mode: agent-serial (lifecycle
  owner).
- **C5c Parse dispatch + staging lifecycle** (`src/scheduler.rs` drain
  seam, `src/parse/`): mime-routed dispatch (`source_objects.mime_type` →
  `run_pdf_parse`/`run_text_parse`) invoked from the scheduler drain after
  successful acquisition import, then `import_parser_bundle` → gate →
  activate; deterministic no-blind-retry guard (no re-dispatch when a
  parse run for the same source object and parser identity already
  failed, §13.5 rule 5); sources with no registered parser logged durably
  (health count at C10b); staging lifecycle — consumed bundles deleted
  after a ready import, failure bundles kept per §12.2, orphaned `.tmp`
  workspaces swept; remove the `src/parse/*` dead-code allows. §9.4,
  §12.1. Checks: cargo checks. Mode: agent-serial (scheduler coupling).
- Health surfacing of held counts, serving-stale counts, and
  stuck-`building` runs defers to C10b (end-state-only development).

### CA — Semantic annotations + memoization (added 2026-07-11; after C5; D8 resolved)

Pulled forward from the post-MVP horizon (2026-07-11 rescope). The model
types (C2d) and the Provenance memoization fields already exist inert;
the annotation table is a plain edit to `sql/fabric/schema.sql` (nothing
has ever run — no migration). Annotations are parse-scoped,
provenance-carrying, rebuilt within the parse lifecycle, and queryable
only while their parse is active (§21 rules). The graph projection (C6f)
consumes entity/relation annotations as its semantic edges, which is why
CA precedes C6.

- **CAa Annotation store + freshness lifecycle**
  (`src/annotations/store.rs`, `sql/fabric/schema.sql` addition):
  hot-plane persistence for SemanticAnnotation records, `freshnessStatus`
  transitions (building/fresh/stale/failed), parse-scoped reads,
  `annotation.*` events added additively to the §33 vocabulary usage.
  §21, §33. Checks: cargo checks; annotations unreadable unless their
  parse is active; freshness state never silently absent. Mode:
  agent-parallel.
- **CAb Producer framework + producers** (`src/annotations/producer.rs`
  plus per-producer modules): producer contract with untrusted-producer
  discipline where external models are involved; provenance per §20 with
  model/prompt/config hashes; post-activation build dispatch from the
  scheduler seam; the D8-selected producers and annotation types (entity
  and relation are required by C6f; summary feeds C6d). §20, §21.
  Checks: cargo checks; every annotation carries full provenance;
  producer failure never mutates active serving state. Mode: agent-serial
  for the contract, then agent-parallel per producer.
- **CAc Memoization cache** (`src/annotations/memo.rs`): §21.2 key
  (content hash of ordered target content + annotationType + producer
  identity hash); eligibility declared per producer
  (pure-function-of-target-content only); reuse recorded honestly via
  the Provenance memoization fields (`memoized`, `memoizedFrom`,
  `memoizationKeyHash`). §21.1–21.3. Checks: cargo checks;
  context-dependent producers cannot be memoized; an auditor can always
  tell whether the model actually ran. Mode: agent-parallel.
- **CAd Required-annotation-set policy** (versioned policy document +
  loader): MVP content is "nothing blocks activation" — every annotation
  type builds post-activation with visible freshness. The policy
  document exists, is versioned and hashed, and the activation readiness
  seam from C5 reads it. §21.4. Checks: cargo checks; activation
  behavior unchanged under the MVP policy content. Mode: agent-serial.
- Known accepted risk (recorded at rescope approval): if D8 selects an
  external LLM producer, its calls run before the deferred QER audit
  tier exists to capture them under Guarantee 4. Producer calls follow
  `DIAGNOSTICS-ONBOARDING.md` boundary logging meanwhile.

### C6 — Retrieval projections (after CA; D4 resolved; D9 for C6f)

- **C6a Lexical builder** (`src/projections/lexical.rs`): FTS5 over the
  new hot plane, parse-scoped, rebuildable from canonical state. §22, §36.
  Mode: agent-parallel.
- **C6b Chunk builder** (`src/projections/chunk.rs`): `units.rs` splitting
  repurposed — input becomes (text, unit-id context) instead of
  `DoclingConversionResult`; caller-supplied tokenizer; ChunkPayload with
  chunker identity/config hash; chunks reference ContentUnit IDs, never
  mint document-slug IDs. After the harvest, delete `src/units.rs` (kept
  as dead source material since CR; named deletion approval item at C6b).
  §22–§23. Mode: agent-parallel.
- **C6c Dense builder + hot dense index** (`src/projections/dense.rs`):
  dense vectors as projections; the in-memory cache successor keyed to
  active parses, with an explicit rebuild-from-canonical path; typed
  shared codec replacing the implicit row-major blob contract. §22, §8.3.
  Mode: agent-parallel.
- **C6d Derived-view builder; summary projection**
  (`src/projections/view.rs`): derived views per §22; the summary
  projection materializes CA summary annotations (per D8's selected
  types) rather than choosing its own producer — D7 dissolved into D8
  (2026-07-11). §22, §36. Mode: agent-parallel.
- **C6e Multi-vector builder** (`src/projections/multivector.rs`):
  ColBERT document-token matrices as `multi_vector` projections per the
  D4 resolution — typed fabric codec replacing the discarded legacy
  flattened-blob plane, persisted per unit, parse-scoped, rebuildable
  from canonical state through the retained inference API
  (`embed_document`). §22. Mode: agent-parallel.
- **C6f Graph projection builder** (`src/projections/graph.rs`):
  traversal projection over canonical UnitRelationships plus CA
  entity/relation annotations (the semantic edges that make graph
  retrieval answer relational/entity-centric queries); projection only —
  the canonical relationship source of truth stays UnitRelationship
  storage (§32); shape per D9. §22. Mode: agent-serial (D9-coupled).
- Cluster checks: projection freshness states implemented; payload
  archival into the artifact store; chunks never served as evidence;
  builders take the model-call gate through the caller-side discipline.

### C7 — Retrieval fabric (after C6; cluster plan APPROVED 2026-07-15)

Channels: dense, lexical, graph (`multi_vector` deferred post-MVP,
2026-07-15 rescope amendment, §5). Two rulings shape every package
(full text in Current Status 2026-07-15): DP1 — all hot-plane reads for
one query execute inside ONE read-only transaction on one connection,
opened before anything else and covering the scope-filtered active
(source_id → parse_id) capture, so capture and reads share one WAL
snapshot (§31.1); DP2 — C7 exposes only a synchronous pipeline
function; admission, AppState wiring, and the endpoint are C8d's.
Dispatch order: C7s first; then C7a ∥ (C7b-1 → C7b-2) ∥ C7c; then C7d;
then the verification workflow.

- **C7s Substrate** (`src/query/mod.rs`, `src/query/model.rs`,
  `src/hot_plane.rs`): module skeleton; shared contract types
  `RetrievalHit` (§24.4), `RetrievalChannel`, `ResolvedScope`
  (§24.2/§24.3) — NO §24 types exist in `src/` (verified 2026-07-15);
  `begin_read_transaction` added to `hot_plane.rs` (read-only twin of
  `begin_write_transaction`, no IMMEDIATE, same logging discipline);
  verify and record the `chunk_text_index` FTS5 content mode for the
  C7b prompts. Checks: cargo battery. Mode: agent-serial,
  pre-dispatch.
- **C7a Retrieval profile** (`src/query/profile.rs`): versioned,
  hashed RetrievalProfile (§24.2 shape) sealed via
  `canonical_sha256_hex_without_field` + `PROFILE_HASH_JSON_KEY`,
  mirroring `annotations/policy.rs` `seal_mvp_policy`; values:
  default_top_k=10, max_top_k=100, rrf_k=60,
  candidate_overfetch_multiplier=3, colbert_candidate_pool_size=100,
  reranker_candidate_pool_size=10, graph_hop_budget=1 (D9); channels
  [dense, lexical, graph]; constraint→ResolvedScope resolution
  (default: all sources). The QueryRequest envelope defers to C8d;
  QueryPlan/planHash defers with the QER audit tier (deviation
  recorded). §24.2–24.3. Checks: cargo; no retrieval knob returns to
  config. Mode: agent-parallel.
- **C7b-1 Dense + lexical channels + fusion** (`src/query/channels.rs`):
  dense exact-cosine over captured `Arc<DensePlane>`s; lexical
  `match_chunks` per captured parse (match strings only via
  `primitives::bm25`); chunk→unit resolution BEFORE fusion (fusion
  keys on unit_id; dense/lexical hits are chunk-grained); RRF via
  `primitives::fusion::fuse_matches`; per-channel trace as log/debug
  only. Scope is enforced by the capture: channels read only captured
  parse_ids. §6, §24.1, §24.4. Checks: no ranked post-filtering for
  scope anywhere. Mode: agent-serial (first of two sequential
  sub-agents; hands RetrievalHit usage and scope-helper signatures to
  C7b-2).
- **C7b-2 Graph channel + D9 tiering** (`src/query/channels.rs`,
  sequential after C7b-1): entry by lexical match of query text
  against normalized entity names (D9: no LLM in the query path);
  `mentions_for_name`/`one_hop_edges` within hop budget 1; D9 tiers
  (tier 1 units connected to >1 matched entity, tier 2 direct
  mentions, tier 3 one-hop). Within-tier name-match strength RULED at
  plan approval 2026-07-15: character length of the matched normalized
  entity name, longer = stronger, then name ascending; final tiebreak
  unitId ascending. Checks: scope applied at the entity lookup.
  Mode: agent-serial.
- **C7c Rerank integration** (`src/query/rerank.rs`,
  `src/projections/multivector.rs`): NEW parse-scoped multi-vector
  query-time loader over `unit_multivector_projections` (only the
  build-time writer exists — verified 2026-07-15), decoding via
  `primitives::codec::decode_colbert_document_vector_blob` (path
  corrected 2026-07-15; the codec lives in `primitives/codec.rs`);
  ColBERT MaxSim via `score_persisted_candidates` over the fused pool
  (persisted document matrices are decoded, never re-embedded, per
  §38 — but the QUERY embeds live, so the call is gated; corrected
  2026-07-15 per the gate ruling in Current Status); final reranker
  via `RerankerBackend::score_candidates`, gate discipline CALLER-SIDE
  via `state::acquire_model_call_gate_on` — `uses_local_model_gate` is
  a predicate only, and the gate is never acquired on the HTTP branch
  nor held across HTTP rerank I/O (corrected 2026-07-15; the original
  "local acquires internally" claim was factually wrong).
  RankingTrace equivalent stays log/debug (QER tier
  deferred). §24, D4. Checks: gate never held across HTTP rerank I/O.
  Mode: agent-parallel (disjoint files from C7b).
- **C7d Pipeline integration** (`src/query/execute.rs`; `main.rs`
  `mod query;` line main-loop): `execute_query(...)` taking explicit
  handles, assuming admission already granted by the C8d caller;
  first act = open read-only connection + DP1 read transaction;
  capture scope-filtered active set inside it; channels → RRF →
  MaxSim → rerank → ranked hits + traces; new fabric
  `QueryStageLatencies` in `src/query/`; DIAGNOSTICS boundary logs
  including the snapshot-held-duration log the DP1 ruling requires.
  DELETE `src/primitives/latency.rs` (named deletion APPROVED
  2026-07-15: its sole planned consumer was C7 and its six
  legacy-shaped stage fields do not fit the fabric pipeline; C7d's
  struct replaces it). All of `src/query/` dead-code-allowed
  "consumed at C8d". Checks: every hot-plane read goes through the
  pipeline's single connection/transaction; cargo battery. Mode:
  agent-serial; `main.rs` line and the deletion main-loop.
- Cluster-wide: expected zero new `ApiError` variants (single-owner
  batched edit if a constructor lands); NO config changes;
  verification = five-dimension adversarially-confirmed workflow with
  finder prompts carrying the §1.2 premise, the honest-option rule,
  and per-dimension checklists (DP1 reads-inside-the-transaction,
  scope non-post-filtering, model-gate discipline among them).

#### C7 fact base (recon 2026-07-15; line numbers may drift — names govern)

- Lexical: `lexical.rs:350` `match_chunks(conn, parse_id, fts_query,
  limit) -> Result<Vec<LexicalMatch>, ApiError>`;
  `LexicalMatch { chunk_id, bm25_rank: f64 }`;
  `primitives/bm25.rs:112` `build_bm25_queries(query) ->
  Option<Bm25Queries>` (`{ strict_query, broad_query }`) is the sole
  FTS5 match-string builder.
- Dense: `dense_cache.rs:260` `snapshot_for_parse(&self, parse_id) ->
  Option<Arc<DensePlane>>`; `DensePlane { vectors (row-major Vec<f32>),
  chunk_ids, norms, dimension }`, per-parse, immutable behind Arc.
- Graph: `graph.rs:549` `mentions_for_name(conn, parse_id,
  normalized_name) -> Result<(String, Vec<String>), ApiError>`;
  `graph.rs:592` `one_hop_edges(...) -> Result<Vec<OneHopEdge>,
  ApiError>` (`{ far_normalized_name, relation_type, target_unit_ids,
  direction }`, both directions UNIONed); `graph.rs:174`
  `normalize_entity_name(raw) -> String`.
- Fusion: `primitives/fusion.rs:44` `fuse_matches(&[DenseMatch],
  &[Bm25Match], top_k, rrf_k) -> Vec<FusedMatch>`; keys on unit_id;
  tie-break score desc then unit_id asc.
- Multi-vector: `primitives/codec.rs:69` `decode_colbert_document_vector_blob(
  unit_id, blob, token_count, row_dimension, expected_dimension) ->
  Result<Vec<f32>, String>`; store table
  `unit_multivector_projections` (`UNIQUE(parse_id, unit_id)`); no
  query-time loader exists — C7c builds it.
- Inference (pinned, §1.5): `dense.rs:165` `embed_query_vector(&self,
  text) -> Result<Vec<f32>, ApiError>` (no gate); `colbert.rs:579`
  `score_persisted_candidates(&self, query,
  &[ColbertDocumentEmbedding]) -> Result<Vec<ColbertCandidateScore>,
  ApiError>` (no gate); `reranker_backend.rs:140`
  `score_candidates(&self, query, &[RerankerCandidateInput]) ->
  Result<Vec<RerankerCandidateScore>, ApiError>`;
  `reranker_backend.rs:120` `uses_local_model_gate` (Local⇒true,
  acquires the gate internally; Http⇒false).
- State: `state.rs:748` `reject_if_active(&self, source_id) ->
  Result<(), CutoverBarrierActive>` (C8d admission probe);
  `state.rs:149` `AdmissionGate` retained "consumed at C8d"; AppState
  holds neither `DenseCache` nor `CutoverRegistry` — both are
  `main.rs`-owned, threaded into the scheduler only
  (`main.rs:492/503/527`).
- Hot plane: `hot_plane.rs:536` `open_read` (READ_ONLY | NO_MUTEX,
  WAL verified per open, busy_timeout 5000 ms); `hot_plane.rs:594`
  `begin_write_transaction` (IMMEDIATE) — C7s adds the read twin.
- Self-hash: `canonical.rs:118`
  `canonical_sha256_hex_without_field`; `canonical.rs:110`
  `PROFILE_HASH_JSON_KEY`; mirror `annotations/policy.rs:77-99`
  `seal_mvp_policy`.
- Concurrency facts DP1 answers: activation deletes no hot rows
  (superseded cleanup is C9); the annotation worker delete-rebuilds
  graph/summary projections for the ACTIVE parse in one IMMEDIATE tx
  (`annotations/worker.rs:1024`) — the per-query read transaction
  isolates queries from both.
- Unverified, C7s resolves before dispatch: `chunk_text_index` FTS5
  content mode (`sql/fabric/schema.sql:355`).

### C8 — Assembly and EvidencePack (COMPLETE 2026-07-15; QER writer deferred 2026-07-11)

As-built (full detail in Current Status 2026-07-15, second session).
Rulings that shaped the cluster: D2 query surface (§4); R2 post-capture
in-transaction barrier probe (amends DP1 ordering); R3 scope
intersection; assembly runs INSIDE `run_pipeline_body` before the
transaction drops (the C7d `execute_query` tx never outlives the call —
"assembly joins the read transaction" was only implementable inside it).

- **C8s Substrate** (`src/assembly/{mod,model}.rs`,
  `sql/fabric/schema.sql`): §25–§27 types field-for-field; submodule
  skeletons; `idx_unit_relationships_parse_from`/`_parse_to`
  (`user_version` = 1). §25–§27.
- **C8a AssemblyPolicy engine** (`src/assembly/{policy,operators}.rs`):
  sealed MVP policy v1 (budgets 30/1/15360; anchor, parent-container +
  heading-path, caption-pair, continuation-chain, text-neighbors rules);
  all seven §25 operators over `unit_relationships` (graph authoritative
  per §19); §25.1 dependency check via the first active-parse
  `conformance_report_json` reader, ONE aggregate warn per query;
  `requiresRelationshipTypes` includes never-emitted
  `continues_on`/`references` (inert-visible by design). §25.
- **C8b EvidencePack construction** (`src/assembly/evidence.rs`):
  `build_evidence_pack` on the caller's transaction; rank-ordered
  anchors; R13 determinism; §26 canonical-units-only enforced
  structurally; first `locators_json` reader; operator-traversed edges
  and `fresh_for_active_parse` annotations behind the R6 evidence
  flags; token budget via caller-supplied ungated closure. No
  `EvidenceFreshness` (QER-tier deferral). §23 rule 3, §26–§27.
- **C8c QER writer — DEFERRED post-MVP (2026-07-11 rescope).** Moved to
  the §5 "QER audit tier"; its seams (C2e QER metadata table, `qer_`
  IDs, C3c boundary timestamps, the hashed C7a RetrievalProfile, and
  now the C8b ContextAssemblyTrace embedded in every pack) stay inert.
- **C8d-1 State + errors + request envelope** (`src/query/request.rs`,
  `src/state.rs`, `src/error.rs`): R6 MVP QueryRequest envelope +
  `ValidatedQuery`; AppState gained
  dense_cache/cutover_registry/search_admission
  (`MAX_IN_FLIGHT_SEARCH: u32 = 1`); `ApiError::CutoverBarrierActive`
  (503/"cutover_barrier_active"; `From` impl in state.rs — recorded
  colbert-diagnostic deviation). §24.3, §6.
- **C8d-2 Pipeline + endpoint + sweep** (`src/query/{execute,profile}.rs`,
  `src/http.rs`): R2 probe; R3
  `capture_active_by_source_ids_and_domains`; assembly stage +
  `assembly_ms` + `evidence_pack` on `QueryPipelineOutcome`; the
  request's `max_final_evidence_units` drives pipeline top_k
  (default request byte-identical to C7); `POST /query` (spec-literal
  path) with admission-permit-first + `spawn_blocking` (http.rs's first
  blocking seam) + debug view-DTO diagnostics; cluster-wide allow sweep
  (sole survivor: `caller_context`). §24.1, §31.1, §34.1 (recorded
  `queryExecutionRecordId` omission). 

C9-facing seams: superseded-parse hot cleanup must cover
`semantic_annotations` and the projection planes; DP1 open-snapshot
semantics remove any need for query leases during deletion;
`envelope::mark_superseded`'s caller lands at C9; C9d completes
`archiving`→`archived`.

### C9 — Lifecycle forensics: snapshots, deletion, rollback (after C8; cluster plan APPROVED 2026-07-16)

Rulings shaping every package (full text in Current Status 2026-07-16):
artifact scope (archive dense/multivector blobs + `semantic_annotations`
+ `chunk_projections` payloads + canonical active-parse rows; reference
the sealed §12.3 parse bundle and raw source bytes by existing uri+hash;
FTS5/graph planes covered by the deletion gate's verified deterministic
rebuild); ReplayProfile stamps evidence `bit_exact` +
retrieval/generation `not_supported` (the `record_replay` upgrade is a
recorded deviation deferred to the QER tier); `scheduled` trigger
deferred post-MVP (no external cadence fact; §35); deactivation =
`deactivated_at` + cutover barrier; rebuild checks re-import, never
re-embed; restore re-imports preserving IDs (annotations from archived
artifacts, never memo re-mints); `annotation_memo` survives cleanup.
The QER-tier deferral means the manifest's `queryExecutionRecords`
section is empty and no drill machinery ships. Dispatch order: C9s →
C9a ∥ C9c → C9b → C9d (serial last: consumes C9a + C9b, touches
activation-adjacent seams) → five-dimension verification.

- **C9s Substrate** (`src/model/snapshot.rs` + `model/mod.rs`
  re-export, `src/identity.rs`, skeletons `src/snapshot.rs`,
  `src/snapshot/verify.rs`, `src/deletion.rs`, `src/restore.rs`,
  `sql/fabric/schema.sql`, `src/hot_plane.rs`; `main.rs` mod lines
  main-loop): the ABSENT §30 types (`ForensicSnapshot`,
  `ForensicSnapshotManifest`, `SnapshotArtifactRef`, `ReplayProfile`
  plus the snapshotType/replay-mode enums) field-for-field under the
  model serde standard (camelCase, deny_unknown_fields,
  skip_serializing_if); the `forensic_snapshots` table mirroring the
  `query_execution_records` metadata-row-plus-archived-manifest
  pattern + paired `FABRIC_TABLE_CONTRACTS` entry (`user_version`
  stays 1; named schema approval granted); `src/identity.rs`
  (`system_version()` via `CARGO_PKG_VERSION`, `build_features()` via
  `cfg!`, `configuration_hash()` via `canonical_sha256_hex_of` —
  secret file paths only, never values; `SPEC_VERSION = "0.3"`);
  pre-declared mod skeletons so parallel packages never contend.
  §30.2–30.4, §30.7, §32. Checks: cargo battery; types round-trip
  canonical serialization; contract matches DDL (drift fatal). Mode:
  agent-serial, pre-dispatch.
- **C9a Snapshot manifests + triggers** (`src/snapshot.rs`):
  content-addressed manifest builder per §30.1–30.4 under the
  artifact-scope ruling; manifest self-hash via
  `canonical_sha256_hex_without_field`; serialize acquisition/deletion
  records and the resolved sealed policies/profiles at snapshot time;
  `parserOutputBundles` section absent (spec-optional; parser output
  is staging-only). Triggers: `pre_activation`/`post_activation`
  (wired by the scheduler AROUND `gate_and_activate`, whose barrier
  hold is internal — neither snapshot runs under a held barrier),
  `pre_deactivation`, and a parameterized `manual`/`incident` fn (HTTP
  at C10a); `scheduled`/`pre_deployment` inert enum variants; NO
  pre-superseded-deletion trigger (corrected 2026-07-16 — §30.3's
  closed snapshotType enum has no such variant; the deletion gate
  reuses the immediately-preceding lifecycle snapshot). ReplayProfile per the ruling. First
  `snapshot.started/completed/failed` emitters; `forensic_snapshots`
  row + `snap_` id (ids.rs mint exists, allow removed). §30, §8.1–8.3,
  §29.1. Checks: manifest completeness vs the §30.4 section list
  (empty QER section allowed); every referenced artifact passes
  `exists()`; no §35 knob; DIAGNOSTICS boundary logs with elapsed +
  artifact counts. Mode: agent-parallel.
- **C9b Verification tiers** (`src/snapshot/verify.rs`): mechanical
  tier — manifest completeness + re-hash of every referenced artifact
  (every snapshot); deletion-gate tier — mechanical + deterministic
  rebuild check: re-import dense/multivector blobs via
  `primitives/codec.rs`, rebuild chunk/lexical/graph from the archived
  canonical rows, compare to hot state — NEVER re-embed. Failure halts
  the affected source's lifecycle only, retains superseded state, no
  auto-retry (§30.5/§31.2); health-visibility seam → C10b. Restore
  drills NOT built (QER-tier deferral). §30.5, §31.2 step 4, §29.1.
  Checks: no deletion proceeds past a failed gate; the rebuild path
  invokes no model call. Mode: agent-parallel.
- **C9c Deletion propagation + access-lost + reappearance**
  (`src/deletion.rs`): §11.3 propagation dispatched from the existing
  synchronous post-drain step in `run_cycle` (beside
  `apply_enumeration_deletions`; deletion never enqueues):
  last-current-location loss → pre-deactivation snapshot → cutover
  barrier acquire (third `acquire` caller) → set
  `source_objects.deactivated_at` → dense-cache `evict_parse` →
  `source.deactivated` event. Access-lost per the recorded resolution:
  source-side scope-enumeration failure → locations `access_lost`,
  freshness clock stops, serving continues,
  `source.access_lost`/`source.access_restored` events (per-file 403
  granularity recorded as a D5/C10f residual). Reappearance (§11.4):
  same-hash restore via C9d's restore fn, clear `deactivated_at`,
  `source.reactivated`. The scheduler wiring line is reported to the
  main loop, not self-edited. §11.2–11.4, §31.1, §30.6. Checks:
  All-scope query invisibility via `deactivated_at` (location status
  alone is insufficient — the capture never joins locations); barrier
  held only across the flag write + evict; no deletion inference from
  failed scans. Mode: agent-parallel.
- **C9d Superseded-state lifecycle + rollback-as-restore**
  (`src/restore.rs`): §31.2 steps 3–5 — locate the gating snapshot
  through the C9s-pinned lookup contract (the `post_activation`
  snapshot in the activation flow, the `pre_deactivation` snapshot in
  the deactivation flow; NEVER re-taken — corrected 2026-07-16) →
  deletion-gate verification → on pass: explicit ordered per-table
  DELETEs for the superseded parse (canonical rows +
  `semantic_annotations` hard-delete + all projection planes, FTS5 via
  the `chunk_id IN (SELECT …)` pattern; NO FK cascades exist;
  `annotation_memo` SURVIVES) → `archiving`→`archived` + `archived_at`
  + `parse.archived` event (first writer of all three) →
  `envelope::mark_superseded` caller lands at supersession completion.
  Rollback/reappearance restore (§31.3/§11.4): re-import canonical
  rows and annotation/projection artifacts preserving IDs, re-import
  dense/multivector blobs, deterministically rebuild
  chunk/lexical/graph, then normal activation through the barrier.
  Internal fns now; `POST /restore` + `POST /snapshots` at C10a
  (accept/discard precedent). §31.2–31.3, §11.4, §8.1. Checks: delete
  order leaves no orphans; memo untouched; restore never
  re-parses/re-embeds; failure-gated end to end (no grace window, no
  auto-retry). Mode: agent-serial (last).
- **C9e External model call records — DEFERRED post-MVP (2026-07-11
  rescope).** Guarantee 4 capture moves to the §5 "QER audit tier".
  Until it lands, external calls (HTTP reranker, any D8 external
  annotation producer) are covered by `DIAGNOSTICS-ONBOARDING.md`
  boundary logging only.
- Cluster-wide: expected `ApiError` additions in one single-owner
  batched edit (`SnapshotVerificationFailed`, `RestoreFailed`, ±
  `SnapshotIncomplete`) on first construction per the permanent
  per-cluster rule; zero config changes; zero deletions; verification =
  five-dimension adversarially-confirmed workflow with finder prompts
  carrying the §1.2 premise, the honest-option rule, and per-dimension
  checklists (§30.4 manifest completeness, rebuild-not-re-embed, §35
  no-knob, the `deactivated_at` capture contract, memo-survives, DP1
  no-lease, comment + diagnostics duties among them).

#### C9 fact base (recon 2026-07-16; line numbers may drift — names govern)

- Model gap: §30 types ABSENT from `src/model/` (`model/mod.rs:1`
  covers §7–§20, §33 only); C9s authors them. Serde standard:
  `model/mod.rs:6–17`.
- IDs: `ids::new_forensic_snapshot_id()` → `snap_` (ids.rs:67,
  dead_code "Consumed by C9"). Convention `ids.rs:19`: rebuilds
  re-import IDs from stored artifacts, never re-derive.
- Artifact store: `ArtifactRef{hash,uri,size_bytes}`
  (artifact_store.rs:50–60); `put_bytes` (109, write-once: same-hash
  dedup no-op, size-mismatch explicit error, temp+atomic rename);
  `put_json` (248, canonical bytes); `put_jsonl` (268, LF-join, no
  trailing LF, order significant); `get_bytes` re-hash-verifies (177);
  `exists` (238). No multi-file bundle abstraction — manifests are
  caller-side (parse-importer pattern).
- Canonical: `canonical_sha256_hex_without_field` (canonical.rs:118,
  the manifest self-hash), `canonical_jsonl_bytes` (65),
  `jsonl_record_set_hash` (83), `canonical_sha256_hex_of` (101).
- Hot plane: `forensic_snapshots` absent — mirror
  `query_execution_records` (schema.sql:224–233: id + hashes +
  archive_uri/archive_hash + created_at). `FABRIC_TABLE_CONTRACTS`
  (hot_plane.rs:89) mirrors DDL, drift fatal (779);
  `FABRIC_VIRTUAL_TABLES` (384). `user_version = 1` (schema.sql:467).
  `begin_write_transaction` IMMEDIATE (hot_plane.rs:594), caller
  namespace; events append on the caller's connection (events.rs:35),
  atomic with the owning tx.
- Activation seams: `gate_and_activate` (activation.rs:196; pre-tx
  seam at 222, post-commit at 240, barrier-held publish slot 270).
  `activate_candidate` (748–834): predecessor → 'archiving' at 754
  ("C9 archive-verify-delete completes it", log at 765). NOTHING sets
  'archived'/`archived_at` (importer inserts None at importer.rs:477).
  Barrier: `CutoverRegistry::acquire` (state.rs:769), callers
  activation.rs:220,370 only; state.rs:203 names deactivation as the
  future co-holder. `accept_held_parse` also publishes the dense
  cache under the held barrier (activation.rs:407).
- Deactivation contract: every capture variant filters
  `active_parse_id IS NOT NULL AND deactivated_at IS NULL`
  (execute.rs:653/688/727/776); `locations.status = 'current'` joins
  only on domain scopes → deactivation MUST set `deactivated_at`.
  `source_objects.deactivated_at` written by NO code today.
- Deletion path: synchronous post-drain in `run_cycle`
  (scheduler.rs:1177–1191); `apply_enumeration_deletions`
  (acquisition.rs:947) sets location `status='deleted'` +
  `deletion_evidence_json`; deletion never enqueues (the queue's only
  reason value is "staged_by_full_scan"). Only
  `absent_from_complete_enumeration` evidence is produced today;
  `source_reported_gone`/`explicit_delete_event` variants exist unused
  (full-scan connector); `access_lost` never set, no
  failureClass→access_lost path. Deactivation deferral comments:
  acquisition.rs:945–946, 695–696, 781–784.
- Supersession/cleanup: `envelope::mark_superseded`
  (projections/envelope.rs:372, no caller — lands at C9d);
  `envelope::delete_for_parse` (409). Delete-by-parse SQL exists in
  the builders: lexical.rs:75–77 (FTS5 via `chunk_id IN (SELECT id
  FROM chunk_projections WHERE parse_id=?)`), dense.rs:86–87,
  multivector.rs:76–77, graph.rs:88–94. NO DELETE exists for
  content_units/unit_relationships/retrieval_projections/parse_runs/
  semantic_annotations (annotations have only `mark_stale`,
  store.rs:408); C9d authors them. `annotation_memo` SURVIVES
  (schema.sql:281–295). NO FK cascades (only two REFERENCES clauses,
  no ON DELETE). Verbatim C9 duty: annotations/worker.rs:25–27.
- Dense cache: `evict_parse` (dense_cache.rs:280, benign no-op if
  absent; no deactivation caller yet); `load_parse` (200);
  `snapshot_for_parse` (260).
- Rebuild determinism: chunk_text_index/chunk_projections/graph
  deterministic-from-hot-rows (lexical.rs:52–57 deterministic order;
  chunk.rs:57 + banked ChunkerConfig hash; graph.rs:259–271 no model
  call); chunk_dense_vectors/unit_multivector_projections
  model-dependent from scratch (dense.rs:276, multivector.rs:242) but
  byte-reproducible via `primitives/codec.rs:24–89` — re-import,
  never re-embed.
- Archival state today: raw sources archived
  (`source_objects.storage_uri`, acquisition.rs:483); canonical parse
  bundle archived with self-hashed manifest
  (`parse_runs.artifact_bundle_uri/_hash`, importer.rs:1150–1239);
  projections hot-plane-only EXCEPT derived views (view.rs:142;
  summary deliberately not re-archived, view.rs:22–24);
  `parser_raw_output_uri` always None (importer.rs:480), staging
  bundles deleted post-completion (scheduler.rs:1495); sealed
  policies/profiles are OnceLock constants, never archived
  (assembly/policy.rs:58, annotations/policy.rs:58,
  query/profile.rs:105).
- Events (all DEFINED in model/event.rs, ZERO mint sites):
  snapshot.started/completed/failed (100–105), drill.completed/failed
  (106–109, no drill.started exists), source.deactivated/reactivated
  (44–46), parse.archived (64), source.access_lost/access_restored
  (40–43).
- App identity: no `CARGO_PKG_VERSION`/`env!` usage anywhere; no
  structured build-feature accessor; no aggregate config hash (only
  per-connector/per-parser hashes, schema.sql:72,122). C9s builds all
  three in `src/identity.rs`.
- DP1: superseded rows stay visible to in-flight queries via their
  open read snapshots (execute.rs:13–24); NO query-lease machinery for
  superseded deletion. Annotation-worker SELECT filters
  `deactivated_at IS NULL` (worker.rs:70–72), so the deactivation flag
  naturally parks a deactivated source's annotation work.

Rulings shaping every package (full text in Current Status 2026-07-16,
C10 planning entry): R1 — never-activated held candidates (superseded
AND discarded) are cleaned via a new HeldSupersession mode gating over
the candidate's OWN pre_activation snapshot, completing
archiving→archived; R2 (D2 finalized) — Operation records + polling
ONLY, no NDJSON anywhere; plus the eight pre-plan resolutions
(async-Operation execution, queue-coupled completion via
`sync_queue.operation_id`, shutdown route retained, protected/public
split, §14 unit inspection via parse-scoped IDs, slot-published health
with the readiness set unchanged, identity onto AppState, REPL
retained). Dispatch order: C10s → C10r → C10a → C10b ∥ C10c → C10d →
C10e → C10f.

- **C10s Substrate** (`src/model/operation.rs` + `model/mod.rs`
  re-export, an operations-store module, `sql/fabric/schema.sql`,
  `src/hot_plane.rs`, `src/ids.rs`; `main.rs` mod lines main-loop):
  §34.6 Operation type field-for-field under the model serde standard;
  NAMED SCHEMA EDITS (approved 2026-07-16): `operations` table
  mirroring the forensic_snapshots metadata-row pattern + paired
  `FABRIC_TABLE_CONTRACTS` entry, and a nullable
  `sync_queue.operation_id` column (`user_version` stays 1 — nothing
  has ever run); store with status-guarded transitions
  (pending→running→succeeded/failed) + an `op_` id mint; skeletons
  pre-declared so later packages never contend. §34.6. Checks: cargo
  battery; contract matches DDL (drift fatal). Mode: agent-serial,
  pre-dispatch.
- **C10r Ruling-1 held-candidate cleanup** (`src/restore.rs`,
  `src/activation.rs`, `src/scheduler.rs`): third
  `SupersededCleanupMode` selecting (subject = the held candidate
  itself, snapshot_type = PreActivation) and completing
  archiving→archived + `parse.archived`; `supersede_other_held`
  returns the superseded ids and `gate_and_activate`/`hold_candidate`
  thread them out; the scheduler cleans post-barrier mirroring the
  predecessor arm (scheduler.rs:1411-1460); `discard_held_parse`'s
  C10a call site drives the same mode. Gate failure → halt/retain (no
  auto-retry); health seam → C10b. The stale activation.rs KNOWN
  CLEANUP GAP comment (904-909) is replaced by the ruling reference.
  §31.2 (extended by ruling), §38, §30.5. Checks: cargo battery; no
  new snapshotType; no snapshot re-take. Mode: agent-serial.
- **C10a API finalization** (`src/http.rs`, new handler modules,
  `src/state.rs`, `src/error.rs`; `main.rs` wiring main-loop): the §34
  surface — `POST /sources` (source_ingest) and
  `POST /sources/{sourceId}/parses` (parser_execution) enqueue via
  `enqueue_coalesced` with the operation_id threaded through
  `sync_queue` and completed by the drain;
  `POST /sources/{sourceId}/parses/{parseId}/activate` +
  `POST /parses/{parseId}/accept` (parse_activation) +
  `POST /parses/{parseId}/discard` (additive `parse_discard`,
  recorded) + `POST /snapshots` (snapshot_creation) + `POST /restore`
  (restore) as async Operations on detached spawn_blocking tasks
  updating the Operation row; the accept call site owes the
  scheduler-arms model (pre/post-activation snapshots + predecessor
  cleanup) and both dispositions drive C10r cleanup;
  `GET /parses?status=held`; inspection `GET /units/{unitId}` and
  `GET /units/{unitId}/relationships` (direction/type filters; parse
  derived from the parse-scoped unit ID, served only if active on its
  source per §14), `GET /sources/{sourceId}` (locations + freshness),
  `GET /sync/status` over the published SyncHealth;
  `GET /operations/{operationId}`; protected `POST /shutdown` via
  `request_shutdown` (immediate confirmation then signal; extra-spec,
  recorded additive). Bearer auth via the retained
  `bearer_token_from_headers` + `authorize_admin_token`;
  protected/public per resolution 4; `ApplicationIdentity` cloned onto
  AppState; expected `ApiError` additions (a 404-class NotFound) in
  the single-owner batched edit. §34, §14, §6. Checks: cargo battery;
  no NDJSON; every async operation leaves a durable Operation row;
  unit inspection never serves a non-active parse. Mode: agent-serial;
  transport wiring + any config touch main-loop.
- **C10b Health expansion** (`src/state.rs`, publisher seams in
  `src/scheduler.rs` + `src/annotations/worker.rs`, `src/types.rs`
  health shapes): the scheduler publishes per-cycle fabric counts
  (held, serving-stale, stuck-`building`, access-lost,
  unparseable-mime, verification-halted); the annotation worker
  publishes its own slot (parked state, freshness counts);
  `AdmissionGate::snapshot` consumed; every count carries an as-of
  label (PRINCIPLES accuracy); per-source-system keying (one system
  at MVP); readiness set UNCHANGED {inference, sync} — every new item
  diagnostic-only. §9.5–9.6, §11.2, §13.4–13.5, §21, §30.5. Checks:
  cargo battery; the health handler opens no connections. Mode:
  agent-serial.
- **C10c CLI rework** (`src/bin/data-store.rs`, exclusive single
  owner): command table + DTOs re-pointed at the C10a surface; a
  polling loop (`GET /operations/{id}`) replaces
  `read_operation_stream`; plain JSON `POST /query` + GET inspection;
  the REPL/rustyline/history/token/config machinery is retained; dead
  with the rework: the NDJSON transport + stream-loss machinery, the
  benchmark renderers, the `raw.storage.retrieval.bm25` panic path,
  and the hardcoded stage strings. Checks: cargo battery; no reference
  to `/v1/operations` remains. Mode: agent-serial.
- **C10d Documentation re-baseline** (`README.md`, `ARCHITECTURE.md`,
  `PROTOCOL.md`, `SPEC-SERVER.md`, `SPEC-CLIENT.md`, `INSTALL.md`,
  `INTERACTIVE.md`): rewritten against the end-state service; each
  file an approval item at draft time. Includes the NAMED CONFIG EDIT
  (approved 2026-07-16): re-wording the stale
  `[client].operation_timeout_seconds` "stream timeout" comments in
  `config.example.toml` + `config.toml` (comment only; zero key
  changes in C10). Mode: agent-parallel drafts; approvals main-loop.
- **C10e Final legacy sweep** (NAMED DELETIONS, approved 2026-07-16
  with this plan): `src/units.rs` (zero consumers) + the
  `ApiError::UnitSplitting` variant; the docling.rs markdown path
  (`DoclingConversionResult`, `convert_source_to_markdown`,
  `read_and_normalize_markdown`, `normalize_markdown` — its "pending
  C6d" retention condition is satisfied: view.rs renders from
  canonical units only); the http.rs NDJSON emitter block
  (`NDJSON_CONTENT_TYPE`, `OPERATION_STREAM_CHANNEL_CAPACITY`,
  `OPERATION_STREAM_CLOSED_MESSAGE`, `NEXT_SERVER_OPERATION_ID`,
  `OperationStreamSendStatus`, `OperationStreamSender`,
  `OperationEmitter` + impl, the `emit_operation_*`/
  `operation_*` free helpers); types.rs `OperationBenchmarks`/
  `BenchmarkStage`/`OperationRequest`/`OperationEvent`/
  `OperationControlRequest`; the `POST /v1/operations/{id}/control`
  reject stub + route; the stale "consumed at C10a" emitter comments;
  `src/source.rs` dead paths + the `SourceResolution` variant and the
  `primitives/{validate,fusion}` orphan check (verify-then-prune,
  each against live references before removal). NOT deleted:
  `docling_activity.rs` (live, consumed by docling.rs). Mode:
  main-loop-driven with an agent executor. Checks: cargo battery at
  zero warnings after every deletion.

#### C10 fact base (recon 2026-07-16; line numbers may drift — names govern)

- Router today: `GET /v1/health` + `POST /query` + the
  `POST /v1/operations/{id}/control` reject stub only (http.rs:56-70);
  no §34.2–34.6 route exists. /query handler pattern: admission permit
  FIRST (`state.try_acquire_search`, http.rs:375), then
  `spawn_blocking` (http.rs:388-390); JoinError → logged 500.
- Auth substrate (KEEP): `bearer_token_from_headers`
  (http.rs:1044-1068); `AppState::authorize_admin_token` →
  `constant_time_eq` (state.rs:363-372, 987-995); the token lives on
  `AppState.admin_shutdown_token`, generated at startup (main.rs:314)
  and published to the owner-only token file (main.rs:378). NO route
  performs auth today.
- Emitter block (DELETE per R2): http.rs:40-53, 490-524, 530-890,
  893-980; types.rs:22-111 substrate. Server-side
  `OperationEvent`/`OperationRequest` references live ONLY inside the
  dead emitter (imported at http.rs:35) — deletion is a coordinated
  http.rs + types.rs edit.
- AppState (state.rs:22-47): config, inference, model_call_gate,
  admin_shutdown_token, shutdown_signal, sync_health, dense_cache,
  cutover_registry, search_admission; ctor state.rs:257-287, call
  site main.rs:533-541. C10a ADDS: an `ApplicationIdentity` clone
  (today moved wholesale into the scheduler, main.rs:498/565) and
  operations-store access; accept's publish needs
  dense_cache/dense_dimension (main.rs:527).
- Admin seams: `accept_held_parse(index_root, registry, dense_cache,
  dense_dimension, parse_run_id) -> Result<ActivationDecision, _>`
  (activation.rs:370-447; owed wiring comment 360-368);
  `discard_held_parse(index_root, parse_run_id)` (activation.rs:457,
  no barrier); `request_snapshot(index_root, identity, snapshot_type,
  created_by, notes)` guarding Manual|Incident (snapshot.rs:197-232);
  `restore_source_from_snapshot(index_root, registry, dense_cache,
  dense_dimension, source_id, parse_id)` (restore.rs:712-719);
  `SupersededCleanupMode`'s two-arm match feeding
  (subject, snapshot_type) at restore.rs:290-295 is R1's seam;
  `supersede_other_held` callers: activation.rs:839 (activate) + 883
  (hold); the predecessor cleanup arm: scheduler.rs:1411-1460 (twin
  at 1598-1640).
- Scheduler enqueue seam: `enqueue_coalesced(index_root,
  source_system, native_uri, reason)` (scheduler.rs:327-394);
  `source_key = "{source_system}:{native_uri}"` (per-location,
  scheduler.rs:317); no command channel exists — HTTP callers use the
  free functions against index_root; `queue_depths`
  (scheduler.rs:510-546) shows the open_read health-inspection
  pattern.
- §34.6 Operation: id, operationType (closed 10-value enum), status
  pending|running|succeeded|failed, targetObjectType/targetObjectId,
  startedAt?, completedAt?, error?, createdAt (spec:2437-2463);
  `GET /operations/{operationId}` (spec:2467). Verified: no Operation
  type in `src/model/`, no operations table in schema.sql.
- Inspection: NO unit-by-id / relationships-by-unit / source-detail
  read helpers exist; nearest patterns: `SELECT_UNIT_CONTENT_SQL`
  (rerank.rs:323, parse-scoped), the relationship indexes
  (schema.sql:188-190), location SELECTs (acquisition.rs:63/110,
  deletion.rs:139). Unit IDs are parse-scoped (`<parseId>:unit:N`) —
  the §14 filter derives from the ID itself. The
  acquisition ImportOutcome fields marked "Consumed at C10a
  (inspection surfaces)" live at acquisition.rs:197-212.
- Health today: `HealthResponse`/`HealthComponent` (types.rs:5-17;
  `details` is `Vec<String>`, no numeric fields); `AppState::health()`
  is in-memory only (state.rs:414-479); ready = inference && sync
  (state.rs:471); `SyncHealth` fields state.rs:58-85
  (corpus-aggregate, not per-source); the cadence EMA is log-only
  internal state (scheduler.rs:2201-2299); `publish_health` per cycle
  (scheduler.rs:869, 2530).
- C10b data sources: held = parse_runs ready+held_reason (no
  counter); serving-stale = failed runs + sync_queue
  detected-not-active (no counter); access-lost = location status
  (deletion.rs:583-627); stuck-building = importer.rs:17-21/316;
  unparseable-mime = warn-only (scheduler.rs:1341-1352);
  verification-halt = logs + retained 'archiving' rows + transient
  SyncHealth.detail (verify.rs:23-29, restore.rs:314-325); annotation
  worker parked/orphan-adopted = log-only
  (worker.rs:29/106-123/147/385-390); staging sweep count log-only
  (scheduler.rs:2189-2194); the `AdmissionSnapshot` seam
  state.rs:158-165/975-982.
- CLI (src/bin/data-store.rs, 2038 lines): reqwest blocking client
  (`build_http_client` :942-947 from
  `[client].operation_timeout_seconds` — the key SURVIVES as the
  poll/request timeout); sole transport call site POSTs the dead
  `/v1/operations` (:21, :1231). REUSABLE: config DTOs/ClientContext
  (:29-58), arg parsing (:597-909), config reads (:911-984), REPL
  (:986-1023), `read_admin_token` (:1183-1199), error-body rendering
  (:520-530, :1546-1610 minus `operation_error`). DEAD:
  OperationRequest/OperationEvent mirrors (:474-518), benchmark
  mirrors + renderers (:313-329, :1867-1894),
  `read_operation_stream` + stream-loss machinery (:1207-1543),
  `StreamRenderer` (:1701-1792, incl. the hardcoded
  `docling_converting` stage string :1745-1746), the
  `render_bm25_diagnostics` panic path (:1622-1659).
- Deletion facts: units.rs = 563 lines, zero consumers, mod at
  main.rs:31; docling.rs markdown path = :49-67, :165-196, :1350-1363
  (no live caller; view.rs builds markdown from unit content,
  view.rs:314-316/388/142); docling_activity.rs LIVE
  (docling.rs:20-21/829/836); source.rs partially live
  (`ResolvedSource` + path-safety) but roots the dead
  `SourceResolution` (error.rs:26-28); `UnitSplitting` dies with
  units.rs (error.rs:41-43).
- Docs to re-baseline (line counts): README 665, ARCHITECTURE 404,
  PROTOCOL 728, SPEC-SERVER 630, SPEC-CLIENT 514, INSTALL 128,
  INTERACTIVE 48.

- **CPa Batched dense passage embedding — IMPLEMENTED 2026-07-17,
  REVERTED 2026-07-18** (see the 2026-07-18 Current Status entry:
  measured net regression — 8.58 ms/true-token batched vs 5.11
  singular; only ~4% raw kernel headroom exists because the 8B
  forward saturates the GPU at batch 1. The revert kept the
  `qwen3.rs::repeat_kv_heads` `.contiguous()` Metal-defect fix and
  the diagnostic bin. Package text below retained as the historical
  record):
  - New runtime API `embed_passage_vectors(texts: &[&str]) ->
    Result<Vec<Vec<f32>>, ApiError>`: tokenize+truncate each text
    (existing `tokenize_truncated`), RIGHT-pad token rows to the
    batch max, stack `[B, seq]`, ONE `forward_hidden`, per-row
    last-REAL-token pooling (narrow at each row's true length − 1,
    NOT `seq_len − 1`), per-row L2 normalize, one CPU readback,
    split rows. Existing single-text entry points and the query
    path are unchanged.
  - Correctness invariant (MUST be commented at the site): under
    CAUSAL attention, right-padding is sound — token i attends only
    to positions ≤ i, so real tokens never attend to padding;
    padded positions' outputs are discarded; the `(1,1,seq,seq)`
    causal mask and position-0 RoPE broadcast unchanged. This is
    why no padding/attention-mask plumbing is needed.
  - `build_dense_vectors` loop batches chunks under
    `DENSE_EMBED_BATCH = 16` (code constant with rationale comment —
    an engineering fact, not a §35 operator knob); per-item
    `validate_vector` → `encode_vector_blob` → INSERT unchanged, so
    persisted row/blob formats are byte-compatible and per-item
    failure attribution (chunk id) is preserved. A batch-forward
    error fails the whole build (current semantics).
  - Logging: one `model_call.started/completed` per batch with
    `text_count = N` (field exists, currently hardcoded 1) and
    total token_count; `dense_build.*` events unchanged.
  - Startup smoke extension in dense.rs: embed a small fixed batch
    AND the same texts singly; require per-row cosine ≈ 1
    (tolerance for BF16). Converts batched-forward wrongness from a
    two-hour cycle discovery into a startup-seconds failure.
  - Recorded acceptance: batched matmul reduction order may perturb
    BF16 values vs batch-1. No cross-build byte contract exists
    (loaders re-validate; the deletion gate compares within-build;
    restore is byte-from-archive), so drift is acceptable and
    recorded.
  - Gate discipline unchanged: the scheduler already holds ONE
    dense permit per parse across the whole loop
    (scheduler.rs:2436-2453); runtimes never acquire.
- **CPb Annotator per-request thinking opt-out** (owned:
  `src/annotations/llm_client.rs`; user-directed 2026-07-17): the
  chat-completions request body gains a constant
  `chat_template_kwargs: {"enable_thinking": false}` field, with a
  comment recording the vLLM-extension coupling (OpenAI-compatible
  servers ignore unknown body fields; on this vLLM it suppresses
  the thinking trace for these calls only). The shared endpoint
  stays thinking-enabled for other clients (user ruling). The
  output-token bound and null-tolerant `content` parse remain
  banked findings, NOT in CPb scope.
- **CPd ColBERT batched document embedding (COMPLETE —
  implemented and verified 2026-07-18; see Current Status. Package
  text retained as the approved-scope record)** (owned: `src/inference/
  colbert.rs`, `src/projections/multivector.rs`): batches the
  DOCUMENT-embedding path only (query embedding and MaxSim stay
  singular). Gate evidence (dense-batch-diagnostic, 2026-07-18):
  flattened batched layer-work 2.43× vs the current per-doc/per-head
  style at 16 docs × seq 128 F32, with the synthetic single mode
  validating against reality (projects 56 ms/doc vs measured 53).
  Design constraints, fixed by measurement and by the CPa lesson:
  - FLATTENED formulation ONLY: rank-2 linears over all documents'
    tokens `(B·S, 768)`, rank-3 attention over `(B·heads, S, 64)`,
    `.contiguous()` after every stride-permuting reshape. The
    rank-4/`broadcast_matmul` formulation measured 0.68× (SLOWER)
    and is excluded by measurement.
  - Padding under ModernBERT's BIDIRECTIONAL attention requires a
    true key-padding mask composed with the existing alternating
    global/local sliding-window masks — the package's named risk.
    The implementation prompt MUST require a mask-composition
    design note against the actual colbert.rs mask code before any
    edit.
  - `build_multivectors` batches per code-constant window (16),
    documents length-sorted within the parse to minimize padding
    waste; per-unit validate/encode/INSERT unchanged (persisted
    blob format byte-compatible); per-unit true-length matrices
    extracted from the padded forward.
  - Mandatory batch-consistency startup smoke (dense-smoke
    precedent — it caught the Metal defect): fixed different-length
    texts embedded batched AND singly, per-token-vector cosine
    gate.
  - Gate discipline unchanged (caller-held permit per parse); the
    singular path remains for query embedding and the smoke's
    reference side.
  - §1.4 ceremony: prompt drafter → adversarial prompt reviewer →
    implementation agent → cargo battery → five-dimension
    adversarially-confirmed verification → fix pass (all Opus).
    ~150–200k; ~85% confidence.

#### CP fact base (recon 2026-07-17, two read-only Opus agents; line numbers may drift — names govern)

- Dense flow: `embed_passage_vector` (dense.rs:102) → `embed_text`
  (dense.rs:306-341): tokenize (`tokenize_truncated`, :421) →
  `[1, seq]` unsqueeze (:315-322) → `forward_hidden` (:323) →
  last-token narrow at `seq_len − 1` (:327-328) → L2 normalize
  (:330, :435-439) → CPU readback (:331-332). Timing spans enclose
  the readback (started :103, completed :138) and are CORRECT
  (`as_millis`); the in-session "elapsed_ms lies" finding was
  withdrawn as a log-extraction artifact.
- qwen3 `forward_hidden` (qwen3.rs:198) threads `batch_size`
  through all attention reshapes (:321-397); causal mask is a
  broadcast `(1,1,seq,seq)` upper-triangular −inf (:549-560); RoPE
  starts every row at position 0 (tensor_ops.rs:4-27); weights BF16
  (qwen3.rs:105); RMSNorm/softmax are hand-rolled Metal-safe F32
  primitives. NO padding mask exists anywhere.
- Dense builder: `build_dense_vectors` (projections/dense.rs:
  158-248); chunks read up front via `SELECT_PARSE_CHUNKS_SQL ...
  ORDER BY id` (:73-80) — ordering is for deterministic logs only;
  loop (:272-312) = embed → `validate_vector` → `encode_vector_blob`
  → INSERT; `DELETE_PARSE_DENSE_SQL` idempotent rebuild (:266);
  caller's tx; `envelope::mark_failed` + `Err` on any failure; NO
  per-item builder logs (per-item `model_call.*` is runtime-side,
  `text_count = 1usize` hardcoded at dense.rs:110 et al.).
- Blob formats are strictly per-item (`encode_f32_blob`,
  codec.rs:44-50: contiguous LE f32, no shape header; dims in
  columns) — batched production keeping per-item encode+insert is
  byte-identical. Loaders re-sort (`ORDER BY chunk_id`), so batch
  production order is irrelevant.
- Gate: caller-side everywhere; scheduler holds one permit per
  role per parse, dense and colbert blocks sequential and
  non-overlapping (scheduler.rs:2430-2474); runtimes never acquire
  (verified — only the pure predicate `uses_local_model_gate` at
  reranker_backend.rs:120).
- ColBERT: `embed_document` (colbert.rs:512) → rank-2 pipeline;
  `ColbertInputPath::forward` hard-rejects `batch_size != 1`
  (:816-821); attention per-head 2D matmuls (:1017) "to stay
  inside Candle Metal's supported operation shapes"; local
  sliding-window mask (:2058) positional only; F32 weights (:756).
  Batching = forward-path rewrite; deferred data-gated.
- Reranker (local): same batch-1 shape (reranker.rs:770-775); not
  on the ingestion path; out of CP scope.
- Annotator client: request = model + messages + temperature only
  (llm_client.rs:218-231, `PRODUCER_TEMPERATURE`); response
  `content: String` is null-intolerant (:99); no output-token
  bound anywhere (banked finding).

### Acceptance traceability (§37 → clusters)

| §37 criteria | Cluster |
| --- | --- |
| 1–5 (connectors, acquisition records, content identity, adaptive cadence, measured freshness) | C3 (per-query freshness recording: deferred with the QER audit tier) |
| 6–9 (single active parse, net-new graphs, gating, failure disposition) | C4–C5 |
| 10 (deletion lifecycle) | C3 (evidence) + C9 (propagation) |
| 11 (typed units, relationships, annotations with provenance/freshness) | C2/C4/CA |
| 12–13 (projections, evidence-only packs, visible assembly policy) | C6–C8 |
| 14–16 (QER, replay guarantees, graded claims) | Deferred post-MVP: QER audit tier (2026-07-11) |
| 17–18 (snapshots, rollback-as-restore) | C9 |
| 19 (scope enforcement, callerContext) | C7–C8 |
| 20 (config = external facts only) | D3, enforced from C2f onward |

Each is resolved with the user before the cluster that depends on it.
Resolution order: D1, D3, D6 (front-loaded, resolved), D4 (resolved),
D8 (resolved), D9 (resolved), then D2 (before C8d, finalized C10), D5
(deferred with its connector). D7 dissolved into D8 (2026-07-11).

- **D1 — Physical storage mapping. RESOLVED 2026-07-07.**
  - Hot plane: new SQLite database at `{index_root}/fabric/fabric.sqlite3`,
    schema files under `sql/fabric/`, own `PRAGMA user_version` sequence
    starting at 1, created only by the `--setup-storage` successor. Legacy
    `data-store.sqlite3` untouched beside it until C10e (2026-07-10
    restructure: the legacy plane's setup path and schema retire at CRb;
    any existing legacy database file is discarded data per §1.2).
  - Artifact store: content-addressed filesystem tree at
    `{index_root}/fabric/artifacts/sha256/<first-2-hex>/<full-hash>`;
    write-once enforced by C2c (same-hash rewrite no-op, same-hash
    conflicting bytes error); atomic temp-file+rename writes.
  - Event log: `system_events` table in the hot plane, written inside the
    owning operation's transaction/boundary discipline.
  - Connection/deadline policy: `journal_mode=WAL` set at setup and
    validated fatally at startup (never repaired at runtime);
    `synchronous=FULL` (Guarantee 1 makes lost-but-served QERs a breach);
    `busy_timeout` and statement deadlines as code constants, not config
    (§35); read paths open `SQLITE_OPEN_READ_ONLY`; `foreign_keys=ON` per
    connection; fresh connection per operation retained.
  - No config change required by D1 itself (paths derive from
    `[storage].index_root`).
  - Accepted risk: single-file writer serialization under autonomous write
    pressure; remedy if material is splitting the sync queue into its own
    database file (layout does not foreclose it).
- **D2 — API surface transition. RESOLVED in full — query surface
  2026-07-15 (auto-ruled under the spec-decides-it rule);
  administrative residual 2026-07-16 (user-ruled).**
  - Consumer query surface: §34.1 plain request/response — `POST /query`
    (spec-literal path; `GET /v1/health` retained as the operator
    readiness route), JSON `QueryRequest` (§24.3) body, one JSON
    response carrying the EvidencePack. `queryExecutionRecordId` is
    omitted until the QER audit tier lands (recorded deviation,
    commented at the response type). Citation: §34.1 "POST /query.
    Body: QueryRequest (§24.3). Response: the EvidencePack plus the
    queryExecutionRecordId."
  - NDJSON streaming for queries was excluded under the honest-option
    rule: a seconds-scale query gains nothing from streamed stage
    progress that the durable `QueryStageLatencies` log does not
    already record, and §34 defines no streamed query transport.
  - Errors: HTTP status + the existing JSON error body (`ApiError`
    `IntoResponse`); the §31.1 cutover rejection is 503 with kind
    `cutover_barrier_active` (retryable).
  - Administrative residual RESOLVED 2026-07-16 (user-ruled): §34.6
    Operation records + polling ONLY — no streamed progress on
    administrative operations; NO NDJSON survives anywhere in the
    end-state API. The retained emitter machinery, the types.rs
    OperationRequest/OperationEvent/OperationBenchmarks/BenchmarkStage
    substrate, and the CLI streaming machinery are reclassified from
    "consumed at C10a" to named deletions (C10e / the C10c rework).
    Basis: at end state administrative operations are rare operator
    overrides (the autonomous pipeline owns routine work); a poll loop
    keeps user-triggered work visibly alive; the durable service log
    remains the authoritative stage record. Honest cost, recorded:
    minutes-long admin re-parses lose live counted progress in the
    CLI (stage detail lives in the log).
- **D3 — §35 vs current configuration. RESOLVED 2026-07-07.**
  - Retained as external facts: `[server].bind_address`;
    `[logging].file_path`, `.level`; `[admin].token_file_path`;
    `[inference].device`, `.device_index`; `[storage].corpus_root`,
    `.index_root`; `[docling].python_path`, `.docling_path`, `.device`;
    all `[models.*]` paths/dimensions/token limits/backend/endpoint/
    model/api_key_file_path; `[models.reranker].timeout_seconds`
    (external-service timeout); `[server].max_request_body_bytes`,
    `.max_ingest_source_chars`, `.max_search_query_chars`
    (request-shape protections, deliberate and operator-visible, not
    capacity guesses).
  - Retained as parser configuration (identity-bearing, folded into
    `parserConfigHash`, dominance-gating trigger):
    `[docling].document_timeout_seconds`, `.pdf_backend`, `.ocr_mode`,
    `.num_threads`, `.page_batch_size`.
  - Removed → versioned policy documents: `default_top_k`, `max_top_k`,
    `rrf_k`, `candidate_overfetch_multiplier`,
    `colbert_candidate_pool_size`, `reranker_candidate_pool_size` → the
    hashed RetrievalProfile (C7a); `min_search_unit_chars`,
    `max_unit_tokens` → chunker configuration under `chunkerConfigHash`
    (C6b). Config may hold the path to the active profile document.
    Removals land at those cutover clusters; keys stay while legacy
    pipelines still read them. (2026-07-10 restructure: the legacy
    readers die at CR, so key removal moves to CRc with the current
    values banked in the CR cluster plan for C7a/C6b.)
  - Removed → superseded or code constants: `max_in_flight_ingest`
    disappears with operator-driven ingest (adaptive scheduler owns load);
    `max_in_flight_search` becomes a code constant beside the admission
    gate (fail-fast retained, value not operator-tunable).
  - `[client]`: single shared config file retained; the server config
    struct gains an explicit validated `[client]` section documented as
    client-owned, enabling `deny_unknown_fields`.
  - Service-root resolution: compile-time `CARGO_MANIFEST_DIR` removed
    (service and CLI); relative paths resolve against the config file's
    parent directory.
  - `deny_unknown_fields` adopted on every config struct at C2f.
  - New sections arrive with their clusters (diffs approved then), e.g.
    `[connectors.filesystem]` with `governance_domain` (C3).
- **D4 — ColBERT disposition. RESOLVED 2026-07-11.** Keep. ColBERT
  persisted token vectors become a `multi_vector` projection in the MVP
  (C6e) with the MaxSim stage retained in the pipeline (C7c). The
  retained inference API (`embed_document`/`score_persisted_candidates`)
  is the producer; the fabric codec replaces the discarded legacy
  flattened-blob plane. Decided as part of the 2026-07-11 MVP rescope
  (multi-vector pulled forward).
- **D5 — Second connector target** (deferred; before its own package,
  post-C3). Which external API-based source system gets the
  incremental-detection connector.
- **D6 — Docling typed-output mode. RESOLVED 2026-07-10.**
  - Decision: the C4c PDF parser worker consumes Docling `--to json`
    (DoclingDocument JSON) as its source of typed candidate units.
    DocTags rejected: bespoke VLM markup needing a custom parser, with
    coordinates quantized to a 0–500 grid — unfit for audit-grade §17
    locators. Markdown remains excluded as a candidate-unit source per
    §12.2; it may ride along as an optional derived-view artifact
    (Docling accepts multiple `--to` flags per run) — decided at C4c/C6d
    plan time.
  - Verified 2026-07-10 against the installed CLI: Docling 2.93.0
    (docling-core 2.74.1) supports `--to json|doctags|...`, and
    `json_docling` is an accepted `--from` input format — the JSON export
    is Docling's own round-trippable canonical serialization, not a lossy
    view.
  - Seam confirmed small: `--to md` hard-coded in `build_docling_args`
    (`src/docling.rs:223-225`); markdown-specific code is limited to
    artifact discovery (`find_markdown_artifact`),
    `read_and_normalize_markdown`, and the `markdown`/`markdown_path`
    result fields. Stderr progress parsing, timeout/kill, and workspace
    handling are format-agnostic. Legacy markdown consumers (`units.rs`,
    `storage.rs`, `operations/ingest.rs`) stay untouched; the JSON path
    lands inside the C4c worker behind the §12 boundary.
  - Docling JSON schema drift across upgrades is absorbed by §12 rule 3
    (parser upgrades create net-new canonical graphs); exporter identity
    is captured in `parserVersion`/`parserConfigHash`.
  - Sample conversion observed 2026-07-10 (approved run; residual risk
    closed): `index/docling-conversions/conversion-sample-d6/
    Attention_Is_All_You_Need.json` — `schema_name: DoclingDocument`,
    `version: 1.10.0`, emitted by Docling 2.93.0 in 17s. Facts C4c
    relies on: typed arrays `texts` (labels observed: text, list_item,
    section_header (+`level`), page_footer, page_header, caption,
    footnote, formula), `tables` (`data.num_rows`/`num_cols`/
    `table_cells` with `row_span`/`col_span`, start/end row/col offset
    indices, `text`, `column_header`/`row_header` flags), `pictures`,
    `groups` (label `list`), reading-order `body.children` `$ref` tree,
    separate `furniture` content layer; `captions: [{$ref}]` arrays on
    tables/pictures give caption pairing as explicit edges; per-item
    `prov[]` = `page_no` + `bbox {l,t,r,b}` object (BOTTOMLEFT
    coordinates; §17 locator wants a 4-array — transform in C4c) +
    `charspan`; `pages` keyed by page number with `size.width/height`;
    `origin.binary_hash` is a cross-check only (the core computes its
    own sourceHash; producers are untrusted). Multi-entry `prov[]`
    marks cross-page items (the continues_on/appears_on trigger).
- **D7 — Summary projection producer. DISSOLVED 2026-07-11 into D8.**
  The summary projection (C6d) materializes CA summary annotations;
  choosing its producer is part of D8's producer/type selection.
- **D8 — Annotation producers and MVP annotation-type set. RESOLVED
  2026-07-13** (three rulings, user-approved individually):
  - Types: `{entity, relation, summary}` only — every MVP type has a
    named consumer (C6f graph, C6d summary); other types are additive
    post-MVP producer modules, no schema or design change.
  - Producers: ONE external OpenAI-compatible chat-completions endpoint
    serves all three, as prompt-based producers over one shared client,
    following the HTTP-reranker discipline (exclusive, explicit failure,
    bounded diagnostics, no fallback). Local generative runtime rejected:
    the codebase has encoder-only inference and building generation in
    Candle would dwarf CA. The accepted Guarantee-4 gap (external calls
    before the audit tier) stands as recorded at the 2026-07-11 rescope.
    Config is external facts only: `[models.annotator]` endpoint, model,
    timeout_seconds, optional api_key_file_path, max_input_chars.
  - Memoization: ALL three producers eligible, made sound by the binding
    CAb input-purity contract (prompt content = ordered target-unit text
    only). The summary producer's target IS the whole document, so the
    §21.3 "whole-document-context summary" ineligibility example does not
    apply — the context is the target and is covered by the composite
    content hash. Producer identity (model, endpoint, prompt,
    max_input_chars) is inside the memo key derivation, so any identity
    change invalidates reuse automatically.
- **D9 — Graph channel query-time semantics. RESOLVED 2026-07-14**
  (four rulings, user-approved individually):
  - **Entry**: lexical match of query text against stored
    entity-annotation names at candidate generation; scope filtering
    applies at the entity lookup; no LLM call in the query path
    (query-time extraction rejected: adds a non-deterministic external
    dependency to the hot search path without removing the name-matching
    step).
  - **Edges**: semantic-only traversal — matched entities' target units,
    relation annotations touching those entities, and the far-end
    entities' target units. Structural UnitRelationships are never
    walked at query time; structural context is C8 AssemblyPolicy's job.
    Entity node identity is the normalized entity name (entityType is
    node metadata, not identity); shared entity names are the only
    cross-source connectivity, per §19.
  - **Timing**: traversal happens at query time over indexed hot-plane
    lookups; no materialized closures (resolution, not a decision —
    precompute's only benefit is eliminating already-cheap lookups,
    while adding staleness/rebuild machinery trailing the
    post-activation annotation worker). Hop budget = 1 relational hop,
    recorded as a C7a RetrievalProfile value.
  - **Ordering** (fusion is rank-only, so within-channel order is the
    entire score): tier 1 = units connected to more than one matched
    entity; tier 2 = direct mentions; tier 3 = one-hop related units;
    within tiers by entity-name match strength; deterministic tiebreak
    by unitId ascending.
