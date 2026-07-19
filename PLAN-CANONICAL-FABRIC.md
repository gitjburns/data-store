# PLAN: Canonical Content Graph and Retrieval Fabric

Reference specification: `canonical_content_graph_retrieval_fabric_v_0_3.md`
(all `§` references below point into that document).

## Current Status

Completed work — the full session-by-session record (2026-07-05 through
the 2026-07-18 CPc-attempt-2 session) was relocated verbatim to
`PLAN-HISTORY.md` (2026-07-18, user-approved); date-cited entry references
resolve there. A second-stage relocation (2026-07-18, user-approved)
retired the completed planning corpus — §1.5–1.6, §2, the §3 spine and
completed cluster texts C1–C10e with all fact bases, CPa/CPb/CPd, the
acceptance table, and the §4 full decision texts — verbatim to the same
file's "Retired planning sections" part; §4 now carries condensed rulings
indexes pointing there by grep anchor. One-line ledger:

- C1 (2026-07-07): seam cuts — execute pipelines out of `http.rs`, pure
  primitives extracted, util unification.
- C2 (2026-07-07): substrate — canonical serialization/hashing, ids,
  artifact store, model types, fabric schema + hot plane, events; D1/D3
  resolved; C2f config rework.
- C3 (2026-07-10): acquisition — filesystem connector, importer,
  coalescing sync queue + knob-free adaptive scheduler; D6 resolved.
- C4 (2026-07-10, two sessions): parsing — §12.2 bundle contract, §13.1
  import gates, conformance, PDF/text workers, Docling JSON path.
- Restructures (2026-07-10/11): end-state-only offline development; MVP
  rescope (QER audit tier deferred; annotations/multi-vector/graph pulled
  into MVP; cluster CR added; 2026-07-15 amendment re-deferred the
  multi_vector CHANNEL).
- CR (2026-07-11): legacy retirement — `src/operations/`, `storage.rs`,
  legacy schema and config keys deleted; retained substrate allow-swept.
- C5 (2026-07-11): activation lifecycle — dominance gating per the union
  ruling, cutover barriers, drain-loop parse dispatch chain.
- CA (2026-07-13): semantic annotations + memoization — store, producers,
  LLM client, memo cache, §21.4 policy, worker thread; D8 resolved.
- C6 (2026-07-14): retrieval projections — chunk/lexical/dense/
  multivector/derived-view builders, dense cache, graph projection; D9
  resolved.
- C7 (2026-07-15): retrieval fabric — sealed RetrievalProfile, dense/
  lexical/graph channels, MaxSim + reranker stages, `execute_query` DP1
  snapshot discipline.
- C8 (2026-07-15): assembly + EvidencePack — sealed AssemblyPolicy, §25
  operators, evidence builder, query envelope, `POST /query`; D2 (query
  side) resolved.
- C9 (2026-07-16): lifecycle forensics — snapshots, verification tiers,
  archive-verify-delete, restore, deletion lifecycle, application
  identity.
- C10s/C10r/C10a (2026-07-16): Operation substrate, HeldSupersession
  cleanup + queue-coupled completion, full §34 HTTP surface with live
  bearer auth.
- C10b/C10c (2026-07-17): health count slots; CLI full rework (polling
  model, typed renderers).
- C10d (2026-07-17): documentation re-baseline — all seven operator docs
  fully replaced from live code reads.
- C10e (2026-07-17): final legacy sweep (NDJSON emitter, markdown path,
  `units.rs`) + the cluster-remainder verification pass; both C10f
  pre-planning rulings implemented.
- C10f (2026-07-17, IN PROGRESS): commissioning — R0–R3 + graceful
  shutdown complete (first cycle 14/14 activated, zero failures); R4–R9
  pending after CPc.
- CP (2026-07-17/18, interim): CPa dense batching reverted (measured
  regression; the candle-Metal `.contiguous()` fix kept permanently);
  F16 rejected (non-finite activations); CPb thinking opt-out VERIFIED
  live; CPd ColBERT batched document embedding complete incl. the
  NaN-sanitize `where_cond` fix and the length-threshold hybrid; CPc
  full re-ingest still pending.

- 2026-07-18 (CPd hybrid session): the CPd length-threshold HYBRID
  ruled (Option A, user-approved 2026-07-18) and COMPLETE —
  implemented main-loop per the approved plan (small ruled fix;
  CPc-session precedent, a disclosed §1.4 deviation including this
  status entry), verified by the cargo battery plus ONE Opus
  refute-by-default review agent over the diff (the
  focused-confirmer precedent) in place of the full five-dimension
  ceremony — the change only routes between two already-validated
  embed paths.
  - multivector.rs: new code constant
    `COLBERT_BATCH_ROUTE_MAX_TOKENS = 128` (engineering-fact
    comment citing the 2026-07-18 interim length-bucketed
    measurements — 4.4x near 16 tokens, break-even ~130, 1.8x
    slower at the 512 cap; revisit against CPc full-corpus data);
    `build_rows` counts each unit's raw-text ColBERT tokens once
    (`count_document_tokens` — same tokenizer call as the C6b chunk
    builder, failures unit-id-attributed via
    `ApiError::InferenceInit`, the runtime's own
    tokenization-failure class), partitions at the threshold,
    embeds long units through the singular `embed_document`
    (per-unit `model_call.*` pairs return for exactly those units)
    and packs short units into 16-doc `embed_documents` windows
    sorted by the routing token counts — retiring the banked
    byte-length proxy sort; per-unit validate→encode→INSERT
    extracted into shared `persist_unit_matrix` (persistence
    byte-identical on both paths); `multivector_build.completed`
    gained `batched_unit_count`/`singular_unit_count` (per-path
    benchmark attribution; the failed arm omits them — routing may
    not have completed at that boundary).
  - colbert.rs: `embed_document` returned to production — stale
    `#[allow(dead_code)]` removed, doc rewritten (long-unit hybrid
    role + diagnostic-bin consumer + pinned byte-compatibility
    reference). No behavior edits in this file.
  - Verification: review checklist all clean (exactly-once routing
    coverage, post-sort indexing, persistence fidelity, gate
    discipline, empty-pool edges, log-field safety, scope
    containment), 12 candidates refuted, ONE MED finding CONFIRMED
    and fixed: the embed paths tokenize the prompt-PREFIXED text
    (`"search_document: [D] " + text`, colbert.rs
    `format_document`), so the embedded sequence runs a fixed few
    tokens longer than the routing count and the original comments
    falsely claimed the two counts always agree. The bias itself is
    benign (a constant ~5-token shift at the gentle break-even; the
    constant delta preserves the packing sort order exactly) —
    fixed as comment corrections at four sites recording the bias
    honestly; counting the prefixed text instead was excluded as
    disproportionate coupling to the runtime's private prompt
    format (recorded routing-boundary bias, threshold unchanged
    at 128).
  - Residual risk (CPc): the hybrid routing is compile-verified
    only; first live execution is the CPc re-ingest, where the
    unchanged batch-consistency smoke still gates the batched path
    at startup and the new per-path counts make stage time directly
    attributable per path.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`.
  - Next: rule on the annotation-worker write-lock starvation
    finding, then the CPc full re-ingest (FRESH approval needed for
    the `index/fabric/` deletion — the plane still holds partial
    aborted cycle #2), then discussion B (parse/embed overlap),
    then C10f runs R4–R9.

- 2026-07-18 (lock-starvation ruling session): the annotation-worker
  WRITE-LOCK STARVATION finding RULED (Option B, user-approved
  2026-07-18 — quiet pre-paid deferral + shutdown-bounded post-paid
  wait; Option A, moving model compute outside the projection-build
  write transaction, BANKED as a post-CPc structural candidate whose
  case notes the same starvation hits C10a admin-Operation
  `insert_pending` writes during bulk ingest) and COMPLETE —
  implemented by one Opus agent-serial package per §1.4, verified by
  one Opus refute-by-default review agent (the CPd-hybrid
  focused-confirmer precedent) plus a directed fix pass.
  - hot_plane.rs: new `WriteTransactionAttempt` +
    `begin_write_transaction_if_free` — SQLITE_BUSY
    (`ErrorCode::DatabaseBusy` after busy_timeout expiry) is
    classified as a typed INFO `Busy` outcome (the authoritative
    writer-contention signal; a scheduler-published in-progress flag
    was excluded as a hand-maintained parallel copy of lock truth);
    every other error keeps the exact existing ERROR +
    `StorageOperation` mapping. `begin_write_transaction` and all
    its callers untouched.
  - annotations/worker.rs: two-category boundary policy (module
    header documents it). PRE-PAID boundaries (`memo_remint`,
    `build_open`, the annotation-derived `projection_build`) defer
    quietly on Busy — no producer call, `deferred` count, ONE
    `annotation_worker.cycle_deferred` INFO, cycle build work ends
    early via typed `BuildFlow` outcomes (never a sentinel error);
    stateless discovery re-finds the work next cycle. POST-PAID
    boundaries (`build_complete`, `build_fail`) retry in a
    shutdown-bounded wait loop — each attempt rides the 5 s
    busy_timeout, `completion_waiting` INFO ~every 5 min, on
    shutdown a `completion_abandoned_shutdown` WARN falling back to
    the existing crash-orphan adoption path; NO retry-count bound —
    the shutdown signal is the bound (knob-free, deliberate).
    Shutdown probe = `ShutdownSignal::wait_timeout(Duration::ZERO)`.
    `record_projection_build_failure` unchanged (already
    best-effort).
  - state.rs: `AnnotationCycleCounts.deferred` added and threaded
    through the cycle summary log, the health publish, and the
    operator health render.
  - Verification: 12 candidates refuted, TWO findings CONFIRMED and
    fixed: (HIGH) the `build_open` boundary deferred silently — no
    count, no log, with comments asserting otherwise — now
    counted+logged at the single `build_work_item` choke point,
    comments corrected; (MED) post-paid shutdown-abandon returned
    `Continue`, so a fresh PAID producer call could start after a
    shutdown request when the writer lock raced free — new
    `BuildFlow::ShutdownAbort` variant ends the cycle, deliberately
    distinct from `Deferred` so a shutdown event is never
    misattributed as lock contention in the deferred count.
  - All checks green, zero warnings: `cargo fmt`, `cargo check`,
    `cargo check --features metal`, `cargo clippy`,
    `cargo clippy --features metal`.
  - Residual risk (CPc): compile-verified only; the contention
    reproduces naturally at the CPc re-ingest, where the `deferred`
    count and quiet-cycle logs become directly observable (and the
    previous every-30s ERROR pairs must be ABSENT from the run's
    log era).
  - Next: the CPc full re-ingest (FRESH approval needed for the
    `index/fabric/` deletion — the plane still holds partial aborted
    cycle #2; release rebuild required first), then discussion B
    (parse/embed overlap), then C10f runs R4–R9.

- 2026-07-19 (HTTP-dense-backend session): CPc attempt 3 ran cleanly
  to ~5/14 sources on a fresh plane (approved deletion +
  `--setup-storage`; a first start at 06:06Z died from an operator
  pipe — the background-mode startup relay chain dies on SIGPIPE if
  its stdout reader closes early, taking the service child with it;
  do not pipe `start.sh` through short-lived readers — clean restart
  06:09Z). Startup batch-consistency smoke PASSED (CPd batched path
  admitted live). Ruling-B OBSERVED WORKING on both boundary
  categories: a real annotator outage (endpoint down 06:32–07:01Z,
  163+ status-none ~1 s failures — the endpoint itself, curl failed
  identically; NOT Little Snitch) parked builds failed with per-call
  attributed ERRORs and clean recovery on restore, and the post-paid
  `completion_waiting` loop rode a ~16-minute writer-lock hold
  (scheduler dense projection build) at the designed ~5-min INFO
  cadence with ZERO error pairs — live evidence for the banked
  Option A structural candidate. Partial measurements: local
  passage_embedding avg 1,678 ms (63% of wall through 5 sources);
  CPd ColBERT hybrid ~4.8 min total (383 batched windows avg 259 ms
  + 1,469 singular). Run KILLED at user direction (SIGTERM + orphan
  Docling child) for the HTTP-dense pivot, user-ruled: implement the
  recorded largest-structural-lever HTTP dense backend NOW,
  re-benchmark after, then a Docling-parallelism design pass
  superseding discussion B; C10f R4–R9 after that.
- 2026-07-19 (same session, package): HTTP dense-embedding backend
  COMPLETE — user-approved design: reranker-pattern exclusive
  Local/Http enum split (`DenseEmbeddingBackend`, NEW
  src/inference/dense_backend.rs), OpenAI-compatible /v1/embeddings
  client (per-`index` reorder, per-vector dimension/finite/
  nonzero-norm validation, client-side L2 normalize, bearer key
  loaded once and never logged), batched `embed_passage_vectors`
  with builder windows of `DENSE_HTTP_BATCH_SIZE = 32` (code
  constant, engineering-fact comment, pending re-benchmark), prompt
  formatting single source of truth
  (`format_dense_query_text`/`format_dense_passage_text` +
  `DENSE_SMOKE_TEXT` in inference/dense.rs — the HTTP path sends
  byte-identical text to what the local runtime tokenizes),
  backend-aware caller-side gating (`uses_local_model_gate`; HTTP
  acquires NOTHING, never held across HTTP I/O; local path
  bit-identical incl. batch-1), http startup smoke replacing the
  local artifact/model load, honest per-backend identity capture
  (key PATH only), `model_call.*` diagnostics with no
  payloads/vectors/secrets. Config (main-loop): [models.dense]
  backend discriminator + exclusive per-backend validation mirroring
  [models.reranker]; config.example.toml updated in kind. One Opus
  agent-serial package; one main-loop config.rs miss (stale
  unconditional dense.path absolute-path check) agent-surfaced and
  fixed. Verified by one Opus refute-by-default review (24
  candidates, 23 refuted, 1 LOW confirmed+fixed: pre-existing stale
  `#[allow(dead_code)]` + future-tense comments on the local embed
  methods, made live-consumed by this package). All checks green,
  zero warnings: cargo fmt, check, check --features metal, clippy,
  clippy --features metal.
- 2026-07-19 (re-benchmark start): dense endpoint model RULED
  (user): Qwen/Qwen3-Embedding-4B AS SERVED on
  http://10.1.0.10:8001 (vLLM), dimension 2560 — a deliberate
  model+hardware change; the dense-stage comparison against the
  149.6-min local-8B baseline conflates the two, and retrieval
  quality shifts to 4B's (recorded). config.toml switched to
  backend="http" (dimension 2560, timeout 60 s, no key); release
  rebuilt; fabric plane deleted (fresh approval consumed) +
  `--setup-storage`; run STARTED 2026-07-19T08:20:46Z with
  dense_http_smoke PASSED live (dimension + unit norm validated
  against the server). Run in progress at entry time; metric
  extraction and before/after comparison follow at run end
  (baseline: 229.7-min cycle, dense 149.6 min, plus the attempt-3
  partials above).

- 2026-07-19 (re-benchmark result + invalidation): the 08:20Z cycle
  COMPLETED in 69.8 min, 14/14 activated, zero failures — 3.3× vs
  the 229.7-min baseline (dense 149.6 → 1.5 min ≈ 100×, 164 HTTP
  batches; ColBERT 30.2 → 15.1 min = 2.0× via the CPd hybrid, 86%
  of units batched; Docling 47.5 min unchanged, now 68% of cycle).
  The post-cycle annotation backfill then exposed the LATENT SERIAL
  DESIGN CEILING: one worker thread, one blocking producer call in
  flight, ~6 calls/min against a continuous-batching server — 2 of
  14 sources examined after 71 min, 5–10 h projected. USER-RULED:
  run INVALIDATED, service killed mid-backfill. Recorded
  corrections: no regression occurred (the annotator was always
  remote+serial; this was its first full-volume run), and the
  morning's 27B server crash was external misconfiguration
  (one-time, fixed, unrelated to client load — earlier
  contention/dead-but-listening attributions were WRONG and are
  retracted; the era-conflation in the 120 s-timeout size table is
  likewise corrected: today's timeouts were all large-input
  marginal-budget cases).
- 2026-07-19 (concurrency package): bounded concurrent HTTP
  dispatch COMPLETE — dense builder fans out batch windows
  (`DENSE_HTTP_CONCURRENT_REQUESTS = 8`, scoped threads, vectors
  persisted serially in chunk order, failed window fails the build
  with nothing persisted); annotation worker fans out producer
  calls in waves (`ANNOTATOR_CONCURRENT_CALLS = 32`;
  prepare/dispatch/commit split with ALL SQLite writes serial on
  the worker thread; ruling-B BuildFlow semantics preserved;
  shutdown probe before each wave, `wave_abandoned_shutdown` WARN).
  One Opus agent-serial package + refute-by-default review: 24
  candidates, 23 refuted, 1 HIGH CONFIRMED — the dense HTTP fan-out
  runs under the scheduler's IMMEDIATE write transaction (writer
  lock across network I/O, the ruling-B hazard class). Ruled: a
  PRE-EXISTING property of the take-the-caller's-tx contract, made
  strictly shorter by waves, ACCEPTED for now and documented
  honestly in dense.rs (comment fix applied); the structural fix is
  the already-banked Option A (embed before the transaction — the
  pattern the worker waves follow). Worker path fully clean.
  Battery zero warnings.
- 2026-07-19 (commercial endpoint switch, user-directed): BOTH
  models moved to OpenRouter. config.toml: [models.dense]
  endpoint https://openrouter.ai/api/v1/embeddings +
  api_key_file_path .data-store-dense-api-key (new owner-only
  secret, user-gitignored); [models.annotator] endpoint
  https://openrouter.ai/api/v1/chat/completions, model
  openai/gpt-5-nano, api_key_file_path .annotator-api-key.
  Startup shakeout, in order: 401 (an OpenAI key had been placed;
  client exonerated by identical curl behavior), 404
  data-policy (user relaxed OpenRouter privacy settings; the
  "OpenRouter has no embeddings" claim from the models-catalog
  probe was WRONG — the catalog just omits embedding models),
  transient 429 engine_overloaded → USER-RULED bounded 429-only
  retry in the dense client (separate package: 3 retries, 2/4/8 s
  backoff, `model_call.http_retry` WARN per attempt,
  `retried_attempts` on terminal logs; amends the dense package's
  original no-retry policy by explicit user instruction; the
  original implementation agent correctly HALTED on the pinned-
  requirement conflict and the package was re-dispatched with a
  self-consistent spec). Embedding model test (user-selected
  candidates, 5×32-batch probes each): qwen/qwen3-embedding-8b
  5/5 OK dim 4096 avg ~3.5 s; openai/text-embedding-3-small 5/5
  OK dim 1536 ~1.3 s. USER-RULED: qwen/qwen3-embedding-8b —
  restores the corpus's original 8B model family; dimension back
  to 4096.
- 2026-07-19 (commercial run + timeout ruling): clean-corpus run
  STARTED 20:45:41Z (fresh plane; smoke passed against OpenRouter).
  gpt-5-nano first live exposure surfaced the same marginal-budget
  timeout class as the 27B: max-input calls die at the 120 s
  whole-request budget (observed as HTTP 200 with body read
  truncated at ~120.3 s). USER-RULED (with the acknowledgment that
  the raise should have been carried through at the endpoint
  switch): [models.annotator].timeout_seconds 120 → 300; config is
  startup-only so the service was restarted 21:03:21Z. VALIDATED
  live post-restart: the 24,500-char entity call completed at
  122.2 s and the 10,950-char relation call at 141.4 s — both
  impossible under 120 s; 58 completed / 3 failed in the first
  window; one HTTP-200-truncated-body transient (~1/59) accepted
  as park-and-retry noise. Run IN PROGRESS at entry time.
- 2026-07-19 (Docling parallelism design, decisions resolved;
  package NOT yet approved): streaming parse pool — the
  parallelizable half is pre-dispatch guards + the Docling child
  (staging-only); the canonical half (import/gates/projections/
  activation/queue completion) stays serial on the scheduler
  thread. Pool admission is DYNAMIC on both axes from observed
  signals, never a constant or config knob (USER-RULED: deployment
  target is a generously specced server with much larger corpora;
  scale assumptions like "14-document scale" are wrong and were
  retracted): memory admission = available system memory vs a
  per-child working-set EMA seeded from the activity monitor's
  measured RSS (~5.8 GB observed); CPU admission = observed
  utilization headroom vs a per-child CPU-demand EMA (USER
  correction: the proposed cores÷num_threads ceiling presumed
  thread saturation — measured child CPU ≈ 97% of ONE core —
  and was rejected as a §35-class guess). Streaming completion
  (no wave barrier — 20× per-doc duration variance). Shutdown
  terminates in-flight children (staging is crash-safe; partially
  retires the banked shutdown-latency item for the parse stage).
  Remote Docling (docling-serve-class) recorded as a legitimate
  future tier on its own merits — the earlier scale-based
  exclusion rationale is RETRACTED; Docling stays local for now
  per user's reasons. Honest scale note: at large admitted pools
  the serial import half becomes the binding constraint; seams =
  banked Option A, then parallel projection builds (future tier).
- 2026-07-19 (documentation-currency ruling, user): standing
  process rule — a package that changes operator-visible surfaces
  or architecture-documented behavior carries its doc updates in
  the same package, verified in the same review (supersedes the
  C10d-era "docs re-baseline once" posture now that surfaces are
  live). Drift status: PLAN caught up by this entry set; the seven
  operator docs are being audited against live code (SPEC-SERVER.md
  confirmed as the living as-built server spec and update target;
  canonical_content_graph_retrieval_fabric_v_0_3.md is the
  normative reference and is NEVER updated). COMPLETE same
  session: audit found PROTOCOL/INTERACTIVE/SPEC-CLIENT clean and
  cited drift in README (1 stale/2 missing), INSTALL (1/2),
  ARCHITECTURE (2/7), SPEC-SERVER (2/1); all four rewritten from
  code-verified claims (two Opus agents, disjoint files; extra
  finds fixed: INSTALL's [inference].device in-process scoping;
  ARCHITECTURE gained the honest writer-lock-across-fan-out
  caveat); refute-by-default review over the edits: 14 candidates,
  14 refuted, ZERO findings, cross-doc constant/event-name
  spelling verified identical to code, no doc overclaims on the
  known empty-marker bug. Observation banked: .data-store-rerank-
  api-key appears in config.example.toml but not .gitignore — add
  it if a reranker key is ever provisioned.

- 2026-07-19 (empty-marker consumer bug, RULED): the commercial run
  surfaced a LATENT C6-era defect — the annotation worker records
  an empty producer result as a fresh annotation row with body `[]`
  BY DESIGN (worker.rs `complete_fresh` empty-marker convention:
  the freshness key stays satisfied, no per-cycle rebuild), but the
  graph projection builder requires a string `name` body field on
  every entity annotation and fails the whole source's
  annotation-derived projection build on the first marker row —
  a deterministic per-cycle rolled-back retry loop. First trigger:
  gpt-5-nano legitimately returns `{"entities":[]}` on small/table
  unit groups (38 of 3,058 entity rows; 2 sources poisoned); the
  prior 27B never returned empty, so the mismatch stayed latent.
  The relation consumer path has the identical hazard (`[]`
  relation markers — the "issue 1" ~16-char responses are exactly
  these). USER-RULED: fix is the FIRST SCOPE OF WORK next session,
  before CPe — consumer-side skip of `[]` marker rows in the
  annotation-derived projection builders (entity AND relation
  paths), visibly counted on the build log (no silent narrowing),
  comments cross-referencing the worker marker convention; no
  producer change, no data cleanup (marker rows are valid);
  poisoned sources self-heal on the post-fix restart's worker
  cycles. FOLLOW-UP RULING (same session): the bug INVALIDATES the
  commercial-endpoint test — every source with any empty
  entity/relation result poisons its own graph build, so the
  annotation chain can never complete on this binary. Test ABORTED
  (SIGTERM ≈21:25Z, 4 sources completed post-restart; clean stop,
  no orphans). The test resumes ONLY after CPd2 — as a fresh
  clean-corpus run (fabric deletion will need its named approval
  then).

## Handoff (written 2026-07-10, offline-development restructure;
supersedes the 2026-07-10 session-end handoff; amended 2026-07-11 for
the MVP rescope, 2026-07-11 after C5 completion, 2026-07-13 after CA
completion, 2026-07-14 after C6 completion, 2026-07-15 after C7
completion, 2026-07-15 after C8 completion, 2026-07-16 for C9 plan
approval, 2026-07-16 after C9 completion, 2026-07-16 for the C10
plan approval, 2026-07-16 after the C10s/C10r/C10a implementation
session, 2026-07-17 after the C10b/C10c implementation session,
2026-07-17 (later) after the C10d documentation session, 2026-07-17
after the C10e legacy-sweep session, 2026-07-17 after the
cluster-remainder verification pass, 2026-07-17 after the C10f
pre-planning rulings, 2026-07-17 after C10f commissioning
session 1 with the CP cluster-plan approval, 2026-07-17 after the
CPa/CPb implementation session, and 2026-07-18 after the
CPc-attempt / Metal-defect-fix / CPa-revert / F16-rejection /
CPd-approval session, and 2026-07-18 after the CPd implementation
session, and 2026-07-18 after the CPc-attempt-2 /
NaN-sanitize-fix / CPb-verification session, and 2026-07-18 after the
CPd hybrid session and the plan-file restructure — completed status
entries relocated verbatim to PLAN-HISTORY.md; see Current Status —
and 2026-07-18 after the lock-starvation ruling-B session, and
2026-07-19 after the HTTP-dense-backend session with the CPc
attempt-3 kill, the 4B ruling, and the re-benchmark start, and
2026-07-19 (later) after the invalidation ruling, the concurrency
package, the OpenRouter switch with the 8b ruling and 429 retry,
the 300 s annotator-timeout ruling, the Docling-parallelism design
resolutions, and the documentation-currency ruling)

State for the next session picking this up:

1. **Operating premise (user-ruled 2026-07-10)**: end-state-only
   development. The app does not need to start, serve, or be functional
   at any point until the programme completes (§1.2). No between-phase
   operability, no legacy behavior preservation, no per-cluster runtime
   verification — all runtime verification consolidates at C10f
   commissioning. The cargo battery (zero warnings) and the
   adversarially-confirmed verification workflows remain mandatory per
   package.
1a. **Next work**: C10f (commissioning). ALL implementation packages
   are COMPLETE — C10s, C10r, C10a (2026-07-16 entry), C10b, C10c
   (2026-07-17 entry), C10d (2026-07-17 later entry), C10e
   (2026-07-17 C10e-session entry) — and the CLUSTER-REMAINDER
   five-dimension verification pass over the C10s–C10e diff is DONE
   (2026-07-17 cluster-remainder entry: spec/diagnostics/integration
   finders clean; its two confirmed findings — the ids.rs stale
   allow and the docling.rs stale markdown-path comments — are
   fixed). All seven re-baselined docs are user-approved; the NAMED
   config comment edits are done. The GET /sync/status counts
   projection was decide-or-drop and is DROPPED (ruled 2026-07-17).
   Both C10f-planning rulings are RESOLVED and implemented
   (2026-07-17 C10f pre-planning entry): the three `ImportOutcome`
   fields are DELETED, and operator-URI containment is a lexical
   HTTP-boundary prescreen on POST /sources (400 `source_resolution`;
   PROTOCOL.md updated). No open rulings block C10f planning.
   COMMISSIONING IS UNDERWAY (2026-07-17 commissioning session 1
   entry): the R0–R9 run sequence is approved; R0–R3 plus a graceful
   shutdown are COMPLETE (first cycle: 14/14 sources activated, zero
   failures, ~3 h 50 m). The 2026-07-18 session then: aborted the
   first CPc attempt (smoke-caught candle-Metal stride defect —
   FIXED permanently via `repeat_kv_heads` `.contiguous()`; then a
   measured batching regression — user-ruled kill), REVERTED CPa
   (no batching win exists for the 8B dense model on this GPU),
   evaluated and REJECTED F16 (non-finite activations under the
   real model; BF16 retained, verdict at `DENSE_COMPUTE_DTYPE`),
   and gated IN + APPROVED package CPd (ColBERT batched document
   embedding, 2.43× measured; full scope and design constraints in
   §3). CPb remains live and still unexecuted. CPd is now COMPLETE (ColBERT
   batched DOCUMENT embedding, implemented and verified 2026-07-18
   — see the 2026-07-18 CPd-implementation-session Current Status
   entry). CPc attempt 2 (2026-07-18 entry) then caught and FIXED a
   CPd sanitize formulation defect (IEEE `NaN * 0.0 = NaN`; now a
   `where_cond` select, smoke-validated live), VERIFIED CPb PASS
   (entity annotation path end-to-end: 2.5–7 s typical calls, 43
   fresh rows, memo + orphan adoption exercised), and measured CPd
   interim performance: net ~25% stage gain, crossover ≈130 tokens.
   The length-threshold HYBRID was then ruled (Option A) and is
   COMPLETE (2026-07-18 CPd-hybrid-session entry, retained in Current
   Status: batch units ≤128 routing tokens, singular path above;
   verified by one Opus refute-by-default review in place of the full
   ceremony). The annotation-worker write-lock starvation finding
   is RULED (Option B) and COMPLETE (2026-07-18 lock-starvation
   entry, Current Status: quiet pre-paid deferral +
   shutdown-bounded post-paid wait; Option A — model compute
   outside the projection-build write transaction — banked as a
   post-CPc structural candidate). The immediate next work is:
   (1) the COMMERCIAL-ENDPOINT clean-corpus run in progress
   (restarted 2026-07-19T21:03:21Z — OpenRouter qwen/qwen3-
   embedding-8b dense + gpt-5-nano annotator, concurrent
   dispatch, 300 s annotator timeout; first run of the full
   annotation chain at usable throughput), watched to annotation
   quiescence, then metric extraction — SUPERSEDED same session:
   the empty-marker bug invalidated the test and it was ABORTED
   (user-ruled; see the empty-marker Current Status entry);
   (2) the operator-doc drift remediation (audit complete; all
   four drifted docs rewritten; refute-by-default review pass is
   the remaining step). NEXT SESSION, in ruled order: (3) CPd2
   empty-marker consumer fix (FIRST scope, user-ruled 2026-07-19);
   (4) the commercial-endpoint clean-corpus test RE-RUN (resumes
   only after CPd2; fresh plane — fabric deletion needs its named
   approval then; benchmark of record + first complete annotation
   chain); (5) R4 query testing (user-sequenced: queries proven
   before Docling implementation); (6) CPe Docling parse pool
   (APPROVED — see §3). The parse/embed-overlap discussion B is
   superseded by the CPe design. C10f runs
   R4–R9 resume within/after (5) and still include the C9 runtime
   verification set (mint/verify/cleanup/deactivate/restore cycles),
   a held-candidate cleanup cycle (ruling 1), the async-Operation
   admin surface incl. the Option A force-stage path, the CLI
   poll-loop and renderer surface (never executed), the C10b health
   counts under real cycles, and the multi-vector overlap diagnostic
   (2026-07-15). Sequence after C10: the programme's MVP completes;
   the QER audit tier and other §5 tiers follow post-MVP. D5
   deferred. (Per the 2026-07-16 ruling, the §1.4 ~150k per-agent
   coherence cap is a guideline; the C10c two-stage decomposition
   under it is precedent for pre-splitting full-file rewrites.)
2. **Process in force** (user-approved; model-assignment rule added
   2026-07-13): each cluster =
   in-session decision resolution → cluster plan approval (explicit;
   config diffs and deletions named separately) → implementation
   workflow (parallel agents on disjoint owned files; `config.rs`
   main-loop-owned; `main.rs`/`error.rs` serialized through one exclusive
   non-parallel agent; mod skeletons and shared contract types
   pre-created before dispatch by a serial substrate agent; Fable is
   orchestration only per §1.4, ruled 2026-07-14) → full cargo battery
   → adversarially-confirmed verification workflow (5 dimensions: spec,
   principles, comments, diagnostics, integration; findings confirmed by
   refute-by-default agents) → fix confirmed findings → log status here
   with approval, re-verifying this Handoff against the new entry in the
   same approval (Handoff-currency rule, §1.4 step 6, added 2026-07-18).
   Three further §1.4 rules in force since 2026-07-15:
   the end-state-only prompt constraint (§1.2 premise in every agent
   prompt), the spec-decides-it auto-ruling rule, and the honest-option
   rule (no false choices; filter applied at both the agent and
   main-loop layers). Agent constraints per §1.4: repo root only, never
   `specs/`, owned files only, read-only shell + cargo fmt/check/clippy,
   no git/tests/servers/deps/deletions. Model assignment per §1.4
   (permanent, 2026-07-13): all subagents run on Opus, Fable 5 is
   main-loop only, and the main agent owes each agent the tightened
   scoping §1.4 specifies.
3. **Conventions the code now relies on** (adds to the C2-era list):
   content-derived hashes via `crate::canonical` (incl.
   `canonical_sha256_hex_without_field` for self-hashed profiles/
   reports/manifests); IDs via `crate::ids` (now incl. `loc_`,
   `syncq_`); timestamps via `primitives::{current_time_ms, utc_now,
   format_utc_timestamp_ms, parse_utc_timestamp_ms}`; persisted detail
   bounding via `util::truncate_persisted_detail` (500); IMMEDIATE
   transaction lifecycle via the namespace-parameterized helpers in
   `hot_plane` (commit-attempt logged); events appended on the caller's
   connection inside the owning transaction; staged producer output is
   plain serde_json (canonicalization happens once, at import);
   recorded-outcome vs infrastructure-`Err` split everywhere untrusted
   producers meet the core. C5 additions: event-payload entries via
   `events::entry` (single shared helper); per-source cutover barriers
   via `state::CutoverRegistry` (one instance per process, `main.rs`
   owns the Arc); parser-input containment via
   `source::resolve_contained_source` (the single canonicalized
   containment authority; `resolve_source_reference` is its PDF
   wrapper); consumed acquisition-bundle cleanup is owned by the
   scheduler and runs only after the entry's whole unit of work
   completes (`complete()` succeeds), never by the importer; parse
   dispatch identity-checks the live file's hash against the run's
   `source_hash` before any worker runs. CA additions: annotation
   freshness transitions ONLY via the `annotations::store` functions
   (event-atomic, status-guarded); producer prompts are named constants
   and producer input is exactly the ordered target-unit text (input
   purity — the memoization-eligibility basis); producer identity
   changes (model, endpoint, prompt, max_input_chars) invalidate memo
   reuse by construction; the memo cache is write-once per key and
   survives parse archival; the annotation worker discovers work
   statelessly per cycle, and any `building` row visible at discovery
   time is a crash orphan to adopt (the single worker completes every
   build within its cycle). C7 additions: the per-query read path opens
   one read-only connection + transaction via
   `hot_plane::begin_read_transaction` (the DP1 read twin of
   `begin_write_transaction`, "query" namespace) as its first act and
   threads `&*tx` to every stage, so all channels read one consistent
   snapshot; query-path local model calls are gated CALLER-SIDE via
   `state::acquire_model_call_gate_on` — including the MaxSim QUERY
   embedding (persisted document matrices are decoded, never
   re-embedded, per §38) — and the gate is never held across SQL or
   HTTP I/O (the HTTP reranker branch acquires nothing);
   `query::channels::CapturedParse` is the sole scope surface handed to
   the channels; every channel/fusion stage log carries `query_id`; and
   the reranker's `content_units.body_json` content resolution is a
   deliberate arm-for-arm mirror of `projections/multivector.rs`
   `evidence_text` with must-stay-in-step comments on both sides (a
   shared helper would couple query/ to projections/). C8 additions:
   the `evidence_text` mirror set is FOUR sites (rerank.rs,
   multivector.rs, assembly/evidence.rs, annotations/producer.rs),
   arm-for-arm identical, each carrying the four-site
   must-stay-in-step banner; `src/assembly/` never imports
   axum/tokio/http and never opens its own connection (takes the
   caller's `&Connection` — the per-query read transaction); request
   validation and cap/default resolution live in
   `query::request::ValidatedQuery` (the handler threads one resolved
   struct, never raw DTO fields); request-layer and assembly-layer
   `EvidenceOptions` are deliberately separate same-shaped types
   mapped field-by-field at the pipeline boundary; the /query handler
   acquires the search admission permit BEFORE `spawn_blocking` and
   holds it across the blocking call; tokenization for the assembly
   token budget is CPU-only and never acquires the model-call gate;
   the per-query correlation id reuses `new_query_execution_record_id()`
   and is a correlation handle only until the QER tier lands.
   2026-07-19 additions: dense embedding is reached ONLY through
   `DenseEmbeddingBackend` (exclusive Local/Http enum, reranker
   pattern, `src/inference/dense_backend.rs`); dense prompt
   formatting has ONE source of truth in `inference/dense.rs`
   (`format_dense_query_text`/`format_dense_passage_text`/
   `DENSE_SMOKE_TEXT` — both backends build final text through them);
   dense gate acquisition is backend-aware caller-side
   (`uses_local_model_gate`, gate never held across HTTP I/O); the
   builder's http path batches via the `DENSE_HTTP_BATCH_SIZE` code
   constant and both paths persist through the shared
   `persist_chunk_vector`.
   2026-07-19 (later) additions: the standing REMOTE-CALL SHAPE is
   scoped-thread fan-out of HTTP calls ONLY, with every SQLite
   write serial on the owning thread (three instances: annotation
   worker waves via `dispatch_and_commit_wave`, dense builder
   windows via `embed_windows_concurrently`, and the designed
   Docling parse pool); transient-class retry exists ONLY in the
   dense HTTP client and ONLY for HTTP 429 (bounded 3×,
   `model_call.http_retry`) — all other failures everywhere remain
   fail-immediately recorded outcomes; commercial-endpoint secrets
   live in owner-only key files named in config
   (`.data-store-dense-api-key`, `.annotator-api-key`); packages
   that change operator-visible surfaces carry their doc updates
   (documentation-currency rule).
4. **Standing open items**: rusqlite `hooks` feature decision for
   wall-clock statement deadlines (Cargo.toml change, needs approval;
   busy_timeout 5s is the only bound; seams commented in
   acquisition.rs open_bounded_* and hot_plane.rs); C4d noted
   `ParseMetrics` has no byte/char count fields (worker logs them;
   adding fields needs approval). AGENTS.md and
   DIAGNOSTICS-ONBOARDING.md retain stale streaming-era sections —
   AGENTS.md's "Operation API and event changes" rule names the
   deleted `POST /v1/operations` + NDJSON event fields, and
   DIAGNOSTICS-ONBOARDING.md keeps the "Operation stream delivery"
   boundary section and Required Context table row (recorded
   2026-07-17 C10f pre-planning entry, PLAN-HISTORY.md; separate
   approval items, not covered by the C10d operator-doc re-baseline).
   Graceful shutdown waits out the full scheduler cycle — the
   shutdown signal is checked per CYCLE, not per drain entry
   (~2 h 20 m observed at R3; SIGTERM needed at both CPc attempts);
   bounding latency to one drain entry NEEDS A RULING (banked
   2026-07-17 commissioning entry, PLAN-HISTORY.md). Docling
   activity monitor logs ~2 INFO lines/sec per conversion —
   log-noise review candidate (banked same entry; multiplies by
   pool size under the Docling-parallelism design — bounding the
   pool's aggregate inspection logging is folded into that
   package). NEW 2026-07-19: annotation "issue 1" — a large share
   of relation producer calls return ~16 output chars in ~220 ms
   (near-empty results paying full round-trip cost); granularity/
   filtering discussion PENDING (any prompt/batching change is
   producer-identity-bearing and memo-invalidating — needs its own
   ruling). Rare OpenRouter HTTP-200-truncated-body transients
   (~1/59 calls) accepted as park-and-retry noise — revisit only
   if the rate climbs. (The C7a
   `resolve_scope` both-present precedence item was RESOLVED
   2026-07-15 by the R3 intersection ruling — see PLAN-HISTORY.md;
   the `reranker_candidate_limit` logging inconsistency retired with
   `src/operations/search.rs` at CRa.)
5. **Runtime state (as of the 2026-07-19 test abort)**: the
   service is STOPPED (test aborted ≈21:25Z under the empty-marker
   invalidation ruling; clean SIGTERM, no orphans). The fabric
   plane holds the PARTIAL INVALIDATED commercial run (ingest
   incomplete — restart interrupted the cycle at 4-of-remaining
   sources; 2 sources marker-poisoned for graph/summary; delete +
   `--setup-storage` before the CPd2-fixed re-run, named approval
   required). Run history this config: started 20:45:41Z,
   restarted 21:03:21Z for the 300 s annotator timeout, aborted
   ≈21:25Z. `config.toml`: [models.dense]
   backend="http", https://openrouter.ai/api/v1/embeddings,
   model qwen/qwen3-embedding-8b, dimension 4096, timeout 60 s,
   key file .data-store-dense-api-key; [models.annotator]
   https://openrouter.ai/api/v1/chat/completions, model
   openai/gpt-5-nano, timeout_seconds 300, key file
   .annotator-api-key. Both key files hold the user's OpenRouter
   key (sk-or-v1), owner-only, gitignored. Release binaries
   CURRENT (2026-07-19: CPd hybrid, ruling-B, HTTP dense backend,
   concurrency waves/windows, 429 retry, lock-hold comment fix).
   The user's local vLLM endpoints (10.1.0.10:8000/:8001) are
   DECOMMISSIONED — earlier Little Snitch/egress notes are
   historical. The 27B-era annotator crash was external
   misconfiguration (user-confirmed, fixed, one-time). Switching
   dense backends or models remains a corpus identity change
   requiring a fresh re-ingest. OPERATIONAL cautions: do not pipe
   `start.sh` through short-lived readers (SIGPIPE kills the
   startup relay chain and the service child — observed
   2026-07-19T06:06Z); config is startup-only, so any config
   change requires a service restart; OpenRouter data-policy
   settings gate which providers serve a model (a 404 "No
   endpoints ... data policy" points at
   openrouter.ai/settings/privacy, not at a wrong slug).
   The staged commissioning corpus is the in-repo `sources/` dir (12
   PDFs + 2 txt; the former out-of-repo symlink was replaced by the
   user 2026-07-17). The LEGACY service process was killed
   2026-07-17 (user-approved); its database
   (`index/data-store.sqlite3` — 120 active docs, 65,369 units)
   remains read-only reference material beside the approved D6
   sample conversion at
   `index/docling-conversions/conversion-sample-d6/`; the fabric
   never reads either. `logs/data-store.log` now carries BOTH legacy
   and fabric eras (timestamps separate them; commissioning metrics
   must filter to 2026-07-17T23:00Z onward; re-benchmark metrics to
   2026-07-19T08:20Z onward).
   Still never executed: the query surface
   (POST /query, R4), the admin Operation routes (R5–R6), and the
   remaining R4–R9 commissioning runs, which resume after the
   re-benchmark.

## 1. Target and Ground Rules

### 1.1 Target

The end state is the full v0.3 specification, reached in two stages
(rescoped 2026-07-11). The committed, step-planned clusters below cover
an MVP that pulls semantic annotations, annotation memoization, and the
multi-vector and graph retrieval channels forward from the spec's own
recommended MVP cut (§36) — a re-sequencing within the full-spec target,
motivated by a production query workload that is both single-fact
passage lookup and relational/entity-centric. The QER audit tier (§28,
§29.2 Guarantees 1/2/4) and the learned-sparse channel are deferred,
alongside the tiers the spec itself defers (verified recompute,
compliance erasure), in §5 "Post-MVP Horizon" as named tiers with their
reserved seams and entry points, without step-level detail. Deferring
the audit tier is a recorded deviation from the spec's committed
guarantees, not a re-sequencing. Deferred tiers receive their own
planning passes when reached.

### 1.2 Transition strategy (decided 2026-07-05; end-state-only ruling added 2026-07-10)

Rebuild as primary path, developed end-state-only. Nothing depends on the
current service staying operational or on already-ingested data surviving,
and — ruled by the user 2026-07-10 — the app does not need to be
functional at ANY point until the programme completes:

- The existing SQLite database contents (schema v4 in `sql/schema.sql`) are
  discarded, not migrated. No data-migration scripts.
- No API/CLI compatibility shims, and no legacy coexistence: legacy
  surfaces are retired at cluster CR rather than kept working between
  clusters. Behavior preservation of legacy code is a non-goal.
- The existing corpus is re-acquired through the new acquisition layer
  (filesystem connector over the corpus root), giving documents genuine
  acquisition provenance instead of synthetic backfill.
- No between-phase operability: no cluster is required to leave the
  service startable, serving, or demonstrable. Compile-correctness — the
  mandatory cargo battery at zero warnings — is the continuous bar, and
  the adversarially-confirmed verification workflows remain mandatory per
  cluster; runtime behavior is verified once, deliberately, at C10f
  commissioning.

### 1.3 Repository process rules that continue to govern

- Runtime never creates or migrates schema. All schema arrives via explicit
  operator-run setup (the `--setup-storage` pattern); schema changes during
  this programme are new setup scripts, run deliberately.
- Mandatory verification after every Rust change: `cargo fmt`,
  `cargo check` (plus `cargo check --features metal` when inference paths
  are touched), `cargo clippy`. No automated tests unless explicitly
  re-enabled. Runtime verification (starting the service, running
  acquisition/parse/search cycles) is consolidated at C10f commissioning,
  each run individually user-approved; clusters do not seek per-cluster
  runtime verification and report unverified behavior as residual risk
  for C10f to retire (2026-07-10 restructure).
- Async stays confined to the HTTP transport shell. All new lifecycle
  machinery (scheduler, importer, activation, snapshotting) is synchronous
  OS-thread work. SQLite access stays synchronous `rusqlite`.
- Every config shape change requires explicit approval and a matching
  `config.example.toml` update.
- `README.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `SPEC-SERVER.md`,
  `SPEC-CLIENT.md` describe surfaces this programme replaces. They are
  re-baselined once, at C10d; clusters do not update them incrementally
  (2026-07-10 restructure — the described surfaces need not function
  during the programme).
- File deletions (legacy module retirement, old schema files) are explicit
  approval items at their cutover package; nothing is deleted implicitly.

### 1.4 Execution model (workflow-oriented; adopted 2026-07-06)

Work is organized as clusters of work packages. Each package declares the
files it owns, the contracts it consumes, its spec sections, and mechanical
acceptance checks. Clusters pipeline along the dependency spine; packages
inside a cluster marked `agent-parallel` run as concurrent workflow agents
over non-overlapping owned files.

Execution modes:

- `main-loop`: performed in-session by the main agent. All config edits,
  `main.rs` wiring, approvals, and judgment-heavy integration.
- `agent-serial`: one implementation subagent working the package's owned
  files under the approved cluster plan.
- `agent-parallel`: a workflow fan-out of implementation agents, one per
  package, dispatched together after the cluster plan is approved.

Model assignment (user-ruled 2026-07-13, permanent): ALL subagents —
implementation, verification finders, and adversarial confirmers — run on
Opus. Fable 5 is orchestration only (user-ruled 2026-07-14): dispatch,
sequencing, structured-verdict reads, decisions and approvals with the
user, and rulings on design-bearing findings. Everything else —
implementation, substrate and schema edits, prompt drafting, artifact
review, fix application, status drafting — is subagent work; Fable
authors only `config.rs` changes and user-approved plan-file writes.
The corollary is a scoping obligation on the main
agent, not a risk acceptance: every agent prompt names the exact in-repo
template pattern to mirror, the owned-file list, and per-package acceptance
checks; finder prompts carry per-dimension checklists citing the specific
spec sections and repo-rule excerpts under test; confirmer prompts state
the finding and the exact criterion that makes it real, requiring
file/line-cited reasoning. If a package appears to need Fable-level
judgment to execute, that is an under-specification signal — fix the
package scoping in-session rather than escalating the agent's model.
Main-loop reads are limited to structured agent verdicts, summaries, and
decision packages; full-artifact review (diffs, prompts, reports,
findings) is performed by reviewer agents with adversarial confirmation,
not by the main loop (user-ruled 2026-07-14).
Per-agent coherence cap (user-ruled 2026-07-14): no agent's planned
cumulative token load (context reads plus output) may exceed ~150k.
Stages that would exceed it are decomposed into sequential sub-agents
with explicit handoff contracts; prompt drafting and prompt review check
planned scope against this cap.

Cluster cycle:

1. Resolve the cluster's open decisions (§4) in-session, one at a time.
   Spec-decides-it rule (user-ruled 2026-07-15): when one option clearly
   aligns most with the spec — citable normative spec text directly
   decides it, and the option conflicts with no recorded ruling and no
   recorded deviation — that option is chosen without asking the user,
   and the ruling is recorded here with its spec citation. Ambiguous
   cases, mixed spec signals, and any conflict with a recorded ruling or
   deviation still go to the user.
   Honest-option rule (user-ruled 2026-07-15): a decision is presented to
   the user only when at least two options survive honest advocacy at this
   project's actual scale and constraints. An option that contradicts
   PRINCIPLES.md, a recorded ruling, a recorded deviation, or that offers
   no benefit surviving its advocate's best case is excluded with a
   one-line reason, never tabled. One viable option plus the strongest
   case against it is a valid resolution and is stated as such, not
   padded into a menu. This rule binds decision packages and option
   analyses produced by agents — recon and decision-package agent prompts
   state it — and the main loop re-applies the filter to every
   agent-produced option table before presenting a ruling request.
2. Present the cluster plan (packages, owned files, checks) for explicit
   approval. Approval covers the listed file writes; config diffs, doc
   rewrites, and deletions remain separately named approval items.
3. Dispatch implementation packages per their execution mode.
4. Every package runs `cargo fmt` / `cargo check` / `cargo clippy`
   (plus `--features metal` when inference paths are touched) before it is
   considered done.
5. Run a verification workflow over the cluster's diff: independent agents
   check spec-section conformance (with file/line citations), `PRINCIPLES.md`
   adherence, comment sufficiency per `AGENTS.md`, and diagnostics-boundary
   coverage per `DIAGNOSTICS-ONBOARDING.md`; findings are adversarially
   confirmed before being reported.
6. Report results, fix confirmed findings, and log cluster status in this
   file's Current Status. The same approval re-verifies the Handoff
   against the new entry and amends any fact it makes stale, so the
   Handoff never lags the log (Handoff-currency rule, added 2026-07-18
   with the PLAN-HISTORY.md relocation — the Handoff is the
   authoritative current-state digest; completed entries leave this file
   at the next relocation once superseded).

Constraints stated in every agent prompt: stay inside the repository root;
never read `specs/`; touch only the package's owned files; read-only shell
plus `cargo fmt`/`cargo check`/`cargo clippy`; no git, no tests, no servers,
no dependency changes, no file deletion; and the end-state-only operating
premise (§1.2, user-ruled 2026-07-15 as a standing prompt constraint): the
app first runs at C10f, after ALL clusters land — never reason from
"X doesn't exist yet"; evaluate every design, option analysis, and finding
against the completed end state, where all planned machinery (through C10)
is live.

Serialization rules that override parallelism (from recon):

- `src/main.rs` `mod` declarations and the single `AppState::new` call site
  are serialized: edited by at most one exclusive non-parallel agent at a
  time (or the main loop), never by parallel agents; parallel agents
  needing a module registration report it instead of editing `main.rs`.
- `src/error.rs` is single-owner per cluster: `ApiError` has three
  exhaustive matches (enum, `status_u16`, `error_kind`) that every new
  failure domain touches. New variants for a cluster are batched into one
  edit by that single owner — an exclusive non-parallel agent or the main
  loop.
- `src/config.rs` is main-loop-owned (approval rule above; also one
  113-line `validate()` function that conflicts under parallel edits).
- `src/bin/colbert-diagnostic.rs` compiles `config.rs` + `error.rs` +
  `inference/mod.rs` via `#[path]` includes; any edit to those files must
  keep that binary compiling.

### 1.5–1.6 — retired (2026-07-18 second-stage relocation)

§1.5 Pinned contracts and §1.6 Recon findings moved to `PLAN-HISTORY.md`
"Retired planning sections" — grep `1.5 Pinned contracts` /
`1.6 Recon findings`. The one still-live pinned contract (the inference
inbound API + caller-side gate discipline) is digested in the §4 cluster
rulings index.

## 3. Clusters and Work Packages

Completed planning corpus retired to `PLAN-HISTORY.md` "Retired planning
sections" (2026-07-18, verbatim): §2 Module Disposition, the dependency
spine, cluster/package texts C1–C9 (C1 seam cuts, C2 substrate, C3
acquisition, C4 parsing, CR legacy retirement, C5 activation, CA
annotations, C6 projections, C7 retrieval, C8 assembly), the C7/C9/C10/CP
fact bases, and the §37 acceptance-traceability table — grep the cluster
heading (e.g. `### C7 — Retrieval fabric`) or package name (e.g.
`C9d Superseded-state`). Only the pending work remains below; operative
rulings from the retired texts are indexed in §4.

### C10 — Operational shell + commissioning (after C9; cluster plan APPROVED 2026-07-16)

C10s–C10e are COMPLETE; their package texts, the cluster's ruling intro,
and the C10 fact base are retired to `PLAN-HISTORY.md` (grep
`C10 fact base` or the package name, e.g. `C10a API finalization`).
R1/R2 and the queue-coupled completion model are indexed in §4. Only
C10f remains:

- **C10f Commissioning (first runtime verification)**: the programme's
  first runtime execution, performed deliberately with the user:
  `--setup-storage` on a clean index root; the autonomous
  scan→acquire→parse→gate→activate cycle over the real corpus,
  including post-activation annotation builds; query execution across
  all three channels (lexical, dense, graph) with annotation-freshness
  and assembly-trace checks; the async-Operation admin surface and a
  held-candidate cleanup cycle (R1); the offline multi-vector overlap
  diagnostic (2026-07-15): exhaustive ColBERT top-100 vs the fused
  pool on sample real queries, quantifying the deferred `multi_vector`
  channel's recall gap from persisted C6e matrices with no new
  infrastructure; a snapshot plus deletion-gate verification (restore
  drills defer with the QER audit tier); startup/readiness/health
  review. Every accumulated "NOT runtime-verified" residual-risk item
  from C2 through C10, including CA, is retired or filed here. Mode:
  main-loop; every run individually user-approved.

### CP — Ingestion performance (commissioning interlude; cluster plan APPROVED 2026-07-17)

Motivation: the first commissioning cycle (2026-07-17) measured 229.7
min for 14 documents — dense passage embedding 149.6 min (65%; 5,120
calls, avg 1,753 ms, batch 1), ColBERT 30.2 min (13%; 34,092 calls,
avg 53 ms), Docling + everything else ≈ 50 min (22%). Dense batching
is the largest contained win. Bulk throughput recurs operationally:
any parser-identity change re-ingests the whole corpus (§13).

CPa (implemented then REVERTED — no dense batching win on this GPU), CPb
(complete, verified live), and CPd (complete; the length-threshold hybrid
followed) have their package texts retired to `PLAN-HISTORY.md` (grep the
package name, e.g. `CPd ColBERT batched`); as-built records live in
Current Status / PLAN-HISTORY.md session entries. The motivation numbers
above are the CPc before-baseline. Remaining:

- **CPc Benchmark re-ingestion (runtime, main-loop, run-by-run
  user-approved)**: FIRST ATTEMPT ABORTED 2026-07-18 (smoke-caught
  Metal defect, then the batching-regression kill — see Current
  Status); the run now happens AFTER CPd and requires FRESH approval
  of the `index/fabric/` deletion (the original named approval was
  consumed; the plane holds a partial aborted cycle). Procedure
  unchanged: cargo battery → release rebuild → `--setup-storage` →
  start → full cycle over the same 14-file corpus → per-purpose
  `model_call` metric extraction filtered to the run's timestamps →
  before/after comparison. Doubles as the first live annotation run
  (CPb) and unblocks the graph channel for R4. THIRD ATTEMPT
  2026-07-19 killed at user direction at ~5/14 sources for the
  HTTP-dense pivot (partial data retained: local passage avg
  1,678 ms; CPd ColBERT hybrid ~4.8 min through 5 sources);
  SUPERSEDED by the HTTP-dense re-benchmark (2026-07-19 Current
  Status entries): that run completed in 69.8 min (3.3×) and was
  then INVALIDATED by the serial-annotation ceiling; the
  commercial-endpoint clean-corpus run (OpenRouter
  qwen/qwen3-embedding-8b @ 4096 + gpt-5-nano, concurrent
  dispatch) is the benchmark of record — see the 2026-07-19
  entries.
- **CPd2 Empty-marker consumer fix (RULED 2026-07-19; FIRST scope
  of work next session, BEFORE CPe)**: consumer-side skip of the
  worker's by-design `[]` empty-marker annotation rows in the
  annotation-derived projection builders — BOTH the entity path
  (graph mentions; the observed poison: "no string `name` body
  field" per-cycle build failure) and the relation path (graph
  edges; same hazard, unobserved only by ordering). Skips are
  VISIBLE: a skipped-marker count on the projection-build
  completion log; comments at both consumers cross-reference the
  worker.rs `complete_fresh` empty-marker convention. No producer
  change; no data cleanup (marker rows are valid by design);
  requires rebuild + restart, after which the poisoned sources'
  builds self-heal via stateless rediscovery. Owned files: the
  annotation-derived projection builders (graph/summary path —
  locate via `build_graph_projection`; verify whether the summary
  consumer needs the same guard before scoping it in or out).
  Mode: one Opus agent-serial package + cargo battery +
  refute-by-default review. Full root cause: 2026-07-19
  empty-marker Current Status entry.
- **CPe Docling parse pool (APPROVED 2026-07-19; implement in a NEW
  session after the commercial-endpoint test completes)**: streaming
  parse pool per the 2026-07-19 design entry (Current Status) — the
  staging-only half (pre-dispatch guards + Docling child) fans out;
  the canonical half (import/gates/projections/activation/queue
  completion) stays serial on the scheduler thread. Pool admission
  DYNAMIC on both axes from observed signals, never a constant or
  config knob: memory (available vs per-child working-set EMA
  seeded from the activity monitor's measured RSS) AND CPU
  (observed utilization headroom vs per-child CPU-demand EMA —
  cores÷num_threads ratios rejected as guesses; measured child
  ≈ 1 core despite --num-threads 10). Streaming completion, no
  wave barrier (20× per-doc duration variance). Shutdown
  TERMINATES in-flight children (staging is crash-safe; partially
  retires the banked shutdown-latency item). Bound the pool's
  aggregate activity-monitor logging (the banked log-noise item
  multiplies by pool size). Owned files: src/scheduler.rs (drain/
  dispatch restructure), src/docling.rs (child-handle pool API);
  no config changes, no new deps; cross-platform resource
  sampling (macOS + Linux, no-new-deps mechanism, getloadavg
  named candidate). Honest scale note: at large admitted pools the
  serial import half binds next (seams: banked Option A, then
  parallel projection builds — future tiers). Remote Docling
  recorded as a legitimate future tier on its own merits; local
  for now (user-ruled). Mode: one Opus agent-serial package +
  cargo battery + refute-by-default review (named constraints:
  queue/Operation lifecycle coupling, bundle-cleanup ordering,
  crash-replay invariants, startup-sweep safety comment).
  Estimate ~300–350k tokens, confidence ~75%; runtime
  verification at the following re-ingest.
- **Deferred**: parse/embed overlap (pipelining Docling on doc N+1
  during doc N's embed) — architectural change to the
  single-scheduler-thread inline-worker design; RE-SEQUENCED
  2026-07-19 into the Docling-parallelism design pass that follows
  the HTTP-dense re-benchmark (with dense moving off-device, Docling
  ~45 min becomes the dominant local cost; concurrent conversion
  needs a user ruling on the scheduler design). The HTTP
  dense-embedding backend formerly recorded here as the largest
  structural lever is IMPLEMENTED (2026-07-19 Current Status
  entry).

## 4. Design Decisions — Rulings Index (condensed 2026-07-18)

Full ruling texts — rationale, rejected alternatives, verified facts —
are in `PLAN-HISTORY.md` "Retired planning sections"; grep the quoted
anchor. Only D5 is still open. These condensed entries are the recorded
rulings the §1.4 spec-decides-it / honest-option rules check against.

- **D1 — Physical storage (RESOLVED 2026-07-07).** Hot plane
  `{index_root}/fabric/fabric.sqlite3` (WAL validated fatally at startup,
  `synchronous=FULL`, read-only read paths, busy_timeout/deadlines as code
  constants, fresh connection per operation); content-addressed write-once
  artifact store `{index_root}/fabric/artifacts/sha256/…`; `system_events`
  in-plane. Anchor: `D1 — Physical storage mapping`.
- **D2 — API surface (RESOLVED 2026-07-15 query / 2026-07-16 admin).**
  §34.1 plain-JSON `POST /query` (QER id omitted until the QER tier,
  recorded deviation); admin = §34.6 Operation rows + polling ONLY; NO
  NDJSON anywhere in the end-state API. Anchor: `D2 — API surface
  transition`.
- **D3 — Configuration (RESOLVED 2026-07-07).** Config holds external
  facts and request-shape protections only; retrieval knobs live in the
  sealed hashed RetrievalProfile; `[docling]` options are identity-bearing
  parser configuration; `deny_unknown_fields` everywhere; relative paths
  resolve against the config file's parent directory. Anchor: `D3 — §35
  vs current configuration`.
- **D4 — ColBERT (RESOLVED 2026-07-11).** Keep: persisted token matrices
  as `multi_vector` projections (C6e) + the MaxSim rerank stage (C7c).
  Anchor: `D4 — ColBERT disposition`.
- **D5 — Second connector target. OPEN** (deferred with its connector,
  post-MVP): which external API-based source system gets the
  incremental-detection connector. Anchor: `D5 — Second connector target`.
- **D6 — Docling output (RESOLVED 2026-07-10).** The PDF worker consumes
  `--to json` (DoclingDocument JSON) as its candidate-unit source; DocTags
  rejected (0–500 quantized coordinates unfit for §17 locators); markdown
  never a candidate-unit source. Verified schema facts for the C4c mapping
  are in the full text. Anchor: `D6 — Docling typed-output mode`.
- **D7 — dissolved into D8 (2026-07-11).**
- **D8 — Annotation producers (RESOLVED 2026-07-13).** Types
  {entity, relation, summary}, each with a named consumer; ONE external
  OpenAI-compatible chat endpoint for all three producers, no fallback;
  all three memo-eligible via the CAb input-purity contract (prompt
  content = ordered target-unit text only; producer identity inside the
  memo key). Anchor: `D8 — Annotation producers`.
- **D9 — Graph channel (RESOLVED 2026-07-14).** Entry = lexical match of
  query text against normalized entity names (no LLM in the query path);
  semantic-only traversal — structural UnitRelationships never walked at
  query time; hop budget 1 (a RetrievalProfile value); deterministic
  tiering: multi-entity units > direct mentions > one-hop, within tiers by
  matched-name length, tiebreak unitId ascending. Anchor: `D9 — Graph
  channel query-time semantics`.

### Cluster rulings index (operative rulings from retired §3 texts)

- **C5 activation gate scope (2026-07-10).** Activation gates on
  canonical state only; §13.6's projection/index prerequisite is an
  explicit commented seam. Anchor: `C5 — Activation lifecycle`.
- **C5 dominance comparison (2026-07-11).** Union over both reports'
  dimension sets, absence-conservative: present-in-active but
  absent-in-candidate compares as worse (hold); candidate-only does not
  block; absent-from-both is equal. Anchor: `C5a Gating + disposition`.
- **C7 DP1 (2026-07-15).** All hot-plane reads for one query run inside
  ONE read-only transaction on one connection, opened first and covering
  the scope-filtered active-set capture — one pinned WAL snapshot.
  Anchor: `C7 — Retrieval fabric`.
- **C7 DP2 (2026-07-15).** C7 exposes only a synchronous pipeline
  function; admission, AppState wiring, and the endpoint are C8d's. Same
  anchor.
- **C8 R2 (2026-07-15).** The cutover-barrier `reject_if_active` probe
  runs post-capture, inside the read transaction (amends DP1 ordering).
  Anchor: `C8 — Assembly and EvidencePack`.
- **C8 R3 (2026-07-15).** Both-present scope constraints intersect
  (sourceIds ∩ governanceDomains). Same anchor.
- **C9 artifact scope (2026-07-16).** Snapshots archive dense/multivector
  blobs + `semantic_annotations` + chunk payloads + canonical active-parse
  rows; the sealed parse bundle and raw source bytes are referenced by
  existing uri+hash; FTS5/graph planes are covered by the deletion gate's
  verified deterministic rebuild. Anchor: `C9 — Lifecycle forensics`.
- **C9 ReplayProfile stamp (2026-07-16).** Evidence `bit_exact`;
  retrieval/generation `not_supported` (the `record_replay` upgrade is a
  recorded deviation deferred to the QER tier). Same anchor.
- **C9 no `scheduled` trigger (2026-07-16).** Deferred post-MVP — no
  external cadence fact exists (§35); `scheduled`/`pre_deployment` are
  inert enum variants. Same anchor.
- **C9 rebuild/restore discipline (2026-07-16).** Deletion-gate rebuild
  checks re-import archived bytes and re-derive — NEVER re-embed; restore
  re-imports preserving IDs; `annotation_memo` survives all cleanup.
  Same anchor.
- **C10 R1 (2026-07-16).** Never-activated held candidates (superseded
  AND discarded) are cleaned via `HeldSupersession`, gating over the
  candidate's OWN `pre_activation` snapshot, completing
  archiving→archived; no new snapshotType, no snapshot re-take. Anchor:
  `C10r Ruling-1 held-candidate cleanup`.
- **C10 queue-coupled completion (2026-07-16).** Ingest/force-re-parse
  Operations thread `operation_id` onto the `sync_queue` row; the
  scheduler drain owns running→terminal; force re-parse rows override the
  unchanged-(mtime,size) prescreen. Anchor: `C10a API finalization`.
- **§1.5 residue.** The only still-pinned cross-module contract: the
  inference inbound API (`embed_*`/`score_*`/`uses_local_model_gate`) +
  the caller-side model-call-gate discipline in `state.rs`. Anchor:
  `1.5 Pinned contracts`.

## 5. Post-MVP Horizon

Named tiers, their reserved seams (already built by the clusters above),
and entry points. No step-level detail; each gets its own planning pass.

- **QER audit tier** (§24.2 planHash, §28, §29.2 Guarantees 1/2/4,
  §30.5 restore drills; deferred 2026-07-11 as a recorded deviation —
  SemanticAnnotations, memoization, multi-vector, and graph moved the
  other way, into MVP scope, at the same rescope): per-query QueryPlan +
  `planHash`, the QueryExecutionRecord writer (C8c shape: embedded
  EvidencePack, freshness record, `retrievalReplayMode:
  "record_replay"`, written before or atomically with the response),
  durable retrieval/ranking trace persistence, scheduled restore drills
  with evidence replay over sampled QERs, external model call records
  (HTTP reranker and any external annotation producer), and the
  `/query-executions` inspection surface. Seams already built: C2e QER
  metadata table, `qer_` IDs, C3c per-source boundary timestamps, the
  hashed C7a RetrievalProfile, and the ContextAssemblyTrace embedded in
  every EvidencePack (C8b). (Corrected 2026-07-15: this entry
  previously claimed C2d built the QER/trace model types; no §24/§28
  model types exist in `src/` — this tier defines them.) Until
  this tier lands, the delivered system answers evidence questions at
  serve time only; retrospective per-query reconstruction is not
  available.
- **Additional retrieval channels** (§22, §24): `multi_vector`
  (deferred 2026-07-15 — an exhaustive MaxSim candidate-generation scan
  measured infeasible at the actual corpus on the 32 GB M1 Max, ~30–60 s
  per query vs 84 ms for the retained C7c fused-pool stage; named
  design: a stateful remote multi-vector index, e.g. Qdrant MaxSim
  multivectors or Vespa late-interaction, operated under the
  HTTP-reranker external-backend discipline — capability claims to be
  verified at that tier's planning pass; C6e matrices persist unchanged,
  so entry is an index-push path plus a channel client with no
  re-embedding; a C9-style remote-cleanup obligation for superseded
  parses attaches; stateless inference endpoints rejected — the cost is
  matrix movement/holding, not FLOPs), `learned_sparse_vector`
  (deferred 2026-07-11 — new model runtime plus weighted-index
  machinery, overlapping the retained lexical stack) and
  `temporal_projection`. Seam: RetrievalProjection envelope, the
  channel-scoped C7a profile, and the C7b channel contract.
- **Verified recompute — Guarantee 3** (§29.4): probe query sets, measured
  per-channel tolerances, numeric-environment capture. Seam:
  `ReplayProfile.channelReplayModes`/`declaredTolerances` in snapshot
  manifests from C9.
- **Entitlement layer** (§6): resolves callers to allowed source sets and
  intersects into ResolvedScope. Seam: callerContext passthrough,
  governanceDomain tagging, scope-at-candidate-generation from C3/C7/C8.
- **Compliance-driven erasure** (§11.5): designed, audited purge operation
  over immutable stores. Deliberately not improvised.
- **Reference-style QERs** (§28): mechanical derivation from embedded
  records if volume demands; never the reverse. Follows the QER audit
  tier.
- **Per-domain index partitioning** (§6 caveat): remedy for corpus-global
  lexical statistics if scoped-score shadowing becomes material.
