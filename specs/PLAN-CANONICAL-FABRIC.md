# PLAN: Canonical Content Graph and Retrieval Fabric

Reference specification: `canonical_content_graph_retrieval_fabric_v_0_3.md`
(all `§` references below point into that document).

## Current Status

Completed work is recorded in `PLAN-HISTORY.md`: the session-by-session
record ("Completed Current Status entries"), the retired planning
sections (grep-anchor cited from §4), and the relocated one-line
ledger. Date-cited entry references resolve there. This file records
current state (the Handoff below) and pending work (§3, §5) only;
history never accumulates here (§1.4 step 6).

Writing rule: state facts in present tense, as if they were always
true. No dates, no event narration, no change rationale, no
"amended/retired/superseded/was" notes — when a fact goes stale,
replace it and delete the old one; PLAN-HISTORY.md records the
transition. Sole exception: §4 ruling anchors may cite dates as
PLAN-HISTORY grep keys.

## Handoff (authoritative current-state digest, amended per session in
place — §1.4 Handoff-currency rule)

State for the next session picking this up:

1. **Operating premise (user-ruled)**: end-state-only
   development. The app does not need to start, serve, or be functional
   at any point until the programme completes (§1.2). No between-phase
   operability, no legacy behavior preservation, no per-cluster runtime
   verification — all runtime verification consolidates at C10f
   commissioning. The cargo battery (zero warnings) and the
   adversarially-confirmed verification workflows remain mandatory per
   package.
1a. **Next work (ruled order)**: all implementation clusters and
   packages through C10e, CP, CPd2, and CA2 are COMPLETE (records in
   PLAN-HISTORY.md). C10f commissioning is UNDERWAY: R0–R4, the
   graceful shutdown, the commercial-endpoint clean-corpus re-run
   (the benchmark of record and first complete annotation chain,
   incl. the first live CPd2/CA2/GateExisting exercise), and the
   derived-boundary normalization change are COMPLETE. The sequence
   from here: (1) the remaining C10f runs R5–R9 — the C9 runtime
   verification set (mint/verify/cleanup/deactivate/restore cycles),
   a held-candidate cleanup cycle (§4 C10 R1), the async-Operation
   admin surface incl. the force re-parse override path, the CLI
   poll-loop and renderer surface (never executed), the C10b health
   counts under real cycles, and the multi-vector overlap
   diagnostic; (2) CPe Docling parse pool (APPROVED — see §3; new
   session). After C10 the programme's MVP completes; the QER audit
   tier and other §5 tiers follow post-MVP. D5 deferred.
2. **Process in force** (user-approved): the §1.4 execution model
   governs in full — the cluster cycle, execution modes, model
   assignment (all subagents on Opus; Fable 5 main-loop only), the
   spec-decides-it / honest-option / end-state-only-prompt rules,
   agent constraints, and serialization rules. Verification is the
   five-dimension adversarially-confirmed workflow (§1.4 step 5).
3. **Conventions the code relies on**:
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
   producers meet the core; event-payload entries via
   `events::entry` (single shared helper); per-source cutover barriers
   via `state::CutoverRegistry` (one instance per process, `main.rs`
   owns the Arc); parser-input containment via
   `source::resolve_contained_source` (the single canonicalized
   containment authority; `resolve_source_reference` is its PDF
   wrapper); consumed acquisition-bundle cleanup is owned by the
   scheduler and runs only after the entry's whole unit of work
   completes (`complete()` succeeds), never by the importer; parse
   dispatch identity-checks the live file's hash against the run's
   `source_hash` before any worker runs; annotation
   freshness transitions ONLY via the `annotations::store` functions
   (event-atomic, status-guarded); producer prompts are named constants
   and producer input is exactly the ordered target-unit text (input
   purity — the memoization-eligibility basis); producer identity
   changes (model, endpoint, prompt, max_input_chars) invalidate memo
   reuse by construction; the memo cache is write-once per key and
   survives parse archival; the annotation worker discovers work
   statelessly per cycle, and any `building` row visible at discovery
   time is a crash orphan to adopt (the single worker completes every
   build within its cycle); the per-query read path opens
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
   shared helper would couple query/ to projections/);
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
   and is a correlation handle only until the QER tier lands;
   dense embedding is reached ONLY through
   `DenseEmbeddingBackend` (exclusive Local/Http enum, reranker
   pattern, `src/inference/dense_backend.rs`); dense prompt
   formatting has ONE source of truth in `inference/dense.rs`
   (`format_dense_query_text`/`format_dense_passage_text`/
   `DENSE_SMOKE_TEXT` — both backends build final text through them);
   dense gate acquisition is backend-aware caller-side
   (`uses_local_model_gate`, gate never held across HTTP I/O); the
   builder's http path batches via the `DENSE_HTTP_BATCH_SIZE` code
   constant and both paths persist through the shared
   `persist_chunk_vector`; the standing REMOTE-CALL SHAPE is
   scoped-thread fan-out of HTTP calls ONLY, with every SQLite
   write serial on the owning thread (three instances: annotation
   worker waves via `dispatch_and_commit_wave`, dense builder
   windows via `embed_windows_concurrently`, and the designed
   Docling parse pool); transient-class retry exists ONLY in the
   dense HTTP client and ONLY for HTTP 429 (bounded 3×,
   `model_call.http_retry`) — all other failures everywhere remain
   fail-immediately recorded outcomes; commercial-endpoint secrets
   live in owner-only key files named in config
   (`.data-store-dense-api-key`, `.annotator-api-key`);
   `normalize_entity_name` (projections/graph.rs) is the single
   normalizer for entity NAMES, entity TYPES, and relation
   PREDICATES at every derived boundary — stored
   `semantic_annotations` rows stay VERBATIM, and graph derivation
   is a THREE-site must-stay-in-step mirror set (builder
   `derive_edges`/`accumulate_mentions`, deletion-gate verifier
   `derive_expected_edge`, restore `rebuild_graph_planes`), with
   the vocabulary aggregation as a fourth normalized consumer.
4. **Standing open items**: rusqlite `hooks` feature decision for
   wall-clock statement deadlines (Cargo.toml change, needs approval;
   busy_timeout 5s is the only bound; seams commented in
   acquisition.rs open_bounded_* and hot_plane.rs). `ParseMetrics`
   has no byte/char count fields (worker logs them; adding fields
   needs approval). Graceful shutdown waits out the full scheduler
   cycle — the shutdown signal is checked per CYCLE, not per drain
   entry, so latency can reach hours; bounding it to one drain entry
   NEEDS A RULING (CPe partially retires this). The Docling activity
   monitor logs ~2 INFO lines/sec per conversion — log-noise review
   candidate; bounding the pool's aggregate inspection logging is
   folded into CPe. Annotation "issue 1": a large share of relation
   producer calls return near-empty results at full round-trip cost;
   granularity/filtering discussion PENDING (any prompt/batching
   change is producer-identity-bearing and memo-invalidating — needs
   its own ruling; under content-scoped satisfaction it re-annotates
   only the frontier unless paired with the re-annotate override).
   The OPERATOR RE-ANNOTATE OVERRIDE is a NAMED PRE-PRODUCTION
   REQUIREMENT (the only production-safe corpus-wide annotation
   refresh under the content-scoped satisfaction ruling — §4 CA2).
   The OPERATOR EPISODE-PURGE surface is a NAMED PRE-PRODUCTION
   REQUIREMENT alongside it (§4 CL1): a designed operation that
   removes a test/sampling episode's annotations, memo rows, and
   events coherently, leaving one operator-purge record.
   Rare OpenRouter HTTP-200-truncated-body transients (~1/59 calls)
   are accepted as park-and-retry noise — revisit only if the rate
   climbs. Annotator 429 shedding NEEDS A RULING: at full fan-out
   the worker saturates OpenRouter's 300 rpm allowance and sheds
   overflow as parked-failed rows (converges, but wastes round
   trips and emits ERROR noise; options: bounded 429 backoff
   mirroring the dense client, or dynamic wave sizing). 9
   annotations sit parked-failed on deterministic malformed model
   JSON (duplicate `entityType` key; invented triple field) and
   retry every cycle — tolerant-parse vs leave-strict NEEDS A
   RULING. The lexical channel's strict-AND FTS form falls back to
   broad-OR only at construction time, never on an empty result —
   intended-contract ruling wanted. `sql/fabric/schema.sql`
   `relation_type` comment predates predicate normalization (stale;
   schema-file edit needs its named approval). Per-channel query
   completion logs are debug!-level and invisible at info; the
   debug:true response diagnostics are the operator window.
5. **Runtime state**: the service is RUNNING (ready=true, bind
   127.0.0.1:8091, daemonized). The fabric plane holds the
   BENCHMARK-OF-RECORD corpus: 14 sources active, all projection
   planes fresh, 3,112/3,121 annotations fresh under the authored
   ruleset identity (9 parked-failed on malformed model output,
   retried each worker cycle), 28 lifecycle snapshots, fabric
   health counters all zero. `policies/annotator-naming.toml`
   carries 4 rules (system-assigned version advanced);
   `policies/entity-match.toml` stays NEUTRAL (both fuzzy classes
   disabled — query-time-only, editable at any restart without
   identity cost). `config.toml`: [models.dense] backend="http",
   https://openrouter.ai/api/v1/embeddings,
   model qwen/qwen3-embedding-8b, dimension 4096, timeout 60 s,
   key file .data-store-dense-api-key; [models.annotator]
   https://openrouter.ai/api/v1/chat/completions, model
   google/gemini-3.1-flash-lite, timeout_seconds 300, key file
   .annotator-api-key. Both key files hold the user's OpenRouter
   key (sk-or-v1), owner-only, gitignored. Release binaries are
   CURRENT (include the derived-boundary normalization change).
   Switching dense backends or models is a corpus identity change
   requiring a fresh re-ingest; switching the annotator model
   re-annotates only the frontier (content-scoped satisfaction).
   Query latency is dominated by the OpenRouter query-embed round
   trip (≈10 s of ≈12.8 s; local stages ms-scale, local reranker
   ≈2.4 s). OPERATIONAL cautions: do not pipe `start.sh`
   through short-lived readers (SIGPIPE kills the startup relay
   chain and the service child); config is startup-only, so any
   config change requires a service restart; OpenRouter data-policy
   settings gate which providers serve a model (a 404 "No
   endpoints ... data policy" points at
   openrouter.ai/settings/privacy, not at a wrong slug); the
   annotator is rate-limited by OpenRouter at 300 rpm (see the §4
   open item).
   The commissioning corpus is the in-repo `sources/` dir (12 PDFs
   + 2 txt). The legacy database (`index/data-store.sqlite3` — 120
   active docs, 65,369 units) is read-only reference material
   beside the D6 sample conversion at
   `index/docling-conversions/conversion-sample-d6/`; the fabric
   never reads either. `logs/data-store.log` carries both legacy
   and fabric eras — benchmark-of-record metrics filter to
   2026-07-20T08:55–09:51Z, R4 query metrics to 2026-07-20T19:36Z
   onward (filter values, not narration).
   Never executed yet: the admin Operation routes and the CLI
   poll-loop/renderer surface (R5–R6), and the remaining R5–R9
   commissioning runs.

## 1. Target and Ground Rules

### 1.1 Target

The end state is the full v0.3 specification, reached in two stages.
The committed, step-planned clusters below cover
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

### 1.2 Transition strategy

Rebuild as primary path, developed end-state-only. Nothing depends on the
current service staying operational or on already-ingested data surviving,
and — user-ruled — the app does not need to be
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
  for C10f to retire.
- Async stays confined to the HTTP transport shell. All new lifecycle
  machinery (scheduler, importer, activation, snapshotting) is synchronous
  OS-thread work. SQLite access stays synchronous `rusqlite`.
- Every config shape change requires explicit approval and a matching
  `config.example.toml` update.
- `README.md`, `ARCHITECTURE.md`, `PROTOCOL.md`, `SPEC-SERVER.md`,
  `SPEC-CLIENT.md` are as-built operator docs. Documentation-currency
  rule: packages that change operator-visible surfaces carry their doc
  updates in the same package.
- File deletions (legacy module retirement, old schema files) are explicit
  approval items at their cutover package; nothing is deleted implicitly.

### 1.4 Execution model (workflow-oriented)

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

Model assignment (user-ruled, permanent): ALL subagents —
implementation, verification finders, and adversarial confirmers — run on
Opus. Fable 5 is orchestration only (user-ruled): dispatch,
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
not by the main loop (user-ruled).
Per-agent coherence cap (user-ruled): an agent's planned cumulative
token load (context reads plus output) stays within ~150k — a
guideline, not a hard limit.
Stages that would exceed it are decomposed into sequential sub-agents
with explicit handoff contracts; prompt drafting and prompt review check
planned scope against this cap.

Cluster cycle:

1. Resolve the cluster's open decisions (§4) in-session, one at a time.
   Spec-decides-it rule (user-ruled): when one option clearly
   aligns most with the spec — citable normative spec text directly
   decides it, and the option conflicts with no recorded ruling and no
   recorded deviation — that option is chosen without asking the user,
   and the ruling is recorded here with its spec citation. Ambiguous
   cases, mixed spec signals, and any conflict with a recorded ruling or
   deviation still go to the user.
   Honest-option rule (user-ruled): a decision is presented to
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
   adherence, comment sufficiency per `AGENTS.md`, diagnostics-boundary
   coverage per `DIAGNOSTICS.md`, and cross-module integration (both
   sides of every seam read and matched); findings are adversarially
   confirmed by refute-by-default agents before being reported.
6. Report results, fix confirmed findings, and bring this file current
   in place under the Current Status writing rule — replace stale
   statements, never narrate transitions — and append the session's
   historical record to PLAN-HISTORY.md, never to this file
   (Handoff-currency rule).

Constraints stated in every agent prompt: stay inside the repository root;
never read `specs/`; touch only the package's owned files; read-only shell
plus `cargo fmt`/`cargo check`/`cargo clippy`; no git commands of ANY kind —
including read-only ones (`status`, `diff`, `log`) and `worktree`
(user-ruled; agents verify with cargo and file reads only);
no tests, no servers, no dependency changes, no file deletion; and the
end-state-only operating
premise (§1.2, a standing user-ruled prompt constraint): the
app first runs at C10f, after ALL clusters land — never reason from
"X doesn't exist yet"; evaluate every design, option analysis, and finding
against the completed end state, where all planned machinery (through C10)
is live.

Serialization rules that override parallelism:

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

## 3. Clusters and Work Packages

Only pending work appears below. Completed cluster and package texts
are in `PLAN-HISTORY.md` "Retired planning sections" — grep the
cluster heading (e.g. `### C7 — Retrieval fabric`) or package name
(e.g. `C9d Superseded-state`); operative rulings are indexed in §4.

### C10 — Operational shell + commissioning (cluster plan APPROVED)

C10s–C10e are COMPLETE (package texts in `PLAN-HISTORY.md` — grep
`C10 fact base` or the package name, e.g. `C10a API finalization`);
R1/R2 and the queue-coupled completion model are indexed in §4. Only
C10f remains:

- **C10f Commissioning (first runtime verification)**: the programme's
  first runtime execution, performed deliberately with the user.
  COMPLETE: `--setup-storage` on a clean index root; the autonomous
  scan→acquire→parse→gate→activate cycle over the real corpus
  including post-activation annotation builds (the benchmark of
  record — Handoff item 1a, era filters in item 5); query execution
  across all three channels (lexical, dense, graph) with
  annotation-freshness and assembly-trace checks (R4);
  startup/readiness/health observation under real cycles.
  REMAINING (R5–R9): the async-Operation admin surface incl. the
  force re-parse override and the CLI poll-loop/renderer surface; a
  held-candidate cleanup cycle (R1); the C9 runtime verification set
  (mint/verify/cleanup/deactivate/restore cycles) beyond the 28
  lifecycle snapshots already minted; the C10b health-counts review
  under non-zero conditions; the offline multi-vector overlap
  diagnostic: exhaustive ColBERT top-100 vs the fused
  pool on sample real queries, quantifying the deferred `multi_vector`
  channel's recall gap from persisted C6e matrices with no new
  infrastructure. Every accumulated "NOT runtime-verified"
  residual-risk item from C2 through C10, including CA, is retired
  or filed here. Mode: main-loop; every run individually
  user-approved.

### CP — Ingestion performance (commissioning interlude; cluster plan APPROVED)

CPa/CPb/CPc/CPd/CPd2 are complete or superseded; package texts and
outcomes are in `PLAN-HISTORY.md` (grep the package name, e.g. `CPd
ColBERT batched`, `CPc Benchmark re-ingestion`). The CPc
before-baseline for the benchmark of record: 229.7 min / 14 documents
(dense passage embedding 149.6 min, ColBERT 30.2 min, Docling + rest
≈ 50 min). Bulk throughput recurs operationally: any parser-identity
change re-ingests the whole corpus (§13). The commercial-endpoint
clean-corpus re-run is the benchmark of record (Handoff item 1a).
Only CPe remains:

- **CPe Docling parse pool (APPROVED; implement in a new session
  after the commercial-endpoint test completes)**: streaming parse
  pool per the design entry (PLAN-HISTORY.md, grep `Docling
  parallelism design`) — the
  staging-only half (pre-dispatch guards + Docling child) fans out;
  the canonical half (import/gates/projections/activation/queue
  completion) stays serial on the scheduler thread. Pool admission
  DYNAMIC on both axes from observed signals, never a constant or
  config knob: memory (available vs per-child working-set EMA
  seeded from the activity monitor's measured RSS) AND CPU
  (observed utilization headroom vs per-child CPU-demand EMA; a
  Docling child measures ≈ 1 core regardless of --num-threads).
  Streaming completion, no wave barrier (20× per-doc duration
  variance). Shutdown TERMINATES in-flight children (staging is
  crash-safe; partially retires the shutdown-latency open item).
  Bound the pool's aggregate activity-monitor logging (the
  log-noise open item multiplies by pool size). Owned files:
  src/scheduler.rs (drain/dispatch restructure), src/docling.rs
  (child-handle pool API); no config changes, no new deps;
  cross-platform resource sampling (macOS + Linux, no-new-deps
  mechanism, getloadavg named candidate). Scale note: at large
  admitted pools the serial import half binds next (seams:
  embed-outside-the-transaction Option A, then parallel projection
  builds — future tiers). Remote Docling is a legitimate future
  tier; local for now (user-ruled). Mode: one Opus agent-serial
  package + cargo battery + refute-by-default review (named
  constraints: queue/Operation lifecycle coupling, bundle-cleanup
  ordering, crash-replay invariants, startup-sweep safety comment).
  Runtime verification at the following re-ingest.

## 4. Design Decisions — Rulings Index

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
- **D3 — Configuration (RESOLVED 2026-07-07; AMENDED 2026-07-19).**
  Config holds external
  facts and request-shape protections only; retrieval knobs live in the
  sealed hashed RetrievalProfile; `[docling]` options are identity-bearing
  parser configuration; `deny_unknown_fields` everywhere; relative paths
  resolve against the config file's parent directory. Anchor: `D3 — §35
  vs current configuration`. AMENDMENT (2026-07-19, CA2): the reserved
  "config may hold the path to a policy document" seam is exercised —
  a `[policies]` section names OPERATOR-EDITABLE external policy
  documents (entity-match, annotator-naming), strictly validated and
  content-hashed at load with SYSTEM-ASSIGNED versions (append-only
  `policy_versions` registry, `policy.changed` event); compile-sealed
  documents stay sealed. Full text: PLAN-HISTORY.md, grep
  `annotation-identity design session`.
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
- **D9 — Graph channel (RESOLVED 2026-07-14; AMENDED 2026-07-19).**
  Entry = lexical match of
  query text against normalized entity names (no LLM in the query path);
  semantic-only traversal — structural UnitRelationships never walked at
  query time; hop budget 1 (a RetrievalProfile value); deterministic
  tiering: multi-entity units > direct mentions > one-hop, within tiers by
  matched-name length, tiebreak unitId ascending. Anchor: `D9 — Graph
  channel query-time semantics`. AMENDMENT (2026-07-19, CA2-P2): entry
  gains deterministic FUZZY classes — acronym derivation and
  token-prefix — with the full as-built ordering chain: tier, class
  (exact > acronym > token_prefix), matched-name char-length DESC,
  name ASC, unitId ASC, parseId ASC (the name and parseId
  discriminators are load-bearing for total determinism);
  `max_fuzzy_candidates` caps fuzzy names PER PARSE. Knobs live in
  the operator-editable entity-match policy document (D3 amendment),
  shipped DISABLED. Full text: PLAN-HISTORY.md, grep
  `annotation-identity design session` / `CA2 implementation session`.
- **CA2 — Annotation identity (RULED 2026-07-19).** Option A:
  satisfaction and reopenable classification key on the CONTENT-SCOPED
  key (`content_key_hash` = memo key minus identity); memo cache stays
  identity-scoped; model switches annotate only the frontier; mixed-
  model corpus consequences ACCEPTED and recorded; `max_input_chars`
  changes require a corpus-wide update (operational rule); the
  operator re-annotate override is a NAMED PRE-PRODUCTION REQUIREMENT;
  rulesets ship NEUTRAL and are authored from observed vocabulary via
  the CA2-P4 inspection surface and the CA2-P5 dry-run mode. Full
  nine-ruling text: PLAN-HISTORY.md, grep
  `annotation-identity design session`.
- **NM1 — Derived-boundary normalization (RULED 2026-07-20).**
  Deterministic casing/whitespace normalization is CODE work, never
  prompt instruction; producer prompts carry only semantic rules.
  `normalize_entity_name` is the single normalizer for entity names,
  entity types, and relation predicates at every derived boundary
  (the three-site graph-derivation mirror set plus the vocabulary
  aggregation — Handoff item 3); stored `semantic_annotations` rows
  stay verbatim (model output is an external payload). Full text:
  PLAN-HISTORY.md, grep `Derived-boundary normalization session`.
- **CL1 — Coherent episode cleanup (RULED 2026-07-20).** Cleanup of
  a test/sampling episode is an operator FEATURE, not a doctrine
  violation: production must support removing an episode without
  audit-failing residue, and a designed purge leaves one honest
  operator-purge record; helpfulness governs — a feature that stops
  being helpful stops being a good idea. Reframes the §11.5 erasure
  deferral's "must not be improvised" as "must be BUILT"; the
  operator episode-purge surface is a named pre-production
  requirement beside the re-annotate override (Handoff item 4). The
  interim sanctioned form is a deliberate one-time operator script.
  Full text: PLAN-HISTORY.md, grep `Episode cleanup ruling`.

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
  §30.5 restore drills; a recorded deviation): per-query QueryPlan +
  `planHash`, the QueryExecutionRecord writer (C8c shape: embedded
  EvidencePack, freshness record, `retrievalReplayMode:
  "record_replay"`, written before or atomically with the response),
  durable retrieval/ranking trace persistence, scheduled restore drills
  with evidence replay over sampled QERs, external model call records
  (HTTP reranker and any external annotation producer), and the
  `/query-executions` inspection surface. Seams already built: C2e QER
  metadata table, `qer_` IDs, C3c per-source boundary timestamps, the
  hashed C7a RetrievalProfile, and the ContextAssemblyTrace embedded in
  every EvidencePack (C8b). No §24/§28 model types exist in `src/`;
  this tier defines them. Until
  this tier lands, the delivered system answers evidence questions at
  serve time only; retrospective per-query reconstruction is not
  available.
- **Additional retrieval channels** (§22, §24): `multi_vector`
  (an exhaustive MaxSim candidate-generation scan measures infeasible
  at the actual corpus on the 32 GB M1 Max, ~30–60 s
  per query vs 84 ms for the retained C7c fused-pool stage; named
  design: a stateful remote multi-vector index, e.g. Qdrant MaxSim
  multivectors or Vespa late-interaction, operated under the
  HTTP-reranker external-backend discipline — capability claims to be
  verified at that tier's planning pass; C6e matrices persist unchanged,
  so entry is an index-push path plus a channel client with no
  re-embedding; a C9-style remote-cleanup obligation for superseded
  parses attaches; stateless inference endpoints rejected — the cost is
  matrix movement/holding, not FLOPs), `learned_sparse_vector`
  (new model runtime plus weighted-index
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
- **Self-learning annotation rulesets** (banked): the
  operator-inspect-and-adjust loop (CA2-P4 vocabulary surface + CA2-P5
  dry-run + operator-edited policy documents) is the MVP tier; a future
  tier derives naming-rule and match-rule candidates from the observed
  vocabulary itself. Seams: the vocabulary aggregation, the
  auto-versioned policy registry, and the D3-amendment document format.
