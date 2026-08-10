//! CAd: dedicated discovery-based annotation build thread (scheduler
//! lifecycle pattern); builds post-activation, never blocks activation or the
//! sync pipeline.
//!
//! The worker is a single std::thread that, each cycle, discovers which
//! required post-activation annotations are missing for every active source
//! and builds them — reusing cached producer output when a §21.2 memo key
//! matches, invoking the producer when it does not.
//!
//! CA2 KEY SPLIT (user-ruled 2026-07-19). Discovery classifies work by the
//! CONTENT key (annotation type × ordered target content hashes, WITHOUT
//! producer identity): a content key already fresh under ANY identity is
//! SATISFIED, so an annotator-model change re-annotates only the frontier
//! (new/changed content, re-parses, failed-row retries) rather than the whole
//! corpus. The memo CACHE stays keyed on the identity-scoped memo key
//! (`memo::lookup`) — cross-identity reuse must remain impossible (memoization
//! honesty). Each `WorkItem` therefore carries BOTH keys; a reopened row minted
//! under a prior identity has its memo key re-stamped at completion (§21 store
//! `complete_fresh`). See `build_source` / `enumerate_work_items`.
//!
//! CONCURRENCY (producer dispatch only). The producer HTTP calls fan out: for
//! each source the worker prepares memo-miss builds serially through their
//! PRE-PAID `build_open` boundary, buffers them into a wave of up to
//! `ANNOTATOR_CONCURRENT_CALLS`, then dispatches the wave's PURE producer calls
//! on scoped OS threads (`std::thread::scope`), joins, and commits each result
//! serially. NOTHING else moves off this thread: discovery, memo lookups/writes,
//! freshness transitions, annotation INSERTs, and every other SQLite write stay
//! on the worker thread and serial (rusqlite is not `Sync`, and the writer lock
//! admits one writer). Memo HITs never dispatch — they re-mint inline with no
//! producer call. See `build_source` / `dispatch_and_commit_wave`.
//!
//! STATELESSNESS is the crash-recovery design. The worker holds no durable
//! per-item progress: every cycle recomputes what is missing from the hot
//! plane alone. An interrupted build (process death between the building-row
//! insert and the completing transaction) leaves an orphaned `building` row;
//! because this single thread completes every build inside the cycle that
//! opened it, any `building` row visible at discovery time IS such an orphan,
//! and discovery reopens it like a failed row — adopting it as the build's
//! building row (see `reopenable_rows_for_parse`) with an
//! `annotation_worker.orphan_adopted` log for the recovery evidence. There is
//! nothing else to reconcile on restart.
//!
//! CONTENTION POLICY (writer-lock contention with the scheduler). The
//! scheduler builds a source's content-derived projections inside ONE IMMEDIATE
//! transaction that holds the hot-plane writer lock across that source's entire
//! dense/ColBERT embed (tens of minutes). A worker write that begins during
//! that window can only get SQLITE_BUSY after the busy_timeout. The worker
//! classifies its write boundaries into two categories:
//!   - PRE-PAID boundaries (`memo_remint`, `build_open`, `projection_build`): no
//!     producer call has been made yet, so on SQLITE_BUSY the worker defers
//!     quietly — the item (or projection build) is counted `deferred`, no
//!     producer runs, and the cycle's build work ENDS EARLY (the lock is
//!     typically held for a multi-minute build, so probing further items would
//!     just burn one busy_timeout per attempt). Nothing is lost: the stateless
//!     next cycle re-discovers the work. (With concurrent dispatch, a PRE-PAID
//!     deferral first FLUSHES any producer wave already buffered — those
//!     producers are paid for — then ends the source's work.)
//!   - POST-PAID boundaries (`build_complete`, `build_fail`): a paid producer
//!     call already succeeded (or its failure evidence exists), so its output
//!     must not be discarded on contention. These WAIT for the writer lock,
//!     re-attempting across busy_timeout windows, bounded ONLY by the shutdown
//!     signal. On shutdown before the lock frees, the worker abandons the
//!     completion and the `building` row is covered by the crash-orphan
//!     adoption path above, exactly like a process exit.
//!
//! CUTOVER STANCE. The worker takes NO cutover barrier: it swaps no active
//! pointers and writes only parse-scoped rows. If a parse is superseded
//! mid-build, the completed rows for the now-archived parse are unreadable via
//! `store::fresh_for_active_parse` by construction (that reader filters on the
//! source's CURRENT active parse), and they are removed by C9 hot cleanup.
//! (NOTE for C9: hot cleanup MUST include semantic_annotations rows in its
//! archive-verify-delete sweep so superseded-parse annotations do not linger.)
//!
//! HEALTH (C10b): the worker publishes an `AnnotationHealth` snapshot into a
//! shared slot each cycle (and on the client-load parked path), mirroring the
//! scheduler's whole-snapshot poison-recovered publish discipline. The snapshot
//! is diagnostic-only — a parked worker never gates service readiness
//! (annotations are non-critical, CAd ruling). Assembly reads only the slot;
//! this owning thread does all the measuring.

use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use rusqlite::{Connection, OptionalExtension, params};
use tracing::{error, info, warn};

use crate::annotations::llm_client::{AnnotatorClient, PRODUCER_TEMPERATURE};
use crate::annotations::memo::{self, MemoItem};
use crate::annotations::policy;
use crate::annotations::producer::{self, Invocation, ProducedAnnotation, ProducerKind};
use crate::annotations::store::{self, NewAnnotation};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::hot_plane::{self, WriteTransactionAttempt};
use crate::model::{ProducerType, Provenance, SemanticAnnotationType};
// The CA2 operator policy module (crate root), distinct from the §21.4
// `crate::annotations::policy` module imported above.
use crate::policy::AnnotatorNamingPolicy;
use crate::primitives::utc_now;
use crate::projections::{envelope, graph, view};
use crate::state::{AnnotationCycleCounts, AnnotationHealth, ShutdownSignal};
use crate::util::{panic_payload_message, truncate_persisted_detail};

/// Idle interval between discovery cycles. A code constant, never config
/// (§35): the cadence is internal pacing for a non-critical background build,
/// not an operator tuning knob. Shutdown interrupts the wait promptly, so this
/// is an upper bound on idle latency, not a hard delay.
const CYCLE_IDLE_INTERVAL: Duration = Duration::from_secs(30);

/// Number of producer HTTP calls dispatched concurrently per wave on scoped OS
/// threads. A code constant, never config (§35): the commercial endpoint handles
/// high concurrency, and annotation call failures park the affected target
/// `failed` and retry next cycle, so this bound tolerates provider 429 bursts.
/// Only the producer HTTP invocation fans out; every SQLite write (discovery,
/// memo, freshness, INSERT, park-failed) stays serial on the worker thread.
const ANNOTATOR_CONCURRENT_CALLS: usize = 32;

/// Maximum failed-row reopens per annotation per PROCESS RUN (user-ruled
/// 2026-07-21, amending the §35 unbounded-retry stance). A code constant,
/// never config (§35). The budget is deliberately in-memory, not durable:
/// the bound exists to stop unbounded paid producer calls against content the
/// model fails on deterministically, and restarts are rare, deliberate
/// operator actions — so a restart re-arming the budget is the intended
/// recovery lever (a naming-policy edit requires one anyway). Durable
/// evidence is unaffected: every attempt still logs and appends its
/// `annotation.failed` event, and exhaustion logs once at ERROR.
const ANNOTATION_RETRY_CAP: u32 = 10;

/// Temperature added per failed-row retry (user-ruled 2026-07-21). First
/// attempts run at the base `PRODUCER_TEMPERATURE` (0.0, maximum memo-friendly
/// reproducibility); retry k samples at `0.1 × k`, deliberately introducing
/// the output variation the §35 retry rationale assumes — a stable failure
/// mode at temperature 0 would otherwise fail identically every attempt.
const RETRY_TEMPERATURE_STEP: f64 = 0.1;

/// Escalation ceiling: the provider-default sampling temperature. Values
/// above 1.0 degrade sampling quality, so the ladder saturates here even if
/// `ANNOTATION_RETRY_CAP` is ever raised past 10.
const RETRY_TEMPERATURE_CEILING: f64 = 1.0;

/// Log-event namespace passed to the shared hot-plane transaction helpers, so
/// every begin/commit/rollback boundary log is attributable to this worker.
const TX_LOG_NAMESPACE: &str = "annotation";

/// Every active source and the parse whose annotations must be built: the
/// discovery scope of one cycle. `active_parse_id` is guaranteed present by
/// the query's WHERE clause.
const SELECT_ACTIVE_SOURCES_SQL: &str = "
SELECT id, active_parse_id FROM source_objects
WHERE active_parse_id IS NOT NULL AND deactivated_at IS NULL";

/// The source's CURRENT active-parse pointer (§14), re-read at projection-build
/// time. The cycle read the pointer once at discovery; a supersession can land
/// between then and the post-cycle projection build, so the annotation-derived
/// projection hook re-reads the pointer and builds ONLY when it still names the
/// parse the annotations were built for — otherwise it would materialize
/// summary/graph envelopes for a parse that is no longer active.
const SELECT_ACTIVE_PARSE_ID_SQL: &str = "
SELECT active_parse_id FROM source_objects WHERE id = ?1";

/// Producer identity stamped on the durable annotation-derived projection-build
/// failure-audit envelope (see `record_projection_build_failure`, mirroring the
/// scheduler's content-derived equivalent). Bumped only if the audit semantics
/// change, so a version change is a visible signal.
const PROJECTION_BUILD_PRODUCER_NAME: &str = "fabric-projection-build";
const PROJECTION_BUILD_PRODUCER_VERSION: &str = "1";

/// Spawn the annotation worker thread: one std::thread that loads the shared
/// producer client, then runs the discovery/build loop until shutdown. The
/// client loads INSIDE the thread so a bad key file parks the worker instead
/// of failing process startup (annotations are non-critical by the CAd
/// ruling). The caller (main) owns the JoinHandle and joins it on shutdown so
/// the worker's clean stop is observable.
///
/// `naming_policy` is the operator's annotator-naming document, loaded once at
/// startup by the main-loop wiring (CA2). Its ordered rules compose into the
/// Entity/Relation producer prompts (CA2-P3) and are identity-bearing: they
/// flow into every prompt, promptHash, identity_hash, and memo-key derivation
/// this worker performs. The document is fixed for the process lifetime, like
/// `annotator_config`.
pub(crate) fn start(
    index_root: PathBuf,
    annotator_config: AnnotatorModelConfig,
    config_root: PathBuf,
    naming_policy: AnnotatorNamingPolicy,
    shutdown: Arc<ShutdownSignal>,
    // C10b diagnostic-only health slot the worker publishes into each cycle (and
    // on the parked path). Same cross-thread Arc/Mutex slot discipline as the
    // scheduler's; `AppState::health()` reads it poison-recovered.
    health_slot: Arc<Mutex<AnnotationHealth>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("annotation-worker".to_string())
        .spawn(move || {
            // Panic containment (diagnostics: spawned tasks must leave durable
            // panic evidence): without this wrapper a worker panic would
            // unwind silently into the joining main thread. The panic is
            // logged and NOT resumed — the thread ends in the observed
            // "panicked" state.
            let panic_slot = Arc::clone(&health_slot);
            let body = catch_unwind(AssertUnwindSafe(|| {
                run_worker(
                    index_root,
                    annotator_config,
                    config_root,
                    naming_policy,
                    shutdown,
                    health_slot,
                )
            }));
            if let Err(payload) = body {
                let message = panic_payload_message(payload.as_ref());
                error!(
                    event = "annotation_worker.thread_panicked",
                    is_panic = true,
                    panic_message = %message,
                    "annotation worker thread panicked"
                );
                // The worker is dead, so its last-cycle counts are stale. Clear
                // the slot to the not-measured default rather than leave a
                // "cycling" snapshot looking current after a panic (mirror of the
                // scheduler's panic republish).
                publish_annotation_health(&panic_slot, &AnnotationHealth::default());
                info!(
                    event = "annotation_worker.thread_stopped",
                    reason = "panicked",
                    "annotation worker thread stopped"
                );
            }
        })
        // spawn only fails on OS thread-exhaustion; the worker is non-critical,
        // so a spawn failure is logged and the process continues without it
        // rather than aborting startup.
        .unwrap_or_else(|source| {
            error!(
                event = "annotation_worker.spawn_failed",
                error = %source,
                "failed to spawn annotation worker thread; annotations will not build"
            );
            // A handle over a thread that immediately returns keeps the caller's
            // join contract intact without special-casing the failure.
            thread::spawn(|| {})
        })
}

/// Thread body: load the producer client once, then run discovery/build cycles
/// at the idle cadence until shutdown. A client load failure (e.g. a bad key
/// file) is NOT fatal — annotations are non-critical (CAd ruling) — so the
/// worker logs the error and parks in the shutdown-wait loop, leaving the
/// process running. C10b surfaces this parked state.
fn run_worker(
    index_root: PathBuf,
    annotator_config: AnnotatorModelConfig,
    config_root: PathBuf,
    naming_policy: AnnotatorNamingPolicy,
    shutdown: Arc<ShutdownSignal>,
    health_slot: Arc<Mutex<AnnotationHealth>>,
) {
    info!(
        event = "annotation_worker.thread_started",
        index_root = %index_root.display(),
        // Bounded fact only: the naming-rule COUNT — operator rule text never
        // enters logs (same discipline as `policy.loaded`).
        naming_rule_count = naming_policy.rules.len(),
        "annotation worker thread started"
    );

    let client = match AnnotatorClient::load(&annotator_config, &config_root) {
        Ok(client) => client,
        Err(source) => {
            error!(
                event = "annotation_worker.client_load_failed",
                error = %source,
                "annotator client failed to load; worker parked (annotations disabled this run)"
            );
            // Publish the parked state so health surfaces "annotations disabled
            // this run" (diagnostic-only — never gates readiness). The detail is
            // bounded, and no prompt/key content is carried.
            publish_annotation_health(
                &health_slot,
                &AnnotationHealth {
                    parked: true,
                    parked_detail: Some(truncate_persisted_detail(&source.to_string())),
                    last_cycle: None,
                    measured_at: None,
                },
            );
            // Park until shutdown: the process keeps running without
            // annotations rather than crash-looping on a config/key fault.
            shutdown.wait();
            info!(
                event = "annotation_worker.thread_stopped",
                reason = "client_load_failed_parked",
                "annotation worker thread stopped after parking on client load failure"
            );
            return;
        }
    };

    // Per-run retry budget: annotation_id → failed-row reopens issued this
    // run (`ANNOTATION_RETRY_CAP`). Owned here — across cycles, never across
    // restarts — as the ONE exception to cycle statelessness; entries only
    // accumulate for failing annotations, so the map stays tiny.
    let mut retry_attempts: HashMap<String, u32> = HashMap::new();

    loop {
        // One cycle failing is never fatal: the error is logged with context
        // and the next cycle re-discovers from the hot plane (statelessness).
        // A completed cycle returns its freshness report, published diagnostic-
        // only below (invariant 4: this owning thread does the measuring). A
        // cycle-wide fault publishes nothing — the last good cycle's counts stay
        // visible with their own (older) as-of, which is more honest than
        // clearing them on a transient scan failure.
        match run_cycle(
            &index_root,
            &annotator_config,
            &naming_policy.rules,
            &client,
            &shutdown,
            &mut retry_attempts,
        ) {
            Ok(report) => publish_cycle_annotation_health(&health_slot, &report),
            Err(source) => error!(
                event = "annotation_worker.cycle_failed",
                error = %source,
                "annotation discovery cycle failed; retrying next cycle"
            ),
        }

        // Shutdown-aware idle: wakes immediately on shutdown, otherwise waits
        // out the cadence. The in-flight work item already finished (build
        // steps are not interruptible mid-item), so this is a clean boundary.
        if shutdown.wait_timeout(CYCLE_IDLE_INTERVAL) {
            break;
        }
    }

    info!(
        event = "annotation_worker.thread_stopped",
        reason = "shutdown_requested",
        "annotation worker thread stopped cleanly"
    );
}

/// One discovery/build cycle: for every active source, enumerate the missing
/// work items for the policy's post-activation types and build each (memo hit,
/// memo miss, or failed-row retry). Per-source and per-item faults are logged
/// and counted, not propagated — one broken source must not stall the rest —
/// so `Err` is reserved for a cycle-wide fault (e.g. the active-source read
/// itself failing). On success it returns the cycle's freshness `CycleReport`
/// for the C10b health publish (the same counts the completion log carries).
/// `retry_attempts` is the thread-owned per-run reopen budget threaded down to
/// each source's discovery gate (see `ANNOTATION_RETRY_CAP`).
fn run_cycle(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    client: &AnnotatorClient,
    shutdown: &ShutdownSignal,
    retry_attempts: &mut HashMap<String, u32>,
) -> Result<CycleReport, ApiError> {
    let started = Instant::now();
    info!(
        event = "annotation_worker.cycle_started",
        "annotation discovery cycle starting"
    );

    let sources = {
        let connection = hot_plane::open_read(index_root)?;
        read_active_sources(&connection)?
    };

    let mut totals = CycleTotals::default();
    for source in &sources {
        match build_source(
            index_root,
            config,
            naming_rules,
            client,
            source,
            shutdown,
            retry_attempts,
        ) {
            Ok((source_counts, flow)) => {
                totals.add(&source_counts);
                if flow == BuildFlow::Deferred {
                    // A pre-paid annotation boundary hit writer contention. The
                    // lock is typically held for a multi-minute scheduler
                    // projection build, so end this cycle's build work early
                    // rather than probing further sources for nothing; the
                    // stateless next cycle re-discovers everything. The deferral
                    // was already counted and logged (`cycle_deferred`) in
                    // `prepare_work_item`, and any paid wave was flushed before the
                    // Deferred return; the projection hook is skipped for this
                    // source since its annotation build did not finish.
                    break;
                }
                if flow == BuildFlow::ShutdownAbort {
                    // A post-paid completion was abandoned on shutdown (WARN
                    // already emitted at the boundary). Stop all build work now
                    // — no new paid producer call may start after a shutdown
                    // request — but fall through to the cycle summary and
                    // health publish so the partial cycle is still reported.
                    // Not a deferral: no count, no cycle_deferred log.
                    break;
                }
                // Post-activation completion hook: the annotation build for this
                // source's active parse finished this cycle. The two
                // annotation-derived projections (summary, then graph) are built
                // ONLY once the annotations they read are all fresh — they
                // materialize FRESH annotations that exist only after the worker
                // completes and the parse is active, so they cannot build in the
                // scheduler's pre-activation content-derived pass. A projection
                // build fault is isolated here (logged, not propagated) exactly
                // like a per-source annotation fault: a broken projection build
                // for one source must not stall the rest of the cycle, and the
                // stateless next cycle re-attempts it.
                match build_annotation_derived_projections(index_root, source) {
                    Ok(BuildFlow::Deferred) => {
                        // The projection build (a pre-paid boundary) hit writer
                        // contention: count the deferral, log it once, and end
                        // the cycle early for the same reason as above.
                        totals.deferred += 1;
                        info!(
                            event = "annotation_worker.cycle_deferred",
                            source_id = %source.source_id,
                            parse_id = %source.active_parse_id,
                            stage = "projection_build",
                            built = totals.built,
                            memoized = totals.memoized,
                            failed = totals.failed,
                            deferred = totals.deferred,
                            "projection build deferred; hot-plane writer lock held \
                             (re-attempt next cycle)"
                        );
                        break;
                    }
                    // Structurally unreachable: the projection build contains
                    // only the pre-paid `projection_build` boundary, and
                    // `ShutdownAbort` is produced solely by the post-paid
                    // completion boundaries. Treated as Continue (a no-op)
                    // rather than a fault so a future refactor cannot turn this
                    // arm into silent work loss without touching this match.
                    Ok(BuildFlow::Continue | BuildFlow::ShutdownAbort) => {}
                    Err(projection_error) => {
                        totals.projection_failures += 1;
                        error!(
                            event = "annotation_worker.projection_build_failed",
                            source_id = %source.source_id,
                            parse_id = %source.active_parse_id,
                            error = %projection_error,
                            "annotation-derived projection build failed for one source; \
                             continuing with remaining sources"
                        );
                    }
                }
            }
            Err(source_error) => {
                // A per-source fault (e.g. its parse units became unreadable)
                // is recorded and skipped; the cycle continues with the rest.
                totals.source_failures += 1;
                error!(
                    event = "annotation_worker.source_failed",
                    source_id = %source.source_id,
                    parse_id = %source.active_parse_id,
                    error = %source_error,
                    "annotation build failed for one source; continuing with remaining sources"
                );
            }
        }
    }

    info!(
        event = "annotation_worker.cycle_completed",
        sources_examined = sources.len(),
        expected = totals.expected,
        missing = totals.missing,
        built = totals.built,
        memoized = totals.memoized,
        failed = totals.failed,
        source_failures = totals.source_failures,
        projection_failures = totals.projection_failures,
        orphans_adopted = totals.orphans_adopted,
        deferred = totals.deferred,
        exhausted = totals.exhausted,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "annotation discovery cycle completed"
    );
    Ok(CycleReport {
        sources_examined: sources.len() as u64,
        totals,
    })
}

/// The freshness outcome of one completed cycle, returned for the C10b health
/// publish. Pairs the number of active sources examined with the folded
/// per-item `CycleTotals`.
struct CycleReport {
    sources_examined: u64,
    totals: CycleTotals,
}

/// One active source and the parse whose annotations must be built.
struct ActiveSource {
    source_id: String,
    active_parse_id: String,
}

/// Per-cycle work-item accounting, aggregated across sources for the cycle
/// completion log.
#[derive(Default)]
struct CycleTotals {
    expected: u64,
    missing: u64,
    built: u64,
    memoized: u64,
    failed: u64,
    source_failures: u64,
    /// Sources whose annotation build succeeded but whose annotation-derived
    /// projection build (summary/graph) failed this cycle. Counted separately
    /// from `source_failures` so the completion-hook failure domain is visible
    /// in the cycle log distinct from the annotation-build failure domain.
    projection_failures: u64,
    /// Crash-orphaned `building` rows adopted for completion this cycle (§21
    /// crash-recovery evidence). Folded from each source's `SourceCounts` so the
    /// per-cycle health count and the completion log agree.
    orphans_adopted: u64,
    /// Items deferred this cycle because a pre-paid write boundary hit
    /// SQLITE_BUSY on the hot-plane writer lock (typically held by a scheduler
    /// projection build). Deferred work is re-discovered next cycle; nothing is
    /// lost. Folded from each source's `SourceCounts`.
    deferred: u64,
    /// Failed rows skipped this cycle because their per-run reopen budget
    /// (`ANNOTATION_RETRY_CAP`) is spent. Exhausted work stays `failed` in the
    /// hot plane and is re-armed only by a restart or a producer identity
    /// change. Folded from each source's `SourceCounts`.
    exhausted: u64,
}

impl CycleTotals {
    /// Fold one source's counts into the cycle totals.
    fn add(&mut self, other: &SourceCounts) {
        self.expected += other.expected;
        self.missing += other.missing;
        self.built += other.built;
        self.memoized += other.memoized;
        self.failed += other.failed;
        self.orphans_adopted += other.orphans_adopted;
        self.deferred += other.deferred;
        self.exhausted += other.exhausted;
    }
}

/// One source's work-item accounting for a cycle.
#[derive(Default)]
struct SourceCounts {
    expected: u64,
    missing: u64,
    built: u64,
    memoized: u64,
    failed: u64,
    /// Crash-orphaned `building` rows adopted for completion for this source
    /// this cycle (§21 crash-recovery evidence).
    orphans_adopted: u64,
    /// Items deferred for this source this cycle because a pre-paid write
    /// boundary hit SQLITE_BUSY (writer-lock contention); re-discovered next
    /// cycle.
    deferred: u64,
    /// Failed rows skipped for this source this cycle because their per-run
    /// reopen budget (`ANNOTATION_RETRY_CAP`) is spent.
    exhausted: u64,
}

/// One unit of work: a single (invocation × matching producer) pair. Entity
/// and Relation each consume every SectionGroup invocation; Summary consumes
/// every Document invocation (ruling A / `producer::invocation_matches_kind`).
/// `Clone` so a memo-miss item can be carried into a `PendingBuild` wave while
/// the enumerated list keeps ownership of the originals.
#[derive(Clone)]
struct WorkItem {
    kind: ProducerKind,
    invocation: Invocation,
    /// The §21.2 memo key for this item's (target content × producer identity).
    /// Keys the memo CACHE lookup (`memo::lookup`) and the memo row / re-stamp:
    /// cache reuse across producer identities must stay impossible (memoization
    /// honesty), so this key deliberately still folds in producer identity.
    key: String,
    /// The CA2 content key (annotation type × ordered target content hashes,
    /// WITHOUT producer identity). Keys SATISFACTION and reopenable matching, so
    /// a model switch re-annotates only the frontier, not the whole corpus.
    /// (CA2 ruling, user-approved 2026-07-19.)
    content_key: String,
}

/// Whether the cycle's build work should keep going or end early. A pre-paid
/// write boundary that hits SQLITE_BUSY returns `Deferred`, which unwinds up to
/// the cycle loop and stops it probing further items/sources: the writer lock is
/// typically held for a multi-minute scheduler projection build, so further
/// attempts would only burn one busy_timeout each. This is a normal-operation
/// control signal, deliberately NOT an error (`ApiError` is reserved for real
/// faults, which still propagate independently). The deferred work is
/// re-discovered by the stateless next cycle.
#[derive(PartialEq, Eq)]
enum BuildFlow {
    Continue,
    Deferred,
    /// A post-paid completion wait was abandoned because shutdown was requested
    /// (the `completion_abandoned_shutdown` WARN was already emitted at the
    /// boundary). Ends the cycle's build work immediately: no NEW paid producer
    /// call may start after a shutdown request — the abandon race usually
    /// resolves because the writer lock freed, so continuing to the next item
    /// would invoke the external producer again post-shutdown. Deliberately
    /// distinct from `Deferred`: a shutdown abandon is NOT lock contention, so
    /// it is neither counted as `deferred` nor logged as `cycle_deferred`.
    ShutdownAbort,
}

/// Outcome of a POST-PAID completion boundary (`build_complete`, `build_fail`).
/// `Committed` means the transaction begin succeeded (the completion then runs
/// normally). `AbandonedShutdown` means shutdown was requested while waiting for
/// the writer lock, so the completion was abandoned WITHOUT writing — the
/// `building` row is then covered by the existing crash-orphan adoption path,
/// exactly like a process exit. There is deliberately NO retry-count bound: the
/// shutdown signal IS the bound (knob-free), because paid producer output must
/// not be discarded on ordinary contention while the process lives.
#[derive(PartialEq, Eq)]
enum CompletionOutcome {
    Committed,
    AbandonedShutdown,
}

/// Result of `run_post_paid_transaction`: either the body ran and committed
/// (carrying its return value for post-commit logging) or the wait was
/// abandoned on shutdown before the writer lock freed. Distinct from
/// `CompletionOutcome` because it carries the committed body value up to the
/// caller, which the caller's post-commit log needs.
enum PostPaidOutcome<T> {
    Committed(T),
    AbandonedShutdown,
}

/// Run a POST-PAID transaction body on a fresh hot-plane write connection,
/// waiting out writer contention until the transaction begins or shutdown is
/// requested. Each `begin_write_transaction_if_free` attempt already blocked up
/// to the connection's busy_timeout inside SQLite, so between attempts we only
/// probe shutdown (non-blocking) before retrying; the wait is bounded SOLELY by
/// the shutdown signal (no retry-count knob — paid producer output must not be
/// discarded on ordinary contention while the process lives). A periodic INFO
/// every `WAIT_LOG_EVERY` consecutive busy attempts (~5 min) makes a long stall
/// visible in the durable log. On a successful begin the `body` runs on the
/// transaction; `Ok` commits and yields `Committed(value)`, `Err` aborts and
/// propagates. On shutdown before the lock frees, WARN and yield
/// `AbandonedShutdown` WITHOUT writing — the `building` row is then covered by
/// the crash-orphan adoption path, exactly like a process exit.
///
/// The body runs INSIDE this helper so the begun transaction never escapes the
/// retry loop (a returned borrow would conflict with the next attempt's
/// re-borrow of the connection). `shutdown.wait_timeout(Duration::ZERO)` is the
/// non-blocking shutdown probe: true iff shutdown is already requested, with the
/// same poison-recovery semantics as the worker's other shutdown checks, so no
/// new probe method is needed.
fn run_post_paid_transaction<T>(
    index_root: &Path,
    operation: &'static str,
    source: &ActiveSource,
    shutdown: &ShutdownSignal,
    mut body: impl FnMut(&rusqlite::Transaction<'_>) -> Result<T, ApiError>,
) -> Result<PostPaidOutcome<T>, ApiError> {
    // ~60 busy attempts × ~5 s busy_timeout ≈ 5 min between stall logs.
    const WAIT_LOG_EVERY: u64 = 60;
    let mut connection = hot_plane::open_write(index_root)?;
    let started = Instant::now();
    let mut busy_attempts: u64 = 0;
    loop {
        // Shutdown is the only bound: probe before each (re)attempt so a
        // shutdown that lands during a busy_timeout wait is honored promptly.
        if shutdown.wait_timeout(Duration::ZERO) {
            warn!(
                event = "annotation_worker.completion_abandoned_shutdown",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                operation,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "shutdown requested while awaiting writer lock; completion abandoned \
                 (building row left for crash-orphan adoption)"
            );
            return Ok(PostPaidOutcome::AbandonedShutdown);
        }
        match hot_plane::begin_write_transaction_if_free(
            &mut connection,
            TX_LOG_NAMESPACE,
            operation,
        )? {
            WriteTransactionAttempt::Begun(tx) => {
                // Lock acquired: run the caller's body, then commit-or-abort. The
                // tx stays scoped to this iteration, so no borrow escapes.
                return match body(&tx) {
                    Ok(value) => {
                        hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, operation)?;
                        Ok(PostPaidOutcome::Committed(value))
                    }
                    Err(source_error) => Err(hot_plane::abort_transaction(
                        tx,
                        TX_LOG_NAMESPACE,
                        operation,
                        source_error,
                    )),
                };
            }
            WriteTransactionAttempt::Busy => {
                busy_attempts += 1;
                if busy_attempts.is_multiple_of(WAIT_LOG_EVERY) {
                    info!(
                        event = "annotation_worker.completion_waiting",
                        source_id = %source.source_id,
                        parse_id = %source.active_parse_id,
                        operation,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "still awaiting writer lock to commit paid producer output"
                    );
                }
            }
        }
    }
}

/// One producer build that has passed its PRE-PAID `build_open` boundary and is
/// waiting to be dispatched in a wave: the durable `building` row already exists
/// (its `building_id`), so all that remains is the pure producer HTTP call and
/// the POST-PAID completion. Carries the prepared `request` (the `NewAnnotation`
/// that drove `build_open`) so the post-paid completion needs no re-derivation.
struct PendingBuild {
    item: WorkItem,
    request: NewAnnotation,
    building_id: String,
    /// Sampling temperature for this build's producer call, decided at the
    /// discovery gate (base for first attempts, retry-ladder value for
    /// reopens) and stamped into the completed row's provenance.
    effective_temperature: f64,
}

/// Discover and build every missing annotation for one source. Reads the
/// active parse's invocation plan once, enumerates the policy's post-activation
/// work items, computes each item's memo key, then classifies each against the
/// present rows: SATISFIED (a fresh row exists — skip), REOPENABLE (only
/// failed rows, or a crash-orphaned building row — reopen one row and rebuild),
/// or ABSENT (build fresh).
///
/// WAVE STRUCTURE (concurrent producer dispatch). Memo HITs re-mint inline on
/// the worker thread (no producer call), exactly as before. Memo MISSes are
/// prepared serially — each passes its PRE-PAID `build_open` boundary on the
/// worker thread (which durably inserts/reopens the `building` row) and is then
/// BUFFERED into a `PendingBuild` wave of up to `ANNOTATOR_CONCURRENT_CALLS`.
/// When the wave fills (or the items run out), `dispatch_and_commit_wave` fans
/// out ONLY the producer HTTP calls on scoped threads, joins, then commits each
/// POST-PAID result serially on the worker thread. Every SQLite write — memo
/// lookup, `build_open`, complete, park-failed — stays on the worker thread and
/// serial; only the pure producer call runs off-thread.
///
/// RULING-B BOUNDARY POLICY is unchanged and sits INSIDE this structure: a
/// PRE-PAID deferral (memo re-mint or `build_open` hitting SQLITE_BUSY) still
/// flushes any already-buffered wave (its producers are already paid for), then
/// ends the source's work with `Deferred`; a POST-PAID `ShutdownAbort` (a wave
/// commit abandoned on shutdown) still ends the source's work immediately. No
/// NEW wave starts after a shutdown request: `dispatch_and_commit_wave` probes
/// shutdown before dispatching, and the caller probes between waves.
///
/// `retry_attempts` is the per-run failed-row reopen budget: a failed row is
/// reopened at most `ANNOTATION_RETRY_CAP` times per run, then skipped as
/// exhausted at this discovery gate (never reaching a paid producer call).
fn build_source(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    client: &AnnotatorClient,
    source: &ActiveSource,
    shutdown: &ShutdownSignal,
    retry_attempts: &mut HashMap<String, u32>,
) -> Result<(SourceCounts, BuildFlow), ApiError> {
    let mut counts = SourceCounts::default();

    // Build the shared invocation plan and per-item keys on a read connection,
    // dropped before any long-running producer call or write transaction.
    let (work_items, present_keys, reopenable) = {
        let connection = hot_plane::open_read(index_root)?;
        let plan = producer::build_invocation_plan(
            &connection,
            &source.active_parse_id,
            config.max_input_chars,
        )?;
        let work_items = enumerate_work_items(&connection, config, naming_rules, &plan)?;
        // Present CONTENT keys (any status) drive the absent-entirely test; the
        // reopenable map holds one reusable row per unsatisfied CONTENT key
        // (failed rows and crash-orphaned building rows alike). CA2 (user-ruled
        // 2026-07-19): satisfaction/reopen are content-scoped, so a model switch
        // re-annotates only the frontier; the memo CACHE lookup in
        // `prepare_work_item` still keys on the identity-scoped memo key.
        let present_keys =
            store::content_key_hashes_for_parse(&connection, &source.active_parse_id)?;
        let reopenable = store::reopenable_rows_for_parse(&connection, &source.active_parse_id)?;
        (work_items, present_keys, reopenable)
    };

    counts.expected = work_items.len() as u64;

    // The current dispatch wave: producer builds past `build_open`, awaiting a
    // concurrent HTTP dispatch. Kept small (<= ANNOTATOR_CONCURRENT_CALLS).
    let mut pending: Vec<PendingBuild> = Vec::new();

    for item in work_items {
        // Reopenable row (if any) is consumed by the PRE-PAID build_open below.
        // Satisfaction/reopen match on the CONTENT key (CA2): a content key
        // already fresh under ANY producer identity is satisfied, so a model
        // switch does not re-annotate already-covered content.
        let reopened = reopenable.get(&item.content_key);
        let is_unsatisfied = reopened.is_some() || !present_keys.contains(&item.content_key);
        if !is_unsatisfied {
            // SATISFIED (a fresh row exists for the content key): nothing to do.
            continue;
        }

        // Effective sampling temperature for this item's producer call: base
        // for first attempts, escalated per retry below (the ladder).
        let mut effective_temperature = PRODUCER_TEMPERATURE;

        if let Some(row) = reopened {
            // REOPENABLE: reuse the chosen row as the build's building row,
            // skipping the building-insert. A crash orphan is adopted where a
            // prior crash abandoned it; leave durable evidence of that recovery
            // so the orphaned row's history is explained.
            if row.status == store::ReopenableStatus::OrphanedBuilding {
                // Count the adoption for the C10b per-cycle health surface, in
                // step with the durable log below.
                counts.orphans_adopted += 1;
                info!(
                    event = "annotation_worker.orphan_adopted",
                    source_id = %source.source_id,
                    parse_id = %source.active_parse_id,
                    annotation_id = %row.annotation_id,
                    producer = producer_label(item.kind),
                    "crash-orphaned building annotation adopted for completion"
                );
            }
            // Per-run reopen budget (user-ruled 2026-07-21): each failed-row
            // reopen spends one attempt; a row observed failed with its budget
            // spent is EXHAUSTED — skipped, counted for health, and left
            // `failed` in the hot plane. The crossing logs ERROR exactly once
            // (the count is then bumped past the cap as the logged-marker), so
            // steady-state cycles skip silently instead of spamming the log.
            // Orphan adoption above is deliberately budget-exempt: adopting a
            // crash orphan completes paid work, it does not re-pay a producer.
            if row.status == store::ReopenableStatus::Failed {
                let attempts = retry_attempts.entry(row.annotation_id.clone()).or_insert(0);
                if *attempts >= ANNOTATION_RETRY_CAP {
                    if *attempts == ANNOTATION_RETRY_CAP {
                        error!(
                            event = "annotation_worker.retry_exhausted",
                            source_id = %source.source_id,
                            parse_id = %source.active_parse_id,
                            annotation_id = %row.annotation_id,
                            producer = producer_label(item.kind),
                            attempts = ANNOTATION_RETRY_CAP,
                            "producer retries exhausted for this run; giving up \
                             (re-armed by restart or producer identity change)"
                        );
                        *attempts += 1;
                    }
                    counts.exhausted += 1;
                    continue;
                }
                *attempts += 1;
                // Retry k samples at 0.1 × k, saturating at the ceiling: the
                // escalation buys the variation retries exist to exploit,
                // while first attempts everywhere stay at the base. Rounded to
                // one decimal so the wire value and the durable provenance
                // stamp read as the intended ladder step (0.1 × k accumulates
                // f64 noise: 0.1 × 7 = 0.7000000000000001), keeping stored
                // temperatures exactly comparable.
                effective_temperature = ((RETRY_TEMPERATURE_STEP * f64::from(*attempts)) * 10.0)
                    .round()
                    .min(RETRY_TEMPERATURE_CEILING * 10.0)
                    / 10.0;
            }
        }
        counts.missing += 1;

        // PRE-PAID phase, worker-thread serial. A memo HIT re-mints inline (no
        // producer call); a memo MISS passes `build_open` and is buffered for the
        // wave. Either boundary may DEFER on SQLITE_BUSY.
        match prepare_work_item(
            index_root,
            config,
            naming_rules,
            source,
            &item,
            reopened,
            effective_temperature,
            &mut counts,
        )? {
            PreparedItem::Memoized => {}
            PreparedItem::Pending(prepared) => {
                pending.push(*prepared);
                if pending.len() >= ANNOTATOR_CONCURRENT_CALLS {
                    // Wave full: dispatch + commit before buffering more, so at
                    // most ANNOTATOR_CONCURRENT_CALLS HTTP calls are ever in flight.
                    let flow = dispatch_and_commit_wave(
                        index_root,
                        config,
                        naming_rules,
                        client,
                        source,
                        &mut pending,
                        &mut counts,
                        shutdown,
                    )?;
                    if flow != BuildFlow::Continue {
                        return Ok((counts, flow));
                    }
                }
            }
            PreparedItem::Deferred => {
                // PRE-PAID deferral: the already-buffered wave's producers are
                // paid for, so flush them before ending the source's work. The
                // deferral was already counted/logged in `prepare_work_item`.
                let flow = flush_pending_wave(
                    index_root,
                    config,
                    naming_rules,
                    client,
                    source,
                    &mut pending,
                    &mut counts,
                    shutdown,
                )?;
                if flow == BuildFlow::ShutdownAbort {
                    // A flush that abandoned on shutdown overrides the deferral:
                    // no new paid work may start, and the shutdown outcome is the
                    // one the cycle acts on.
                    return Ok((counts, BuildFlow::ShutdownAbort));
                }
                return Ok((counts, BuildFlow::Deferred));
            }
        }
    }

    // Items exhausted: flush the trailing partial wave.
    let flow = flush_pending_wave(
        index_root,
        config,
        naming_rules,
        client,
        source,
        &mut pending,
        &mut counts,
        shutdown,
    )?;
    Ok((counts, flow))
}

/// Flush a non-empty pending wave (dispatch + commit), returning `Continue` when
/// the wave is empty or fully committed and `ShutdownAbort` when a POST-PAID
/// commit was abandoned on shutdown. A pre-paid deferral cannot occur here (the
/// buffered items already passed `build_open`), so `Deferred` is never returned.
// Eight positional args after threading the CA2-P3 naming rules alongside the
// model config; codebase-standard `allow` rather than an unrelated refactor.
#[allow(clippy::too_many_arguments)]
fn flush_pending_wave(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    client: &AnnotatorClient,
    source: &ActiveSource,
    pending: &mut Vec<PendingBuild>,
    counts: &mut SourceCounts,
    shutdown: &ShutdownSignal,
) -> Result<BuildFlow, ApiError> {
    if pending.is_empty() {
        return Ok(BuildFlow::Continue);
    }
    dispatch_and_commit_wave(
        index_root,
        config,
        naming_rules,
        client,
        source,
        pending,
        counts,
        shutdown,
    )
}

/// Outcome of preparing one unsatisfied work item on the worker thread (the
/// PRE-PAID phase). `Memoized` re-minted from cache with no producer call;
/// `Pending` passed `build_open` and is ready for the concurrent producer wave;
/// `Deferred` hit SQLITE_BUSY at a pre-paid boundary (already counted/logged).
///
/// `Pending` is boxed because `PendingBuild` (a `WorkItem` + `NewAnnotation`) is
/// far larger than the other unit variants; boxing keeps this transient
/// control-flow enum small to move rather than sizing every value to the largest
/// variant.
enum PreparedItem {
    Memoized,
    Pending(Box<PendingBuild>),
    Deferred,
}

/// PRE-PAID phase for one work item, worker-thread serial (mirrors the former
/// `build_work_item`, minus the producer call). Memo HIT re-mints inline; memo
/// MISS passes the pre-paid `build_open` boundary and returns a `PendingBuild`
/// for the concurrent wave carrying `effective_temperature` (the discovery
/// gate's base-or-ladder decision; memo re-mints ignore it — no call runs). A
/// SQLITE_BUSY at either pre-paid boundary is counted as `deferred`, logged
/// once (`cycle_deferred`), and returned as `Deferred`.
// Eight positional args after threading the gate's effective temperature;
// codebase-standard `allow` rather than an unrelated refactor.
#[allow(clippy::too_many_arguments)]
fn prepare_work_item(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
    effective_temperature: f64,
    counts: &mut SourceCounts,
) -> Result<PreparedItem, ApiError> {
    // Look up the memo cache on a read connection dropped before any write.
    let cached = {
        let connection = hot_plane::open_read(index_root)?;
        memo::lookup(&connection, &item.key)?
    };

    if let Some(entry) = cached {
        // `remint_from_memo` opens a PRE-PAID (`memo_remint`) write boundary:
        // on writer contention it defers without a model call.
        if remint_from_memo(
            index_root,
            config,
            naming_rules,
            source,
            item,
            reopened,
            &entry,
        )? == BuildFlow::Deferred
        {
            counts.deferred += 1;
            info!(
                event = "annotation_worker.cycle_deferred",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                stage = "memo_remint",
                built = counts.built,
                memoized = counts.memoized,
                failed = counts.failed,
                deferred = counts.deferred,
                "annotation build deferred; hot-plane writer lock held (re-attempt next cycle)"
            );
            return Ok(PreparedItem::Deferred);
        }
        counts.memoized += 1;
        return Ok(PreparedItem::Memoized);
    }

    // Memo MISS: pass the PRE-PAID `build_open` boundary (durable in-flight
    // truth) and buffer for the concurrent producer wave.
    match open_producer_build(index_root, config, naming_rules, source, item, reopened)? {
        Some((request, building_id)) => Ok(PreparedItem::Pending(Box::new(PendingBuild {
            item: item.clone(),
            request,
            building_id,
            effective_temperature,
        }))),
        None => {
            counts.deferred += 1;
            info!(
                event = "annotation_worker.cycle_deferred",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                stage = "build_open",
                built = counts.built,
                memoized = counts.memoized,
                failed = counts.failed,
                deferred = counts.deferred,
                "annotation build deferred; hot-plane writer lock held (re-attempt next cycle)"
            );
            Ok(PreparedItem::Deferred)
        }
    }
}

/// Enumerate the concrete work items for the policy's post-activation types
/// over a parse's invocation plan. Reading the policy here is what consumes
/// `post_activation_types`: each required annotation type maps to its producer
/// kind, and that producer takes every invocation it matches (ruling A). The
/// memo key is computed per item so discovery can classify it.
fn enumerate_work_items(
    conn: &Connection,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    plan: &[Invocation],
) -> Result<Vec<WorkItem>, ApiError> {
    let policy = policy::active_policy()?;

    let mut items = Vec::new();
    for annotation_type in &policy.post_activation_types {
        let kind = producer_kind_for(*annotation_type)?;
        for invocation in plan {
            if !producer::invocation_matches_kind(kind, invocation) {
                continue;
            }
            // Both keys derive from one shared material builder in `memo`: the
            // memo key folds in producer identity (cache scope) — which, per
            // CA2-P3, includes the COMPOSED prompt's hash, so a naming-policy
            // edit invalidates memo reuse — the content key does not
            // (satisfaction scope, never sees the rules). (CA2, user-ruled
            // 2026-07-19.)
            let key = memo::memoization_key_hash(conn, kind, config, naming_rules, invocation)?;
            let content_key = memo::content_key_hash(conn, kind, invocation)?;
            items.push(WorkItem {
                kind,
                invocation: invocation.clone(),
                key,
                content_key,
            });
        }
    }
    Ok(items)
}

/// Produce the build's visible `building` row inside the caller's
/// transaction: reuse the reopened row — flipping a `failed` row back to
/// building via the guarded transition, adopting a crash-orphaned `building`
/// row as-is since it already holds the in-flight status and flipping it
/// would fabricate a transition that never happened — or insert a new one.
fn reopen_or_insert_building(
    tx: &rusqlite::Transaction<'_>,
    reopened: Option<&store::ReopenableRow>,
    request: &store::NewAnnotation,
) -> Result<String, ApiError> {
    match reopened {
        Some(row) => {
            if row.status == store::ReopenableStatus::Failed {
                store::retry_failed(tx, &row.annotation_id)?;
            }
            Ok(row.annotation_id.clone())
        }
        None => store::insert_building(tx, request),
    }
}

/// MEMO HIT (ruling D.1): re-mint the cached invocation output as annotation
/// rows in ONE transaction, with NO model call. The building row (freshly
/// inserted, or the reopened row) completes to item 1; items 2..N are
/// inserted directly fresh. Every re-minted row's final provenance records
/// `memoized: true`, `memoizedFrom: <that item's original annotation id>`, and
/// the memoization key hash, so an auditor always knows the model did not run
/// (§21.3 honesty).
fn remint_from_memo(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
    entry: &memo::MemoEntry,
) -> Result<BuildFlow, ApiError> {
    let request = new_annotation_request(config, naming_rules, source, item)?;

    let mut connection = hot_plane::open_write(index_root)?;
    // PRE-PAID boundary: nothing has been produced yet, so writer contention is
    // a quiet defer (no model call), not a wait — the caller ends the cycle.
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_LOG_NAMESPACE,
        "memo_remint",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(BuildFlow::Deferred),
    };
    let body = (|| -> Result<(), ApiError> {
        // The building row is either freshly inserted or the reopened row;
        // either way it completes to the first cached item.
        let building_id = reopen_or_insert_building(&tx, reopened, &request)?;

        let mut items = entry.items.iter();
        let Some(first) = items.next() else {
            // A cached entry always holds at least one item (memo::record
            // refuses to store an empty array); an empty entry is corruption.
            return Err(ApiError::StorageOperation {
                message: format!("memo entry for key {} is empty on re-mint", item.key),
            });
        };
        let first_provenance = memoized_provenance(&request.provenance, item, first);
        // Re-stamp the memo key to THIS invocation's identity (CA2): a
        // content-scoped reopen may have adopted a row minted under a different
        // producer identity. `item.key` is the memo key just used for the cache
        // lookup, so the completed row keys the cache on the reused identity.
        store::complete_fresh(
            &tx,
            &building_id,
            &first.body,
            first.confidence,
            &first_provenance,
            &item.key,
        )?;

        for extra in items {
            let extra_provenance = memoized_provenance(&request.provenance, item, extra);
            store::insert_fresh(
                &tx,
                &request,
                &extra.body,
                extra.confidence,
                &extra_provenance,
            )?;
        }
        Ok(())
    })();
    if let Err(source_error) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "memo_remint",
            source_error,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "memo_remint")?;

    info!(
        event = "annotation_worker.memo_hit",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        producer = producer_label(item.kind),
        reminted = entry.items.len(),
        "memo hit: cached producer output re-minted without a model call"
    );
    Ok(BuildFlow::Continue)
}

/// MEMO MISS (ruling D.2), PRE-PAID half. Pass the `build_open` boundary: derive
/// the `NewAnnotation` request and insert ONE visible `building` row (or reopen
/// the chosen row) in a committed transaction, so the in-flight truth is durable
/// (§21 rule 3) BEFORE any producer call. This is a PRE-PAID boundary — no
/// producer call has been made — so writer contention defers quietly rather than
/// waiting: `Ok(None)` signals SQLITE_BUSY (the caller counts/logs the deferral),
/// `Ok(Some((request, building_id)))` hands the durable row to the wave. The
/// producer HTTP call and the POST-PAID completion happen later in the wave, off
/// this pre-paid boundary, so a full busy_timeout is never burned before any
/// paid work.
fn open_producer_build(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
) -> Result<Option<(NewAnnotation, String)>, ApiError> {
    let request = new_annotation_request(config, naming_rules, source, item)?;

    let mut connection = hot_plane::open_write(index_root)?;
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_LOG_NAMESPACE,
        "build_open",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(None),
    };
    let building_id = match reopen_or_insert_building(&tx, reopened, &request) {
        Ok(id) => id,
        Err(source_error) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "build_open",
                source_error,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "build_open")?;
    Ok(Some((request, building_id)))
}

/// Dispatch one wave of `PendingBuild`s: fan the pure producer HTTP calls out on
/// scoped OS threads (up to `ANNOTATOR_CONCURRENT_CALLS`, the buffer cap), join,
/// then commit each POST-PAID result SERIALLY on the worker thread. Drains
/// `pending`; on return the wave is empty and its builds are committed (or the
/// cycle is ending on shutdown).
///
/// WHY WRITES STAY SERIAL: `producer::invoke` is the only pure unit — it takes
/// `&AnnotatorClient` (which is `Sync`: it holds a `reqwest::blocking::Client`,
/// `Send + Sync`, plus immutable String/PathBuf/Option fields) — so every thread
/// borrows the same client with no `Arc`. No SQLite handle crosses the scope;
/// `complete_build`/`fail_build` run here, after the join, one at a time, so all
/// hot-plane writes remain serial on this thread and per-target attribution
/// (built vs failed, which `building_id`) is preserved exactly as the old
/// per-item flow.
///
/// WHY JOIN BEFORE COMMIT: the results are matched back to their `PendingBuild`
/// in dispatch order (`scope` joins every thread before it returns; the explicit
/// per-handle joins additionally map a panicked producer thread to a producer
/// error so that target parks `failed` rather than poisoning the wave).
///
/// SHUTDOWN RULE: the shutdown probe is honored BEFORE dispatching — if shutdown
/// is already requested, no new wave starts (the buffered `building` rows are
/// left for crash-orphan adoption, exactly like a process exit) and
/// `ShutdownAbort` is returned. Once a wave is dispatched its in-flight calls
/// complete (bounded by the HTTP timeout); each POST-PAID commit then waits out
/// writer contention bounded by shutdown, and the FIRST commit abandoned on
/// shutdown ends the wave with `ShutdownAbort` (remaining committed-nothing
/// targets are likewise left as crash orphans).
// Eight positional args after threading the CA2-P3 naming rules alongside the
// model config; codebase-standard `allow` rather than an unrelated refactor.
#[allow(clippy::too_many_arguments)]
fn dispatch_and_commit_wave(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    client: &AnnotatorClient,
    source: &ActiveSource,
    pending: &mut Vec<PendingBuild>,
    counts: &mut SourceCounts,
    shutdown: &ShutdownSignal,
) -> Result<BuildFlow, ApiError> {
    if pending.is_empty() {
        return Ok(BuildFlow::Continue);
    }

    // No NEW wave starts after a shutdown request: mirror the existing
    // ShutdownAbort flow. The buffered building rows are durable and covered by
    // crash-orphan adoption next run, exactly like a process exit.
    if shutdown.wait_timeout(Duration::ZERO) {
        warn!(
            event = "annotation_worker.wave_abandoned_shutdown",
            source_id = %source.source_id,
            parse_id = %source.active_parse_id,
            wave_size = pending.len(),
            "shutdown requested before dispatching producer wave; wave abandoned \
             (building rows left for crash-orphan adoption)"
        );
        pending.clear();
        return Ok(BuildFlow::ShutdownAbort);
    }

    let wave = std::mem::take(pending);

    // Phase 2a fan-out: ONLY the pure producer HTTP call runs off-thread. Results
    // are collected in dispatch order so each maps back to its `PendingBuild`.
    let produced: Vec<Result<Vec<ProducedAnnotation>, ApiError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = wave
            .iter()
            .map(|build| {
                // Shared `&client` crosses the scope boundary by reference (Sync);
                // `item.kind`/`item.invocation` are borrowed for this scope only,
                // as is the shared `naming_rules` slice (immutable, Sync).
                scope.spawn(move || {
                    producer::invoke(
                        build.item.kind,
                        client,
                        &build.item.invocation,
                        naming_rules,
                        build.effective_temperature,
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(result) => result,
                // A panicked producer thread becomes a producer error so its
                // target parks `failed` (retried next cycle) rather than
                // unwinding into and poisoning the whole wave/worker.
                Err(_) => Err(ApiError::AnnotationProducer {
                    message: "annotation producer thread panicked".to_string(),
                }),
            })
            .collect()
    });

    // Phase 2b commit: serial on the worker thread, one target at a time,
    // preserving per-target success/failure attribution.
    for (build, outcome) in wave.into_iter().zip(produced) {
        match outcome {
            Ok(produced_items) => {
                // POST-PAID: producer output in hand. `complete_build` waits out
                // writer contention (bounded by shutdown); a shutdown-abandon
                // leaves the building row for crash-orphan adoption and ABORTS the
                // wave so no further paid completion is attempted post-shutdown.
                if complete_build(
                    index_root,
                    config,
                    naming_rules,
                    source,
                    &build.item,
                    &build.request,
                    &build.building_id,
                    &produced_items,
                    build.effective_temperature,
                    shutdown,
                )? == CompletionOutcome::AbandonedShutdown
                {
                    return Ok(BuildFlow::ShutdownAbort);
                }
                counts.built += 1;
            }
            Err(source_error) => {
                // The producer failed (network/endpoint/parse). Park the building
                // row failed with bounded detail; the warn carries endpoint
                // context via the error text (prompt/output never logged).
                // POST-PAID (the failure evidence exists): `fail_build` waits out
                // contention; a shutdown-abandon likewise aborts the wave.
                if fail_build(
                    index_root,
                    source,
                    &build.item,
                    &build.building_id,
                    &source_error,
                    shutdown,
                )? == CompletionOutcome::AbandonedShutdown
                {
                    return Ok(BuildFlow::ShutdownAbort);
                }
                counts.failed += 1;
            }
        }
    }
    Ok(BuildFlow::Continue)
}

/// Phase 2b success: complete item 1 into the building row, insert items 2..N
/// directly fresh, and record the memo entry — all in ONE transaction, so the
/// cache row and the annotation rows it caches commit together. Each row's
/// final provenance records the concrete confidence plus the effective
/// sampling temperature the producer call ran at; the memo entry caches the
/// full item array keyed by the shared memo key.
// Ten positional args after threading `&ShutdownSignal` for the post-paid
// contention wait, the CA2-P3 naming rules, and the effective temperature;
// codebase-standard `allow` rather than an unrelated refactor.
#[allow(clippy::too_many_arguments)]
fn complete_build(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    source: &ActiveSource,
    item: &WorkItem,
    request: &NewAnnotation,
    building_id: &str,
    produced_items: &[ProducedAnnotation],
    effective_temperature: f64,
    shutdown: &ShutdownSignal,
) -> Result<CompletionOutcome, ApiError> {
    // POST-PAID: wait out writer contention for the paid producer output (bounded
    // by shutdown); a shutdown-abandon leaves the building row for crash-orphan
    // adoption. The body yields the reminted count for the post-commit log.
    let reminted = match run_post_paid_transaction(
        index_root,
        "build_complete",
        source,
        shutdown,
        |tx| -> Result<usize, ApiError> {
            let Some((first, rest)) = produced_items.split_first() else {
                // An empty producer result (e.g. no entities found) still
                // completes the building row: the "no annotations" answer is
                // recorded as an empty-body fresh row so the key is satisfied and
                // not rebuilt each cycle. The completed row carries an empty JSON
                // array body.
                //
                // MUST STAY IN STEP (three consumer sites): this empty `[]` body
                // is a BY-DESIGN marker for ALL three producer kinds (entity,
                // relation, summary), NOT a corrupt annotation. Every
                // annotation-derived projection builder that reads these bodies
                // must recognize `[]` and SKIP it (visibly counted), rather than
                // treating it as malformed and failing the whole source's build.
                // The three consumer skip sites are:
                //   - `crate::projections::graph::accumulate_mentions`
                //     (`src/projections/graph.rs`) — entity marker skip.
                //   - `crate::projections::graph::derive_edges`
                //     (`src/projections/graph.rs`) — relation marker skip.
                //   - `crate::projections::view::build_summary`
                //     (`src/projections/view.rs`) — summary marker skip.
                // Changing this marker's shape (anything other than exactly `[]`)
                // requires changing ALL THREE consumer skips together, or the
                // marker becomes a per-cycle rolled-back build-retry poison again.
                let empty_body = serde_json::Value::Array(Vec::new());
                let provenance =
                    completed_provenance(&request.provenance, None, effective_temperature);
                // CA2 memo-key re-stamp: `item.key` is the running producer's
                // memo key; a reopened row minted under another identity gets
                // its cache key corrected here (content key unchanged).
                store::complete_fresh(tx, building_id, &empty_body, None, &provenance, &item.key)?;
                // Nothing to cache: an empty invocation output has no reusable
                // items, so no memo row is written.
                return Ok(0);
            };

            // Item 1 completes the building row; the memo items collect every
            // produced item paired with the annotation id it was minted as, so a
            // later reuse can name the exact per-item memoizedFrom target.
            let first_provenance =
                completed_provenance(&request.provenance, first.confidence, effective_temperature);
            // CA2 memo-key re-stamp to the running producer's identity (`item.key`);
            // the same key is recorded on the memo row below, so cache and row agree.
            store::complete_fresh(
                tx,
                building_id,
                &first.body,
                first.confidence,
                &first_provenance,
                &item.key,
            )?;
            let mut memo_items = vec![MemoItem {
                body: first.body.clone(),
                confidence: first.confidence,
                original_annotation_id: building_id.to_string(),
            }];

            for extra in rest {
                let extra_provenance = completed_provenance(
                    &request.provenance,
                    extra.confidence,
                    effective_temperature,
                );
                let extra_id = store::insert_fresh(
                    tx,
                    request,
                    &extra.body,
                    extra.confidence,
                    &extra_provenance,
                )?;
                memo_items.push(MemoItem {
                    body: extra.body.clone(),
                    confidence: extra.confidence,
                    original_annotation_id: extra_id,
                });
            }

            // Cache write atomic with the truth it caches (ruling D.2). The
            // producer identity hash is recomputed from config plus the CA2-P3
            // naming rules — the same composed-prompt identity the memo key was
            // derived under — so the cache row records the exact identity.
            let identity_hash = item.kind.identity_hash(config, naming_rules)?;
            memo::record(
                tx,
                &item.key,
                item.kind.annotation_type(),
                &identity_hash,
                &memo_items,
            )?;
            Ok(memo_items.len())
        },
    )? {
        PostPaidOutcome::Committed(count) => count,
        PostPaidOutcome::AbandonedShutdown => return Ok(CompletionOutcome::AbandonedShutdown),
    };

    info!(
        event = "annotation_worker.build_completed",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        producer = producer_label(item.kind),
        produced = produced_items.len(),
        cached = reminted,
        "producer invocation completed and cached"
    );
    Ok(CompletionOutcome::Committed)
}

/// Phase 2b failure: park the building row failed with bounded detail in one
/// transaction. The producer error is also warn-logged with endpoint context
/// (which rides in the error text); prompt content and model output never
/// enter the log. The failed row stays visible (§21 rule 3) and is retried
/// next cycle via the failed-only discovery path.
fn fail_build(
    index_root: &Path,
    source: &ActiveSource,
    item: &WorkItem,
    building_id: &str,
    producer_error: &ApiError,
    shutdown: &ShutdownSignal,
) -> Result<CompletionOutcome, ApiError> {
    let detail = truncate_persisted_detail(&producer_error.to_string());

    // POST-PAID: the producer failure evidence exists and must be recorded, so
    // wait out writer contention (bounded by shutdown); a shutdown-abandon leaves
    // the building row for crash-orphan adoption (retried like any interrupted
    // build next run).
    match run_post_paid_transaction(index_root, "build_fail", source, shutdown, |tx| {
        store::mark_failed(tx, building_id, &detail)
    })? {
        PostPaidOutcome::Committed(()) => {}
        PostPaidOutcome::AbandonedShutdown => return Ok(CompletionOutcome::AbandonedShutdown),
    }

    warn!(
        event = "annotation_worker.build_failed",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        producer = producer_label(item.kind),
        annotation_id = %building_id,
        error = %producer_error,
        "producer invocation failed; annotation parked failed and will be retried next cycle"
    );
    Ok(CompletionOutcome::Committed)
}

/// Assemble the `NewAnnotation` request for one work item: the invocation's
/// ordered target unit ids are exactly the resulting annotation's
/// `targetUnitIds` (CAb identity chain), and the planned producer provenance
/// plus memo key are carried up front (§21 rule 3).
fn new_annotation_request(
    config: &AnnotatorModelConfig,
    naming_rules: &[String],
    source: &ActiveSource,
    item: &WorkItem,
) -> Result<NewAnnotation, ApiError> {
    let target_unit_ids = item
        .invocation
        .targets
        .iter()
        .map(|target| target.unit_id.clone())
        .collect::<Vec<_>>();
    let provenance =
        producer::planned_provenance(item.kind, config, naming_rules, &item.invocation.targets)?;

    Ok(NewAnnotation {
        source_id: source.source_id.clone(),
        parse_id: source.active_parse_id.clone(),
        target_unit_ids,
        annotation_type: item.kind.annotation_type(),
        provenance,
        memoization_key_hash: item.key.clone(),
        content_key_hash: item.content_key.clone(),
    })
}

/// Derive the final provenance of a freshly-produced (model-run) annotation
/// from the planned provenance: the model ran, so the memoization fields stay
/// absent (this row is the ORIGINAL, not a reuse), the concrete confidence is
/// filled in, and the sampling temperature the call actually ran at is
/// stamped (base or retry-ladder value — the audit distinction between a
/// deterministic first attempt and an escalated retry, since temperature is
/// not producer-identity-bearing).
fn completed_provenance(
    planned: &Provenance,
    confidence: Option<f64>,
    effective_temperature: f64,
) -> Provenance {
    let mut provenance = planned.clone();
    provenance.confidence = confidence;
    provenance.temperature = Some(effective_temperature);
    provenance
}

/// Derive the final provenance of a MEMO-REUSED annotation from the planned
/// provenance: `memoized: true`, `memoizedFrom` set to the cached item's
/// original annotation id, and the memoization key hash recorded, so an
/// auditor can see the model did not run for this row (§21.3 honesty). The
/// reused item's confidence is carried through unchanged.
fn memoized_provenance(planned: &Provenance, item: &WorkItem, memo_item: &MemoItem) -> Provenance {
    let mut provenance = planned.clone();
    provenance.confidence = memo_item.confidence;
    provenance.memoized = Some(true);
    provenance.memoized_from = Some(memo_item.original_annotation_id.clone());
    provenance.memoization_key_hash = Some(item.key.clone());
    provenance
}

/// Map one required annotation type to the producer that emits it. The MVP
/// policy's post-activation set is exactly {entity, relation, summary}; any
/// other type would be a policy/producer mismatch surfaced loudly rather than
/// silently skipped.
fn producer_kind_for(annotation_type: SemanticAnnotationType) -> Result<ProducerKind, ApiError> {
    match annotation_type {
        SemanticAnnotationType::Entity => Ok(ProducerKind::Entity),
        SemanticAnnotationType::Relation => Ok(ProducerKind::Relation),
        SemanticAnnotationType::Summary => Ok(ProducerKind::Summary),
        other => Err(ApiError::AnnotationProducer {
            message: format!(
                "no MVP producer emits annotation type {other:?}; the required-annotation-set \
                 policy names a type without a producer"
            ),
        }),
    }
}

/// Compact producer label for logs (never prompt or output text).
fn producer_label(kind: ProducerKind) -> &'static str {
    kind.producer_name()
}

/// Read every active source and its active parse for one discovery pass. The
/// query guarantees `active_parse_id` is non-null, so the read is total.
fn read_active_sources(conn: &Connection) -> Result<Vec<ActiveSource>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_ACTIVE_SOURCES_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to prepare active-source query: {source}"),
            })?;
    let rows = statement
        .query_map(params![], |row| {
            Ok(ActiveSource {
                source_id: row.get(0)?,
                active_parse_id: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query active sources: {source}"),
        })?;

    let mut sources = Vec::new();
    for row in rows {
        let source = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read active-source row: {source}"),
        })?;
        sources.push(source);
    }
    Ok(sources)
}

/// Post-activation completion hook: build the two annotation-derived
/// projections (summary, then graph) for one source's active parse, once the
/// annotations that feed them are all fresh.
///
/// WHY POST-ACTIVATION (and why here, not in the scheduler's content-derived
/// pass): both builders read FRESH annotations via
/// `annotations::store::fresh_for_active_parse`, whose active-parse subselect
/// yields rows only after the parse is the source's active parse AND the worker
/// has completed the entity/relation/summary annotations. That state exists
/// only after an annotation build cycle for the active parse — this hook — so
/// pre-activation there would be nothing to materialize.
///
/// GUARDS (only build when the parse is still active AND its annotations are
/// fresh):
///   1. Active-parse re-check: the pointer is re-read here (not trusted from
///      the cycle-start read) because a supersession can land mid-cycle; a
///      pointer that no longer names `active_parse_id` skips the build so no
///      summary/graph envelope is ever materialized for a non-active parse.
///   2. Annotation-completeness: if ANY required annotation of the parse is
///      still `failed` or `building` (a crash orphan), the annotation build is
///      incomplete this cycle — the projections would materialize a partial
///      graph/summary — so the build is skipped and re-attempted next cycle.
///
/// IDEMPOTENCE: each projection type is `envelope::delete_for_parse`-swept
/// immediately before its builder (rebuild replaces rather than accumulates),
/// and the graph builder clears its own payload rows; so a re-run on a later
/// cycle over the same fresh annotations produces identical rows. Both builds
/// ride ONE hot-plane write transaction, so the parse's annotation-derived
/// projection set commits or rolls back together (mirror of the scheduler's
/// content-derived single-transaction discipline).
///
/// CUTOVER STANCE: like the annotation build, this takes NO cutover barrier —
/// it swaps no active pointer and writes only parse-scoped projection rows for
/// the parse the guards confirmed active.
///
/// FAILURE ISOLATION / AUDIT: on any builder Err the transaction is rolled
/// back (discarding partial writes AND the builders' own on-tx `failed`
/// markers), then the failure is recorded durably in a SEPARATE committed
/// transaction (FAILURE-AUDIT invariant, mirroring
/// `scheduler::record_projection_build_failure`) so `projection.failed`
/// survives for the operator even though the build tx vanished.
fn build_annotation_derived_projections(
    index_root: &Path,
    source: &ActiveSource,
) -> Result<BuildFlow, ApiError> {
    // Guard 1 + 2 on a read connection dropped before the write transaction.
    let should_build = {
        let connection = hot_plane::open_read(index_root)?;
        let active_parse_id = read_active_parse_id(&connection, &source.source_id)?;
        if active_parse_id.as_deref() != Some(source.active_parse_id.as_str()) {
            // The parse was superseded (or the source deactivated) between the
            // cycle-start read and now: skip so no projection is built for a
            // non-active parse.
            info!(
                event = "annotation_worker.projection_build_skipped",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                reason = "parse_no_longer_active",
                "skipping annotation-derived projection build; parse is no longer active"
            );
            false
        } else if !parse_annotations_complete(&connection, &source.active_parse_id)? {
            // Some required annotation is still failed/building: the annotation
            // build is incomplete, so building projections now would materialize
            // a partial summary/graph. Skip; the next cycle re-attempts.
            info!(
                event = "annotation_worker.projection_build_skipped",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                reason = "annotations_incomplete",
                "skipping annotation-derived projection build; annotations not yet all fresh"
            );
            false
        } else {
            true
        }
    };
    if !should_build {
        return Ok(BuildFlow::Continue);
    }

    let started = Instant::now();
    info!(
        event = "annotation_worker.projection_build_started",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        "annotation-derived projection build starting"
    );

    let mut connection = hot_plane::open_write(index_root)?;
    // PRE-PAID boundary: the projection build produces nothing external, so on
    // writer contention it defers quietly (caller counts it and ends the cycle)
    // — the stateless next cycle re-attempts the whole build.
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_LOG_NAMESPACE,
        "projection_build",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(BuildFlow::Deferred),
    };
    // The whole build rides `tx`; the first builder Err aborts it below, so no
    // partial annotation-derived projection set ever commits.
    let build =
        build_annotation_projection_transaction(&tx, &source.source_id, &source.active_parse_id);
    match build {
        Ok(()) => {
            hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "projection_build")?;
            info!(
                event = "annotation_worker.projection_build_succeeded",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "annotation-derived projection build committed (summary + graph)"
            );
            Ok(BuildFlow::Continue)
        }
        Err(error) => {
            // Roll back the build tx: this discards partial inserts AND the
            // builders' own on-tx `failed` markers. To keep `projection.failed`
            // durable for the operator, the failure is re-recorded on a fresh
            // committed transaction below (FAILURE-AUDIT invariant).
            let error =
                hot_plane::abort_transaction(tx, TX_LOG_NAMESPACE, "projection_build", error);
            error!(
                event = "annotation_worker.projection_build_failure",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "annotation-derived projection build failed; build transaction rolled back"
            );
            record_projection_build_failure(
                &mut connection,
                &source.source_id,
                &source.active_parse_id,
                &error,
            );
            Err(error)
        }
    }
}

/// The fallible body of the annotation-derived projection build: summary then
/// graph, each preceded by its type's `envelope::delete_for_parse` sweep for
/// rebuild idempotence, on the shared `tx`. Summary is built before graph for a
/// deterministic build order; both are annotation-derived and independent, so
/// the order is a convention, not a dependency. Split out so its single caller
/// owns the commit / rollback-and-audit decision.
fn build_annotation_projection_transaction(
    tx: &rusqlite::Transaction<'_>,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    // Summary: materializes the parse's fresh summary annotations.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::Summary)?;
    view::build_summary(tx, tx, source_id, parse_id)?;

    // Graph: materializes the parse's fresh entity + relation annotations into
    // the mention/edge lookup surface.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::GraphProjection)?;
    graph::build_graph_projection(tx, tx, source_id, parse_id)?;

    Ok(())
}

/// Read the source's CURRENT active-parse pointer for the build-time re-check
/// (see guard 1 in `build_annotation_derived_projections`). `None` means the
/// source has no active parse now (deactivated or never activated), which the
/// caller treats as "parse no longer active" and skips.
fn read_active_parse_id(conn: &Connection, source_id: &str) -> Result<Option<String>, ApiError> {
    conn.query_row(SELECT_ACTIVE_PARSE_ID_SQL, params![source_id], |row| {
        row.get::<_, Option<String>>(0)
    })
    .optional()
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read active parse for source {source_id}: {source}"),
    })
    .map(Option::flatten)
}

/// Whether every required annotation of the parse is fresh — i.e. no `failed`
/// or crash-orphaned `building` row remains (see guard 2). Reuses the discovery
/// reader `store::reopenable_rows_for_parse`, which returns exactly the
/// failed/building rows with no fresh sibling: an empty result means the
/// annotation build is complete for the parse, so its annotation-derived
/// projections may build. This is the "annotations are fresh" precondition the
/// summary/graph builders assume.
fn parse_annotations_complete(conn: &Connection, parse_id: &str) -> Result<bool, ApiError> {
    Ok(store::reopenable_rows_for_parse(conn, parse_id)?.is_empty())
}

/// Record a durable `projection.failed` audit marker after the annotation-derived
/// projection build tx rolled back (FAILURE-AUDIT invariant, mirror of
/// `scheduler::record_projection_build_failure`). The build's own on-tx `failed`
/// markers vanished with the rollback, so this opens a FRESH committed
/// transaction and writes one `building`→`failed` envelope carrying the bounded
/// failure detail, so `projection.failed` survives for the operator. Best-effort:
/// a failure to record the audit is logged but never masks the original build
/// error the caller returns (the build already failed; the projections simply do
/// not materialize this cycle). The generic `Summary` type is used purely as the
/// audit marker's carrier — the failure is per-parse, not per-channel.
fn record_projection_build_failure(
    connection: &mut Connection,
    source_id: &str,
    parse_id: &str,
    build_error: &ApiError,
) {
    let outcome = (|| -> Result<(), ApiError> {
        let tx = hot_plane::begin_write_transaction(
            connection,
            TX_LOG_NAMESPACE,
            "projection_build_failure",
        )?;
        let body = (|| -> Result<(), ApiError> {
            let projection_id = envelope::insert_building(
                &tx,
                &envelope::NewProjection {
                    source_id: source_id.to_string(),
                    parse_id: parse_id.to_string(),
                    projection_type: envelope::ProjectionType::Summary,
                    input_unit_ids: None,
                    input_annotation_ids: None,
                    producer: projection_failure_producer(),
                    index_name: None,
                    index_partition: None,
                },
            )?;
            envelope::mark_failed(&tx, &projection_id, &build_error.to_string())
        })();
        match body {
            Ok(()) => {
                hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "projection_build_failure")
            }
            Err(source) => Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "projection_build_failure",
                source,
            )),
        }
    })();
    if let Err(audit_error) = outcome {
        error!(
            event = "annotation_worker.projection_build.failure_audit_failed",
            source_id,
            parse_id,
            error = %audit_error,
            "durable projection.failed audit could not be recorded; original build error stands"
        );
    }
}

/// The producer provenance stamped on the durable failure-audit envelope. Names
/// the annotation worker's projection-build hook as a `System` producer so the
/// `projection.failed` event is attributable to the integration wiring, not a
/// specific channel builder (the failure is per-parse, not per-channel).
fn projection_failure_producer() -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: PROJECTION_BUILD_PRODUCER_NAME.to_string(),
        producer_version: Some(PROJECTION_BUILD_PRODUCER_VERSION.to_string()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: None,
    }
}

/// Publish one whole `AnnotationHealth` snapshot into the shared slot (C10b),
/// mirroring the scheduler's whole-snapshot poison-recovered publish. A poisoned
/// lock still guards a valid (stale) snapshot, so poison is recovered — a
/// panicked reader must not silence the annotation health surface forever — and
/// the write replaces the value wholly.
fn publish_annotation_health(slot: &Mutex<AnnotationHealth>, snapshot: &AnnotationHealth) {
    let mut guard = match slot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!(
                event = "annotation_worker.health_slot_poisoned",
                "annotation health slot lock was poisoned; recovering and publishing"
            );
            poisoned.into_inner()
        }
    };
    *guard = snapshot.clone();
}

/// Map one completed cycle's `CycleReport` into an `AnnotationHealth` snapshot
/// and publish it (C10b). Not parked (a completed cycle proves the client
/// loaded), the freshness counts carry the cycle's as-of timestamp (invariant
/// 2), and a clock failure loses this publish visibly — the last good cycle's
/// counts stay with their own older as-of — rather than stamping a guessed time.
fn publish_cycle_annotation_health(slot: &Mutex<AnnotationHealth>, report: &CycleReport) {
    let measured_at = match utc_now() {
        Ok(now) => now,
        Err(source) => {
            error!(
                event = "annotation_worker.health_timestamp_failed",
                error = %source,
                "failed to format annotation-cycle as-of timestamp; health not republished"
            );
            return;
        }
    };
    let totals = &report.totals;
    let snapshot = AnnotationHealth {
        parked: false,
        parked_detail: None,
        last_cycle: Some(AnnotationCycleCounts {
            sources_examined: report.sources_examined,
            expected: totals.expected,
            missing: totals.missing,
            built: totals.built,
            memoized: totals.memoized,
            failed: totals.failed,
            source_failures: totals.source_failures,
            projection_failures: totals.projection_failures,
            orphans_adopted: totals.orphans_adopted,
            deferred: totals.deferred,
            exhausted: totals.exhausted,
        }),
        measured_at: Some(measured_at),
    };
    publish_annotation_health(slot, &snapshot);
}
