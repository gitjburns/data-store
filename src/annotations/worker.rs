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
//!     re-attempting across busy_timeout windows until shutdown or maintenance
//!     cancellation. Cancellation discards output, rolls back uncommitted writes,
//!     and leaves `building` rows for rebuild deletion or orphan recovery. The
//!     storage lease remains held until scoped HTTP tasks and SQLite work stop.
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
//! shared slot at document work/commit boundaries and at cycle completion.
//! Updates preserve the other observations under the existing health lock. The snapshot
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
use tracing::{debug, error, info, warn};

use crate::annotations::llm_client::{AnnotatorClient, PRODUCER_TEMPERATURE};
use crate::annotations::memo::{self, MemoItem};
use crate::annotations::policy;
use crate::annotations::producer::{
    self, Invocation, InvocationFailure, ProducedAnnotation, ProducerKind,
};
use crate::annotations::progress::{self, DocumentProgress, WorkState};
use crate::annotations::store::{self, NewAnnotation};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::hot_plane::{self, WriteTransactionAttempt};
use crate::maintenance::{AnnotationCancelReason, AnnotationCancellation};
use crate::model::{ProducerType, Provenance, SemanticAnnotationType};
use crate::primitives::utc_now;
use crate::projections::{envelope, graph, view};
use crate::state::{AnnotationCycleCounts, AnnotationHealth, ShutdownSignal};
use crate::types::{AnnotationActivity, AnnotationProgressCount};
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

/// Monotonic eligibility without adding arbitrarily large configured durations
/// to an Instant. Copies retain the same start time while a cycle does other work.
#[derive(Clone, Copy)]
struct RetryDelay {
    started_at: Instant,
    duration: Duration,
}

impl RetryDelay {
    /// Account for work elapsed since failure instead of restarting the wait per scan.
    fn remaining(self) -> Duration {
        self.duration.saturating_sub(self.started_at.elapsed())
    }
}

/// Independent budgets owned by the serial worker. A snapshot accompanies each
/// observed result into persistence; u64 counters can exceed any u32 retry limit.
#[derive(Clone, Copy, Default)]
struct AnnotationRetryState {
    invalid_outputs: u64,
    execution_failures: u64,
    delay: Option<RetryDelay>,
    exhaustion_reported: bool,
}

impl AnnotationRetryState {
    /// Report retry eligibility without spending or resetting either retry budget.
    fn work_state(&self, config: &AnnotatorModelConfig) -> WorkState {
        if self.exhausted_category(config).is_some() {
            WorkState::Exhausted
        } else if let Some(delay) = self.delay
            && !delay.remaining().is_zero()
        {
            WorkState::RetryWaiting {
                started_at: delay.started_at,
                duration: delay.duration,
            }
        } else {
            WorkState::Failed
        }
    }

    /// The initial attempt plus N retries permits N+1 failures in either category.
    fn exhausted_category(&self, config: &AnnotatorModelConfig) -> Option<&'static str> {
        if self.invalid_outputs > u64::from(config.annotation_max_retries) {
            Some("invalid_output")
        } else if self.execution_failures > u64::from(config.execution_max_retries) {
            Some("execution_failure")
        } else {
            None
        }
    }

    /// Spend only the failed category's allowance; operator cancellation spends neither.
    fn record_failure(&mut self, failure: &InvocationFailure, config: &AnnotatorModelConfig) {
        match failure {
            InvocationFailure::InvalidOutput(_) => self.invalid_outputs += 1,
            InvocationFailure::Call(_) | InvocationFailure::Internal(_) => {
                self.execution_failures += 1
            }
            InvocationFailure::Cancelled(_) => return,
        }
        if self.exhausted_category(config).is_some() {
            self.delay = None;
            return;
        }
        let seconds = match failure {
            InvocationFailure::InvalidOutput(_) => config.annotation_retry_interval_seconds,
            _ => execution_retry_delay_seconds(self.execution_failures, config),
        };
        self.delay = Some(RetryDelay {
            started_at: Instant::now(),
            duration: Duration::from_secs(seconds),
        });
    }

    /// Reach 1.0 on the last allowed malformed-output retry, independently of call failures.
    fn temperature(&self, config: &AnnotatorModelConfig) -> f64 {
        if config.annotation_max_retries == 0 {
            PRODUCER_TEMPERATURE
        } else {
            (self.invalid_outputs as f64 / f64::from(config.annotation_max_retries)).min(1.0)
        }
    }
}

/// Double only execution-failure waits. Saturation reaches even a u64 ceiling in
/// at most 64 doublings; annotation retry intervals never pass through this cap.
fn execution_retry_delay_seconds(failures: u64, config: &AnnotatorModelConfig) -> u64 {
    let mut seconds = config.execution_retry_initial_delay_seconds;
    for _ in 1..failures {
        if seconds >= config.execution_retry_max_delay_seconds {
            break;
        }
        seconds = seconds
            .saturating_mul(2)
            .min(config.execution_retry_max_delay_seconds);
    }
    seconds
}

/// Budgets span cycles; scheduling and the call-failure flag belong to one cycle.
/// Keeping that flag through source-level write errors prevents further calls
/// after an endpoint failure already observed in a joined wave.
#[derive(Default)]
struct RetryState {
    outputs: HashMap<String, AnnotationRetryState>,
    call_failed_in_cycle: bool,
    next_retry: Option<RetryDelay>,
}

impl RetryState {
    /// Consider only work encountered this cycle, avoiding wake loops for retired IDs.
    fn consider_retry(&mut self, delay: RetryDelay) {
        if self
            .next_retry
            .is_none_or(|current| delay.remaining() < current.remaining())
        {
            self.next_retry = Some(delay);
        }
    }

    /// Wake for a short retry while retaining ordinary discovery scans during long waits.
    fn next_cycle_delay(&self) -> Duration {
        self.next_retry.map_or(CYCLE_IDLE_INTERVAL, |delay| {
            CYCLE_IDLE_INTERVAL.min(delay.remaining())
        })
    }
}

/// Log-event namespace passed to the shared hot-plane transaction helpers, so
/// every begin/commit/rollback boundary log is attributable to this worker.
const TX_LOG_NAMESPACE: &str = "annotation";

/// Every active source and the parse whose annotations must be built: the
/// discovery scope of one cycle. `active_parse_id` is guaranteed present by
/// the query's WHERE clause. The correlated aggregate retains every current
/// location without multiplying producer work for multiply located content.
const SELECT_ACTIVE_SOURCES_SQL: &str = "
SELECT id, active_parse_id,
       (SELECT json_group_array(native_uri) FROM source_locations
        WHERE source_id = source_objects.id AND status = 'current')
FROM source_objects
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
/// Producer prompts contain only their single annotation goal; operator naming
/// rules are not appended to requests or incorporated into producer memo identity.
pub(crate) fn start(
    index_root: PathBuf,
    annotator_config: AnnotatorModelConfig,
    config_root: PathBuf,
    shutdown: Arc<ShutdownSignal>,
    maintenance: Arc<crate::maintenance::MaintenanceGate>,
    // C10b diagnostic-only health slot the worker publishes into each cycle (and
    // on the parked path). Same cross-thread Arc/Mutex slot discipline as the
    // scheduler's; `AppState::health()` reads it poison-recovered.
    health_slot: Arc<Mutex<AnnotationHealth>>,
) -> thread::JoinHandle<()> {
    let context = crate::util::LogContext::new("worker", "annotation");
    thread::Builder::new()
        .name("annotation-worker".to_string())
        .spawn(move || {
            let _entered = context.enter();
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
                    shutdown,
                    maintenance,
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
    shutdown: Arc<ShutdownSignal>,
    maintenance: Arc<crate::maintenance::MaintenanceGate>,
    health_slot: Arc<Mutex<AnnotationHealth>>,
) {
    info!(
        event = "annotation_worker.thread_started",
        index_root = %index_root.display(),
        "annotation worker thread started"
    );

    let client = match AnnotatorClient::load(
        &annotator_config,
        &config_root,
        maintenance.annotation_cancellation(),
    ) {
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
                    documents: None,
                    inventory_measured_at: None,
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

    // Both failure budgets and their timers span cycles, but reset on restart or
    // rebuild. Historical failure text is not replayed or classified.
    let mut output_retries = RetryState::default();
    let mut generation = 0;
    let mut delay = Duration::ZERO;

    loop {
        // Keep admission through the cycle's health publication so clearing
        // cannot race a model result, database write, or stale count publish.
        let permit = match maintenance.worker_permit("annotations", delay, generation, &shutdown) {
            Ok(Some(permit)) => permit,
            Ok(None) => break,
            Err(source) => {
                error!(
                    event = "annotation_worker.maintenance_wait_failed",
                    error = %source,
                    "annotation worker could not acquire storage admission"
                );
                publish_annotation_health(
                    &health_slot,
                    &AnnotationHealth {
                        parked: true,
                        parked_detail: Some(truncate_persisted_detail(&format!(
                            "maintenance admission failed: {source}"
                        ))),
                        last_cycle: None,
                        measured_at: None,
                        documents: None,
                        inventory_measured_at: None,
                    },
                );
                info!(
                    event = "annotation_worker.thread_stopped",
                    reason = "maintenance_wait_failed",
                    "annotation worker thread stopped"
                );
                return;
            }
        };
        if permit.generation() != generation {
            generation = permit.generation();
            // Old annotation IDs may recur after rebuilding identical content;
            // their previous retry budget must not limit this fresh corpus.
            output_retries = RetryState::default();
        }
        // One cycle failing is never fatal: the error is logged with context
        // and the next cycle re-discovers from the hot plane (statelessness).
        // A completed cycle returns its freshness report, published diagnostic-
        // only below (invariant 4: this owning thread does the measuring). A
        // cycle-wide fault publishes nothing — the last good cycle's counts stay
        // visible with their own (older) as-of, which is more honest than
        // clearing them on a transient scan failure.
        let context =
            crate::util::LogContext::new("annotation_cycle", &crate::util::diagnostic_id("cycle"));
        context.record("trigger", "annotation_discovery");
        let _entered = context.enter();
        match run_cycle(
            &index_root,
            &annotator_config,
            &client,
            &shutdown,
            &mut output_retries,
            &health_slot,
        ) {
            Ok(Some(report)) if client.cancellation().reason().is_none() => {
                publish_cycle_annotation_health(&health_slot, &report);
            }
            // Cancellation is control flow, not a completed freshness measurement.
            Ok(_) => {
                if let Some(reason) = client.cancellation().reason() {
                    stop_document_activity(&health_slot, reason.label());
                }
            }
            Err(source) => error!(
                event = "annotation_worker.cycle_failed",
                error = %source,
                "annotation discovery cycle failed; retrying next cycle"
            ),
        }

        // Idle without admission; successful maintenance wakes the next cycle
        // immediately instead of waiting out the normal annotation cadence.
        drop(permit);
        // Retry eligibility is per annotation. A five-second retry wakes this
        // loop early; a long backoff does not stop ordinary discovery scans.
        delay = output_retries.next_cycle_delay();
    }

    stop_document_activity(&health_slot, "shutdown");
    info!(
        event = "annotation_worker.thread_stopped",
        reason = "shutdown_requested",
        "annotation worker thread stopped cleanly"
    );
}

/// A stopped worker cannot leave a document looking active or still discovering.
/// Durable completion is retained; the next admitted cycle reconstructs work states.
fn stop_document_activity(slot: &Mutex<AnnotationHealth>, reason: &str) {
    update_annotation_health(slot, |health| {
        if let Some(documents) = &mut health.documents {
            for document in documents {
                if !matches!(
                    document.activity,
                    AnnotationActivity::Complete
                        | AnnotationActivity::NoWork
                        | AnnotationActivity::Unavailable
                ) {
                    document.activity = AnnotationActivity::Stopped;
                    document.detail = Some(format!("Annotation worker stopped: {reason}."));
                    // This control outcome does not remeasure coverage or retry timers.
                }
            }
        }
    });
}

/// One discovery/build cycle: for every active source, enumerate the missing
/// work items for the policy's post-activation types and build each (memo hit,
/// memo miss, or failed-row retry). Per-source and per-item faults are logged
/// and counted, not propagated — one broken source must not stall the rest —
/// so `Err` is reserved for a cycle-wide fault (e.g. the active-source read
/// itself failing). On success it returns the cycle's freshness `CycleReport`
/// for the C10b health publish (the same counts the completion log carries).
/// `output_retries` tracks observed failures, not failed-row reopens;
/// call failures end scheduling after the current wave finishes committing.
fn run_cycle(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    client: &AnnotatorClient,
    shutdown: &ShutdownSignal,
    output_retries: &mut RetryState,
    health_slot: &Mutex<AnnotationHealth>,
) -> Result<Option<CycleReport>, ApiError> {
    let cancellation = client.cancellation();
    if let Some(reason) = cancellation.reason() {
        return cancelled_cycle(reason);
    }
    output_retries.call_failed_in_cycle = false;
    output_retries.next_retry = None;
    let started = Instant::now();
    debug!(
        event = "annotation_worker.cycle_started",
        "annotation discovery cycle starting"
    );

    let sources = {
        let connection = hot_plane::open_read(index_root)?;
        read_active_sources(&connection)?
    };

    // Measure the entire discovered scope before a slow source begins model work.
    // Only compact observations survive this pass; source text is released per plan.
    let documents = sources
        .iter()
        .map(|source| {
            progress::unmeasured(
                &source.source_id,
                &source.active_parse_id,
                &source.source_paths,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    update_annotation_health(health_slot, |health| {
        health.documents = Some(documents);
        health.inventory_measured_at = annotation_measured_at();
    });
    let mut unavailable = std::collections::HashSet::new();
    for source in &sources {
        if let Some(reason) = cancellation.reason() {
            return cancelled_cycle(reason);
        }
        if let Err(source_error) =
            read_source_plan(index_root, config, source, true).and_then(|plan| {
                plan.progress(source, config, output_retries, health_slot)
                    .map(drop)
            })
        {
            record_source_failure(source, &source_error, false, health_slot);
            unavailable.insert(source.source_id.clone());
        }
    }

    let mut totals = CycleTotals::default();
    let mut sources_examined = 0usize;
    for source in &sources {
        if let Some(reason) = cancellation.reason() {
            return cancelled_cycle(reason);
        }
        // Source discovery is a snapshot; an early-ended cycle has not examined
        // the remaining sources. Keep logs and published health honest about that.
        sources_examined += 1;
        if unavailable.contains(&source.source_id) {
            totals.source_failures += 1;
            continue;
        }
        let context = crate::util::LogContext::new("annotation_source", &source.source_id);
        context.record("source_id", source.source_id.as_str());
        context.record("parse_id", source.active_parse_id.as_str());
        context.record("source_paths", source.source_paths.as_str());
        let _entered = context.enter();
        match build_source(
            index_root,
            config,
            client,
            source,
            shutdown,
            output_retries,
            health_slot,
        ) {
            Ok((source_counts, flow)) => {
                if let BuildFlow::Cancelled(reason) = flow {
                    return cancelled_cycle(reason);
                }
                totals.add(&source_counts);
                // This context is stamped once after source work; projection
                // boundary records inherit the final annotation coverage.
                context.record(
                    "annotation_progress",
                    source_counts.progress_count().to_string(),
                );
                if flow == BuildFlow::CallFailed {
                    // The wave has recorded all successes/failures. Stop here
                    // rather than multiplying an endpoint outage across sources.
                    break;
                }
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
                match build_annotation_derived_projections(index_root, source, config, cancellation)
                {
                    Ok(BuildFlow::Cancelled(reason)) => return cancelled_cycle(reason),
                    Ok(BuildFlow::Deferred) => {
                        // The projection build (a pre-paid boundary) hit writer
                        // contention: count the deferral, log it once, and end
                        // the cycle early for the same reason as above.
                        totals.deferred += 1;
                        debug!(
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
                    // `ShutdownAbort` and `CallFailed` originate in producer
                    // waves, not this projection boundary. Treated as Continue (a no-op)
                    // rather than a fault so a future refactor cannot turn this
                    // arm into silent work loss without touching this match.
                    Ok(BuildFlow::Continue | BuildFlow::ShutdownAbort | BuildFlow::CallFailed) => {}
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
                // is recorded and skipped unless a joined wave also observed a
                // call failure: persistence errors must not erase cycle-stop intent.
                totals.source_failures += 1;
                record_source_failure(
                    source,
                    &source_error,
                    output_retries.call_failed_in_cycle,
                    health_slot,
                );
                if output_retries.call_failed_in_cycle {
                    break;
                }
            }
        }
    }

    if let Some(reason) = cancellation.reason() {
        return cancelled_cycle(reason);
    }
    // Discovery and expected writer contention are DEBUG even with work waiting.
    // Exhaustion is reported once at ERROR; unchanged exhausted work is routine
    // discovery. Actual changes and new failures retain an INFO cycle summary.
    let report_activity = totals.built > 0
        || totals.memoized > 0
        || totals.failed > 0
        || totals.source_failures > 0
        || totals.projection_failures > 0
        || totals.orphans_adopted > 0;
    if report_activity {
        info!(
            event = "annotation_worker.cycle_completed",
            sources_examined,
            sources_discovered = sources.len(),
            sources_unexamined = sources.len() - sources_examined,
            missing_scope = "eligible_work_items_examined_before_build",
            failed_scope = "new_failures_this_cycle",
            work_counts_exclude_failed_sources = totals.source_failures > 0,
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
    } else {
        debug!(
            event = "annotation_worker.cycle_completed",
            sources_examined,
            sources_discovered = sources.len(),
            sources_unexamined = sources.len() - sources_examined,
            missing_scope = "eligible_work_items_examined_before_build",
            failed_scope = "new_failures_this_cycle",
            work_counts_exclude_failed_sources = totals.source_failures > 0,
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
    }
    Ok(Some(CycleReport {
        sources_examined: sources_examined as u64,
        totals,
    }))
}

/// Share the existing source-failure record between inventory and dispatch.
/// A failed measurement is explicit; it does not replace coverage with zero.
fn record_source_failure(
    source: &ActiveSource,
    source_error: &ApiError,
    call_failure_cycle_ended: bool,
    health_slot: &Mutex<AnnotationHealth>,
) {
    let mut annotation_progress = AnnotationProgressCount::default();
    update_annotation_health(health_slot, |health| {
        if let Some(document) = health.documents.as_mut().and_then(|documents| {
            documents.iter_mut().find(|document| {
                document.source_id == source.source_id
                    && document.parse_id == source.active_parse_id
            })
        }) {
            annotation_progress = document.progress;
            document.activity = AnnotationActivity::Unavailable;
            document.detail = Some(truncate_persisted_detail(&source_error.to_string()));
            // Keep the last actual accounting timestamp when measurement fails.
        }
    });
    error!(
        event = "annotation_worker.source_failed",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        error = %source_error,
        call_failure_cycle_ended,
        annotation_progress = %annotation_progress,
        "annotation build failed for one source"
    );
}

/// End a cancelled cycle without publishing partial freshness as completed work.
fn cancelled_cycle(reason: AnnotationCancelReason) -> Result<Option<CycleReport>, ApiError> {
    info!(
        event = "annotation_worker.cycle_cancelled",
        reason = reason.label(),
        "annotation cycle cancelled; unfinished results discarded and storage work stopped"
    );
    Ok(None)
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
    /// JSON array of current locations at discovery; empty means none recorded.
    source_paths: String,
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
    /// Failed rows skipped because either per-run retry allowance is spent.
    /// Restart/rebuild resets the counters; source counts fold into this total.
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
struct SourceCounts<'slot> {
    /// Per-item accounting is retained through the source's commits and early exits.
    progress: Option<DocumentProgress<'slot>>,
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
    /// Failed rows skipped for this source because either retry allowance is spent.
    exhausted: u64,
}

impl SourceCounts<'_> {
    /// Missing observations remain unavailable in diagnostic records.
    fn progress_count(&self) -> AnnotationProgressCount {
        self.progress
            .as_ref()
            .map(DocumentProgress::count)
            .unwrap_or_default()
    }

    /// Publish a measured work transition without changing historical cycle totals.
    fn transition(&mut self, key: &str, state: WorkState) -> Result<(), ApiError> {
        if let Some(progress) = &mut self.progress {
            progress.transition(key, state)?;
        }
        Ok(())
    }

    /// Preserve the owning boundary's reason while work waits or stops.
    fn activity(&mut self, activity: AnnotationActivity, detail: Option<String>) {
        if let Some(progress) = &mut self.progress {
            progress.activity(activity, detail);
        }
    }
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
    /// Maintenance cancels the entire cycle without failure or retry accounting.
    Cancelled(AnnotationCancelReason),
    /// A call failed; the complete wave has been recorded, and the next cycle
    /// owns retrying it. Distinct from SQLite contention and output exhaustion.
    CallFailed,
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
/// shutdown or maintenance signal is the bound, because paid producer output
/// must not be discarded on ordinary contention while the process lives.
#[derive(PartialEq, Eq)]
enum CompletionOutcome {
    Committed,
    AbandonedShutdown,
    Cancelled(AnnotationCancelReason),
}

/// Result of `run_post_paid_transaction`: either the body ran and committed
/// (carrying its return value for post-commit logging) or the wait was
/// abandoned on shutdown before the writer lock freed. Distinct from
/// `CompletionOutcome` because it carries the committed body value up to the
/// caller, which the caller's post-commit log needs.
enum PostPaidOutcome<T> {
    Committed(T),
    AbandonedShutdown,
    Cancelled(AnnotationCancelReason),
}

/// Run a POST-PAID transaction body on a fresh hot-plane write connection,
/// waiting out writer contention until the transaction begins or cancellation is
/// requested. Each `begin_write_transaction_if_free` attempt already blocked up
/// to the connection's busy_timeout inside SQLite, so between attempts we only
/// probe shutdown and maintenance before retrying; the wait is bounded by
/// their cancellation signals (paid producer output must not be
/// discarded on ordinary contention while the process lives). A periodic INFO
/// every `WAIT_LOG_EVERY` consecutive busy attempts (~5 min) makes a long stall
/// visible in the durable log. On a successful begin the `body` runs on the
/// transaction; `Ok` commits unless cancellation arrived during the body,
/// in which case it rolls back and yields `Cancelled`. `Err` aborts and
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
    cancellation: &AnnotationCancellation,
    counts: &mut SourceCounts,
    mut body: impl FnMut(&rusqlite::Transaction<'_>) -> Result<T, ApiError>,
) -> Result<PostPaidOutcome<T>, ApiError> {
    // ~60 busy attempts × ~5 s busy_timeout ≈ 5 min between stall logs.
    const WAIT_LOG_EVERY: u64 = 60;
    if let Some(reason) = cancellation.reason() {
        return Ok(PostPaidOutcome::Cancelled(reason));
    }
    let mut connection = hot_plane::open_write(index_root)?;
    let started = Instant::now();
    let mut busy_attempts: u64 = 0;
    loop {
        // Maintenance also discards paid output; never retry a busy writer after
        // cancellation. SQLite remains synchronous and releases before admission.
        if let Some(reason) = cancellation.reason() {
            return Ok(PostPaidOutcome::Cancelled(reason));
        }
        if shutdown.wait_timeout(Duration::ZERO) {
            warn!(
                event = "annotation_worker.completion_abandoned_shutdown",
                annotation_progress = %counts.progress_count(),
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
                counts.activity(AnnotationActivity::AwaitingCommit, None);
                if let Some(reason) = cancellation.reason() {
                    rollback_cancelled(tx, operation, reason)?;
                    return Ok(PostPaidOutcome::Cancelled(reason));
                }
                // Lock acquired: run the caller's body, then commit-or-abort. The
                // tx stays scoped to this iteration, so no borrow escapes.
                return match body(&tx) {
                    Ok(value) => {
                        if let Some(reason) = cancellation.reason() {
                            rollback_cancelled(tx, operation, reason)?;
                            return Ok(PostPaidOutcome::Cancelled(reason));
                        }
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
                if busy_attempts == 1 {
                    counts.activity(
                        AnnotationActivity::WaitingForStorage,
                        Some("Waiting for SQLite to persist annotation results.".to_string()),
                    );
                }
                if busy_attempts.is_multiple_of(WAIT_LOG_EVERY) {
                    info!(
                        event = "annotation_worker.completion_waiting",
                        annotation_progress = %counts.progress_count(),
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

/// Roll back cancelled work explicitly so logs distinguish confirmed cleanup
/// from an actual rollback failure. Cancellation itself is never a storage error.
fn rollback_cancelled(
    tx: rusqlite::Transaction<'_>,
    operation: &'static str,
    reason: AnnotationCancelReason,
) -> Result<(), ApiError> {
    tx.rollback().map_err(|source| {
        error!(
            event = "annotation_worker.cancellation_rollback_failed",
            operation,
            reason = reason.label(),
            error = %source,
            durable_outcome = "unconfirmed",
            "cancelled annotation transaction rollback failed"
        );
        ApiError::StorageOperation {
            message: format!("failed to roll back cancelled {operation} transaction: {source}"),
        }
    })?;
    info!(
        event = "annotation_worker.transaction_cancelled",
        operation,
        reason = reason.label(),
        durable_outcome = "rolled_back",
        "cancelled annotation transaction rolled back"
    );
    Ok(())
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
    /// The same invocation context accompanies its HTTP thread and later commit.
    log_context: crate::util::LogContext,
    /// Measures only buffering after build_open, not model or database work.
    prepared_at: Instant,
}

/// One read snapshot supplies both dispatch and progress with the same coverage facts.
struct SourcePlan {
    work_items: Vec<WorkItem>,
    present_keys: std::collections::HashSet<String>,
    reopenable: HashMap<String, store::ReopenableRow>,
}

/// Read a plan without holding SQLite across model work or health publication.
fn read_source_plan(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    inventory_only: bool,
) -> Result<SourcePlan, ApiError> {
    let (work_items, present_keys, reopenable) = {
        let mut connection = hot_plane::open_read(index_root)?;
        // Pin plan and coverage together; activation may run on the scheduler.
        let connection = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .map_err(|error| ApiError::StorageOperation {
                message: format!(
                    "failed to read annotation plan for {} in one snapshot: {error}",
                    source.active_parse_id
                ),
            })?;
        // Measuring progress must not duplicate the existing dispatch plan log.
        let plan_builder = if inventory_only {
            producer::measure_invocation_plan
        } else {
            producer::build_invocation_plan
        };
        let plan = plan_builder(&connection, &source.active_parse_id, config.max_input_chars)?;
        let work_items = enumerate_work_items(&connection, config, &plan)?;
        // Only fresh CONTENT keys satisfy coverage. The reopenable map holds
        // one reusable row per unsatisfied CONTENT key
        // (failed rows and crash-orphaned building rows alike). CA2 (user-ruled
        // 2026-07-19): satisfaction/reopen are content-scoped, so a model switch
        // re-annotates only the frontier; the memo CACHE lookup in
        // `prepare_work_item` still keys on the identity-scoped memo key.
        let present_keys =
            store::fresh_content_key_hashes_for_parse(&connection, &source.active_parse_id)?;
        let reopenable = store::reopenable_rows_for_parse(&connection, &source.active_parse_id)?;
        (work_items, present_keys, reopenable)
    };

    Ok(SourcePlan {
        work_items,
        present_keys,
        reopenable,
    })
}

impl SourcePlan {
    /// Reconstruct committed work and classify unfinished keys from current retry
    /// state. A persisted building row at discovery is an orphan, never live work.
    fn progress<'slot>(
        &self,
        source: &ActiveSource,
        config: &AnnotatorModelConfig,
        retries: &RetryState,
        slot: &'slot Mutex<AnnotationHealth>,
    ) -> Result<DocumentProgress<'slot>, ApiError> {
        let planned = self
            .work_items
            .iter()
            .map(|item| {
                let state = if self.present_keys.contains(&item.content_key) {
                    WorkState::Completed
                } else if let Some(row) = self.reopenable.get(&item.content_key) {
                    match retries.outputs.get(&row.annotation_id) {
                        Some(retry)
                            if retry.invalid_outputs > 0 || retry.execution_failures > 0 =>
                        {
                            retry.work_state(config)
                        }
                        // Merely adopting an orphan creates an empty retry entry;
                        // it does not prove any failed producer attempt occurred.
                        _ if row.status == store::ReopenableStatus::Failed => WorkState::Failed,
                        _ => WorkState::Pending,
                    }
                } else {
                    WorkState::Pending
                };
                (item.content_key.clone(), item.kind.annotation_type(), state)
            })
            .collect();
        DocumentProgress::new(
            slot,
            progress::unmeasured(
                &source.source_id,
                &source.active_parse_id,
                &source.source_paths,
            )?,
            &policy::active_policy()?.post_activation_types,
            planned,
        )
    }
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
/// Each failure category has its own configured retry allowance. Reopening a
/// row, waiting for eligibility, or deferring on contention spends neither budget.
fn build_source<'slot>(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    client: &AnnotatorClient,
    source: &ActiveSource,
    shutdown: &ShutdownSignal,
    output_retries: &mut RetryState,
    health_slot: &'slot Mutex<AnnotationHealth>,
) -> Result<(SourceCounts<'slot>, BuildFlow), ApiError> {
    let mut counts = SourceCounts::default();
    let cancellation = client.cancellation();
    if let Some(reason) = cancellation.reason() {
        return Ok((counts, BuildFlow::Cancelled(reason)));
    }

    // Build the shared invocation plan and per-item keys on a read connection,
    // dropped before any long-running producer call or write transaction.
    let plan = read_source_plan(index_root, config, source, false)?;
    counts.progress = Some(plan.progress(source, config, output_retries, health_slot)?);
    let SourcePlan {
        work_items,
        present_keys,
        reopenable,
    } = plan;
    counts.expected = work_items.len() as u64;

    // The current dispatch wave: producer builds past `build_open`, awaiting a
    // concurrent HTTP dispatch. Kept small (<= ANNOTATOR_CONCURRENT_CALLS).
    let mut pending: Vec<PendingBuild> = Vec::new();

    for item in work_items {
        if let Some(reason) = cancellation.reason() {
            // Buffered rows are recoverable; no new producer work is owed for them.
            return Ok((counts, BuildFlow::Cancelled(reason)));
        }
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

        // Equal text at another location has a distinct completion key but may
        // share this memo. Commit its pending producer before looking up the
        // cache again, avoiding duplicate paid calls and colliding memo INSERTs.
        if pending.iter().any(|build| build.item.key == item.key) {
            let flow = flush_pending_wave(
                index_root,
                config,
                client,
                source,
                &mut pending,
                &mut counts,
                output_retries,
                shutdown,
            )?;
            if flow != BuildFlow::Continue {
                return Ok((counts, flow));
            }
        }

        let context = item.invocation.log_context();
        let prior_invalid_outputs = reopened
            .and_then(|row| output_retries.outputs.get(&row.annotation_id))
            .map_or(0, |retry| retry.invalid_outputs);
        context.record("prior_invalid_outputs", prior_invalid_outputs);
        context.record(
            "trigger",
            match reopened {
                Some(row) if row.status == store::ReopenableStatus::OrphanedBuilding => {
                    "orphan_recovery"
                }
                Some(_) => "failed_annotation_retry",
                None => "missing_annotation",
            },
        );
        if let Some(row) = reopened {
            context.record("annotation_id", row.annotation_id.as_str());
        }
        let _entered = context.enter();
        // Effective sampling temperature for this item's producer call: base
        // for first attempts, escalated per retry below (the ladder).
        let mut effective_temperature = PRODUCER_TEMPERATURE;

        if let Some(row) = reopened {
            // REOPENABLE: reuse the chosen row as the build's building row,
            // skipping the building-insert. A crash orphan is adopted where a
            // prior crash abandoned it; leave durable evidence of that recovery
            // so the orphaned row's history is explained.
            // Reopening spends neither allowance. Each observed producer failure
            // charges its category after dispatch; exhaustion takes precedence over timers.
            {
                let retry = output_retries
                    .outputs
                    .entry(row.annotation_id.clone())
                    .or_default();
                if let Some(category) = retry.exhausted_category(config) {
                    counts.transition(&item.content_key, WorkState::Exhausted)?;
                    if !retry.exhaustion_reported {
                        error!(
                            event = "annotation_worker.retry_exhausted",
                            source_id = %source.source_id,
                            parse_id = %source.active_parse_id,
                            annotation_id = %row.annotation_id,
                            producer = producer_label(item.kind),
                            attempts = retry.invalid_outputs + retry.execution_failures,
                            annotation_progress = %counts.progress_count(),
                            attempts_scope = "failed_chain_attempts_in_this_process",
                            annotation_max_retries = config.annotation_max_retries,
                            execution_max_retries = config.execution_max_retries,
                            failure_class = category,
                            output_failures = retry.invalid_outputs,
                            execution_failures = retry.execution_failures,
                            "producer retries exhausted for this run; giving up \
                             (re-armed by restart or rebuild)"
                        );
                        retry.exhaustion_reported = true;
                    }
                    counts.exhausted += 1;
                    continue;
                }
                if let Some(delay) = retry.delay
                    && !delay.remaining().is_zero()
                {
                    counts.transition(&item.content_key, retry.work_state(config))?;
                    debug!(event = "annotation_worker.retry_wait", annotation_id = %row.annotation_id,
                        annotation_progress = %counts.progress_count(),
                        retry_in_seconds = delay.remaining().as_secs_f64(),
                        "annotation retry is not yet eligible");
                    output_retries.consider_retry(delay);
                    continue;
                }
                effective_temperature = retry.temperature(config);
                retry.delay = None;
            }
            if row.status == store::ReopenableStatus::OrphanedBuilding {
                // Count the adoption for the C10b per-cycle health surface, in
                // step with the durable log below.
                counts.orphans_adopted += 1;
                info!(
                    event = "annotation_worker.orphan_adopted",
                    annotation_progress = %counts.progress_count(),
                    source_id = %source.source_id,
                    parse_id = %source.active_parse_id,
                    annotation_id = %row.annotation_id,
                    producer = producer_label(item.kind),
                    "crash-orphaned building annotation adopted for completion"
                );
            }
        }
        counts.missing += 1;
        // Preparation owns this chain until its commit or an explicit deferral.
        counts.transition(&item.content_key, WorkState::Running)?;

        // PRE-PAID phase, worker-thread serial. A memo HIT re-mints inline (no
        // producer call); a memo MISS passes `build_open` and is buffered for the
        // wave. Either boundary may DEFER on SQLITE_BUSY.
        match prepare_work_item(
            index_root,
            config,
            source,
            &item,
            reopened,
            effective_temperature,
            &mut counts,
            cancellation,
        )? {
            PreparedItem::Memoized => {}
            PreparedItem::Cancelled(reason) => return Ok((counts, BuildFlow::Cancelled(reason))),
            PreparedItem::Pending(prepared) => {
                pending.push(*prepared);
                if pending.len() >= ANNOTATOR_CONCURRENT_CALLS {
                    // Wave full: dispatch + commit before buffering more, so at
                    // most ANNOTATOR_CONCURRENT_CALLS HTTP calls are ever in flight.
                    let flow = dispatch_and_commit_wave(
                        index_root,
                        config,
                        client,
                        source,
                        &mut pending,
                        &mut counts,
                        output_retries,
                        shutdown,
                    )?;
                    if flow != BuildFlow::Continue {
                        return Ok((counts, flow));
                    }
                }
            }
            PreparedItem::Deferred => {
                let state =
                    if reopened.is_some_and(|row| row.status == store::ReopenableStatus::Failed) {
                        WorkState::Failed
                    } else {
                        WorkState::Pending
                    };
                counts.transition(&item.content_key, state)?;
                // PRE-PAID deferral: the already-buffered wave's producers are
                // paid for, so flush them before ending the source's work. The
                // deferral was already counted/logged in `prepare_work_item`.
                let flow = flush_pending_wave(
                    index_root,
                    config,
                    client,
                    source,
                    &mut pending,
                    &mut counts,
                    output_retries,
                    shutdown,
                )?;
                if flow != BuildFlow::Continue {
                    // Preserve a flushed wave's shutdown or call-failure signal
                    // rather than relabeling it as the earlier SQLite deferral.
                    return Ok((counts, flow));
                }
                counts.activity(
                    AnnotationActivity::WaitingForStorage,
                    Some("Annotation writes deferred by SQLite contention.".to_string()),
                );
                return Ok((counts, BuildFlow::Deferred));
            }
        }
    }

    // Items exhausted: flush the trailing partial wave.
    let flow = flush_pending_wave(
        index_root,
        config,
        client,
        source,
        &mut pending,
        &mut counts,
        output_retries,
        shutdown,
    )?;
    Ok((counts, flow))
}

/// Flush a non-empty pending wave (dispatch + commit), returning `Continue` when
/// the wave is empty or fully committed without call failures, `CallFailed`
/// after recording a wave with call failures, or `ShutdownAbort` when a POST-PAID
/// commit was abandoned on shutdown. A pre-paid deferral cannot occur here (the
/// buffered items already passed `build_open`), so `Deferred` is never returned.
// The source-scoped wave inputs include worker-owned retry state; keep the
// existing explicit boundary rather than introduce a pass-through context.
#[allow(clippy::too_many_arguments)]
fn flush_pending_wave(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    client: &AnnotatorClient,
    source: &ActiveSource,
    pending: &mut Vec<PendingBuild>,
    counts: &mut SourceCounts,
    output_retries: &mut RetryState,
    shutdown: &ShutdownSignal,
) -> Result<BuildFlow, ApiError> {
    if pending.is_empty() {
        return Ok(BuildFlow::Continue);
    }
    dispatch_and_commit_wave(
        index_root,
        config,
        client,
        source,
        pending,
        counts,
        output_retries,
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
    Cancelled(AnnotationCancelReason),
}

/// PRE-PAID phase for one work item, worker-thread serial (mirrors the former
/// `build_work_item`, minus the producer call). Memo HIT re-mints inline; memo
/// MISS passes the pre-paid `build_open` boundary and returns a `PendingBuild`
/// for the concurrent wave carrying `effective_temperature` (the discovery
/// gate's base-or-ladder decision; memo re-mints ignore it — no call runs). A
/// SQLITE_BUSY at either pre-paid boundary is counted as `deferred`, logged
/// once (`cycle_deferred`), and returned as `Deferred`.
// Keep the established source/item preparation inputs and cancellation explicit.
#[allow(clippy::too_many_arguments)]
fn prepare_work_item(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
    effective_temperature: f64,
    counts: &mut SourceCounts,
    cancellation: &AnnotationCancellation,
) -> Result<PreparedItem, ApiError> {
    if let Some(reason) = cancellation.reason() {
        return Ok(PreparedItem::Cancelled(reason));
    }
    // Look up the memo cache on a read connection dropped before any write.
    let cached = {
        let connection = hot_plane::open_read(index_root)?;
        memo::lookup(&connection, &item.key)?
    };

    if let Some(entry) = cached {
        // `remint_from_memo` opens a PRE-PAID (`memo_remint`) write boundary:
        // on writer contention it defers without a model call.
        let flow = remint_from_memo(
            index_root,
            config,
            source,
            item,
            reopened,
            &entry,
            cancellation,
            counts,
        )?;
        if let BuildFlow::Cancelled(reason) = flow {
            return Ok(PreparedItem::Cancelled(reason));
        }
        if flow == BuildFlow::Deferred {
            counts.deferred += 1;
            debug!(
                event = "annotation_worker.cycle_deferred",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                stage = "memo_remint",
                annotation_progress = %counts.progress_count(),
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
    let opened = open_producer_build(index_root, config, source, item, reopened, cancellation)?;
    // A cancelled open leaves no new work to dispatch and must not count as
    // contention. The admission lease keeps this signal latched until we exit.
    if let Some(reason) = cancellation.reason() {
        return Ok(PreparedItem::Cancelled(reason));
    }
    match opened {
        Some((request, building_id)) => {
            let log_context = crate::util::LogContext::current();
            if reopened.is_none() {
                log_context.record("annotation_id", building_id.as_str());
            }
            Ok(PreparedItem::Pending(Box::new(PendingBuild {
                item: item.clone(),
                request,
                building_id,
                effective_temperature,
                log_context,
                prepared_at: Instant::now(),
            })))
        }
        None => {
            counts.deferred += 1;
            debug!(
                event = "annotation_worker.cycle_deferred",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                stage = "build_open",
                annotation_progress = %counts.progress_count(),
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
            // Both keys share content material. Only the memo key includes the
            // producer's single-goal prompt contracts; changing a prompt prevents
            // cache reuse without making already-fresh content unsatisfied.
            let key = memo::memoization_key_hash(conn, kind, config, invocation)?;
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
// Keep cancellation explicit at this existing source/item transaction boundary.
#[allow(clippy::too_many_arguments)]
fn remint_from_memo(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
    entry: &memo::MemoEntry,
    cancellation: &AnnotationCancellation,
    counts: &mut SourceCounts,
) -> Result<BuildFlow, ApiError> {
    if let Some(reason) = cancellation.reason() {
        return Ok(BuildFlow::Cancelled(reason));
    }
    let request = new_annotation_request(config, source, item)?;

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
    if let Some(reason) = cancellation.reason() {
        rollback_cancelled(tx, "memo_remint", reason)?;
        return Ok(BuildFlow::Cancelled(reason));
    }
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
    // A rebuild that arrived during synchronous replay must not publish that replay.
    if let Some(reason) = cancellation.reason() {
        rollback_cancelled(tx, "memo_remint", reason)?;
        return Ok(BuildFlow::Cancelled(reason));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "memo_remint")?;
    counts.transition(&item.content_key, WorkState::Completed)?;

    info!(
        event = "annotation_worker.memo_hit",
        annotation_progress = %counts.progress_count(),
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
/// waiting: `Ok(None)` signals SQLITE_BUSY or cancellation (the caller checks
/// the latched cancellation reason before counting/logging a deferral),
/// `Ok(Some((request, building_id)))` hands the durable row to the wave. The
/// producer HTTP call and the POST-PAID completion happen later in the wave, off
/// this pre-paid boundary, so a full busy_timeout is never burned before any
/// paid work.
fn open_producer_build(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    item: &WorkItem,
    reopened: Option<&store::ReopenableRow>,
    cancellation: &AnnotationCancellation,
) -> Result<Option<(NewAnnotation, String)>, ApiError> {
    if cancellation.reason().is_some() {
        return Ok(None);
    }
    let request = new_annotation_request(config, source, item)?;

    let mut connection = hot_plane::open_write(index_root)?;
    let tx = match hot_plane::begin_write_transaction_if_free(
        &mut connection,
        TX_LOG_NAMESPACE,
        "build_open",
    )? {
        WriteTransactionAttempt::Begun(tx) => tx,
        WriteTransactionAttempt::Busy => return Ok(None),
    };
    if let Some(reason) = cancellation.reason() {
        rollback_cancelled(tx, "build_open", reason)?;
        return Ok(None);
    }
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
    if let Some(reason) = cancellation.reason() {
        rollback_cancelled(tx, "build_open", reason)?;
        return Ok(None);
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "build_open")?;
    Ok(Some((request, building_id)))
}

/// Dispatch one wave of `PendingBuild`s: fan the pure producer HTTP calls out on
/// scoped OS threads (up to `ANNOTATOR_CONCURRENT_CALLS`, the buffer cap), join,
/// then commit each POST-PAID result SERIALLY on the worker thread. Drains
/// `pending`; on return the wave is empty and its builds are committed (or the
/// cycle is ending on cancellation).
///
/// WHY WRITES STAY SERIAL: `producer::invoke` is the only pure unit — it takes
/// `&AnnotatorClient` (which owns the shared HTTP runtime and cancellation watch)
/// — so each scoped thread borrows the same client. No SQLite handle crosses the scope;
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
/// CANCELLATION RULE: maintenance is checked before dispatch and after every
/// scoped HTTP thread joins, before accounting or persistence. Cancelled waves
/// discard both successes and failures. Every transaction checks again before
/// its body and commit; no storage admission is released with live writers.
/// Buffered `building` rows remain for rebuild deletion or orphan recovery.
// The source-scoped wave inputs include worker-owned retry state; keep the
// existing explicit boundary rather than introduce a pass-through context.
#[allow(clippy::too_many_arguments)]
fn dispatch_and_commit_wave(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    client: &AnnotatorClient,
    source: &ActiveSource,
    pending: &mut Vec<PendingBuild>,
    counts: &mut SourceCounts,
    output_retries: &mut RetryState,
    shutdown: &ShutdownSignal,
) -> Result<BuildFlow, ApiError> {
    let cancellation = client.cancellation();
    if let Some(reason) = cancellation.reason() {
        pending.clear();
        return Ok(BuildFlow::Cancelled(reason));
    }
    if pending.is_empty() {
        return Ok(BuildFlow::Continue);
    }

    // No NEW wave starts after a shutdown request: mirror the existing
    // ShutdownAbort flow. The buffered building rows are durable and covered by
    // crash-orphan adoption next run, exactly like a process exit.
    if shutdown.wait_timeout(Duration::ZERO) {
        warn!(
            event = "annotation_worker.wave_abandoned_shutdown",
            annotation_progress = %counts.progress_count(),
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
    // No result commits during fan-out, so one immutable committed-count snapshot
    // is authoritative for every existing call/stage/transcript entry in this wave.
    let annotation_progress = counts.progress_count();
    counts.activity(AnnotationActivity::Running, None);

    // Phase 2a fan-out: ONLY the pure producer HTTP call runs off-thread. Results
    // are collected in dispatch order so each maps back to its `PendingBuild`.
    let produced: Vec<Result<Vec<ProducedAnnotation>, InvocationFailure>> =
        std::thread::scope(|scope| {
            let handles: Vec<_> = wave
                .iter()
                .map(|build| {
                    // Shared `&client` crosses the scope boundary by reference (Sync);
                    // `item.kind`/`item.invocation` are borrowed for this scope only.
                    scope.spawn(move || {
                        // Scoped threads do not inherit tracing's thread-local span.
                        let _entered = build.log_context.enter();
                        build.log_context.record(
                            "dispatch_wait_ms",
                            build.prepared_at.elapsed().as_millis() as u64,
                        );
                        producer::invoke(
                            build.item.kind,
                            client,
                            &build.item.invocation,
                            build.effective_temperature,
                            Some(annotation_progress),
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
                    Err(payload) => {
                        Err(InvocationFailure::Internal(ApiError::AnnotationProducer {
                            message: format!(
                                "annotation producer thread panicked: {}",
                                panic_payload_message(payload.as_ref())
                            ),
                        }))
                    }
                })
                .collect()
        });

    // Scoped joins finish every HTTP task before we release storage admission.
    // Cancellation wins over successes and failures alike, before retry debt or
    // durable result state is created for this discarded wave.
    if let Some(reason) = cancellation.reason().or_else(|| {
        produced.iter().find_map(|outcome| match outcome {
            Err(InvocationFailure::Cancelled(reason)) => Some(*reason),
            _ => None,
        })
    }) {
        // Cancellation discards persistence and retry debt, not failure evidence
        // already returned by joined producers. Record those errors in their
        // invocation contexts without validating or changing annotation state.
        for (build, outcome) in wave.iter().zip(&produced) {
            if let Err(source_error) = outcome
                && !matches!(source_error, InvocationFailure::Cancelled(_))
            {
                let _entered = build.log_context.enter();
                warn!(
                    event = "annotation_worker.discarded_failure",
                    annotation_progress = %counts.progress_count(),
                    source_id = %source.source_id,
                    parse_id = %source.active_parse_id,
                    producer = producer_label(build.item.kind),
                    annotation_id = %build.building_id,
                    failure_class = source_error.class(),
                    error = %source_error,
                    cancellation_reason = reason.label(),
                    disposition = "discarded_without_persistence",
                    retry_accounted = false,
                    failure_accounted = false,
                    "producer failure retained in diagnostics; cancelled wave discards persistence and accounting"
                );
            }
        }
        info!(
            event = "annotation_worker.wave_cancelled",
            annotation_progress = %counts.progress_count(),
            source_id = %source.source_id,
            parse_id = %source.active_parse_id,
            reason = reason.label(),
            discarded_results = produced.len(),
            "producer wave joined; cancelled results discarded without retry or failure accounting"
        );
        return Ok(BuildFlow::Cancelled(reason));
    }

    // Classify every joined result before any fallible persistence. Otherwise
    // an earlier SQL failure could hide later invalid outputs or call failures.
    let mut call_failures = 0;
    let produced: Vec<_> = wave
        .iter()
        .zip(produced)
        .map(|(build, outcome)| {
            let _entered = build.log_context.enter();
            let retry_snapshot = if let Err(source_error) = &outcome {
                // Account for the observed result, never for scheduling a retry.
                // This state stays on the worker; no model thread mutates it.
                let retry = output_retries
                    .outputs
                    .entry(build.building_id.clone())
                    .or_default();
                // Cancellation was handled above; execution and malformed-output
                // failures now spend independent allowances and select their own timer.
                retry.record_failure(source_error, config);
                if matches!(&source_error, InvocationFailure::Call(_)) {
                    call_failures += 1;
                    output_retries.call_failed_in_cycle = true;
                }
                warn!(
                    event = "annotation_worker.producer_failed",
                    annotation_progress = %counts.progress_count(),
                    source_id = %source.source_id,
                    parse_id = %source.active_parse_id,
                    producer = producer_label(build.item.kind),
                    annotation_id = %build.building_id,
                    failure_class = source_error.class(),
                    output_failures = retry.invalid_outputs,
                    execution_failures = retry.execution_failures,
                    annotation_max_retries = config.annotation_max_retries,
                    execution_max_retries = config.execution_max_retries,
                    temperature = build.effective_temperature,
                    error = %source_error,
                    "producer failure observed before persistence"
                );
                *retry
            } else {
                AnnotationRetryState::default()
            };
            if let Some(delay) = retry_snapshot.delay {
                output_retries.consider_retry(delay);
            }
            (outcome, retry_snapshot)
        })
        .collect();

    counts.activity(AnnotationActivity::AwaitingCommit, None);
    // A call failure stops FUTURE waves only; record this wave's results using
    // the existing storage-failure and shutdown boundaries.
    for (build, (outcome, retry_snapshot)) in wave.into_iter().zip(produced) {
        if let Some(reason) = cancellation.reason() {
            return Ok(BuildFlow::Cancelled(reason));
        }
        let _entered = build.log_context.enter();
        match outcome {
            Ok(produced_items) => {
                // POST-PAID: producer output in hand. `complete_build` waits out
                // writer contention (bounded by shutdown); a shutdown-abandon
                // leaves the building row for crash-orphan adoption and ABORTS the
                // wave so no further paid completion is attempted post-shutdown.
                match complete_build(
                    index_root,
                    config,
                    source,
                    &build.item,
                    &build.request,
                    &build.building_id,
                    &produced_items,
                    build.effective_temperature,
                    shutdown,
                    cancellation,
                    counts,
                )? {
                    CompletionOutcome::Committed => {}
                    CompletionOutcome::AbandonedShutdown => return Ok(BuildFlow::ShutdownAbort),
                    CompletionOutcome::Cancelled(reason) => {
                        return Ok(BuildFlow::Cancelled(reason));
                    }
                }
                counts.built += 1;
            }
            Err(source_error) => {
                // The producer failed (network/endpoint/parse). Park the building
                // row failed with bounded detail; the warn carries endpoint
                // context via the error text (prompt/output never logged).
                // POST-PAID (the failure evidence exists): `fail_build` waits out
                // contention; a shutdown-abandon likewise aborts the wave.
                match fail_build(
                    index_root,
                    source,
                    &build.item,
                    &build.building_id,
                    &source_error,
                    &retry_snapshot,
                    config,
                    shutdown,
                    cancellation,
                    counts,
                )? {
                    CompletionOutcome::Committed => {}
                    CompletionOutcome::AbandonedShutdown => return Ok(BuildFlow::ShutdownAbort),
                    CompletionOutcome::Cancelled(reason) => {
                        return Ok(BuildFlow::Cancelled(reason));
                    }
                }
                counts.failed += 1;
            }
        }
    }
    if call_failures > 0 {
        warn!(
            event = "annotation_worker.call_failure_cycle_ended",
            annotation_progress = %counts.progress_count(),
            source_id = %source.source_id,
            parse_id = %source.active_parse_id,
            call_failures,
            retry_scheduling = "per_annotation",
            "wave results recorded; ending cycle after call failure before scheduling more work"
        );
        Ok(BuildFlow::CallFailed)
    } else {
        Ok(BuildFlow::Continue)
    }
}

/// Phase 2b success: complete item 1 into the building row, insert items 2..N
/// directly fresh, and record the memo entry — all in ONE transaction, so the
/// cache row and the annotation rows it caches commit together. Each row's
/// final provenance records the concrete confidence plus the effective
/// sampling temperature the producer call ran at; the memo entry caches the
/// full item array keyed by the shared memo key.
// Completion carries persistence ownership, cancellation, and effective sampling
// temperature explicitly so paid output follows the worker's serial write boundary.
#[allow(clippy::too_many_arguments)]
fn complete_build(
    index_root: &Path,
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    item: &WorkItem,
    request: &NewAnnotation,
    building_id: &str,
    produced_items: &[ProducedAnnotation],
    effective_temperature: f64,
    shutdown: &ShutdownSignal,
    cancellation: &AnnotationCancellation,
    counts: &mut SourceCounts,
) -> Result<CompletionOutcome, ApiError> {
    let persistence_started = Instant::now();
    // POST-PAID: wait out writer contention for the paid producer output (bounded
    // by shutdown); a shutdown-abandon leaves the building row for crash-orphan
    // adoption. The body yields the reminted count for the post-commit log.
    let reminted = match run_post_paid_transaction(
        index_root,
        "build_complete",
        source,
        shutdown,
        cancellation,
        counts,
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
            // identity uses the same single-goal prompt contracts as the memo key.
            let identity_hash = item.kind.identity_hash(config)?;
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
        PostPaidOutcome::Cancelled(reason) => return Ok(CompletionOutcome::Cancelled(reason)),
    };

    counts.transition(&item.content_key, WorkState::Completed)?;
    info!(
        event = "annotation_worker.build_completed",
        annotation_progress = %counts.progress_count(),
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        producer = producer_label(item.kind),
        annotation_id = building_id,
        produced = produced_items.len(),
        cached = reminted,
        annotation_state = "fresh_committed",
        persistence_elapsed_ms = persistence_started.elapsed().as_millis() as u64,
        "validated annotation output committed"
    );
    Ok(CompletionOutcome::Committed)
}

/// Phase 2b failure: park the building row failed with bounded detail in one
/// transaction. The producer error is also warn-logged with endpoint context
/// (which rides in the error text); prompt content and model output never
/// enter the log. Independent counts and the timer explain whether the next
/// cycle may retry without inferring policy from the error text.
// The existing failure boundary also carries maintenance cancellation explicitly.
#[allow(clippy::too_many_arguments)]
fn fail_build(
    index_root: &Path,
    source: &ActiveSource,
    item: &WorkItem,
    building_id: &str,
    producer_error: &InvocationFailure,
    retry: &AnnotationRetryState,
    config: &AnnotatorModelConfig,
    shutdown: &ShutdownSignal,
    cancellation: &AnnotationCancellation,
    counts: &mut SourceCounts,
) -> Result<CompletionOutcome, ApiError> {
    if let InvocationFailure::Cancelled(reason) = producer_error {
        return Ok(CompletionOutcome::Cancelled(*reason));
    }
    let detail = truncate_persisted_detail(&producer_error.to_string());

    // POST-PAID: the producer failure evidence exists and must be recorded, so
    // wait out writer contention (bounded by shutdown); a shutdown-abandon leaves
    // the building row for crash-orphan adoption (retried like any interrupted
    // build next run).
    match run_post_paid_transaction(
        index_root,
        "build_fail",
        source,
        shutdown,
        cancellation,
        counts,
        |tx| store::mark_failed(tx, building_id, &detail),
    )? {
        PostPaidOutcome::Committed(()) => {}
        PostPaidOutcome::AbandonedShutdown => return Ok(CompletionOutcome::AbandonedShutdown),
        PostPaidOutcome::Cancelled(reason) => return Ok(CompletionOutcome::Cancelled(reason)),
    }

    counts.transition(&item.content_key, retry.work_state(config))?;
    warn!(
        event = "annotation_worker.build_failed",
        annotation_progress = %counts.progress_count(),
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        producer = producer_label(item.kind),
        annotation_id = %building_id,
        error = %producer_error,
        failure_class = producer_error.class(),
        output_failures = retry.invalid_outputs,
        execution_failures = retry.execution_failures,
        annotation_max_retries = config.annotation_max_retries,
        execution_max_retries = config.execution_max_retries,
        retry_delay_seconds = retry.delay.map(|delay| delay.duration.as_secs()),
        retry_in_seconds = retry.delay.map(|delay| delay.remaining().as_secs_f64()),
        next_action = if retry.exhausted_category(config).is_some() {
            "await_retry_budget_reset"
        } else {
            "retry_after_delay"
        },
        annotation_state = "failed_committed",
        "producer invocation failed; annotation parked failed for retry-policy evaluation"
    );
    Ok(CompletionOutcome::Committed)
}

/// Assemble the `NewAnnotation` request for one work item: the invocation's
/// ordered target unit ids are exactly the resulting annotation's
/// `targetUnitIds` (CAb identity chain), and the planned producer provenance
/// plus memo key are carried up front (§21 rule 3).
fn new_annotation_request(
    config: &AnnotatorModelConfig,
    source: &ActiveSource,
    item: &WorkItem,
) -> Result<NewAnnotation, ApiError> {
    let target_unit_ids = item
        .invocation
        .targets
        .iter()
        .map(|target| target.unit_id.clone())
        .collect::<Vec<_>>();
    let provenance = producer::planned_provenance(item.kind, config, &item.invocation.targets)?;

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
                source_paths: row.get(2)?,
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
///   2. Every required key in the current excerpt plan must have fresh output.
///      Legacy failed/building rows outside that plan remain historical records
///      and cannot block publication of a fully completed new plan.
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
    config: &AnnotatorModelConfig,
    cancellation: &AnnotationCancellation,
) -> Result<BuildFlow, ApiError> {
    if let Some(reason) = cancellation.reason() {
        return Ok(BuildFlow::Cancelled(reason));
    }
    // Guard 1 + 2 on a read connection dropped before the write transaction.
    let should_build = {
        let connection = hot_plane::open_read(index_root)?;
        let active_parse_id = read_active_parse_id(&connection, &source.source_id)?;
        if active_parse_id.as_deref() != Some(source.active_parse_id.as_str()) {
            // The parse was superseded (or the source deactivated) between the
            // cycle-start read and now: skip so no projection is built for a
            // non-active parse.
            debug!(
                event = "annotation_worker.projection_build_skipped",
                source_id = %source.source_id,
                parse_id = %source.active_parse_id,
                reason = "parse_no_longer_active",
                "skipping annotation-derived projection build; parse is no longer active"
            );
            false
        } else if !parse_annotations_complete(&connection, &source.active_parse_id, config)? {
            // Some required annotation is still failed/building: the annotation
            // build is incomplete, so building projections now would materialize
            // a partial summary/graph. Skip; the next cycle re-attempts.
            debug!(
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
    if let Some(reason) = cancellation.reason() {
        return Ok(BuildFlow::Cancelled(reason));
    }

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
    if let Some(reason) = cancellation.reason() {
        rollback_cancelled(tx, "projection_build", reason)?;
        return Ok(BuildFlow::Cancelled(reason));
    }
    // An expected busy writer is a deferral, not a build start at INFO.
    info!(
        event = "annotation_worker.projection_build_started",
        source_id = %source.source_id,
        parse_id = %source.active_parse_id,
        "annotation-derived projection build starting"
    );

    // The whole build rides `tx`; the first builder Err aborts it below, so no
    // partial annotation-derived projection set ever commits.
    let build =
        build_annotation_projection_transaction(&tx, &source.source_id, &source.active_parse_id);
    match build {
        Ok(()) => {
            if let Some(reason) = cancellation.reason() {
                rollback_cancelled(tx, "projection_build", reason)?;
                return Ok(BuildFlow::Cancelled(reason));
            }
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
                "annotation-derived projection build failed; rollback attempted, recording failure audit"
            );
            record_projection_build_failure(
                &mut connection,
                &source.source_id,
                &source.active_parse_id,
                &error,
                cancellation,
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

/// Admit projections only after every current excerpt/type has fresh coverage.
/// Old grouping keys may retain failed rows, but they no longer describe owed work.
fn parse_annotations_complete(
    conn: &Connection,
    parse_id: &str,
    config: &AnnotatorModelConfig,
) -> Result<bool, ApiError> {
    let fresh = store::fresh_content_key_hashes_for_parse(conn, parse_id)?;
    let plan = producer::build_invocation_plan(conn, parse_id, config.max_input_chars)?;
    let policy = policy::active_policy()?;
    for annotation_type in &policy.post_activation_types {
        let kind = producer_kind_for(*annotation_type)?;
        for invocation in &plan {
            if producer::invocation_matches_kind(kind, invocation)
                && !fresh.contains(&memo::content_key_hash(conn, kind, invocation)?)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
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
    cancellation: &AnnotationCancellation,
) {
    // A real build error remains logged even if maintenance cancels its separate
    // audit write. Never label a cancelled or rolled-back audit as committed.
    let outcome = (|| -> Result<Option<AnnotationCancelReason>, ApiError> {
        if let Some(reason) = cancellation.reason() {
            return Ok(Some(reason));
        }
        let tx = hot_plane::begin_write_transaction(
            connection,
            TX_LOG_NAMESPACE,
            "projection_build_failure",
        )?;
        if let Some(reason) = cancellation.reason() {
            rollback_cancelled(tx, "projection_build_failure", reason)?;
            return Ok(Some(reason));
        }
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
                if let Some(reason) = cancellation.reason() {
                    rollback_cancelled(tx, "projection_build_failure", reason)?;
                    return Ok(Some(reason));
                }
                hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "projection_build_failure")?;
                Ok(None)
            }
            Err(source) => Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "projection_build_failure",
                source,
            )),
        }
    })();
    match outcome {
        Err(audit_error) => {
            error!(
                event = "annotation_worker.projection_build.failure_audit_failed",
                source_id,
                parse_id,
                error = %audit_error,
                "durable projection.failed audit could not be recorded; original build error stands"
            );
        }
        Ok(Some(reason)) => info!(
            event = "annotation_worker.projection_build.failure_audit_cancelled",
            source_id,
            parse_id,
            reason = reason.label(),
            "projection failure audit cancelled; original build failure remains in the log"
        ),
        Ok(None) => {
            // The original build failed, but its failure marker committed separately.
            info!(
                event = "annotation_worker.projection_build.failure_audit_committed",
                source_id, parse_id, "projection failure audit committed"
            );
        }
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

/// Mutate one observation under the existing poison-recovered health boundary.
/// Cycle publication must not replace newer per-document commit observations.
pub(super) fn update_annotation_health(
    slot: &Mutex<AnnotationHealth>,
    update: impl FnOnce(&mut AnnotationHealth),
) {
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
    update(&mut guard);
}

/// Replace the full slot only when the worker becomes unavailable.
fn publish_annotation_health(slot: &Mutex<AnnotationHealth>, snapshot: &AnnotationHealth) {
    update_annotation_health(slot, |health| *health = snapshot.clone());
}

/// Reuse the timestamp failure boundary for cycle and live document observations.
pub(super) fn annotation_measured_at() -> Option<String> {
    match utc_now() {
        Ok(now) => Some(now),
        Err(source) => {
            error!(
                event = "annotation_worker.health_timestamp_failed",
                error = %source,
                "failed to format annotation observation time; measurement time unavailable"
            );
            None
        }
    }
}

/// Map one completed cycle's `CycleReport` into an `AnnotationHealth` snapshot
/// and publish it (C10b). Not parked (a completed cycle proves the client
/// loaded), the freshness counts carry the cycle's as-of timestamp (invariant
/// 2), and a clock failure loses this publish visibly — the last good cycle's
/// counts stay with their own older as-of — rather than stamping a guessed time.
fn publish_cycle_annotation_health(slot: &Mutex<AnnotationHealth>, report: &CycleReport) {
    let Some(measured_at) = annotation_measured_at() else {
        return;
    };
    let totals = &report.totals;
    update_annotation_health(slot, |snapshot| {
        snapshot.parked = false;
        snapshot.parked_detail = None;
        snapshot.last_cycle = Some(AnnotationCycleCounts {
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
        });
        snapshot.measured_at = Some(measured_at);
    });
}
