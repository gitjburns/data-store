use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tracing::{error, info};

use crate::{
    config::ServiceConfig,
    error::ApiError,
    identity::ApplicationIdentity,
    inference::InferenceRuntime,
    projections::dense_cache::DenseCache,
    types::{HealthComponent, HealthCount, HealthResponse},
};

#[derive(Debug)]
pub struct AppState {
    pub config: ServiceConfig,
    inference: Result<InferenceRuntime, ApiError>,
    model_call_gate: Arc<ExclusiveGate>,
    admin_shutdown_token: String,
    shutdown_signal: Arc<ShutdownSignal>,
    // Written every cycle by the sync scheduler thread, read by health();
    // the Arc/Mutex exist because those are different threads sharing one
    // small snapshot slot.
    sync_health: Arc<Mutex<SyncHealth>>,
    // C10b diagnostic-only fabric counts, written every cycle by the sync
    // scheduler thread and read by health(). Separate slot from sync_health so
    // the per-source-system fabric backlog counts publish independently of the
    // readiness-critical sync summary, and neither can gate the other. Same
    // cross-thread Arc/Mutex slot discipline as sync_health.
    fabric_health: Arc<Mutex<FabricHealth>>,
    // C10b diagnostic-only annotation-worker health, written every cycle by the
    // annotation worker thread (its own owning thread) and read by health(). A
    // distinct slot because a distinct thread owns it; poison-recovered on read
    // exactly like sync_health.
    annotation_health: Arc<Mutex<AnnotationHealth>>,
    // Shared active dense cache (§1.6 successor): the C7 dense retrieval
    // channel scores against this same process-global plane cache the
    // scheduler populates on activation. Held as an `Arc` so the `/query`
    // handler can clone it into its blocking pipeline call.
    dense_cache: Arc<DenseCache>,
    // Per-source cutover-barrier registry (§31.1): the query path probes it
    // post-capture to reject queries targeting a source mid-cutover. The same
    // registry the scheduler acquires barriers on, so activation and query
    // rejection serialize on one set of per-source barriers.
    cutover_registry: Arc<CutoverRegistry>,
    // Fail-fast search admission gate (D3): a single in-flight-search window
    // the `/query` handler acquires a permit on before running the pipeline.
    // Saturation surfaces the existing `ServiceUnavailable` path (R7) — no new
    // error variant.
    search_admission: AdmissionGate,
    // §30.2 application identity captured once at startup (main.rs). The C10a
    // snapshot (POST /snapshots) and restore (POST /restore) admin routes stamp
    // it into `request_snapshot`/the restore path. Held as an owned clone (the
    // type is a cheap Clone of a few strings) so a detached admin task can pass
    // it by reference without reaching a global — main.rs clones the identity
    // into AppState BEFORE the original moves into `scheduler::start`.
    application_identity: ApplicationIdentity,
}

/// Live health snapshot of the sync scheduler (spec §9.5–§9.6): queue
/// backlog, coalescing, last-cycle counters, effective cadence, and achieved
/// freshness. Published into a shared slot by the scheduler thread every
/// cycle and assembled into the `sync` health component by
/// `AppState::health()`.
///
/// Defined here rather than in `crate::scheduler` so the dependency stays
/// one-way: the scheduler already depends on this module (`ShutdownSignal`),
/// while this module never needs to know the scheduler exists.
#[derive(Debug, Clone)]
pub struct SyncHealth {
    /// Sync-subsystem operational readiness: true once the scheduler thread
    /// has validated the fabric plane and is cycling. Any fatal scheduler
    /// startup failure clears it (with `detail` saying why); the fabric
    /// plane is required operational state, so this flag participates in
    /// top-level service readiness.
    pub fabric_ready: bool,
    /// Why the subsystem is not ready, or the most recent cycle-level error
    /// while it keeps running; None when the last cycle was clean.
    pub detail: Option<String>,
    /// Queue backlog depth by state (spec §9.4 rule 1: what is pending,
    /// what is in flight, what has failed).
    pub pending: u64,
    pub in_flight: u64,
    pub failed: u64,
    /// Total later detections coalesced into currently queued rows
    /// (spec §9.5 health visibility).
    pub coalesced_total: u64,
    /// Counters of the most recent completed scan/drain cycle.
    pub last_cycle: Option<SyncCycleStats>,
    /// Current effective detection cadence in milliseconds (spec §9.5);
    /// None until the first scan establishes it.
    pub cadence_ms: Option<u64>,
    /// RFC3339 UTC timestamp of the last fully successful cycle — achieved
    /// freshness as measured truth (spec §9.6), never a target.
    pub last_success_at: Option<String>,
}

/// Per-cycle boundary counters of one completed sync cycle (spec §9.6
/// boundary timestamps: change observed, acquired, imported), kept for
/// health so an operator sees the last cycle without reading logs.
#[derive(Debug, Clone)]
pub struct SyncCycleStats {
    /// Native URIs the scan observed to exist (staged or not).
    pub enumerated: u64,
    /// Bundles staged this cycle (new or changed items).
    pub staged: u64,
    /// Items skipped because their known state matched the prescreen.
    pub skipped: u64,
    /// Queue entries drained into successful imports.
    pub imported: u64,
    /// Scan-item failures, staged bundles whose manifest was unreadable at
    /// enqueue time and whose direct import did not succeed, drain
    /// failures/rejections, and recorded parse failures from drain-time parse
    /// dispatch this cycle. A recorded parse failure completes its entry (the
    /// failed parse_runs row is the durable record), so one entry can count
    /// in both `imported` and `failures`: they measure different boundaries.
    pub failures: u64,
    /// Locations evidenced deleted by this cycle's complete enumeration.
    pub deletions: u64,
    /// Wall-clock cost of the whole cycle (scan through drain).
    pub elapsed_ms: u64,
}

/// Per-source-system fabric diagnostic counts (C10b, spec §13.4–13.5, §11.2,
/// §30.5), published every cycle by the scheduler thread into a shared slot and
/// surfaced diagnostic-only by `AppState::health()`. Keyed by `source_system`
/// (plan resolution 6): exactly one source-system at MVP, but the shape is a
/// per-`source_system` map so it never assumes a single global bucket. These
/// counts NEVER gate readiness — a degraded diagnostic must not make a running
/// service report unavailable.
#[derive(Debug, Clone, Default)]
pub struct FabricHealth {
    /// One count bucket per source_system. Empty until the first scheduler
    /// cycle publishes; a source-system absent from the map has not yet been
    /// measured this run.
    pub by_source_system: HashMap<String, FabricSourceCounts>,
    /// RFC3339 UTC timestamp of the cycle that measured these counts — the
    /// as-of marker (spec §9.6 measured truth). None before the first cycle
    /// publishes, so an operator never reads a count without knowing when it
    /// was taken.
    pub measured_at: Option<String>,
}

/// The fabric diagnostic counts for one source-system (C10b). Every field is a
/// backlog/fault count the scheduler cycle reads via a bounded `open_read`
/// SELECT (the `queue_depths` health-inspection precedent) and publishes; none
/// gates readiness.
#[derive(Debug, Clone, Default)]
pub struct FabricSourceCounts {
    /// Held candidates: `parse_runs` rows `status='ready'` with a non-null
    /// `held_reason` (§13.4 disposition backlog).
    pub held: u64,
    /// Serving-stale: failed parse runs plus `sync_queue` rows detected but not
    /// active (§13.5) — content still served while a fresh parse is owed.
    pub serving_stale: u64,
    /// Access-lost locations: `source_locations.status='access_lost'` (§11.2
    /// lost-access, distinct from deletion).
    pub access_lost: u64,
    /// Stuck-building: `parse_runs` rows left in `building` after a
    /// canonical-side infrastructure fault (crash-orphaned import wreckage).
    pub stuck_building: u64,
    /// Unparseable-MIME: failed acquisition records whose failure was the
    /// no-registered-parser routing outcome (§13.5 warn-only today).
    pub unparseable_mime: u64,
    /// Verification-halted: `parse_runs` rows retained in `archiving` because a
    /// snapshot verification gate failed and halted without auto-retry (§30.5).
    pub verification_halted: u64,
}

/// Annotation-worker health slot (C10b, spec §21): the worker's own parked
/// state and its most recent cycle's freshness counts, published each cycle by
/// the worker thread and surfaced diagnostic-only by `AppState::health()`.
/// Corpus-aggregate today (no source-system keying): the worker discovers
/// across all active sources per cycle, so its counts are a single run-wide
/// bucket, unlike the per-source-system fabric counts.
#[derive(Debug, Clone, Default)]
pub struct AnnotationHealth {
    /// True when the worker parked on a client-load failure (a bad key/config
    /// file): annotations are disabled for the run, but the process keeps
    /// serving (CAd ruling). Diagnostic-only — never gates readiness.
    pub parked: bool,
    /// Why the worker parked, when `parked` is true; None while it is cycling.
    pub parked_detail: Option<String>,
    /// Counts of the most recent completed discovery/build cycle.
    pub last_cycle: Option<AnnotationCycleCounts>,
    /// RFC3339 UTC timestamp of the last published cycle — the as-of marker for
    /// `last_cycle`. None before the first cycle completes.
    pub measured_at: Option<String>,
}

/// Per-cycle freshness counts of one annotation discovery/build cycle (C10b),
/// mirroring the worker's `CycleTotals` accounting so an operator sees the last
/// cycle without reading logs.
#[derive(Debug, Clone, Default)]
pub struct AnnotationCycleCounts {
    /// Active sources examined this cycle.
    pub sources_examined: u64,
    /// Work items expected across all sources.
    pub expected: u64,
    /// Work items found missing (built or memoized this cycle).
    pub missing: u64,
    /// Items built via a producer model call.
    pub built: u64,
    /// Items satisfied from the §21.2 memo cache with no model call.
    pub memoized: u64,
    /// Items whose producer invocation failed (parked failed, retried later).
    pub failed: u64,
    /// Whole-source faults skipped this cycle.
    pub source_failures: u64,
    /// Annotation-derived projection builds that failed this cycle.
    pub projection_failures: u64,
    /// Crash-orphaned `building` rows adopted for completion this cycle (§21
    /// crash-recovery evidence).
    pub orphans_adopted: u64,
    /// Items deferred this cycle because the hot-plane writer lock was held
    /// (typically by a scheduler projection build); re-attempted next cycle.
    pub deferred: u64,
}

impl AnnotationHealth {
    /// Initial slot value published before the worker thread has run a cycle:
    /// honestly not parked and with no measured cycle, so a health probe hitting
    /// the spawn window reports "no cycle yet" instead of a guess.
    pub fn startup_pending() -> Self {
        Self::default()
    }
}

impl SyncHealth {
    /// Initial slot value published before the scheduler thread has run its
    /// fabric validation: honestly not-ready, so a health probe hitting the
    /// spawn window reports pending validation instead of a guess.
    pub fn startup_pending() -> Self {
        Self {
            fabric_ready: false,
            detail: Some("sync scheduler validation pending".to_string()),
            pending: 0,
            in_flight: 0,
            failed: 0,
            coalesced_total: 0,
            last_cycle: None,
            cadence_ms: None,
            last_success_at: None,
        }
    }
}

// The admission machinery below (permit, snapshot, gate) lost its consumers
// when the legacy ingest/search pipelines were retired at cluster CR. Per the
// D3 resolution the fail-fast search admission gate returns at C8d-1 with a
// code-constant capacity: `AppState::search_admission` now holds an
// `AdmissionGate`, and the `/query` handler acquires an `AdmissionPermit`
// through `try_acquire_search`, so `AdmissionPermit`, `AdmissionGate`, and
// `AdmissionGate::try_acquire` are live. `AdmissionSnapshot` and
// `AdmissionGate::snapshot` are consumed at C10b: `AppState::admission_snapshot`
// surfaces the search-gate counters diagnostic-only in `health()`.
#[derive(Debug)]
pub struct AdmissionPermit {
    in_flight: Arc<AtomicUsize>,
}

// Retained with the model-call gate (see acquire_model_call_gate); consumed
// at C6c/C7.
#[allow(dead_code)]
#[derive(Debug)]
pub struct ModelCallPermit {
    gate: Arc<ExclusiveGate>,
    operation_id: String,
    model_role: &'static str,
    call_purpose: &'static str,
    acquired_at: Instant,
}

// Consumed at C10b: `AppState::admission_snapshot` reads it and `health()`
// surfaces the search-gate window (max/in-flight) as a diagnostic-only count.
#[derive(Debug)]
pub struct AdmissionSnapshot {
    pub max_in_flight: usize,
    pub in_flight: usize,
}

// Consumed by C8d-1: `AppState::search_admission` holds one, and the `/query`
// handler admits searches through `try_acquire`.
#[derive(Debug)]
struct AdmissionGate {
    max_in_flight: usize,
    in_flight: Arc<AtomicUsize>,
}

/// Cross-thread shutdown latch signaled once by the protected shutdown operation.
#[derive(Debug, Default)]
pub struct ShutdownSignal {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Exclusive waiting gate primitive shared by the model-call serializer and
/// the per-source cutover barriers (`CutoverRegistry`). Owners differ only in
/// log identity: each instance carries a static event name and message label
/// so poison recovery stays attributable to the right subsystem.
// `pub(crate)` only so the scheduler's `ProjectionRuntime` can name the shared
// gate handle type it threads from `AppState::model_call_gate_handle`; all
// fields stay private, so the gate's poison-recovery discipline remains the sole
// owner of the lock. The type is otherwise opaque to callers — they only pass
// the `Arc` into `acquire_model_call_gate_on`.
#[derive(Debug)]
pub(crate) struct ExclusiveGate {
    /// Full tracing event name emitted on poison recovery
    /// (e.g. `model_gate.lock_poisoned`).
    poison_event: &'static str,
    /// Gate name interpolated into poison log messages (e.g. "model gate").
    label: &'static str,
    busy: Mutex<bool>,
    released: Condvar,
}

/// Per-source cutover barrier registry (spec §31.1). Parse activation and
/// source deactivation swap the source's active-parse pointer behind a brief
/// exclusive barrier; this registry hands out one barrier per source so
/// distinct sources never contend. §31.1 invariants:
///
/// - The barrier covers the single active-pointer swap plus its paired
///   in-memory publish, nothing else.
/// - Brevity contract: a barrier hold is a few bounded SQLite statements —
///   milliseconds. Non-disruptiveness rests on brevity and per-source scope.
/// - Queries targeting a source whose barrier is active are rejected with a
///   retryable error and never partially executed; queries already in flight
///   execute entirely against their captured pre-cutover snapshots.
///
/// The C6c dense-cache swap now happens inside this barrier scope: activation
/// publishes the dense plane under the held guard, so one `acquire` covers the
/// pointer swap and the cache publish together. The query-side consumer of
/// `reject_if_active` is the R2 post-capture barrier probe in
/// `query::execute::run_pipeline_body`, which probes each captured source and
/// aborts the whole query with a retryable rejection before any retrieval stage.
#[derive(Debug)]
pub(crate) struct CutoverRegistry {
    // source_id -> that source's barrier gate. Entries are never reaped:
    // growth is bounded by the number of distinct source objects the service
    // has ever activated, which is acceptable for this small per-entry
    // footprint.
    barriers: Mutex<HashMap<String, Arc<ExclusiveGate>>>,
}

/// Held cutover barrier for one source; dropping it reopens the source to
/// queries and wakes blocked activation writers. Holders must keep the §31.1
/// brevity contract: only the active-pointer swap and its paired in-memory
/// publish happen under the guard.
#[derive(Debug)]
pub(crate) struct CutoverBarrierGuard {
    gate: Arc<ExclusiveGate>,
    source_id: String,
    acquired_at: Instant,
}

/// Retryable rejection for a query that targeted a source mid-cutover (spec
/// §31.1: rejected, never partially executed). A later consumer maps this to
/// a retryable API error; standard client retry suffices because barrier
/// holds last milliseconds.
// Consumed at C8d-1: `error::ApiError`'s `From<CutoverBarrierActive>` maps this
// into the retryable `CutoverBarrierActive` API error. The `reject_if_active`
// producer of this value is wired into the query path at C8d-2.
#[derive(Debug)]
pub(crate) struct CutoverBarrierActive {
    /// Source whose barrier was active, carried so the API mapping can name
    /// the rejected target.
    pub(crate) source_id: String,
}

impl AppState {
    /// Build shared application state for HTTP handlers.
    // The constructor threads the process-wide handles the HTTP surface shares
    // (config, inference, gates, health slot, caches, registry, identity); each
    // is a distinct owned/Arc input the main loop constructs once, so they stay
    // positional rather than being grouped into a parameter struct that would add
    // an abstraction the single call site does not need (same convention as
    // `scheduler::start`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ServiceConfig,
        inference: Result<InferenceRuntime, ApiError>,
        admin_shutdown_token: String,
        shutdown_signal: Arc<ShutdownSignal>,
        sync_health: Arc<Mutex<SyncHealth>>,
        fabric_health: Arc<Mutex<FabricHealth>>,
        annotation_health: Arc<Mutex<AnnotationHealth>>,
        dense_cache: Arc<DenseCache>,
        cutover_registry: Arc<CutoverRegistry>,
        application_identity: ApplicationIdentity,
    ) -> Self {
        let model_call_gate =
            Arc::new(ExclusiveGate::new("model_gate.lock_poisoned", "model gate"));

        // D3: the search admission window is exactly one in-flight search. The
        // gate primitive counts a `u32` capacity, so the intent-carrying
        // constant is declared as `u32` to feed `AdmissionGate::new` without a
        // cast while keeping "one in-flight search" explicit at the call site.
        const MAX_IN_FLIGHT_SEARCH: u32 = 1;
        let search_admission = AdmissionGate::new(MAX_IN_FLIGHT_SEARCH);

        Self {
            config,
            inference,
            model_call_gate,
            admin_shutdown_token,
            shutdown_signal,
            sync_health,
            fabric_health,
            annotation_health,
            dense_cache,
            cutover_registry,
            search_admission,
            application_identity,
        }
    }

    /// The shared active dense cache the C7 dense retrieval channel scores
    /// against. Returned as `&Arc` so the `/query` handler can clone the handle
    /// into its blocking pipeline call while the borrow stays cheap on the hot
    /// path.
    // Consumed at C8d-2 (the /query handler threads it into execute_query).
    pub(crate) fn dense_cache(&self) -> &Arc<DenseCache> {
        &self.dense_cache
    }

    /// The per-source cutover-barrier registry (§31.1) the query path probes to
    /// reject queries targeting a source mid-cutover.
    // Consumed at C8d-2 (the /query handler threads it into the pipeline's
    // post-capture barrier probe).
    pub(crate) fn cutover_registry(&self) -> &Arc<CutoverRegistry> {
        &self.cutover_registry
    }

    /// The §30.2 application identity the snapshot/restore admin routes stamp
    /// into `snapshot::request_snapshot` and the restore path. Returned by
    /// reference so a detached admin task borrows it for the duration of its
    /// domain call without cloning on the hot path.
    // Consumed at C10a (POST /snapshots and POST /restore detached tasks).
    pub(crate) fn application_identity(&self) -> &ApplicationIdentity {
        &self.application_identity
    }

    /// Try to admit one search into the fail-fast admission window, returning a
    /// permit that releases its slot on drop. The `/query` handler holds the
    /// permit for the whole call; saturation returns the existing
    /// `ServiceUnavailable` (503) — no dedicated admission error (R7).
    // Consumed at C8d-2 (the /query handler acquires the permit before
    // spawn_blocking and holds it for the whole call).
    pub(crate) fn try_acquire_search(
        &self,
        operation: &'static str,
    ) -> Result<AdmissionPermit, ApiError> {
        self.search_admission.try_acquire(operation)
    }

    /// Capture the search-admission gate's current window (max/in-flight) for
    /// the C10b diagnostic health surface. In-memory only — reads two atomics,
    /// opens no connection — so the health handler stays connection-free.
    // Consumed by `health()` for the diagnostic-only `search_admission`
    // component.
    fn admission_snapshot(&self) -> AdmissionSnapshot {
        self.search_admission.snapshot()
    }

    /// Return the initialized inference runtime or an explicit readiness error.
    pub fn inference(&self) -> Result<&InferenceRuntime, ApiError> {
        self.inference
            .as_ref()
            .map_err(|source| ApiError::InferenceInit {
                message: source.to_string(),
            })
    }

    /// Wait for exclusive access to the shared accelerator-backed model runtimes.
    // Consumer-less since cluster CR; the caller-side model-call-gate
    // discipline is a pinned contract for the fabric consumers (C6c/C7).
    #[allow(dead_code)]
    pub fn acquire_model_call_gate(
        &self,
        operation_id: &str,
        model_role: &'static str,
        call_purpose: &'static str,
    ) -> Result<ModelCallPermit, ApiError> {
        // Delegates to the free acquisition function on THIS AppState's gate
        // handle, so the HTTP path and the scheduler path (which acquires on the
        // same shared handle via `model_call_gate_handle`) run byte-identical
        // acquisition logging and share one process-global serializer.
        acquire_model_call_gate_on(
            &self.model_call_gate,
            operation_id,
            model_role,
            call_purpose,
        )
    }

    /// Hand out a clone of the SHARED model-call gate handle so a non-`AppState`
    /// caller (the sync scheduler's projection build step) serializes local model
    /// calls on the very same gate the HTTP handlers use — process-global
    /// serialization is the invariant (spec §1.5 gate discipline). The scheduler
    /// pairs this with `acquire_model_call_gate_on` so acquisition logging is
    /// identical to `AppState::acquire_model_call_gate`.
    pub(crate) fn model_call_gate_handle(&self) -> Arc<ExclusiveGate> {
        Arc::clone(&self.model_call_gate)
    }

    /// Validate the startup-scoped admin token without exposing the expected value.
    // Consumed by the C10a `authorize_request` surface in http.rs, which gates
    // the protected operation routes.
    pub fn authorize_admin_token(&self, candidate: &str) -> Result<(), ApiError> {
        if constant_time_eq(candidate.as_bytes(), self.admin_shutdown_token.as_bytes()) {
            return Ok(());
        }

        Err(ApiError::Unauthorized {
            message: "invalid admin token".to_string(),
        })
    }

    /// Signal the HTTP server to drain active work and exit.
    // Consumed by the C10a `post_shutdown` route in http.rs.
    pub fn request_shutdown(&self) -> Result<(), ApiError> {
        info!(
            event = "shutdown.signal.requested",
            stage = "signal_requesting",
            "shutdown signal requested"
        );
        match self.shutdown_signal.request() {
            Ok(true) => {
                info!(
                    event = "shutdown.signal.sent",
                    stage = "signal_sent",
                    "shutdown signal sent"
                );
                Ok(())
            }
            Ok(false) => {
                info!(
                    event = "shutdown.signal.already_requested",
                    stage = "signal_already_requested",
                    "shutdown signal was already requested"
                );
                Ok(())
            }
            Err(source) => {
                error!(
                    event = "shutdown.signal.failed",
                    stage = "signal_locking",
                    error = %source,
                    "shutdown signal lock failed"
                );
                Err(ApiError::InternalIo { message: source })
            }
        }
    }

    /// Clone the last-published sync-scheduler health snapshot (spec §9.5–§9.6)
    /// for the public `GET /sync/status` inspection route. Reads the same shared
    /// slot `health()` reads and recovers a poisoned lock identically (the
    /// scheduler thread panicked mid-publish, but the snapshot inside is a valid
    /// last-published value, so the route keeps answering).
    // Consumed at C10a (GET /sync/status).
    pub(crate) fn sync_health_snapshot(&self) -> SyncHealth {
        match self.sync_health.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                error!(
                    event = "health.sync_slot_poisoned",
                    "sync health slot lock was poisoned; reporting last published snapshot"
                );
                poisoned.into_inner().clone()
            }
        }
    }

    /// Return current service health and readiness diagnostics.
    pub fn health(&self) -> HealthResponse {
        let inference_component = match &self.inference {
            Ok(runtime) => HealthComponent {
                name: "inference".to_string(),
                ready: true,
                details: readiness_details("readiness-critical", runtime.health_details()),
                counts: Vec::new(),
            },
            Err(error) => HealthComponent {
                name: "inference".to_string(),
                ready: false,
                details: readiness_details("readiness-critical", vec![error.to_string()]),
                counts: Vec::new(),
            },
        };
        let logging_component = HealthComponent {
            name: "logging".to_string(),
            ready: true,
            details: readiness_details(
                "diagnostic-only",
                vec![
                    format!(
                        "file logging initialized before HTTP bind: {}",
                        self.config
                            .logging
                            .resolved_file_path(self.config.config_root())
                            .display()
                    ),
                    format!("level {}", self.config.logging.level.as_str()),
                ],
            ),
            counts: Vec::new(),
        };
        // The sync slot is written by the scheduler thread; a poisoned lock
        // means that thread panicked mid-publish. The snapshot inside is
        // still a valid last-published SyncHealth, so recover and report it
        // — health must keep answering. The scheduler's catch_unwind
        // handler logs the panic and republishes a not-ready snapshot (with
        // the panic message in `detail`), so after it runs the recovered
        // value already reports the panic.
        let sync_snapshot = match self.sync_health.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                error!(
                    event = "health.sync_slot_poisoned",
                    "sync health slot lock was poisoned; reporting last published snapshot"
                );
                poisoned.into_inner().clone()
            }
        };
        let sync_component = HealthComponent {
            name: "sync".to_string(),
            ready: sync_snapshot.fabric_ready,
            details: readiness_details("readiness-critical", sync_health_details(&sync_snapshot)),
            counts: Vec::new(),
        };
        // C10b diagnostic-only components. Each is assembled from an in-memory
        // slot (or two atomics for admission); NONE opens a connection and NONE
        // gates readiness — a degraded diagnostic must never make a running
        // service report unavailable (invariant 1).
        let fabric_component = self.fabric_component();
        let annotation_component = self.annotation_component();
        let admission_component = self.admission_component();
        // Components that gate readiness determine the top-level flag:
        // inference and the sync scheduler's fabric-plane readiness (the
        // fabric hot plane replaced the legacy storage_cache component at
        // cluster CR). Diagnostic-only components remain visible without
        // making a running service appear unavailable. The C10b additions
        // (fabric, annotation, search_admission) are diagnostic-only and are
        // deliberately EXCLUDED from this conjunction.
        let ready = inference_component.ready && sync_component.ready;
        let components = vec![
            inference_component,
            sync_component,
            logging_component,
            fabric_component,
            annotation_component,
            admission_component,
        ];

        HealthResponse {
            service: "data-store".to_string(),
            ready,
            components,
        }
    }

    /// Assemble the diagnostic-only `fabric` health component (C10b) from the
    /// scheduler-published fabric-counts slot. Poison-recovered read (the
    /// scheduler thread may have panicked mid-publish; the snapshot inside is
    /// still a valid last-published value, so health keeps answering).
    /// `ready` is always true: these are backlog/fault diagnostics, not a
    /// readiness gate — an operator watches the counts, but they never make the
    /// service report unavailable (invariant 1).
    fn fabric_component(&self) -> HealthComponent {
        let snapshot = match self.fabric_health.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                error!(
                    event = "health.fabric_slot_poisoned",
                    "fabric health slot lock was poisoned; reporting last published snapshot"
                );
                poisoned.into_inner().clone()
            }
        };
        let (details, counts) = fabric_health_view(&snapshot);
        HealthComponent {
            name: "fabric".to_string(),
            ready: true,
            details: readiness_details("diagnostic-only", details),
            counts,
        }
    }

    /// Assemble the diagnostic-only `annotation` health component (C10b) from
    /// the worker-published slot. Poison-recovered read, same rationale as the
    /// fabric slot. `ready` is always true: annotations are non-critical (CAd
    /// ruling), so even a parked worker never gates readiness.
    fn annotation_component(&self) -> HealthComponent {
        let snapshot = match self.annotation_health.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                error!(
                    event = "health.annotation_slot_poisoned",
                    "annotation health slot lock was poisoned; reporting last published snapshot"
                );
                poisoned.into_inner().clone()
            }
        };
        let (details, counts) = annotation_health_view(&snapshot);
        HealthComponent {
            name: "annotation".to_string(),
            ready: true,
            details: readiness_details("diagnostic-only", details),
            counts,
        }
    }

    /// Assemble the diagnostic-only `search_admission` health component (C10b)
    /// from the in-memory admission-gate snapshot (§30.5). In-memory only (two
    /// atomics), opens no connection. `ready` is always true: saturation is a
    /// transient fail-fast condition surfaced on the `/query` path (R7), not a
    /// service-readiness gate. The as-of marker is "live": the snapshot is the
    /// instantaneous atomic read at health-assembly time, not a cached value.
    fn admission_component(&self) -> HealthComponent {
        let snapshot = self.admission_snapshot();
        let details = vec![format!(
            "search admission window in-flight {} / max {}",
            snapshot.in_flight, snapshot.max_in_flight
        )];
        let counts = vec![
            HealthCount {
                label: "in_flight".to_string(),
                source_system: None,
                value: snapshot.in_flight as u64,
                as_of: ADMISSION_AS_OF_LIVE.to_string(),
            },
            HealthCount {
                label: "max_in_flight".to_string(),
                source_system: None,
                value: snapshot.max_in_flight as u64,
                as_of: ADMISSION_AS_OF_LIVE.to_string(),
            },
        ];
        HealthComponent {
            name: "search_admission".to_string(),
            ready: true,
            details: readiness_details("diagnostic-only", details),
            counts,
        }
    }
}

/// As-of marker for the search-admission counts: unlike the cycle-published
/// fabric/annotation counts (which carry the measuring cycle's timestamp), the
/// admission snapshot is read instantaneously from two atomics at
/// health-assembly time, so its as-of is the literal "live" rather than a stale
/// timestamp — honest about the fact that it is a point-in-time read.
const ADMISSION_AS_OF_LIVE: &str = "live";

/// As-of placeholder used when a cycle-published slot has no measured-at
/// timestamp yet (before the first cycle completes). A count that has never
/// been measured still carries an explicit marker rather than a fabricated
/// timestamp, so a value is never presented as current without saying when.
const AS_OF_NOT_YET_MEASURED: &str = "not-yet-measured";

impl Drop for AdmissionPermit {
    /// Return the held admission slot when the permit leaves scope.
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Acquire the exclusive model-call gate on a shared gate handle and return a
/// `ModelCallPermit` that releases it on drop. This is the ONE acquisition
/// discipline both entry points share: `AppState::acquire_model_call_gate`
/// (HTTP path) and the sync scheduler's projection build step (via
/// `AppState::model_call_gate_handle`) both call it, so the `model_gate.waiting`
/// / `.acquired` / `.failed` logging and the acquire semantics are byte-identical
/// no matter which caller holds the gate. Poison recovery lives inside
/// `ExclusiveGate::acquire`, so the process-global serializer survives a holder
/// panic.
pub(crate) fn acquire_model_call_gate_on(
    gate: &Arc<ExclusiveGate>,
    operation_id: &str,
    model_role: &'static str,
    call_purpose: &'static str,
) -> Result<ModelCallPermit, ApiError> {
    let wait_started = Instant::now();
    info!(
        event = "model_gate.waiting",
        operation_id, model_role, call_purpose, "model execution gate wait started"
    );
    if let Err(source) = gate.acquire() {
        let error = ApiError::InferenceInit {
            message: format!(
                "model execution gate failed before {model_role} {call_purpose}: {source}"
            ),
        };
        error!(
            event = "model_gate.failed",
            operation_id,
            model_role,
            call_purpose,
            error = %error,
            wait_ms = wait_started.elapsed().as_millis() as u64,
            "model execution gate acquisition failed"
        );
        return Err(error);
    }
    info!(
        event = "model_gate.acquired",
        operation_id,
        model_role,
        call_purpose,
        wait_ms = wait_started.elapsed().as_millis() as u64,
        "model execution gate acquired"
    );

    Ok(ModelCallPermit {
        gate: Arc::clone(gate),
        operation_id: operation_id.to_string(),
        model_role,
        call_purpose,
        acquired_at: Instant::now(),
    })
}

impl Drop for ModelCallPermit {
    /// Release exclusive model access and log the release side of the model-call boundary.
    fn drop(&mut self) {
        info!(
            event = "model_gate.released",
            operation_id = %self.operation_id,
            model_role = self.model_role,
            call_purpose = self.call_purpose,
            held_ms = self.acquired_at.elapsed().as_millis() as u64,
            "model execution gate released"
        );
        self.gate.release();
    }
}

impl Drop for CutoverBarrierGuard {
    /// Release the source's cutover barrier and wake blocked activation
    /// writers, logging the release side of the activation publish boundary
    /// with the hold duration — durable evidence that the §31.1 brevity
    /// contract (milliseconds) held.
    fn drop(&mut self) {
        info!(
            event = "cutover_barrier.released",
            source_id = %self.source_id,
            held_ms = self.acquired_at.elapsed().as_millis() as u64,
            "cutover barrier released"
        );
        self.gate.release();
    }
}

impl ShutdownSignal {
    /// Mark shutdown as requested and wake all waiters; returns false when already requested.
    pub fn request(&self) -> Result<bool, String> {
        let mut requested = self
            .requested
            .lock()
            .map_err(|source| format!("shutdown signal lock is poisoned: {source}"))?;
        if *requested {
            return Ok(false);
        }
        *requested = true;
        self.changed.notify_all();
        Ok(true)
    }

    /// Block the calling thread until shutdown has been requested.
    pub fn wait(&self) {
        // A poisoned latch means a holder panicked while flipping the flag.
        // Proceeding to shutdown keeps the failure visible instead of leaving
        // the server waiting forever on a broken latch.
        let mut requested = match self.requested.lock() {
            Ok(requested) => requested,
            Err(_poisoned) => {
                error!(
                    event = "shutdown.signal.lock_poisoned",
                    stage = "signal_waiting",
                    "shutdown signal lock was poisoned while waiting; proceeding to shutdown"
                );
                return;
            }
        };
        while !*requested {
            match self.changed.wait(requested) {
                Ok(guard) => requested = guard,
                Err(_poisoned) => {
                    error!(
                        event = "shutdown.signal.lock_poisoned",
                        stage = "signal_waiting",
                        "shutdown signal wait was poisoned; proceeding to shutdown"
                    );
                    return;
                }
            }
        }
    }

    /// Block the calling thread until shutdown is requested or `timeout`
    /// elapses; returns true when shutdown was requested. Same
    /// poison-recovery invariant as `wait`: a poisoned latch means a holder
    /// panicked while flipping the flag, and reporting shutdown (true)
    /// keeps the failure visible by letting the waiter proceed to a clean
    /// stop instead of waiting forever on a broken latch.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut requested = match self.requested.lock() {
            Ok(requested) => requested,
            Err(_poisoned) => {
                error!(
                    event = "shutdown.signal.lock_poisoned",
                    stage = "signal_timed_waiting",
                    "shutdown signal lock was poisoned while waiting; proceeding to shutdown"
                );
                return true;
            }
        };
        while !*requested {
            // Re-derive the remaining budget each pass so spurious condvar
            // wakeups never extend the wait beyond the deadline.
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            requested = match self.changed.wait_timeout(requested, remaining) {
                Ok((guard, _timeout_result)) => guard,
                Err(_poisoned) => {
                    error!(
                        event = "shutdown.signal.lock_poisoned",
                        stage = "signal_timed_waiting",
                        "shutdown signal wait was poisoned; proceeding to shutdown"
                    );
                    return true;
                }
            };
        }
        true
    }
}

impl ExclusiveGate {
    /// Create an idle gate that reports poison recovery under the given
    /// static event name and message label.
    fn new(poison_event: &'static str, label: &'static str) -> Self {
        Self {
            poison_event,
            label,
            busy: Mutex::new(false),
            released: Condvar::new(),
        }
    }

    /// Block the calling thread until exclusive access is acquired.
    ///
    /// The Result is kept for the caller's diagnostic error path even though
    /// poison is recovered on every branch.
    fn acquire(&self) -> Result<(), String> {
        // The bool guarded by this lock stays valid after a holder panic, so
        // poison is recovered on acquire and release alike; std poison is
        // sticky, and failing here would turn one panic into permanent
        // failures for every later acquirer (model calls or activations).
        let mut busy = match self.busy.lock() {
            Ok(busy) => busy,
            Err(poisoned) => {
                error!(
                    event = self.poison_event,
                    stage = "gate_acquiring",
                    "{} lock was poisoned during acquire; recovering",
                    self.label
                );
                poisoned.into_inner()
            }
        };
        while *busy {
            busy = match self.released.wait(busy) {
                Ok(busy) => busy,
                Err(poisoned) => {
                    error!(
                        event = self.poison_event,
                        stage = "gate_waiting",
                        "{} wait was poisoned; recovering",
                        self.label
                    );
                    poisoned.into_inner()
                }
            };
        }
        *busy = true;
        Ok(())
    }

    /// Report whether the gate is currently held, without queueing or
    /// waiting for release: the busy mutex is only ever held to flip the
    /// flag, never across a holder's critical section, so this peek is
    /// bounded by a flag flip and never waits out a barrier hold.
    fn is_busy(&self) -> bool {
        // Same recovery rule as acquire/release: the bool stays valid.
        let busy = match self.busy.lock() {
            Ok(busy) => busy,
            Err(poisoned) => {
                error!(
                    event = self.poison_event,
                    stage = "gate_probing",
                    "{} lock was poisoned during probe; recovering",
                    self.label
                );
                poisoned.into_inner()
            }
        };
        *busy
    }

    /// Release exclusive access and wake one waiting acquirer.
    fn release(&self) {
        let mut busy = match self.busy.lock() {
            Ok(busy) => busy,
            Err(poisoned) => {
                // The poisoned lock still holds a valid bool; recover it so a
                // panicked holder cannot deadlock every later acquirer.
                error!(
                    event = self.poison_event,
                    stage = "gate_releasing",
                    "{} lock was poisoned during release; recovering",
                    self.label
                );
                poisoned.into_inner()
            }
        };
        *busy = false;
        self.released.notify_one();
    }
}

impl CutoverRegistry {
    /// Create an empty registry; each source's barrier is created lazily on
    /// its first writer-side acquisition.
    pub(crate) fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }

    /// Writer-side blocking acquisition of one source's cutover barrier
    /// (spec §13.6, §31.1). This is the activation publish boundary, so
    /// acquisition and release (guard Drop) are durably logged with wait and
    /// hold durations. Blocks only behind a concurrent cutover of the same
    /// source; distinct sources never contend.
    pub(crate) fn acquire(&self, source_id: &str) -> CutoverBarrierGuard {
        let wait_started = Instant::now();
        info!(
            event = "cutover_barrier.acquiring",
            source_id, "cutover barrier acquisition started"
        );
        let gate = self.barrier_for(source_id);
        // ExclusiveGate::acquire recovers lock poison on every branch and
        // never returns Err today; the Result shape is retained for the
        // model-gate caller's diagnostic path. Keep loud evidence if that
        // invariant ever breaks, because a guard over an unacquired gate
        // would release another writer's hold on Drop.
        if let Err(detail) = gate.acquire() {
            error!(
                event = "cutover_barrier.acquire_error",
                source_id,
                error = %detail,
                "cutover barrier gate returned an unexpected acquire error"
            );
        }
        info!(
            event = "cutover_barrier.acquired",
            source_id,
            wait_ms = wait_started.elapsed().as_millis() as u64,
            "cutover barrier acquired"
        );
        CutoverBarrierGuard {
            gate,
            source_id: source_id.to_string(),
            acquired_at: Instant::now(),
        }
    }

    /// Query-side probe (spec §31.1): reject a query targeting a source
    /// whose barrier is active with a retryable error — no queueing, no
    /// waiting for release. A source with no registry entry has never begun
    /// a cutover, so it passes. The answer is instantaneous truth, not a
    /// reservation: a barrier may activate right after a pass, which is safe
    /// because in-flight queries execute entirely against their captured
    /// pre-cutover snapshots.
    pub(crate) fn reject_if_active(&self, source_id: &str) -> Result<(), CutoverBarrierActive> {
        // Look up without inserting: the query side must not grow the map
        // for sources that never activate.
        let gate = match self.barriers.lock() {
            Ok(map) => map.get(source_id).cloned(),
            Err(poisoned) => {
                // Same recovery rule as barrier_for: the map stays valid.
                error!(
                    event = "cutover_barrier.registry_lock_poisoned",
                    stage = "registry_probing",
                    source_id,
                    "cutover registry lock was poisoned during probe; recovering"
                );
                poisoned.into_inner().get(source_id).cloned()
            }
        };
        match gate {
            Some(gate) if gate.is_busy() => Err(CutoverBarrierActive {
                source_id: source_id.to_string(),
            }),
            _ => Ok(()),
        }
    }

    /// Fetch or lazily create the source's barrier gate. The registry map
    /// lock is held only for the map operation itself, never across a
    /// barrier hold, so lookups stay bounded even mid-cutover.
    fn barrier_for(&self, source_id: &str) -> Arc<ExclusiveGate> {
        // The map stays structurally valid after a holder panic (nothing
        // here can leave an entry torn), so recover poison instead of
        // letting one panic wedge every later activation — the same sticky-
        // poison rule ExclusiveGate itself follows.
        let mut map = match self.barriers.lock() {
            Ok(map) => map,
            Err(poisoned) => {
                error!(
                    event = "cutover_barrier.registry_lock_poisoned",
                    stage = "registry_locking",
                    source_id,
                    "cutover registry lock was poisoned; recovering"
                );
                poisoned.into_inner()
            }
        };
        Arc::clone(map.entry(source_id.to_string()).or_insert_with(|| {
            Arc::new(ExclusiveGate::new(
                "cutover_barrier.lock_poisoned",
                "cutover barrier",
            ))
        }))
    }
}

impl fmt::Display for CutoverBarrierActive {
    /// Name the rejected source and the retryable nature of the rejection
    /// for the eventual API-error mapping.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cutover barrier active for source {}; retry shortly",
            self.source_id
        )
    }
}

impl From<CutoverBarrierActive> for ApiError {
    /// Map a query-side cutover-barrier rejection into its retryable API error
    /// (§31.1), preserving the rejected source id via the source type's
    /// `Display`. The mapping lives here, not in `error.rs`, so `error.rs`
    /// carries no `crate::state` reference — the `colbert-diagnostic` bin
    /// includes `error.rs` alone, without a `state` module.
    fn from(rejection: CutoverBarrierActive) -> Self {
        ApiError::CutoverBarrierActive {
            message: rejection.to_string(),
        }
    }
}

/// Render one SyncHealth snapshot as operator-facing detail lines, keeping
/// every snapshot field visible (spec §9.5: cadence, backlog, and coalescing
/// are health-visible; §9.6: achieved freshness is reported truthfully).
fn sync_health_details(snapshot: &SyncHealth) -> Vec<String> {
    let mut details = Vec::new();
    if let Some(detail) = &snapshot.detail {
        details.push(format!("detail: {detail}"));
    }
    details.push(format!(
        "queue pending {} in-flight {} failed {}",
        snapshot.pending, snapshot.in_flight, snapshot.failed
    ));
    details.push(format!("coalesced total {}", snapshot.coalesced_total));
    match &snapshot.last_cycle {
        Some(cycle) => details.push(format!(
            "last cycle: enumerated {} staged {} skipped {} imported {} failures {} \
             deletions {} elapsed_ms {}",
            cycle.enumerated,
            cycle.staged,
            cycle.skipped,
            cycle.imported,
            cycle.failures,
            cycle.deletions,
            cycle.elapsed_ms
        )),
        None => details.push("no sync cycle completed yet".to_string()),
    }
    match snapshot.cadence_ms {
        Some(cadence_ms) => details.push(format!("effective cadence {cadence_ms} ms")),
        None => details.push("cadence not yet established".to_string()),
    }
    match &snapshot.last_success_at {
        Some(at) => details.push(format!("last successful sync {at}")),
        None => details.push("no successful sync yet".to_string()),
    }
    details
}

/// Render one `FabricHealth` snapshot into operator-facing detail lines plus the
/// additive typed `HealthCount`s (C10b). Every count carries the slot's
/// measured-at as-of marker (or `not-yet-measured` before the first cycle) and
/// its owning `source_system`, so a per-source-system backlog value is never
/// read without knowing when and for which system it was measured. The counts
/// are emitted per source-system in a stable order (the map is sorted by key)
/// so the surface is deterministic across probes.
fn fabric_health_view(snapshot: &FabricHealth) -> (Vec<String>, Vec<HealthCount>) {
    let as_of = snapshot
        .measured_at
        .clone()
        .unwrap_or_else(|| AS_OF_NOT_YET_MEASURED.to_string());
    let mut details = Vec::new();
    let mut counts = Vec::new();

    if snapshot.by_source_system.is_empty() {
        details.push("no fabric cycle measured yet".to_string());
        return (details, counts);
    }

    details.push(format!("measured at {as_of}"));
    // Sorted iteration keeps the detail lines and counts deterministic across
    // probes even though the underlying map is unordered.
    let mut systems: Vec<&String> = snapshot.by_source_system.keys().collect();
    systems.sort();
    for system in systems {
        // Present by construction: the key came from the map's own key set.
        let Some(source_counts) = snapshot.by_source_system.get(system) else {
            continue;
        };
        details.push(format!(
            "{system}: held {} serving-stale {} access-lost {} stuck-building {} \
             unparseable-mime {} verification-halted {}",
            source_counts.held,
            source_counts.serving_stale,
            source_counts.access_lost,
            source_counts.stuck_building,
            source_counts.unparseable_mime,
            source_counts.verification_halted,
        ));
        // Each named backlog/fault count becomes its own typed HealthCount,
        // scoped to this source_system and stamped with the cycle's as-of.
        for (label, value) in [
            ("held", source_counts.held),
            ("serving_stale", source_counts.serving_stale),
            ("access_lost", source_counts.access_lost),
            ("stuck_building", source_counts.stuck_building),
            ("unparseable_mime", source_counts.unparseable_mime),
            ("verification_halted", source_counts.verification_halted),
        ] {
            counts.push(HealthCount {
                label: label.to_string(),
                source_system: Some(system.clone()),
                value,
                as_of: as_of.clone(),
            });
        }
    }
    (details, counts)
}

/// Render one `AnnotationHealth` snapshot into operator-facing detail lines plus
/// additive typed `HealthCount`s (C10b). The parked state is surfaced up front
/// (a parked worker means annotations are disabled for the run), then the last
/// cycle's freshness counts each carry the slot's measured-at as-of. Corpus-
/// aggregate (no `source_system` key), unlike the fabric counts.
fn annotation_health_view(snapshot: &AnnotationHealth) -> (Vec<String>, Vec<HealthCount>) {
    let as_of = snapshot
        .measured_at
        .clone()
        .unwrap_or_else(|| AS_OF_NOT_YET_MEASURED.to_string());
    let mut details = Vec::new();
    let mut counts = Vec::new();

    if snapshot.parked {
        let reason = snapshot
            .parked_detail
            .as_deref()
            .unwrap_or("client load failure");
        details.push(format!("worker parked: {reason}"));
    } else {
        details.push("worker cycling".to_string());
    }

    match &snapshot.last_cycle {
        Some(cycle) => {
            details.push(format!(
                "last cycle (as of {as_of}): sources {} expected {} missing {} built {} \
                 memoized {} failed {} source-failures {} projection-failures {} \
                 orphans-adopted {} deferred {}",
                cycle.sources_examined,
                cycle.expected,
                cycle.missing,
                cycle.built,
                cycle.memoized,
                cycle.failed,
                cycle.source_failures,
                cycle.projection_failures,
                cycle.orphans_adopted,
                cycle.deferred,
            ));
            for (label, value) in [
                ("sources_examined", cycle.sources_examined),
                ("expected", cycle.expected),
                ("missing", cycle.missing),
                ("built", cycle.built),
                ("memoized", cycle.memoized),
                ("failed", cycle.failed),
                ("source_failures", cycle.source_failures),
                ("projection_failures", cycle.projection_failures),
                ("orphans_adopted", cycle.orphans_adopted),
                ("deferred", cycle.deferred),
            ] {
                counts.push(HealthCount {
                    label: label.to_string(),
                    source_system: None,
                    value,
                    as_of: as_of.clone(),
                });
            }
        }
        None => details.push("no annotation cycle completed yet".to_string()),
    }
    (details, counts)
}

/// Prefix health details with the component's readiness role for operators.
fn readiness_details(role: &str, details: Vec<String>) -> Vec<String> {
    let mut output = Vec::with_capacity(details.len() + 1);
    output.push(format!("role: {role}"));
    output.extend(details);
    output
}

impl AdmissionGate {
    /// Create a concurrency gate with a fixed positive capacity.
    fn new(max_in_flight: u32) -> Self {
        let max_in_flight = max_in_flight as usize;

        Self {
            max_in_flight,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Acquire one permit immediately or fail with a visible saturation diagnostic.
    fn try_acquire(&self, operation: &'static str) -> Result<AdmissionPermit, ApiError> {
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.max_in_flight {
                return Err(ApiError::ServiceUnavailable {
                    message: format!(
                        "{operation} admission saturated: in-flight {current}/{}",
                        self.max_in_flight
                    ),
                });
            }
            // Compare-exchange keeps admission fail-fast and lock-free: a lost
            // race retries against the observed count instead of waiting.
            match self.in_flight.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(AdmissionPermit {
                        in_flight: Arc::clone(&self.in_flight),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Capture exact current gate counters without mutating admission state.
    // Consumed at C10b: `AppState::admission_snapshot` reads it for the
    // diagnostic-only search-admission health count.
    fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            max_in_flight: self.max_in_flight,
            in_flight: self.in_flight.load(Ordering::Acquire),
        }
    }
}

/// Compare one submitted token against the expected token without short-circuiting mismatched bytes.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for (index, right_value) in right.iter().enumerate() {
        let left_value = left.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_value ^ right_value);
    }

    diff == 0
}
