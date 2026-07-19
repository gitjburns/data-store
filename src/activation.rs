//! Parse activation lifecycle (spec §13.2–§13.6, §14, §31.1): unattended
//! gating of ready ParseRuns, held-parse disposition (§13.4), and the cutover
//! transaction that swaps `source_objects.active_parse_id` behind the
//! per-source barrier in `crate::state`.
//!
//! Gating rules:
//! - Changed content activates (§13.2): a ready parse for a source with no
//!   active predecessor activates once canonical state is complete.
//! - Unchanged content is dominance-gated (§13.3) over the conformance
//!   report `dimensions` map. Ruling (2026-07-11): union comparison,
//!   absence-conservative — a dimension present in the active report but
//!   absent from the candidate compares as worse (hold); present in the
//!   candidate but absent from the active does not block; absent from both
//!   is equal.
//!
//! No absolute quality thresholds exist anywhere in this module (§13, §35):
//! every gate is either a definitional status/identity check or the relative
//! dominance comparison between two measured reports.
//!
//! Caller-contract vs infrastructure split: a disposition or gating call on
//! a run that is not in the required state (ready, held vs not held) is an
//! explicit `BadRequest` — the caller asked for an impossible transition —
//! while SQL/clock faults and broken persisted invariants surface as
//! `StorageOperation`/`InternalIo`. All state transitions are status-guarded
//! in their WHERE clauses, and every SystemEvent is appended on the same
//! transaction as the transition it records (the `crate::events` invariant).

use std::{collections::BTreeMap, path::Path, time::Instant};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::{Map, Value};
use tracing::{error, info, warn};

use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::model::{ConformanceReport, ParseHeldReason, ParseRunStatus, SystemEventType};
use crate::primitives::utc_now;
use crate::projections::dense_cache::DenseCache;
use crate::projections::envelope::{self, ProjectionType};
use crate::state::CutoverRegistry;

/// The content-derived retrieval projection types that MUST be fresh for a
/// candidate parse before it may activate (spec §22–§23; C6). A parse becomes
/// the source's searchable truth at cutover, so activating without its
/// retrieval-targeting projections would publish a parse that queries cannot
/// reach through the lexical/dense/multivector/view channels. The graph
/// projection (C6f) is deliberately NOT required here — it is not a
/// retrieval-targeting channel gated by the C6 integration ruling. The set is
/// built once and reused by every prerequisite check.
const REQUIRED_CONTENT_DERIVED_PROJECTIONS: [ProjectionType; 5] = [
    ProjectionType::Chunk,
    ProjectionType::LexicalDocument,
    ProjectionType::DenseVector,
    ProjectionType::MultiVector,
    ProjectionType::DerivedView,
];

/// Log-event namespace this module passes to the shared hot-plane
/// transaction helpers, so boundary logs stay attributable to activation.
const TX_LOG_NAMESPACE: &str = "activation";

/// SystemEvent object_type for parse_runs rows (mirrors the private constant
/// in `crate::parse::importer`; a shared-constant consolidation candidate
/// once a third emitter appears).
const OBJECT_TYPE_PARSE_RUN: &str = "parse_run";

/// Barrier-key discovery read, executed BEFORE the cutover barrier is
/// acquired. The run→source binding is immutable (§12), so this lookup
/// discovers only the barrier key, never decision state — status, hold, and
/// conformance are all re-read inside the barrier-protected transaction.
const SELECT_PARSE_RUN_SOURCE_SQL: &str = "
SELECT source_id FROM parse_runs WHERE id = ?1";

/// Candidate load inside the cutover transaction: everything the gating
/// decision reads from the run itself.
const SELECT_CANDIDATE_RUN_SQL: &str = "
SELECT source_id, status, held_reason, conformance_report_json
FROM parse_runs WHERE id = ?1";

/// The source's current active-parse pointer (§14): NULL means no active
/// predecessor, so §13.2 (changed content activates) applies.
const SELECT_ACTIVE_PARSE_ID_SQL: &str = "
SELECT active_parse_id FROM source_objects WHERE id = ?1";

/// Predecessor load for the §13.3 dominance comparison: the run the pointer
/// names must be `active` and must carry the conformance report it was
/// activated with.
const SELECT_PREDECESSOR_RUN_SQL: &str = "
SELECT status, conformance_report_json FROM parse_runs WHERE id = ?1";

/// Predecessor transition on activation: active → archiving. C9
/// archive-verify-delete completes it to archived (spec §31.2); archived_at
/// is set only by that completion, never here.
const UPDATE_PREDECESSOR_ARCHIVING_SQL: &str = "
UPDATE parse_runs SET status = 'archiving'
WHERE id = ?1 AND status = 'active'";

/// Candidate transition to active. held_reason is cleared in the same
/// statement so a force-activated held run (§13.4) leaves the held state
/// atomically with its activation.
const UPDATE_CANDIDATE_ACTIVE_SQL: &str = "
UPDATE parse_runs
SET status = 'active', activated_at = ?2, held_reason = NULL
WHERE id = ?1 AND status = 'ready'";

/// THE cutover pointer write (spec §31.1): the single per-source pointer the
/// barrier exists to protect.
const UPDATE_SOURCE_ACTIVE_PARSE_SQL: &str = "
UPDATE source_objects SET active_parse_id = ?2 WHERE id = ?1";

/// Other held candidates of the same source (§12 rule 4: at most one held
/// candidate; a newer candidate supersedes it). `id <> ?2` excludes the run
/// currently being activated or held.
const SELECT_OTHER_HELD_RUNS_SQL: &str = "
SELECT id FROM parse_runs
WHERE source_id = ?1 AND status = 'ready' AND held_reason IS NOT NULL AND id <> ?2";

/// Superseded held candidate leaves the disposition queue: ready(held) →
/// archiving, completed to archived by C9 (§31.2).
const UPDATE_HELD_RUN_SUPERSEDED_SQL: &str = "
UPDATE parse_runs SET status = 'archiving'
WHERE id = ?1 AND status = 'ready' AND held_reason IS NOT NULL";

/// Hold transition (§13.3): the run stays `ready` and non-queryable; only
/// held_reason changes. The `held_reason IS NULL` guard keeps a double-hold
/// a visible invariant breach instead of a silent overwrite.
const UPDATE_CANDIDATE_HELD_SQL: &str = "
UPDATE parse_runs SET held_reason = ?2
WHERE id = ?1 AND status = 'ready' AND held_reason IS NULL";

/// Discard disposition (§13.4): ready(held) → archiving, completed by C9.
/// The canonical parse bundle stays in the artifact store — discard removes
/// the run from the disposition queue, not from the audit record.
const UPDATE_DISCARD_HELD_SQL: &str = "
UPDATE parse_runs SET status = 'archiving'
WHERE id = ?1 AND status = 'ready' AND held_reason IS NOT NULL";

/// Outcome of one unattended gating pass (spec §13.2–§13.3). `Held` carries
/// the regressed dimension names so the caller can log or surface the hold
/// without re-reading the event log. `Activated` carries the superseded
/// predecessor id (None on first-time activation with no predecessor) so the
/// caller can drive the §31.2 superseded-cleanup of that predecessor without
/// re-reading the pointer — the run the cutover moved to `archiving`.
///
/// Both arms carry `superseded_held_ids` (Ruling 1): reaching a disposition —
/// activate OR hold — supersedes any older held candidate of the same source
/// (`supersede_other_held`, §12 rule 4), moving it to `archiving`. Those ids are
/// threaded out here so the scheduler drives their §31.2 cleanup post-barrier
/// with `SupersededCleanupMode::HeldSupersession`, exactly as the predecessor id
/// drives `ActivationSupersession`. Normally empty or one id; defensively more
/// under drift (see `supersede_other_held`). This is DISTINCT from
/// `superseded_predecessor_id`: the predecessor is the outgoing ACTIVE parse the
/// cutover replaced, while these are never-activated HELD candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActivationDecision {
    Activated {
        superseded_predecessor_id: Option<String>,
        superseded_held_ids: Vec<String>,
    },
    Held {
        regressed_dimensions: Vec<String>,
        superseded_held_ids: Vec<String>,
    },
}

/// One §13.3 regression: the candidate measured worse than the active parse
/// on this dimension. These are the durable deltas the parse.held event and
/// the warn log carry.
#[derive(Debug, Clone)]
struct RegressedDimension {
    name: String,
    /// None when the dimension is absent from the candidate report — the
    /// absence-conservative arm of the union comparison (module docs).
    candidate: Option<f64>,
    active: f64,
}

/// What the gate transaction decided, kept internal so the terminal
/// decision logging (after commit — logs before commit could record a
/// decision that rolls back) has the full deltas, while the public
/// `ActivationDecision` stays the compact pre-agreed shape.
enum GateOutcome {
    Activated {
        predecessor_id: Option<String>,
        // Ruling 1: older held candidates this disposition superseded, for the
        // caller's post-barrier §31.2 HeldSupersession cleanup.
        superseded_held_ids: Vec<String>,
    },
    Held {
        deltas: Vec<RegressedDimension>,
        superseded_held_ids: Vec<String>,
    },
}

/// The candidate run's decision state, re-read inside the barrier-protected
/// transaction (the pre-barrier lookup discovered only the source_id).
struct CandidateRun {
    source_id: String,
    status: ParseRunStatus,
    held_reason: Option<ParseHeldReason>,
    conformance: Option<ConformanceReport>,
}

/// Inputs of the shared activate path (§13.6 steps a–e), grouped so
/// `activate_candidate` reads like the spec sequence instead of a parameter
/// list.
struct ActivationContext<'a> {
    candidate_id: &'a str,
    source_id: &'a str,
    predecessor_id: Option<&'a str>,
}

/// Unattended activation gate for one ready, un-held ParseRun (spec §13.2,
/// §13.3, §13.6): acquire the source's cutover barrier, then in one
/// IMMEDIATE transaction re-read the candidate, compare against the active
/// predecessor's conformance report when one exists, and either perform the
/// cutover or hold the candidate with heldReason = conformance_regression.
/// Calling this on a run that is not ready-and-not-held is a caller-contract
/// `BadRequest`; `Err` otherwise means an infrastructure fault and no state
/// changed (the transaction rolled back).
pub(crate) fn gate_and_activate(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    parse_run_id: &str,
) -> Result<ActivationDecision, ApiError> {
    let started = Instant::now();
    info!(
        event = "activation.gate_started",
        parse_run_id, "parse activation gate starting"
    );

    let mut connection = hot_plane::open_write(index_root)?;
    // Pre-barrier read for barrier keying only (see the SQL constant's
    // comment): the run→source binding is immutable, so this is not
    // decision state and needs no barrier protection.
    let source_id = lookup_source_id(&connection, parse_run_id)?;

    // Acquired before ANY decision-state read and held across the whole
    // read-decide-swap, so an admin accept (accept_held_parse) can never
    // interleave between this gate's read and its pointer write. The hold
    // is still a few bounded SQLite statements — reads, guarded updates,
    // event appends, commit — honoring the §31.1 brevity contract.
    let _barrier_guard = registry.acquire(&source_id);

    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "gate_and_activate")?;
    let outcome = match gate_transaction_body(&tx, parse_run_id, &source_id) {
        Ok(outcome) => outcome,
        Err(source) => {
            let source =
                hot_plane::abort_transaction(tx, TX_LOG_NAMESPACE, "gate_and_activate", source);
            error!(
                event = "activation.gate_failed",
                parse_run_id,
                source_id,
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parse activation gate failed; no state changed"
            );
            return Err(source);
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "gate_and_activate")?;

    // Terminal decision logs come after the commit so they record durable
    // truth; the barrier stays held until this function returns (guard
    // drop), which also logs the release with its hold duration.
    match outcome {
        GateOutcome::Activated {
            predecessor_id,
            superseded_held_ids,
        } => {
            info!(
                event = "activation.decision",
                parse_run_id,
                source_id,
                decision = "activated",
                "activation gate decided: activate"
            );
            info!(
                event = "activation.cutover_committed",
                parse_run_id,
                source_id,
                predecessor_parse_id = predecessor_id.as_deref().unwrap_or("none"),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "active-parse pointer swapped and committed"
            );
            // Dense-cache publish (spec §1.6 ingest-publish-invariant successor):
            // the in-memory plane swap is the paired half of the durable pointer
            // write, so it runs AFTER the commit above and BEFORE the barrier
            // releases (guard drops at function return) — the two together form
            // one publish the barrier serializes against another publish of the
            // same source. Load the newly-active parse's plane from the durable
            // chunk_dense_vectors rows just committed, then evict the superseded
            // predecessor's plane so the cache tracks exactly the active parses.
            publish_dense_cache(
                dense_cache,
                &connection,
                parse_run_id,
                predecessor_id.as_deref(),
                dense_dimension,
            )?;
            // Thread the predecessor out to the caller: the §31.2 superseded
            // cleanup (scheduler wiring) deletes this predecessor's hot rows and
            // completes its archiving → archived transition, verifying over the
            // post-activation snapshot the scheduler mints next. The superseded
            // held ids ride out too (Ruling 1) for their own HeldSupersession
            // cleanup — distinct from the predecessor, gated over each held
            // candidate's own pre_activation snapshot.
            Ok(ActivationDecision::Activated {
                superseded_predecessor_id: predecessor_id,
                superseded_held_ids,
            })
        }
        GateOutcome::Held {
            deltas,
            superseded_held_ids,
        } => {
            info!(
                event = "activation.decision",
                parse_run_id,
                source_id,
                decision = "held",
                "activation gate decided: hold"
            );
            // The §13.3 deltas at warn level; bounded because the dimension
            // set is small and code-defined (crate::parse::conformance).
            warn!(
                event = "activation.held",
                parse_run_id,
                source_id,
                regressed_dimensions = %render_deltas(&deltas),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "candidate held: conformance regression against the active parse"
            );
            Ok(ActivationDecision::Held {
                regressed_dimensions: deltas.into_iter().map(|delta| delta.name).collect(),
                superseded_held_ids,
            })
        }
    }
}

/// Publish the dense-cache half of a cutover (spec §1.6 successor): load the
/// newly-active parse's dense plane and evict the superseded predecessor's. The
/// CALLER holds the source's cutover barrier across the durable pointer commit
/// AND this call, so the durable write and this in-memory swap are one publish
/// that another publish of the same source cannot interleave with (see
/// `DenseCache::load_parse`). The load reads the just-committed
/// `chunk_dense_vectors` rows through C6c-1's loader; eviction of the
/// predecessor keeps the cache holding exactly the currently-active parses.
///
/// A load failure IS propagated: the durable pointer already swapped, but a
/// searchable active parse with no loaded dense plane is a broken publish, so
/// the gate returns Err and the failure is loud rather than silently serving an
/// empty dense channel. Eviction of the predecessor is best-effort inside
/// `evict_parse` (an absent plane is a benign no-op).
fn publish_dense_cache(
    dense_cache: &DenseCache,
    connection: &Connection,
    activated_parse_id: &str,
    predecessor_parse_id: Option<&str>,
    dense_dimension: usize,
) -> Result<(), ApiError> {
    dense_cache.load_parse(connection, activated_parse_id, dense_dimension)?;
    if let Some(predecessor_parse_id) = predecessor_parse_id {
        // The predecessor is no longer the active parse, so its plane is evicted
        // under the same barrier hold that published the new one.
        dense_cache.evict_parse(predecessor_parse_id);
    }
    Ok(())
}

/// Force-activation disposition of one held parse (spec §13.4, admin
/// surface at C10a): same barrier and cutover transaction as the unattended
/// gate — the dominance comparison is skipped by explicit administrative
/// decision, recorded by a parse.accepted event appended before
/// parse.activated. Requires the run to be `ready` WITH held_reason set.
///
/// Publish invariant: because this disposition swaps the active-parse pointer
/// exactly as `gate_and_activate` does, it must mirror the gate's paired
/// dense-cache publish. After the commit, while the barrier guard is still
/// held, it loads the newly-active parse's dense plane and evicts the
/// predecessor's — a searchable active parse with no loaded dense plane is a
/// broken publish, so `dense_cache`/`dense_dimension` are threaded in and the
/// load failure propagates identically to the gate path.
// Consumed by the C10a accept handler in http.rs (admin held-parse disposition
// surface), which threads the dense_cache/dense_dimension params for the paired
// publish.
//
// Returns `ActivationDecision::Activated { superseded_predecessor_id }` (never
// `Held` — accept is an unconditional force-activation) so the accept path
// carries the SAME predecessor contract as the unattended gate. The C10a
// handler drives the §31.2 superseded cleanup of that predecessor via
// `drive_activation_cleanup`.
pub(crate) fn accept_held_parse(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    parse_run_id: &str,
) -> Result<ActivationDecision, ApiError> {
    let started = Instant::now();
    info!(
        event = "activation.disposition.accept_started",
        parse_run_id, "held-parse accept disposition starting"
    );

    let mut connection = hot_plane::open_write(index_root)?;
    // Same barrier-key-only pre-read as gate_and_activate.
    let source_id = lookup_source_id(&connection, parse_run_id)?;

    // The same per-source barrier the scheduler gate takes, so an accept can
    // never interleave with a concurrent unattended activation of a newer
    // candidate for this source.
    let _barrier_guard = registry.acquire(&source_id);

    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "accept_held_parse")?;
    let (predecessor_id, superseded_held_ids) =
        match accept_transaction_body(&tx, parse_run_id, &source_id) {
            Ok(outcome) => outcome,
            Err(source) => {
                let source =
                    hot_plane::abort_transaction(tx, TX_LOG_NAMESPACE, "accept_held_parse", source);
                error!(
                    event = "activation.disposition.accept_failed",
                    parse_run_id,
                    source_id,
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "held-parse accept failed; no state changed"
                );
                return Err(source);
            }
        };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "accept_held_parse")?;

    info!(
        event = "activation.cutover_committed",
        parse_run_id,
        source_id,
        predecessor_parse_id = predecessor_id.as_deref().unwrap_or("none"),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "active-parse pointer swapped and committed by accept disposition"
    );
    // Dense-cache publish, mirroring gate_and_activate exactly: the in-memory
    // plane swap is the paired half of the durable pointer write, so it runs
    // AFTER the commit above and BEFORE the barrier releases (guard drops at
    // function return). Load the newly-active parse's plane and evict the
    // predecessor's, with identical failure propagation — a load failure
    // returns Err because a searchable active parse with no loaded dense plane
    // is a broken publish.
    publish_dense_cache(
        dense_cache,
        &connection,
        parse_run_id,
        predecessor_id.as_deref(),
        dense_dimension,
    )?;
    info!(
        event = "activation.disposition.accepted",
        parse_run_id,
        source_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "held parse force-activated"
    );
    // Uniform predecessor contract with the unattended gate: accept always
    // activates, so this is always `Activated`, carrying the predecessor id for
    // C10a's §31.2 superseded cleanup, plus any other superseded held ids
    // (Ruling 1) for C10a's HeldSupersession cleanup of each.
    Ok(ActivationDecision::Activated {
        superseded_predecessor_id: predecessor_id,
        superseded_held_ids,
    })
}

/// Discard disposition of one held parse (spec §13.4, admin surface at
/// C10a): the run moves ready(held) → archiving with a parse.discarded
/// event, and the canonical bundle stays in the artifact store. No cutover
/// barrier is taken because no active-parse pointer changes: queries can never
/// observe a partial state from a discard, so barrier protection would protect
/// nothing.
///
/// CLEANUP SEAM (Ruling 1): this function ONLY moves the run to `archiving`
/// (via `UPDATE_DISCARD_HELD_SQL` in `discard_transaction_body`). Its §31.2
/// archive-verify-delete is driven by the C10a discard HTTP handler AFTER this
/// returns, exactly as accept's is: the handler calls
/// `restore::complete_superseded_parse(index_root, source_id, parse_run_id,
/// SupersededCleanupMode::HeldSupersession)`, gating over this run's own
/// pre_activation snapshot. Discard takes NO cutover barrier (see above and
/// main.rs), so — unlike the gate/accept paths whose cleanup runs post-barrier —
/// the discard cleanup runs directly in the handler with no barrier release to
/// wait on. This function must NOT call the cleanup itself: the driver is the
/// HTTP call site, not built here.
// Consumed by the C10a discard handler in http.rs (admin held-parse
// disposition surface).
pub(crate) fn discard_held_parse(index_root: &Path, parse_run_id: &str) -> Result<(), ApiError> {
    let started = Instant::now();
    info!(
        event = "activation.disposition.discard_started",
        parse_run_id, "held-parse discard disposition starting"
    );

    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "discard_held_parse",
    )?;
    let source_id = match discard_transaction_body(&tx, parse_run_id) {
        Ok(source_id) => source_id,
        Err(source) => {
            let source =
                hot_plane::abort_transaction(tx, TX_LOG_NAMESPACE, "discard_held_parse", source);
            error!(
                event = "activation.disposition.discard_failed",
                parse_run_id,
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "held-parse discard failed; no state changed"
            );
            return Err(source);
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "discard_held_parse")?;

    info!(
        event = "activation.disposition.discarded",
        parse_run_id,
        source_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "held parse discarded to archiving"
    );
    Ok(())
}

/// The gate transaction (spec §13.2/§13.3): re-read the candidate under the
/// barrier, enforce the ready-and-not-held caller contract, then activate
/// (no predecessor, or candidate dominates) or hold (any regressed
/// dimension).
fn gate_transaction_body(
    tx: &Transaction<'_>,
    parse_run_id: &str,
    barrier_source_id: &str,
) -> Result<GateOutcome, ApiError> {
    let candidate = load_candidate_run(tx, parse_run_id)?;
    verify_barrier_key(parse_run_id, &candidate, barrier_source_id)?;

    // Caller contract: the unattended gate takes only ready, un-held runs
    // (held runs wait for explicit §13.4 disposition; every other status has
    // no legal transition to active).
    if candidate.status != ParseRunStatus::Ready || candidate.held_reason.is_some() {
        return Err(ApiError::BadRequest {
            message: format!(
                "parse run {parse_run_id} is not gateable: status is {}, held_reason {}; \
                 gate_and_activate requires status ready with no held_reason",
                wire_name(&candidate.status, "parse run status")?,
                describe_held_reason(candidate.held_reason.as_ref())?,
            ),
        });
    }
    verify_activation_prerequisites(tx, parse_run_id, &candidate)?;

    let active_parse_id = load_active_parse_id(tx, &candidate.source_id)?;
    let Some(predecessor_id) = active_parse_id else {
        // §13.2: no active predecessor means this content lifecycle has
        // nothing current to protect — the parse activates unconditionally.
        let superseded_held_ids = activate_candidate(
            tx,
            &ActivationContext {
                candidate_id: parse_run_id,
                source_id: &candidate.source_id,
                predecessor_id: None,
            },
        )?;
        return Ok(GateOutcome::Activated {
            predecessor_id: None,
            superseded_held_ids,
        });
    };

    // §13.3 dominance comparison over the two dimensions maps. Both reports
    // are required: the importer persists a conformance report with every
    // ready transition and activation preserves it, so absence here is a
    // broken persisted invariant, not a producer outcome.
    let active_report = load_active_predecessor_report(tx, &predecessor_id)?;
    let candidate_report =
        candidate
            .conformance
            .as_ref()
            .ok_or_else(|| ApiError::StorageOperation {
                message: format!(
                    "ready parse run {parse_run_id} has no persisted conformance report; \
                 the dominance comparison cannot run"
                ),
            })?;
    let deltas = regressed_dimensions(&candidate_report.dimensions, &active_report.dimensions);

    if deltas.is_empty() {
        let superseded_held_ids = activate_candidate(
            tx,
            &ActivationContext {
                candidate_id: parse_run_id,
                source_id: &candidate.source_id,
                predecessor_id: Some(&predecessor_id),
            },
        )?;
        Ok(GateOutcome::Activated {
            predecessor_id: Some(predecessor_id),
            superseded_held_ids,
        })
    } else {
        let superseded_held_ids = hold_candidate(
            tx,
            parse_run_id,
            &candidate.source_id,
            &predecessor_id,
            &deltas,
        )?;
        Ok(GateOutcome::Held {
            deltas,
            superseded_held_ids,
        })
    }
}

/// The accept transaction (spec §13.4): enforce the ready-and-held caller
/// contract, record the administrative decision (parse.accepted, appended
/// before parse.activated), then run the same activate path as the
/// unattended gate — no dominance comparison, by definition of the
/// disposition. Returns the predecessor id (for the caller's terminal log) and
/// the ids of any OTHER held candidates the activate path superseded (Ruling 1;
/// §12 rule 4 makes this normally empty, defensively >0 under drift) so the
/// caller drives their post-barrier HeldSupersession cleanup.
fn accept_transaction_body(
    tx: &Transaction<'_>,
    parse_run_id: &str,
    barrier_source_id: &str,
) -> Result<(Option<String>, Vec<String>), ApiError> {
    let candidate = load_candidate_run(tx, parse_run_id)?;
    verify_barrier_key(parse_run_id, &candidate, barrier_source_id)?;

    // Caller contract: accept is defined only for held runs.
    let Some(held_reason) = candidate.held_reason.as_ref().copied() else {
        return Err(ApiError::BadRequest {
            message: format!(
                "parse run {parse_run_id} is not held: status is {}, held_reason none; \
                 accept_held_parse requires status ready with held_reason set",
                wire_name(&candidate.status, "parse run status")?,
            ),
        });
    };
    if candidate.status != ParseRunStatus::Ready {
        return Err(ApiError::BadRequest {
            message: format!(
                "parse run {parse_run_id} is not held: status is {}; \
                 accept_held_parse requires status ready with held_reason set",
                wire_name(&candidate.status, "parse run status")?,
            ),
        });
    }
    verify_activation_prerequisites(tx, parse_run_id, &candidate)?;

    let predecessor_id = load_active_parse_id(tx, &candidate.source_id)?;

    // The administrative decision is recorded before its consequence:
    // parse.accepted, then the activate path appends parse.activated.
    let payload = Map::from_iter([
        entry("sourceId", &candidate.source_id),
        entry("heldReason", &wire_name(&held_reason, "held reason")?),
    ]);
    let event = new_system_event(
        SystemEventType::ParseAccepted,
        OBJECT_TYPE_PARSE_RUN,
        parse_run_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;

    let superseded_held_ids = activate_candidate(
        tx,
        &ActivationContext {
            candidate_id: parse_run_id,
            source_id: &candidate.source_id,
            predecessor_id: predecessor_id.as_deref(),
        },
    )?;
    Ok((predecessor_id, superseded_held_ids))
}

/// The discard transaction (spec §13.4): enforce the ready-and-held caller
/// contract, move the run to archiving, and record parse.discarded. Returns
/// the source id for the caller's terminal log.
fn discard_transaction_body(tx: &Transaction<'_>, parse_run_id: &str) -> Result<String, ApiError> {
    let candidate = load_candidate_run(tx, parse_run_id)?;

    // Caller contract: discard is defined only for held runs.
    let Some(held_reason) = candidate.held_reason.as_ref().copied() else {
        return Err(ApiError::BadRequest {
            message: format!(
                "parse run {parse_run_id} is not held: status is {}, held_reason none; \
                 discard_held_parse requires status ready with held_reason set",
                wire_name(&candidate.status, "parse run status")?,
            ),
        });
    };
    if candidate.status != ParseRunStatus::Ready {
        return Err(ApiError::BadRequest {
            message: format!(
                "parse run {parse_run_id} is not held: status is {}; \
                 discard_held_parse requires status ready with held_reason set",
                wire_name(&candidate.status, "parse run status")?,
            ),
        });
    }

    let updated = tx
        .execute(UPDATE_DISCARD_HELD_SQL, params![parse_run_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to discard held parse run {parse_run_id}: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("held parse run {parse_run_id} ready → archiving (discard)"),
    )?;

    let payload = Map::from_iter([
        entry("sourceId", &candidate.source_id),
        entry("heldReason", &wire_name(&held_reason, "held reason")?),
    ]);
    let event = new_system_event(
        SystemEventType::ParseDiscarded,
        OBJECT_TYPE_PARSE_RUN,
        parse_run_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;
    Ok(candidate.source_id)
}

/// Activation prerequisite check (spec §13.6, §21.4, §22–§23). Runs inside the
/// barrier-protected gate transaction on the CANDIDATE parse, BEFORE its pointer
/// swap. Two prerequisites:
///
/// 1. Annotation policy (§21.4): activation gates only on the annotation types
///    in the required-annotation-set policy's blocking set. Reading the policy
///    (rather than assuming the MVP content) is what makes a future non-empty
///    blocking set take effect here without a seam change.
///
/// 2. Content-derived projections (§22–§23; C6): the five retrieval-targeting
///    projection types (chunk, lexical, dense, multivector, derived view) must
///    all be FRESH for the candidate parse. A parse becomes the source's
///    searchable truth at cutover, so it must NOT activate without the
///    projections queries reach it through — otherwise the active parse would be
///    unsearchable. Freshness is read via `envelope::fresh_types_for_parse`
///    (keyed by parse_id, NOT the active-parse-scoped reader — the candidate is
///    not active yet) so the retrieval_projections SQL stays owned by the
///    envelope module.
///
/// `parse_run_id` is the candidate parse id (the pointer target); `tx` is the
/// gate's barrier-protected transaction, whose `Connection` the freshness read
/// borrows.
fn verify_activation_prerequisites(
    tx: &Transaction<'_>,
    parse_run_id: &str,
    _candidate: &CandidateRun,
) -> Result<(), ApiError> {
    // §21.4 annotation blocking set (see doc). Sealing the versioned policy can
    // fail loudly, so surface that rather than silently skipping the gate.
    let policy = crate::annotations::policy::active_policy()?;
    for _blocking_type in &policy.blocking_types {
        // Seam: when a future policy version adds blocking types, implement the
        // fresh-annotation lookup for `_candidate` here — each blocking type
        // must have fresh annotations before activation. Today the empty set
        // makes activation gate on canonical state only (the C5 ruling), so
        // there is nothing to query.
    }

    // §22–§23 content-derived projection freshness for the candidate parse. A
    // Transaction derefs to Connection, so the envelope reader runs on the same
    // barrier-protected transaction the gate holds.
    let fresh_types = envelope::fresh_types_for_parse(tx, parse_run_id)?;
    let missing: Vec<ProjectionType> = REQUIRED_CONTENT_DERIVED_PROJECTIONS
        .into_iter()
        .filter(|required| !fresh_types.contains(required))
        .collect();
    if !missing.is_empty() {
        // Loud, mirror of the gate's other caller-contract errors: a candidate
        // whose retrieval projections are not fresh must not become searchable
        // truth. The scheduler's build step (which runs before the gate) is the
        // producer of these projections, so a miss here means that build did not
        // complete for this parse.
        let missing_names: Vec<String> = missing
            .iter()
            .map(|projection_type| wire_name(projection_type, "projection type"))
            .collect::<Result<_, _>>()?;
        return Err(ApiError::StorageOperation {
            message: format!(
                "parse run {parse_run_id} cannot activate: content-derived projections not fresh \
                 for the candidate parse ({}); a parse must not activate without its \
                 retrieval-targeting projections",
                missing_names.join(", ")
            ),
        });
    }
    info!(
        event = "activation.prerequisites_ok",
        parse_run_id,
        required_projection_count = REQUIRED_CONTENT_DERIVED_PROJECTIONS.len(),
        "activation prerequisites satisfied: content-derived projections fresh"
    );
    Ok(())
}

/// The shared activate path (spec §13.6 as one transaction, steps a–e):
/// predecessor active → archiving, candidate ready → active, the §31.1
/// pointer write, supersession of any other held candidate, and the
/// parse.activated event. Used verbatim by the unattended gate and the
/// accept disposition; the caller owns the surrounding transaction and
/// barrier. Returns the ids of any older held candidates superseded in step (d)
/// so the caller can drive their §31.2 HeldSupersession cleanup post-barrier
/// (Ruling 1).
fn activate_candidate(
    tx: &Transaction<'_>,
    ctx: &ActivationContext<'_>,
) -> Result<Vec<String>, ApiError> {
    // (a) Predecessor leaves active. §33 defines no event for the start of
    // archiving (parse.archived belongs to C9's completion, §31.2), so this
    // durable log line is the record of the transition's beginning.
    if let Some(predecessor_id) = ctx.predecessor_id {
        let updated = tx
            .execute(UPDATE_PREDECESSOR_ARCHIVING_SQL, params![predecessor_id])
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to move predecessor parse run {predecessor_id} to archiving: {source}"
                ),
            })?;
        expect_single_row(
            updated,
            &format!("predecessor parse run {predecessor_id} active → archiving"),
        )?;
        info!(
            event = "activation.predecessor_archiving",
            parse_run_id = ctx.candidate_id,
            source_id = ctx.source_id,
            predecessor_parse_id = predecessor_id,
            "predecessor parse moved to archiving; C9 archive-verify-delete completes it"
        );
    }

    // (b) Candidate becomes the active parse.
    let activated_at = utc_now()?;
    let updated = tx
        .execute(
            UPDATE_CANDIDATE_ACTIVE_SQL,
            params![ctx.candidate_id, activated_at],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to activate parse run {}: {source}",
                ctx.candidate_id
            ),
        })?;
    expect_single_row(
        updated,
        &format!("candidate parse run {} ready → active", ctx.candidate_id),
    )?;

    // (c) THE cutover pointer write (spec §31.1): after this statement
    // commits, the candidate is what queries see (§14).
    let updated = tx
        .execute(
            UPDATE_SOURCE_ACTIVE_PARSE_SQL,
            params![ctx.source_id, ctx.candidate_id],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to set active_parse_id on source {} to {}: {source}",
                ctx.source_id, ctx.candidate_id
            ),
        })?;
    expect_single_row(
        updated,
        &format!(
            "source object {} active_parse_id → {}",
            ctx.source_id, ctx.candidate_id
        ),
    )?;

    // (d) §12 rule 4: a newer candidate reaching disposition supersedes any
    // previously held one. Its ids are threaded out for the caller's Ruling-1
    // post-barrier HeldSupersession cleanup.
    let superseded_held_ids = supersede_other_held(tx, ctx.source_id, ctx.candidate_id)?;

    // (e) The activation event, atomic with the cutover it records.
    let mut payload = Map::from_iter([
        entry("sourceId", ctx.source_id),
        entry("activatedParseId", ctx.candidate_id),
    ]);
    if let Some(predecessor_id) = ctx.predecessor_id {
        payload.insert(
            "predecessorParseId".to_owned(),
            Value::String(predecessor_id.to_owned()),
        );
    }
    let event = new_system_event(
        SystemEventType::ParseActivated,
        OBJECT_TYPE_PARSE_RUN,
        ctx.candidate_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;
    Ok(superseded_held_ids)
}

/// The hold path (spec §13.3): held_reason = conformance_regression while
/// the run stays ready and the predecessor keeps serving, supersession of
/// any older held candidate, and the parse.held event carrying the durable
/// per-dimension deltas. Returns the ids of any older held candidates this new
/// hold superseded so the caller can drive their §31.2 HeldSupersession cleanup
/// post-barrier (Ruling 1) — a held candidate can itself supersede an even older
/// held one.
fn hold_candidate(
    tx: &Transaction<'_>,
    candidate_id: &str,
    source_id: &str,
    predecessor_id: &str,
    deltas: &[RegressedDimension],
) -> Result<Vec<String>, ApiError> {
    let reason = wire_name(&ParseHeldReason::ConformanceRegression, "held reason")?;
    let updated = tx
        .execute(UPDATE_CANDIDATE_HELD_SQL, params![candidate_id, reason])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to hold parse run {candidate_id}: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("candidate parse run {candidate_id} ready → ready(held)"),
    )?;

    let superseded_held_ids = supersede_other_held(tx, source_id, candidate_id)?;

    let mut payload = Map::from_iter([
        entry("sourceId", source_id),
        entry("activeParseId", predecessor_id),
    ]);
    payload.insert("regressedDimensions".to_owned(), deltas_payload(deltas)?);
    let event = new_system_event(
        SystemEventType::ParseHeld,
        OBJECT_TYPE_PARSE_RUN,
        candidate_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;
    Ok(superseded_held_ids)
}

/// Move every OTHER held candidate of the source to archiving, one
/// parse.hold_superseded event each naming superseded and superseding ids
/// (spec §12 rule 4). At most one such run should exist; the loop tolerates
/// drift defensively so each superseded run still gets its own event.
///
/// Ruling 1 (plan §3 C10r / Current Status 2026-07-16) closed the former cleanup
/// gap: the held candidate(s) moved to `archiving` here are threaded OUT to the
/// caller (via `ActivationDecision`, both the `Activated` and `Held` arms) so the
/// scheduler drives their §31.2 archive-verify-delete post-barrier with
/// `SupersededCleanupMode::HeldSupersession`, gating over each candidate's own
/// pre_activation snapshot. Without that wiring a superseded held run would sit
/// at `archiving` with its projections/annotations undeleted.
///
/// Cardinality: §12 rule 4 permits at most ONE held candidate per source, so the
/// returned Vec normally holds 0 or 1 id. The loop still tolerates drift (>1)
/// defensively — each superseded run gets its own event AND its own cleanup id —
/// so a `Vec` (not `Option`) is returned to keep every superseded run cleaned;
/// collapsing to `Option` would silently drop cleanup for a drift-extra row.
fn supersede_other_held(
    tx: &Transaction<'_>,
    source_id: &str,
    superseding_id: &str,
) -> Result<Vec<String>, ApiError> {
    let mut statement =
        tx.prepare(SELECT_OTHER_HELD_RUNS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare held-candidate lookup for source {source_id}: {source}"
                ),
            })?;
    let ids: Vec<String> = statement
        .query_map(params![source_id, superseding_id], |row| row.get(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query held candidates for source {source_id}: {source}"),
        })?
        .collect::<Result<_, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read held-candidate rows for source {source_id}: {source}"),
        })?;

    for superseded_id in &ids {
        let updated = tx
            .execute(UPDATE_HELD_RUN_SUPERSEDED_SQL, params![superseded_id])
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to supersede held parse run {superseded_id}: {source}"),
            })?;
        expect_single_row(
            updated,
            &format!("superseded held parse run {superseded_id} ready(held) → archiving"),
        )?;
        let payload = Map::from_iter([
            entry("sourceId", source_id),
            entry("supersededParseId", superseded_id),
            entry("supersedingParseId", superseding_id),
        ]);
        let event = new_system_event(
            SystemEventType::ParseHoldSuperseded,
            OBJECT_TYPE_PARSE_RUN,
            superseded_id,
            Some(payload),
        )?;
        append_event(tx, &event)?;
        info!(
            event = "activation.hold_superseded",
            source_id,
            superseded_parse_id = superseded_id.as_str(),
            superseding_parse_id = superseding_id,
            "older held candidate superseded"
        );
    }
    // Thread the superseded held ids out for the caller's post-barrier §31.2
    // cleanup (Ruling 1): normally 0 or 1, defensively >1 under drift.
    Ok(ids)
}

/// The §13.3 dominance comparison (module-docs ruling): union over both key
/// sets, absence-conservative. Iterating the ACTIVE report's keys realizes
/// the whole union rule — a key only in the candidate never blocks, and a
/// key absent from both is vacuously equal — so only active-side keys can
/// regress. Values compare with plain `>=` (every dimension is oriented
/// higher-is-better, a documented contract in `crate::parse::conformance`);
/// a non-finite persisted value can never satisfy `>=` and therefore holds,
/// which is the safe direction, though canonical JSON cannot encode one.
fn regressed_dimensions(
    candidate: &BTreeMap<String, f64>,
    active: &BTreeMap<String, f64>,
) -> Vec<RegressedDimension> {
    let mut regressed = Vec::new();
    for (name, active_value) in active {
        match candidate.get(name) {
            Some(candidate_value) if *candidate_value >= *active_value => {}
            Some(candidate_value) => regressed.push(RegressedDimension {
                name: name.clone(),
                candidate: Some(*candidate_value),
                active: *active_value,
            }),
            // Absent from the candidate: compares as worse (hold).
            None => regressed.push(RegressedDimension {
                name: name.clone(),
                candidate: None,
                active: *active_value,
            }),
        }
    }
    regressed
}

/// Pre-barrier lookup of the run's source id — the cutover barrier key.
/// A missing run is a caller-contract error: gating and disposition are
/// always invoked with an id read from durable state.
fn lookup_source_id(connection: &Connection, parse_run_id: &str) -> Result<String, ApiError> {
    connection
        .query_row(SELECT_PARSE_RUN_SOURCE_SQL, params![parse_run_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to look up parse run {parse_run_id}: {source}"),
        })?
        .ok_or_else(|| ApiError::BadRequest {
            message: format!("parse run {parse_run_id} does not exist"),
        })
}

/// Load the candidate's decision state inside the transaction. Persisted
/// status/held_reason strings are re-typed through the model enums so a
/// value outside the schema CHECK set fails loudly instead of being
/// string-compared into a wrong branch.
fn load_candidate_run(tx: &Transaction<'_>, parse_run_id: &str) -> Result<CandidateRun, ApiError> {
    let row = tx
        .query_row(SELECT_CANDIDATE_RUN_SQL, params![parse_run_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to load parse run {parse_run_id}: {source}"),
        })?;
    let Some((source_id, status_text, held_text, conformance_json)) = row else {
        return Err(ApiError::BadRequest {
            message: format!("parse run {parse_run_id} does not exist"),
        });
    };

    let status: ParseRunStatus = wire_value(&status_text, "parse run status")?;
    let held_reason: Option<ParseHeldReason> = held_text
        .as_deref()
        .map(|text| wire_value(text, "parse run held_reason"))
        .transpose()?;
    let conformance: Option<ConformanceReport> = conformance_json
        .as_deref()
        .map(|json| {
            serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "persisted conformance report of parse run {parse_run_id} is unparseable: \
                     {source}"
                ),
            })
        })
        .transpose()?;

    Ok(CandidateRun {
        source_id,
        status,
        held_reason,
        conformance,
    })
}

/// Confirm the source read inside the transaction matches the barrier key
/// from the pre-barrier lookup. The binding is immutable (§12), so a
/// mismatch means this thread holds the WRONG source's barrier and must not
/// proceed to a pointer write it left unprotected.
fn verify_barrier_key(
    parse_run_id: &str,
    candidate: &CandidateRun,
    barrier_source_id: &str,
) -> Result<(), ApiError> {
    if candidate.source_id == barrier_source_id {
        return Ok(());
    }
    Err(ApiError::StorageOperation {
        message: format!(
            "parse run {parse_run_id} source changed between barrier keying ({barrier_source_id}) \
             and the cutover transaction ({}); the immutable run→source binding is broken",
            candidate.source_id
        ),
    })
}

/// Load the source's active-parse pointer. The source row itself must exist
/// (parse_runs hard-references it), so a missing row is a broken persisted
/// invariant, not caller input.
fn load_active_parse_id(tx: &Transaction<'_>, source_id: &str) -> Result<Option<String>, ApiError> {
    tx.query_row(SELECT_ACTIVE_PARSE_ID_SQL, params![source_id], |row| {
        row.get::<_, Option<String>>(0)
    })
    .optional()
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to load active_parse_id of source {source_id}: {source}"),
    })?
    .ok_or_else(|| ApiError::StorageOperation {
        message: format!(
            "source object {source_id} referenced by a parse run does not exist; \
             the parse_runs → source_objects reference is broken"
        ),
    })
}

/// Load the active predecessor's conformance report for the §13.3
/// comparison. Every departure from the expected shape — missing row,
/// non-active status, absent report — is a broken persisted invariant: the
/// pointer names it active, and activation always preserves the report the
/// run was gated with.
fn load_active_predecessor_report(
    tx: &Transaction<'_>,
    predecessor_id: &str,
) -> Result<ConformanceReport, ApiError> {
    let row = tx
        .query_row(SELECT_PREDECESSOR_RUN_SQL, params![predecessor_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to load active predecessor parse run {predecessor_id}: {source}"
            ),
        })?;
    let Some((status_text, conformance_json)) = row else {
        return Err(ApiError::StorageOperation {
            message: format!(
                "active_parse_id names parse run {predecessor_id}, which does not exist"
            ),
        });
    };
    let status: ParseRunStatus = wire_value(&status_text, "parse run status")?;
    if status != ParseRunStatus::Active {
        return Err(ApiError::StorageOperation {
            message: format!(
                "active_parse_id names parse run {predecessor_id}, whose status is {}; \
                 the §14 active-parse invariant is broken",
                wire_name(&status, "parse run status")?
            ),
        });
    }
    let Some(json) = conformance_json else {
        return Err(ApiError::StorageOperation {
            message: format!(
                "active parse run {predecessor_id} has no persisted conformance report; \
                 the dominance comparison cannot run"
            ),
        });
    };
    serde_json::from_str(&json).map_err(|source| ApiError::StorageOperation {
        message: format!(
            "persisted conformance report of active parse run {predecessor_id} is \
             unparseable: {source}"
        ),
    })
}

/// Enforce that a status-guarded UPDATE hit exactly one row. Zero rows means
/// the guarded state vanished between read and write inside one IMMEDIATE
/// transaction — an invariant breach to surface, never a silent no-op.
fn expect_single_row(updated: usize, what: &str) -> Result<(), ApiError> {
    if updated == 1 {
        return Ok(());
    }
    Err(ApiError::StorageOperation {
        message: format!("{what} updated {updated} rows; the status guard did not match"),
    })
}

/// Recover a model enum's snake_case wire name from its serde rename, so
/// persisted values and messages always match the schema CHECK vocabulary
/// without a second hand-maintained name table (mirrors
/// `crate::events::event_type_wire_name`).
fn wire_name<T: Serialize>(value: &T, what: &'static str) -> Result<String, ApiError> {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => Ok(name),
        // Unreachable for a plain renamed enum; kept explicit so a future
        // representation change fails loudly instead of persisting or
        // logging a non-wire value.
        other => Err(ApiError::InternalIo {
            message: format!("{what} did not serialize to a wire string: {other:?}"),
        }),
    }
}

/// Re-type one persisted wire string through its model enum. The schema
/// CHECK constraints make an unknown value unreachable through this code
/// base, so a failure here means the database was written by something else
/// — a fault, not caller input.
fn wire_value<T: serde::de::DeserializeOwned>(
    text: &str,
    what: &'static str,
) -> Result<T, ApiError> {
    serde_json::from_value(Value::String(text.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!("persisted {what} value {text} is not a known wire value: {source}"),
        }
    })
}

/// Render an optional held reason for caller-contract error messages.
fn describe_held_reason(reason: Option<&ParseHeldReason>) -> Result<String, ApiError> {
    match reason {
        Some(reason) => wire_name(reason, "held reason"),
        None => Ok("none".to_owned()),
    }
}

/// Render the §13.3 deltas as the parse.held event's payload array:
/// one `{dimension, candidateValue, activeValue}` object per regression,
/// candidateValue null when the dimension is absent from the candidate.
fn deltas_payload(deltas: &[RegressedDimension]) -> Result<Value, ApiError> {
    let mut items = Vec::with_capacity(deltas.len());
    for delta in deltas {
        let mut object = Map::new();
        object.insert("dimension".to_owned(), Value::String(delta.name.clone()));
        object.insert(
            "candidateValue".to_owned(),
            match delta.candidate {
                Some(value) => number_value(value)?,
                None => Value::Null,
            },
        );
        object.insert("activeValue".to_owned(), number_value(delta.active)?);
        items.push(Value::Object(object));
    }
    Ok(Value::Array(items))
}

/// Compact one-line delta rendering for the warn log ("active -> candidate"
/// per dimension); bounded because the dimension set is code-defined and
/// small (`crate::parse::conformance`).
fn render_deltas(deltas: &[RegressedDimension]) -> String {
    deltas
        .iter()
        .map(|delta| match delta.candidate {
            Some(candidate) => format!("{} {} -> {}", delta.name, delta.active, candidate),
            None => format!("{} {} -> absent", delta.name, delta.active),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Convert one f64 into a JSON number, rejecting non-finite values
/// explicitly instead of silently persisting null (mirrors the panic-free
/// Result policy; canonical reports cannot contain non-finite values).
fn number_value(value: f64) -> Result<Value, ApiError> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| ApiError::InternalIo {
            message: format!("conformance dimension value {value} is not JSON-representable"),
        })
}
