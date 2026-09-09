//! Durable sync queue and knob-free adaptive acquisition scheduler
//! (spec §9.4–§9.6): latest-state coalescing over the `sync_queue` hot-plane
//! table, a single std::thread scan/drain cycle driver, cadence adapted from
//! observed signals only, and truthful freshness reporting into the shared
//! sync health slot. Implemented by work package C3c; C5c added the
//! drain-loop parse chain (mime routing, the §13.5 no-blind-retry guard,
//! worker dispatch, parse import, activation gating) and the startup sweep
//! of orphaned parser temp workspaces, completing the §9.4 rule that ONE
//! queue drives acquisition → parse → gate → activation.
//!
//! Queue policy:
//!
//! - The queue is operational state, not audit state: enqueue, claim,
//!   complete, and fail emit no system events, because the durable audit
//!   trail of every acquisition attempt is the AcquisitionRecord written by
//!   `crate::acquisition`. The only queue-related events are the
//!   sync.backpressure_entered/_exited transitions (spec §33), which record
//!   scheduler adaptation, not per-item outcomes.
//! - Failed entries are terminal for the scheduler: deterministic failure on
//!   identical input fails identically (the spec §13.5 disposition rule
//!   applied to acquisition), so retrying without new source state would
//!   learn nothing. Only a NEW detection (`enqueue_coalesced`) re-pends a
//!   failed row — the spec's re-parse-on-new-state rule.
//! - Completed entries are deleted: drained work leaves no queue residue, so
//!   queue depth stays an honest backlog measure.
//!
//! Connection policy (D1): every queue operation opens its own hot-plane
//! connection and, when it needs read-then-write atomicity, one IMMEDIATE
//! transaction — the same policy as `crate::acquisition`.

use std::{
    collections::BTreeSet,
    fs, io,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Map, Value};
use tracing::{debug, error, info, warn};

use crate::{
    // Parse routing matches on the same mime constants acquisition writes
    // into source_objects.mime_type, so the routing vocabulary is
    // single-sourced.
    acquisition::{self, AcquisitionContext, ImportOutcome, MIME_TYPE_PDF, MIME_TYPE_PLAIN_TEXT},
    activation::{self, ActivationDecision},
    artifact_store::ArtifactStore,
    config::{DoclingConfig, StorageConfig},
    connectors::{
        AcquisitionBundleManifest, BUNDLE_MANIFEST_FILE_NAME, ScanError, acquisition_staging_root,
        bundle_dir_for, filesystem::FilesystemConnector,
    },
    error::ApiError,
    events::{append_event, new_system_event},
    hot_plane,
    identity::ApplicationIdentity,
    ids::new_sync_queue_entry_id,
    inference::{ColbertRuntime, DenseEmbeddingBackend},
    model::{
        OperationStatus, ParseRunStatus, ParserCapabilityProfile, ProducerType, Provenance,
        SyncQueueEntry, SyncQueueState, SystemEventType,
    },
    parse::{
        bundle::{BUNDLE_DIR_NAME_PREFIX, BUNDLE_TEMP_DIR_SUFFIX, parse_staging_root},
        importer::{ImportedParseStatus, import_parser_bundle},
        pdf_worker, text_worker,
    },
    primitives::utc_now,
    projections::{chunk, dense, dense_cache::DenseCache, envelope, lexical, multivector, view},
    source::{corpus_relative_source, resolve_contained_source, resolve_source_reference},
    state::{
        CutoverRegistry, ExclusiveGate, FabricHealth, FabricSourceCounts, ShutdownSignal,
        SyncCycleStats, SyncHealth, acquire_model_call_gate_on,
    },
    util::{panic_payload_message, truncate_persisted_detail},
};

/// Looks up the row owning one coalescing key (UNIQUE source_key); state is
/// fetched so coalescing can log which prior state it superseded.
const SELECT_QUEUE_ROW_BY_SOURCE_KEY_SQL: &str = "
SELECT id, state FROM sync_queue WHERE source_key = ?1";

/// Inserts one fresh pending entry: no attempts yet, nothing coalesced,
/// created_at anchored at this first detection (spec §9.4 rule 1: lag stays
/// answerable from created_at).
const INSERT_QUEUE_ROW_SQL: &str = "
INSERT INTO sync_queue (
  id, source_key, source_system, native_uri, detected_at, reason, state,
  attempt_count, last_attempt_at, last_error, coalesced_count, created_at,
  operation_id
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, NULL, NULL, 0, ?7, ?8)";

/// Coalesces a new detection into the existing row for its source_key (spec
/// §9.4 rule 2: at most one pending change per source): detected_at advances
/// to the latest observation, the row returns to pending, and the coalesce
/// counter increments. attempt_count/last_attempt_at/last_error deliberately
/// stay — they are true history of the last attempt — while the state reset
/// is what re-pends a failed row: a NEW detection supersedes a failed
/// attempt (the spec's re-parse-on-new-state rule applied to acquisition).
/// operation_id uses COALESCE(new, existing): a queue-coupled detection (?4
/// = Some) links the row to its Operation, while an autonomous detection (?4 =
/// NULL) coalescing into a row that already carries an Operation must NOT drop
/// that link — the drain still owes that Operation a terminal transition.
const COALESCE_QUEUE_ROW_SQL: &str = "
UPDATE sync_queue
SET detected_at = ?2, reason = ?3, state = 'pending',
    coalesced_count = coalesced_count + 1,
    operation_id = COALESCE(?4, operation_id)
WHERE source_key = ?1";

/// Lists every claimable entry, oldest observation first, in every column of
/// the sync_queue table (the typed model mirrors this order). in_flight rows
/// are reclaimed alongside pending ones: the single scheduler thread drains
/// synchronously on the same thread that claims, so any in_flight row
/// observed at claim time is stale by construction (a crashed process or a
/// failed complete/fail write left it behind), never live work.
/// Deliberately uncapped (a documented exemption from the row-cap policy):
/// the coalescing invariant — at most one row per source_key, bounded by
/// corpus size — is the accepted bound for one drain pass.
const SELECT_PENDING_ENTRIES_SQL: &str = "
SELECT id, source_key, source_system, native_uri, detected_at, reason, state,
       attempt_count, last_attempt_at, last_error, coalesced_count, created_at
FROM sync_queue WHERE state IN ('pending', 'in_flight')
ORDER BY detected_at, id";

/// Marks one claimed entry in flight and counts the attempt.
const MARK_ENTRY_IN_FLIGHT_SQL: &str = "
UPDATE sync_queue
SET state = 'in_flight', attempt_count = attempt_count + 1, last_attempt_at = ?2
WHERE id = ?1";

/// Removes one drained entry; the audit trail of the work itself lives in
/// acquisition_records, never in queue residue.
const DELETE_COMPLETED_ENTRY_SQL: &str = "DELETE FROM sync_queue WHERE id = ?1";

/// Parks one entry as failed with its bounded error text; terminal until a
/// new detection re-pends it via COALESCE_QUEUE_ROW_SQL.
const MARK_ENTRY_FAILED_SQL: &str = "
UPDATE sync_queue SET state = 'failed', last_error = ?2 WHERE id = ?1";

/// The §34.6 Operation link of one queue row (NULL for autonomous detections;
/// set for queue-coupled HTTP-enqueued rows). Read at the drain boundaries that
/// own the coupled Operation's running→terminal transitions.
const SELECT_ENTRY_OPERATION_ID_SQL: &str = "
SELECT operation_id FROM sync_queue WHERE id = ?1";

/// The native URIs of the PENDING sync-queue entries that carry a linked
/// Operation, for one source system (Option A ruling, 2026-07-16). This set is
/// the prescreen override: `operation_id IS NOT NULL` is the STRUCTURAL
/// discriminator of an operator-enqueued request (`POST /sources`,
/// `POST /sources/{id}/parses`) — the only rows that need force-staging past
/// the unchanged-prescreen, because the autonomous full-scan detections
/// (`operation_id IS NULL`) are enqueued at staging time and already have their
/// bundles. Restricted to `state = 'pending'`: a failed or in_flight
/// operator row is not the current cycle's owed work. `native_uri IS NOT NULL`
/// is always true structurally; the filter is only on state and the operation
/// link.
const SELECT_PENDING_OPERATOR_URIS_SQL: &str = "
SELECT native_uri FROM sync_queue
WHERE source_system = ?1 AND state = 'pending' AND operation_id IS NOT NULL";

/// Backlog depth and coalesced totals grouped by state, for health
/// (spec §9.4 rule 1, §9.5 health visibility).
const SELECT_QUEUE_DEPTHS_SQL: &str = "
SELECT state, COUNT(*), COALESCE(SUM(coalesced_count), 0)
FROM sync_queue GROUP BY state";

/// The authoritative stored mime type of one imported source object. Parse
/// routing reads THIS column — never a re-derivation from the URI — so the
/// router can never disagree with what the acquisition importer recorded
/// the content as.
const SELECT_SOURCE_MIME_TYPE_SQL: &str = "
SELECT mime_type FROM source_objects WHERE id = ?1";

/// C10b fabric diagnostic counts, computed inside the scheduler cycle on a
/// bounded read connection (the `queue_depths` health-inspection precedent) and
/// published diagnostic-only into the fabric health slot. Each is a single
/// bounded aggregate; none opens work, none gates readiness.
///
/// Held candidates: `status='ready'` with a non-null `held_reason` (§13.4).
const SELECT_HELD_COUNT_SQL: &str = "
SELECT COUNT(*) FROM parse_runs WHERE status = 'ready' AND held_reason IS NOT NULL";

/// Stuck-building: crash-orphaned import wreckage the drain deliberately leaves
/// `building` (§13.5). The single scheduler thread completes every build inline,
/// so any `building` row is stale wreckage, not live work.
const SELECT_STUCK_BUILDING_COUNT_SQL: &str = "
SELECT COUNT(*) FROM parse_runs WHERE status = 'building'";

/// Verification-halted: runs retained in `archiving` because a snapshot
/// verification gate failed and halted without auto-retry (§30.5).
const SELECT_VERIFICATION_HALTED_COUNT_SQL: &str = "
SELECT COUNT(*) FROM parse_runs WHERE status = 'archiving'";

/// Serving-stale (failed-parse half): parse runs in the terminal `failed`
/// disposition. Combined with the queue-backlog half below (§13.5): content is
/// still served while a fresh parse is owed.
const SELECT_FAILED_PARSE_COUNT_SQL: &str = "
SELECT COUNT(*) FROM parse_runs WHERE status = 'failed'";

/// Serving-stale (queue-backlog half): sync_queue rows for this source_system
/// that are detected but not yet drained to activation (pending, in_flight, or
/// failed). Completed rows are deleted, so any remaining row is owed work.
const SELECT_QUEUE_BACKLOG_COUNT_SQL: &str = "
SELECT COUNT(*) FROM sync_queue
WHERE source_system = ?1 AND state IN ('pending', 'in_flight', 'failed')";

/// Access-lost locations for this source_system: `status='access_lost'` (§11.2
/// lost-access — the source was unreadable, which asserts nothing about whether
/// its items still exist, distinct from deletion).
const SELECT_ACCESS_LOST_COUNT_SQL: &str = "
SELECT COUNT(*) FROM source_locations
WHERE source_system = ?1 AND status = 'access_lost'";

/// Unparseable-MIME: imported, still-active source objects whose stored
/// authoritative mime type has no registered parser route (neither PDF nor
/// plain text). The durable, honest measure of the no-parser dispatch outcome
/// (§13.5) — the routing itself is warn-only, but the source object it left
/// unparsed persists with its unroutable mime_type.
const SELECT_UNPARSEABLE_MIME_COUNT_SQL: &str = "
SELECT COUNT(*) FROM source_objects
WHERE deactivated_at IS NULL AND mime_type NOT IN (?1, ?2)";

/// Every prior ParseRun matching one (source, parser identity/configuration)
/// tuple — the spec §13.5 rule 5 no-blind-retry key. source_id is 1:1 with
/// source_hash (spec §10 dedup), so a match means identical bytes through an
/// identical parser, which fails (or succeeds) identically. Ordered so the
/// guard decision is deterministic when defensive drift leaves multiple
/// matches.
const SELECT_PARSE_RUNS_BY_IDENTITY_SQL: &str = "
SELECT id, status, held_reason FROM parse_runs
WHERE source_id = ?1 AND parser_name = ?2 AND parser_version = ?3
  AND parser_config_hash = ?4
ORDER BY created_at, id";

/// `reason` recorded on entries enqueued because a full scan staged a bundle
/// for a new or changed item.
const REASON_STAGED_BY_FULL_SCAN: &str = "staged_by_full_scan";

/// Multiplicative cadence growth after a cycle that observed no changes:
/// quiet sources are sampled progressively more rarely (spec §9.5 observed
/// change frequency). A code constant, never config (§9.5 is knob-free).
const QUIET_CYCLE_GROWTH: f64 = 1.5;

/// Multiplicative cadence growth while the queue is non-empty at cycle end:
/// the pipeline is not draining as fast as detection produces work, so
/// sampling slows and coalescing sheds load (spec §9.5 pipeline
/// backpressure).
const BACKPRESSURE_GROWTH: f64 = 2.0;

/// Multiplicative cadence growth after a failed cycle: errors throttle from
/// the source side (spec §9.5 source-system pushback) and keep the scheduler
/// from crash-looping against a broken dependency.
const ERROR_CYCLE_GROWTH: f64 = 2.0;

/// Base retry delay when a cycle fails before any scan has established a
/// measured cadence; the first successful scan's duration replaces it.
const ERROR_RETRY_BASE_MS: u64 = 1_000;

/// Weight of the newest observation in the inter-change-interval EMA: one
/// third new keeps the estimate responsive to real churn shifts without
/// whipsawing on one busy cycle.
const INTER_CHANGE_EMA_WEIGHT: f64 = 0.3;

/// Log-event namespace this module passes to the shared hot-plane
/// transaction helpers, so boundary logs stay attributable to the scheduler.
const TX_LOG_NAMESPACE: &str = "scheduler";

/// Model-gate identity for the dense-embedding batch of the projection build.
/// Kept identical to `dense::DENSE_MODEL_ROLE`/`DENSE_CALL_PURPOSE` (private
/// there) so the `model_gate.*` events this thread emits carry the same
/// role/purpose the dense builder's own boundary logs use.
const DENSE_MODEL_ROLE: &str = "dense";
const DENSE_CALL_PURPOSE: &str = "passage_embedding";

/// Model-gate identity for the ColBERT multi-vector batch of the projection
/// build; the colbert role is acquired separately from and never overlaps the
/// dense role (see `build_projection_transaction`).
const COLBERT_MODEL_ROLE: &str = "colbert";
const COLBERT_CALL_PURPOSE: &str = "document_embedding";

/// Producer identity stamped on the durable projection-build failure-audit
/// envelope (see `record_projection_build_failure`). Bumped only if the audit
/// semantics change, so a version change is a visible signal.
const PROJECTION_BUILD_PRODUCER_NAME: &str = "fabric-projection-build";
const PROJECTION_BUILD_PRODUCER_VERSION: &str = "1";

/// Queue backlog measured by state plus the coalesced-detection total, the
/// operator's answer to "what is pending, in flight, failed, and how much
/// was coalesced" (spec §9.4 rule 1).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QueueDepths {
    pub(crate) pending: u64,
    pub(crate) in_flight: u64,
    pub(crate) failed: u64,
    pub(crate) coalesced_total: u64,
}

/// Parser worker a routed mime type resolves to (spec §12: one worker per
/// supported input class).
#[derive(Debug, Clone, Copy)]
enum ParseRoute {
    Pdf,
    PlainText,
}

/// The inference and cache handles the drain-loop parse chain needs to build
/// the content-derived retrieval projections (C6). Threaded from `main` through
/// `scheduler::start` so the scheduler thread never constructs inference or a
/// gate of its own.
///
/// Ownership rule: the `gate` here is a CLONE of the SAME `Arc<ExclusiveGate>`
/// AppState holds (`AppState::model_call_gate_handle`), so local model calls on
/// this thread and on the HTTP path serialize on ONE process-global gate — the
/// §1.5 invariant. The `dense`/`colbert` runtimes are cheap `Clone` handles over
/// shared model state; the `dense_dimension`/`colbert_dimension` are the
/// config-declared expected widths (`config.models.{dense,colbert}.dimension`),
/// validated against every produced vector by the builders. `dense_cache` is the
/// shared active dense cache the activation wiring publishes into under the
/// cutover barrier (a SECOND clone belongs in AppState for the C7 query path —
/// see the C7 seam note at the construction site).
pub(crate) struct ProjectionRuntime {
    pub(crate) dense: DenseEmbeddingBackend,
    pub(crate) colbert: ColbertRuntime,
    pub(crate) dense_dimension: usize,
    pub(crate) colbert_dimension: usize,
    /// Shared process-global model-call serializer (same instance AppState
    /// uses); acquired via `acquire_model_call_gate_on` so acquisition logging
    /// matches the HTTP path byte-for-byte.
    pub(crate) gate: Arc<ExclusiveGate>,
    pub(crate) dense_cache: Arc<DenseCache>,
}

/// What the parse-chain PREFIX (`parse_chain_prefix`: route → guard →
/// containment → identity check → worker → import) needs — deliberately free of
/// any projection-build, gate, or snapshot handle. `storage` re-packages the
/// exact corpus/index roots main read from config.storage — no new configuration
/// is introduced — because `crate::source::resolve_source_reference` takes the
/// typed config shape.
///
/// OWNERSHIP BOUNDARY (CA2-P5): this split exists because the annotation
/// dry-run pass runs the prefix WITHOUT inference — a `ProjectionRuntime`'s
/// dense/colbert handles are constructible only from an initialized
/// `InferenceRuntime`, which the dry-run mode never initializes by design. The
/// prefix must therefore never grow a projection/gate dependency; anything the
/// gate continuation needs belongs on `ParseDispatchContext` instead.
struct ParsePrefixContext {
    storage: StorageConfig,
    docling: DoclingConfig,
}

/// Everything the FULL drain-loop parse chain needs beyond the queue entry
/// itself, grouped so `run_cycle` keeps a short signature: the prefix inputs
/// plus the gate continuation's handles. `projections` carries the
/// inference/cache handles the post-import, pre-activation content-derived
/// build step consumes — consumed ONLY by `gate_ready_parse`, never by the
/// prefix (see `ParsePrefixContext`'s ownership boundary).
struct ParseDispatchContext {
    prefix: ParsePrefixContext,
    registry: Arc<CutoverRegistry>,
    projections: ProjectionRuntime,
    /// The §30.2 application identity captured once at startup and threaded to
    /// every snapshot-minting site on the parse chain and the deletion path
    /// (2026-07-16 ruling — explicit threading, never a global).
    identity: ApplicationIdentity,
}

/// Recorded outcome of one entry's parse chain. Every arm completes the
/// queue entry (nothing-to-parse and recorded parse failures are outcomes,
/// not faults); infrastructure faults surface as `Err` from
/// `dispatch_parse_chain` instead and park the entry failed.
enum ParseChainOutcome {
    /// Nothing to parse or nothing to redo: no parser for the stored mime,
    /// or the §13.5 no-blind-retry guard matched a prior run.
    Skipped,
    /// A ready run was gated (activated or held) — freshly imported, or the
    /// crash-recovery gate of a pre-existing ready run.
    Gated,
    /// The parse failed as a recorded outcome (durable failed run row);
    /// counted as a cycle failure.
    ParseFailed,
}

/// Decision of the spec §13.5 rule 5 no-blind-retry guard over the prior
/// runs of one (source, parser identity) tuple.
enum NoRetryGuardDecision {
    /// No prior run of this identity: dispatch the worker.
    Dispatch,
    /// Only stale `building` wreckage exists: surface it and dispatch anyway
    /// so one crash cannot permanently block the source.
    DispatchOverStaleBuilding { stale_run_id: String },
    /// A ready, un-held run already exists: skip the worker and gate that
    /// run (crash-recovery idempotence for an import that committed ready
    /// but was never gated).
    GateExisting { parse_run_id: String },
    /// A prior run makes re-parsing pointless or forbidden; `reason` is the
    /// compact operator-facing label logged with the skip.
    Skip {
        parse_run_id: String,
        reason: &'static str,
    },
}

/// Outcome of the parse-chain PREFIX — route → no-retry guard → containment →
/// content-identity → worker → importer — up to (and including) the point where
/// the importer returns a committed parse run, but BEFORE any content-derived
/// projection build, snapshot, or activation gate. Both the normal
/// `dispatch_parse_chain` and the CA2-P5 annotation dry-run pass share this
/// prefix (see `parse_chain_prefix`); the normal path continues to build/gate,
/// the dry-run path truncates here (mechanic 1: leave the ready row un-held for
/// the next normal cycle's §13.5 GateExisting adoption). `bundle_dir` is carried
/// only for `FreshReady` because the fresh path is the only arm that owns a
/// consumable parser bundle to remove after gating (the GateExisting arm's
/// bundle was already removed by the import that first committed the ready run).
enum ParseChainPrefix {
    /// Nothing to parse, guard skip, or content-changed skip: terminal for this
    /// entry, no ready run produced.
    Skipped,
    /// A recorded parse failure (durable failed run row) — counted as a cycle
    /// failure by the caller.
    ParseFailed,
    /// The §13.5 guard matched a pre-existing ready, un-held run: gate THAT run
    /// (crash-recovery idempotence). No fresh worker ran; no bundle to remove.
    GateExisting { parse_run_id: String },
    /// A fresh worker produced a ready run the importer just committed. `source_id`
    /// is carried alongside so the dry-run pass can record the (source, parse)
    /// pair without a second read; `bundle_dir` is the consumed parser bundle the
    /// normal gate path removes after activation.
    FreshReady {
        parse_run_id: String,
        source_id: String,
        bundle_dir: PathBuf,
    },
}

/// Compose the UNIQUE coalescing key of one queue row: connector-scoped
/// source identity per the schema §9.4 contract.
fn source_key(source_system: &str, native_uri: &str) -> String {
    format!("{source_system}:{native_uri}")
}

/// Enqueue one detected change with latest-state coalescing (spec §9.4 rule
/// 2): an existing row for the source_key — in ANY state — absorbs the
/// detection (advancing detected_at, re-pending the row); otherwise a fresh
/// pending row is inserted. No system event is emitted: queue state is
/// operational, and the audit trail is the acquisition records the drain
/// produces.
/// `operation_id` links a queue-coupled (HTTP-enqueued) row to its §34.6
/// Operation so the drain can complete that Operation when the work finishes;
/// the autonomous pipeline's own detections carry `None` (no Operation). It is
/// persisted into `sync_queue.operation_id` inside this enqueue transaction (see
/// INSERT/COALESCE SQL: a fresh row stores it, a coalesced row keeps whichever
/// of the new-or-existing link is non-null).
pub(crate) fn enqueue_coalesced(
    index_root: &Path,
    source_system: &str,
    native_uri: &str,
    reason: &str,
    operation_id: Option<&str>,
) -> Result<(), ApiError> {
    let key = source_key(source_system, native_uri);
    let now = utc_now()?;

    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "enqueue_coalesced")?;
    let body = (|| -> Result<(), ApiError> {
        let existing: Option<(String, String)> = tx
            .query_row(SELECT_QUEUE_ROW_BY_SOURCE_KEY_SQL, params![key], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to look up sync queue row for {key}: {source}"),
            })?;

        match existing {
            Some((entry_id, prior_state)) => {
                tx.execute(
                    COALESCE_QUEUE_ROW_SQL,
                    params![key, now, reason, operation_id],
                )
                .map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "failed to coalesce sync queue entry {entry_id} ({key}): {source}"
                    ),
                })?;
                info!(
                    event = "scheduler.queue_coalesced",
                    entry_id,
                    source_system,
                    native_uri,
                    prior_state,
                    reason,
                    operation_id = operation_id.unwrap_or("none"),
                    "detection coalesced into existing sync queue entry"
                );
            }
            None => {
                let entry_id = new_sync_queue_entry_id()?;
                tx.execute(
                    INSERT_QUEUE_ROW_SQL,
                    params![
                        entry_id,
                        key,
                        source_system,
                        native_uri,
                        now,
                        reason,
                        now,
                        operation_id
                    ],
                )
                .map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "failed to insert sync queue entry {entry_id} ({key}): {source}"
                    ),
                })?;
                info!(
                    event = "scheduler.queue_enqueued",
                    entry_id,
                    source_system,
                    native_uri,
                    reason,
                    operation_id = operation_id.unwrap_or("none"),
                    "new sync queue entry enqueued"
                );
            }
        }
        Ok(())
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "enqueue_coalesced",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "enqueue_coalesced")?;
    // The preceding enqueue/coalesce record describes the write attempt;
    // confirm durability here while generic transaction mechanics use DEBUG.
    info!(
        event = "scheduler.queue_committed",
        source_system,
        native_uri,
        reason,
        operation_id = operation_id.unwrap_or("none"),
        "sync queue submission committed"
    );
    Ok(())
}

/// Claim every pending entry — plus every stale in_flight entry — for one
/// drain pass: each is marked in_flight with the attempt counted, all in one
/// transaction, and returned already reflecting its post-claim durable state
/// (state, attempt_count, and last_attempt_at match what was committed).
/// Reclaiming in_flight rows is safe because the single scheduler thread
/// drains synchronously on the thread that claims, so an in_flight row seen
/// here is stale by construction. A reclaimed row's bundle is still present
/// (consumed bundles are removed only after complete() succeeds), so the
/// replay re-imports idempotently and re-runs the parse chain; if the bundle
/// is nevertheless gone (crash mid-removal), the importer's missing-bundle
/// rejection parks the row failed, unlatching backpressure.
pub(crate) fn claim_pending(index_root: &Path) -> Result<Vec<SyncQueueEntry>, ApiError> {
    let now = utc_now()?;

    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "claim_pending")?;
    let body = (|| -> Result<Vec<SyncQueueEntry>, ApiError> {
        let mut entries = select_pending_entries(&tx)?;
        for entry in &mut entries {
            tx.execute(MARK_ENTRY_IN_FLIGHT_SQL, params![entry.id, now])
                .map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "failed to mark sync queue entry {} in flight: {source}",
                        entry.id
                    ),
                })?;
            // Mirror the UPDATE into the returned value so callers hold the
            // committed row state, not the pre-claim snapshot.
            entry.state = SyncQueueState::InFlight;
            entry.attempt_count += 1;
            entry.last_attempt_at = Some(now.clone());
        }
        Ok(entries)
    })();
    let entries = match body {
        Ok(entries) => entries,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "claim_pending",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "claim_pending")?;

    debug!(
        event = "scheduler.queue_claimed",
        claimed = entries.len(),
        "pending (and stale in-flight) sync queue entries claimed for drain"
    );
    Ok(entries)
}

/// Complete one drained entry by deleting its row: the work's durable record
/// is the acquisition record the import wrote, so drained work leaves no
/// queue residue.
///
/// QUEUE-COUPLED OPERATION CONTRACT (Escalation 1): when the row carries an
/// `operation_id`, the C10a HTTP handler wrote ONLY the `pending` Operation row;
/// the drain owns the full running→terminal lifecycle for queue-coupled
/// operation types. `mark_running` was issued at drain dispatch, so the store's
/// status-guarded `running → succeeded` transition here fails loudly if that
/// dispatch step was skipped. The operation_id is read from the row BEFORE the
/// DELETE (it is gone afterward), and `mark_succeeded` runs AFTER the row is
/// removed — the queue unit of work is finished at that point.
pub(crate) fn complete(index_root: &Path, entry_id: &str) -> Result<(), ApiError> {
    let connection = hot_plane::open_write(index_root)?;
    let operation_id: Option<String> = connection
        .query_row(SELECT_ENTRY_OPERATION_ID_SQL, params![entry_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read operation link for completed sync queue entry {entry_id}: {source}"
            ),
        })?
        .flatten();
    let removed = connection
        .execute(DELETE_COMPLETED_ENTRY_SQL, params![entry_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to delete completed sync queue entry {entry_id}: {source}"),
        })?;
    // A missing row means the entry vanished between claim and complete —
    // impossible under the single-scheduler design, so it is worth a warning
    // rather than silence.
    if removed == 0 {
        warn!(
            event = "scheduler.queue_complete_missing",
            entry_id, "completed sync queue entry was already gone"
        );
    } else {
        info!(
            event = "scheduler.queue_completed",
            entry_id,
            operation_id = operation_id.as_deref().unwrap_or("none"),
            "sync queue entry drained and removed"
        );
    }
    // Queue-coupled operation reaches its terminal success only after the queue
    // row is gone. Drops through cleanly for autonomous rows (operation_id None).
    if let Some(operation_id) = &operation_id {
        crate::operations::mark_succeeded(index_root, operation_id)?;
    }
    Ok(())
}

/// Park one entry as failed with bounded error text. Failed entries are NOT
/// retried by the scheduler — deterministic failure on identical input fails
/// identically (spec §13.5 applied to acquisition) — and stay visible in
/// queue depths until a new detection re-pends them via `enqueue_coalesced`.
///
/// QUEUE-COUPLED OPERATION CONTRACT (Escalation 1): when the row carries an
/// `operation_id`, the paired Operation is driven to its terminal `failed`
/// state here so a queue-coupled failure never leaves a poll target stuck at
/// pending/running. `operations::mark_failed` is guarded `running → failed`, so
/// this first ensures the operation is `running`: a fail reached BEFORE drain
/// dispatch (e.g. a malformed import, before `mark_operation_running_at_dispatch`
/// ran) leaves it `pending`, and this flips it running → failed so it still
/// reaches a terminal record. `mark_failed` bounds the detail itself, so the raw
/// `error_detail` is passed through.
pub(crate) fn fail(index_root: &Path, entry_id: &str, error_detail: &str) -> Result<(), ApiError> {
    let bounded = truncate_persisted_detail(error_detail);
    let operation_id = entry_operation_id(index_root, entry_id)?;
    let connection = hot_plane::open_write(index_root)?;
    let updated = connection
        .execute(MARK_ENTRY_FAILED_SQL, params![entry_id, bounded])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark sync queue entry {entry_id} failed: {source}"),
        })?;
    drop(connection);
    if updated == 0 {
        warn!(
            event = "scheduler.queue_fail_missing",
            entry_id, "failed sync queue entry was already gone"
        );
    } else {
        warn!(
            event = "scheduler.queue_failed",
            entry_id,
            error = %bounded,
            operation_id = operation_id.as_deref().unwrap_or("none"),
            "sync queue entry parked as failed; a new detection will re-pend it"
        );
    }
    // Drive the paired Operation to its terminal `failed` state. The
    // `running → failed` guard means a still-`pending` operation (fail before
    // drain dispatch) must first be flipped running; an already-terminal one is
    // left as-is (a re-fail of the same entry must not double-transition).
    if let Some(operation_id) = &operation_id {
        drive_operation_failed(index_root, operation_id, error_detail)?;
    }
    Ok(())
}

/// Move a queue-coupled Operation to `failed`, first flipping it `running` if it
/// is still `pending` (a pre-dispatch fail), so the `running → failed` guard is
/// satisfied. A missing operation row (queue row named an absent operation) or a
/// terminal one is a broken/idempotent case handled without a loud panic: a
/// terminal operation is left untouched; an absent one surfaces loudly.
fn drive_operation_failed(
    index_root: &Path,
    operation_id: &str,
    error_detail: &str,
) -> Result<(), ApiError> {
    let Some(operation) = crate::operations::get(index_root, operation_id)? else {
        return Err(ApiError::StorageOperation {
            message: format!(
                "sync queue fail links operation {operation_id}, but that operation row is absent"
            ),
        });
    };
    match operation.status {
        OperationStatus::Pending => {
            crate::operations::mark_running(index_root, operation_id)?;
            crate::operations::mark_failed(index_root, operation_id, error_detail)?;
        }
        OperationStatus::Running => {
            crate::operations::mark_failed(index_root, operation_id, error_detail)?;
        }
        // Already terminal: leave it (re-park of the same entry, or a race the
        // single-scheduler design forbids — either way, no double-transition).
        OperationStatus::Succeeded | OperationStatus::Failed => {}
    }
    Ok(())
}

/// Read one claimed entry's §34.6 Operation link (NULL for autonomous rows) on
/// a read-only connection. Called at drain dispatch so the drain can flip the
/// coupled Operation `pending → running` before it does the entry's work — the
/// C10a HTTP handler wrote only `pending`, and the drain owns the rest of the
/// lifecycle for queue-coupled operation types.
fn entry_operation_id(index_root: &Path, entry_id: &str) -> Result<Option<String>, ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    let operation_id: Option<String> = connection
        .query_row(SELECT_ENTRY_OPERATION_ID_SQL, params![entry_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read operation link for sync queue entry {entry_id}: {source}"
            ),
        })?
        .flatten();
    Ok(operation_id)
}

/// Load the prescreen override set (Option A ruling, 2026-07-16): the
/// `native_uri`s of the PENDING sync-queue entries that carry a linked Operation
/// for `source_system`. These are exactly the operator-enqueued requests
/// (`POST /sources`, `POST /sources/{id}/parses`) — see
/// SELECT_PENDING_OPERATOR_URIS_SQL for why `operation_id IS NOT NULL` is the
/// structural discriminator and why autonomous entries need no override. The
/// caller subtracts this set from the prescreen `known` state so the connector
/// force-stages these files unconditionally: their whole purpose (a parser
/// rollout over unchanged content, §34.2) is defeated if the unchanged-prescreen
/// skips them and the drain never gets a bundle. Read-only connection; one
/// SELECT needs no transaction.
fn pending_operator_override_uris(
    index_root: &Path,
    source_system: &str,
) -> Result<BTreeSet<String>, ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    let mut statement = connection
        .prepare(SELECT_PENDING_OPERATOR_URIS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare pending operator override query: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to query pending operator override URIs of {source_system}: {source}"
            ),
        })?;
    let mut uris = BTreeSet::new();
    for row in rows {
        let native_uri = row.map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read pending operator override row of {source_system}: {source}"
            ),
        })?;
        uris.insert(native_uri);
    }
    Ok(uris)
}

/// Flip a queue-coupled entry's Operation `pending → running` at drain dispatch.
/// The C10a HTTP handler wrote ONLY the `pending` row; from here the drain owns
/// the lifecycle, so `mark_running` MUST run before ANY terminal path (complete
/// or fail) — the store's `running → succeeded`/`running → failed` transitions
/// are status-guarded and fail loudly on a still-`pending` row otherwise.
///
/// Crash-replay tolerance: a reclaimed stale in_flight row may already have been
/// marked running by the drain that died mid-entry. `mark_running` is guarded
/// `pending → running` and would fail loudly on the replay, so this checks the
/// current status first and only transitions a still-`pending` operation; an
/// already-running one is left as-is (the replay owns its terminal transition).
/// Autonomous rows (operation_id None) are a no-op.
fn mark_operation_running_at_dispatch(index_root: &Path, entry_id: &str) -> Result<(), ApiError> {
    let Some(operation_id) = entry_operation_id(index_root, entry_id)? else {
        return Ok(());
    };
    let Some(operation) = crate::operations::get(index_root, &operation_id)? else {
        // The linked operation row is gone though the queue row named it — a
        // broken invariant worth surfacing loudly rather than silently draining.
        return Err(ApiError::StorageOperation {
            message: format!(
                "sync queue entry {entry_id} links operation {operation_id}, but that operation \
                 row is absent"
            ),
        });
    };
    match operation.status {
        OperationStatus::Pending => {
            crate::operations::mark_running(index_root, &operation_id)?;
            info!(
                event = "scheduler.operation_running",
                entry_id,
                operation_id = operation_id.as_str(),
                "queue-coupled operation marked running at drain dispatch"
            );
        }
        // Already running on a crash replay: leave it; this drain still owns the
        // terminal transition via complete()/fail().
        OperationStatus::Running => {}
        // A terminal operation whose queue row is still present is a broken
        // invariant (the drain deletes/parks the row when it terminates the op).
        OperationStatus::Succeeded | OperationStatus::Failed => {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "sync queue entry {entry_id} links operation {operation_id} already in a \
                     terminal state; the queue row should have been removed or parked"
                ),
            });
        }
    }
    Ok(())
}

/// Measure queue backlog by state plus the coalesced total, on a read-only
/// connection (health inspection must never contend as a writer).
pub(crate) fn queue_depths(index_root: &Path) -> Result<QueueDepths, ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    let mut statement = connection
        .prepare(SELECT_QUEUE_DEPTHS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare sync queue depth query: {source}"),
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query sync queue depths: {source}"),
        })?;

    let mut depths = QueueDepths::default();
    for row in rows {
        let (state, count, coalesced) = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read sync queue depth row: {source}"),
        })?;
        let count = queue_count(count, "sync queue depth")?;
        // The state column is constrained by the schema CHECK; parsing it
        // through the model enum keeps this match exhaustive against the
        // one source of truth for the value set.
        match queue_state_from_wire(&state)? {
            SyncQueueState::Pending => depths.pending = count,
            SyncQueueState::InFlight => depths.in_flight = count,
            SyncQueueState::Failed => depths.failed = count,
        }
        depths.coalesced_total += queue_count(coalesced, "sync queue coalesced total")?;
    }
    Ok(depths)
}

/// Measure the C10b fabric diagnostic counts for one source_system on a bounded
/// read connection (the `queue_depths` health-inspection precedent: health reads
/// must never contend as a writer). Runs INSIDE the scheduler cycle, which
/// already owns read connections — the health handler never computes these
/// (invariant 4). Each count is a single bounded aggregate.
///
/// KEYING (plan resolution 6): the result is attributed to `source_system`. The
/// location/queue counts are genuinely per-source-system (those tables carry the
/// column); the parse-run and source-object counts (`held`, `stuck_building`,
/// `verification_halted`, the failed-parse half of `serving_stale`,
/// `unparseable_mime`) are corpus-global today because `parse_runs` and
/// `source_objects` carry no source_system column, and are attributed to the one
/// operating source_system at MVP. The publish shape is still a per-source-system
/// map, so a second source-system would slot in without a shape change.
fn fabric_counts(index_root: &Path, source_system: &str) -> Result<FabricSourceCounts, ApiError> {
    let connection = hot_plane::open_read(index_root)?;

    let held = read_scalar_count(&connection, SELECT_HELD_COUNT_SQL, params![], "held")?;
    let stuck_building = read_scalar_count(
        &connection,
        SELECT_STUCK_BUILDING_COUNT_SQL,
        params![],
        "stuck-building",
    )?;
    let verification_halted = read_scalar_count(
        &connection,
        SELECT_VERIFICATION_HALTED_COUNT_SQL,
        params![],
        "verification-halted",
    )?;
    let failed_parse = read_scalar_count(
        &connection,
        SELECT_FAILED_PARSE_COUNT_SQL,
        params![],
        "failed-parse",
    )?;
    let queue_backlog = read_scalar_count(
        &connection,
        SELECT_QUEUE_BACKLOG_COUNT_SQL,
        params![source_system],
        "queue-backlog",
    )?;
    let access_lost = read_scalar_count(
        &connection,
        SELECT_ACCESS_LOST_COUNT_SQL,
        params![source_system],
        "access-lost",
    )?;
    let unparseable_mime = read_scalar_count(
        &connection,
        SELECT_UNPARSEABLE_MIME_COUNT_SQL,
        params![MIME_TYPE_PDF, MIME_TYPE_PLAIN_TEXT],
        "unparseable-mime",
    )?;

    Ok(FabricSourceCounts {
        held,
        // Serving-stale is the failed-parse backlog plus the undrained queue
        // backlog: both mean content is served while a fresh parse is owed.
        serving_stale: failed_parse + queue_backlog,
        access_lost,
        stuck_building,
        unparseable_mime,
        verification_halted,
    })
}

/// Measure the fabric diagnostic counts for the operating source_system and
/// publish them into the shared slot with a fresh as-of timestamp (C10b). Called
/// once per cycle by the scheduler thread (the owning thread — invariant 4). A
/// read failure (or a clock failure for the as-of stamp) is logged and the slot
/// is left at its last-published value: a stale-but-marked snapshot is more
/// honest than a partial or timestamp-less overwrite, and this diagnostic must
/// never disturb the sync summary or the adaptive cadence.
fn publish_cycle_fabric_health(
    index_root: &Path,
    source_system: &str,
    fabric_slot: &Mutex<FabricHealth>,
) {
    let counts = match fabric_counts(index_root, source_system) {
        Ok(counts) => counts,
        Err(source) => {
            error!(
                event = "scheduler.fabric_counts_failed",
                error = %source,
                "fabric diagnostic count read failed; fabric health left at last-known values"
            );
            return;
        }
    };
    // Every count carries the measuring cycle's as-of (invariant 2). A clock
    // failure loses this publish visibly rather than stamping a guessed time.
    let measured_at = match utc_now() {
        Ok(now) => now,
        Err(source) => {
            error!(
                event = "scheduler.fabric_counts_timestamp_failed",
                error = %source,
                "failed to format fabric-count as-of timestamp; fabric health not republished"
            );
            return;
        }
    };
    let mut snapshot = FabricHealth {
        by_source_system: std::collections::HashMap::new(),
        measured_at: Some(measured_at),
    };
    // Per-source-system map keyed by the operating source_system (plan
    // resolution 6): one entry at MVP, but the shape admits more without change.
    snapshot
        .by_source_system
        .insert(source_system.to_string(), counts);
    publish_fabric_health(fabric_slot, &snapshot);
}

/// Run one bounded scalar `COUNT(*)` health-inspection query and return it as a
/// non-negative u64. A negative counter is treated as row corruption (via
/// `queue_count`) rather than silently wrapped.
fn read_scalar_count(
    connection: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
    what: &'static str,
) -> Result<u64, ApiError> {
    let value: i64 = connection
        .query_row(sql, params, |row| row.get(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read {what} fabric count: {source}"),
        })?;
    queue_count(value, "fabric health count").map_err(|_| ApiError::StorageOperation {
        message: format!("{what} fabric count {value} is negative; row is corrupt"),
    })
}

/// Spawn the sync scheduler thread: one std::thread running fabric
/// validation, then the adaptive scan/drain loop until shutdown. `docling`
/// and `registry` feed the drain-loop parse chain (C5c): the PDF worker's
/// configuration and the per-source cutover barriers activation swaps
/// behind. The caller (main) owns the JoinHandle and joins it after the
/// HTTP server exits so scheduler shutdown is observable.
// The parameter list is the thread's full startup input set (roots, connector
// identity, the C6 projection runtime, shutdown, and the health slot); grouping
// them into a struct would only rename the same fields, so the arity allow is
// clearer than a pass-through wrapper.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start(
    corpus_root: PathBuf,
    index_root: PathBuf,
    governance_domain: String,
    docling: DoclingConfig,
    registry: Arc<CutoverRegistry>,
    // The C6 content-derived build handles (inference runtimes, expected
    // dimensions, the shared model-call gate, the active dense cache). Owned by
    // main and moved in here so the scheduler thread never constructs inference
    // or a gate of its own — the gate must be the SAME process-global instance
    // AppState holds (see ProjectionRuntime's ownership rule).
    projections: ProjectionRuntime,
    // The §30.2 application identity captured once by main after config load and
    // moved into the scheduler thread (2026-07-16 ruling); threaded to every
    // snapshot-minting site rather than read from a global.
    identity: ApplicationIdentity,
    shutdown: Arc<ShutdownSignal>,
    maintenance: Arc<crate::maintenance::MaintenanceGate>,
    health_slot: Arc<Mutex<SyncHealth>>,
    // C10b diagnostic-only fabric-counts slot, published every cycle alongside
    // the readiness-critical sync summary. A separate slot from `health_slot`
    // so the two publish independently and neither gates the other.
    fabric_slot: Arc<Mutex<FabricHealth>>,
) -> Result<thread::JoinHandle<()>, ApiError> {
    thread::Builder::new()
        .name("sync-scheduler".to_string())
        .spawn(move || {
            // Panic containment (diagnostics: spawned tasks must leave
            // durable panic evidence): without this wrapper a scheduler
            // panic would unwind silently into the joining main thread. The
            // panic is logged, health is republished not-ready (the slot
            // publish recovers a poisoned lock), and the panic is NOT
            // resumed — the thread ends in the observed "panicked" state.
            let panic_slot = Arc::clone(&health_slot);
            let panic_fabric_slot = Arc::clone(&fabric_slot);
            let body = catch_unwind(AssertUnwindSafe(|| {
                run_scheduler(
                    corpus_root,
                    index_root,
                    governance_domain,
                    docling,
                    registry,
                    projections,
                    identity,
                    shutdown,
                    maintenance,
                    health_slot,
                    fabric_slot,
                )
            }));
            if let Err(payload) = body {
                let message = panic_payload_message(payload.as_ref());
                error!(
                    event = "scheduler.thread_panicked",
                    is_panic = true,
                    panic_message = %message,
                    "sync scheduler thread panicked"
                );
                // The panic health snapshot deliberately replaces the whole
                // slot: counters reset, but fabric_ready=false plus the
                // detail make the sync subsystem honestly unavailable.
                let mut health = SyncHealth::startup_pending();
                health.fabric_ready = false;
                health.detail = Some(format!("sync scheduler thread panicked: {message}"));
                publish_health(&panic_slot, &health);
                // The fabric diagnostic counts are stale once the publisher is
                // dead, so clear them to the empty (not-measured) default rather
                // than leave last-cycle numbers looking current after a panic.
                publish_fabric_health(&panic_fabric_slot, &FabricHealth::default());
                info!(
                    event = "scheduler.thread_stopped",
                    reason = "panicked",
                    "sync scheduler thread stopped"
                );
            }
        })
        .map_err(|source| ApiError::InternalIo {
            message: format!("failed to spawn sync scheduler thread: {source}"),
        })
}

/// Thread body: validate the fabric plane exactly once (the scheduler never
/// repairs or migrates schema and never crash-loops — a failed gate is a
/// clean, health-visible exit), sweep orphaned parse temp workspaces, then
/// run scan/drain cycles at the adaptive cadence until shutdown is
/// requested.
// Same startup input set as `start` (which just forwards it here); see the
// arity-allow rationale there.
#[allow(clippy::too_many_arguments)]
fn run_scheduler(
    corpus_root: PathBuf,
    index_root: PathBuf,
    governance_domain: String,
    docling: DoclingConfig,
    registry: Arc<CutoverRegistry>,
    projections: ProjectionRuntime,
    identity: ApplicationIdentity,
    shutdown: Arc<ShutdownSignal>,
    maintenance: Arc<crate::maintenance::MaintenanceGate>,
    health_slot: Arc<Mutex<SyncHealth>>,
    fabric_slot: Arc<Mutex<FabricHealth>>,
) {
    info!(
        event = "scheduler.thread_started",
        corpus_root = %corpus_root.display(),
        index_root = %index_root.display(),
        "sync scheduler thread started"
    );

    // The thread keeps its own snapshot and publishes whole copies, so the
    // slot always holds an internally consistent SyncHealth.
    let mut health = SyncHealth::startup_pending();

    // Startup validation and staging cleanup use storage too; an interrupted
    // rebuild must park this thread before either can inspect partial state.
    let startup_permit = match maintenance.worker_permit("scheduler", Duration::ZERO, 0, &shutdown)
    {
        Ok(Some(permit)) => permit,
        Ok(None) => {
            info!(
                event = "scheduler.thread_stopped",
                reason = "shutdown_requested",
                "sync scheduler stopped before storage validation"
            );
            return;
        }
        Err(source) => {
            error!(
                event = "scheduler.maintenance_wait_failed",
                stage = "startup",
                error = %source,
                "sync scheduler could not acquire storage admission"
            );
            health.detail = Some(format!("maintenance admission failed: {source}"));
            publish_health(&health_slot, &health);
            publish_fabric_health(&fabric_slot, &FabricHealth::default());
            info!(
                event = "scheduler.thread_stopped",
                reason = "maintenance_wait_failed",
                "sync scheduler thread stopped"
            );
            return;
        }
    };

    // Fabric-plane gate: the hot plane must exist with the exact expected
    // schema before any queue or acquisition work. Failure is terminal for
    // the thread, visible in health, and repaired only by the operator
    // setup path (--setup-storage), never at runtime.
    if let Err(source) = validate_fabric_plane(&index_root) {
        error!(
            event = "scheduler.fabric_validation_failed",
            index_root = %index_root.display(),
            error = %source,
            "fabric plane validation failed; sync scheduler will not run"
        );
        health.fabric_ready = false;
        health.detail = Some(format!("fabric plane validation failed: {source}"));
        publish_health(&health_slot, &health);
        info!(
            event = "scheduler.thread_stopped",
            reason = "fabric_validation_failed",
            "sync scheduler thread stopped"
        );
        return;
    }

    let staging_root = acquisition_staging_root(&index_root);
    // Connector construction validates the effective configuration; an
    // invalid shape is as terminal (and as health-visible) as a failed
    // fabric gate — the scheduler has nothing valid to drive.
    let connector = match FilesystemConnector::new(
        corpus_root.clone(),
        staging_root.clone(),
        governance_domain,
    ) {
        Ok(connector) => connector,
        Err(source) => {
            error!(
                event = "scheduler.connector_config_invalid",
                corpus_root = %corpus_root.display(),
                error = %source,
                "filesystem connector configuration invalid; sync scheduler will not run"
            );
            health.fabric_ready = false;
            health.detail = Some(format!(
                "filesystem connector configuration invalid: {source}"
            ));
            publish_health(&health_slot, &health);
            info!(
                event = "scheduler.thread_stopped",
                reason = "connector_config_invalid",
                "sync scheduler thread stopped"
            );
            return;
        }
    };
    // Connector identity for failure records and enumeration evidence.
    let context = AcquisitionContext {
        connector_name: connector.connector_name().to_string(),
        connector_version: connector.connector_version().to_string(),
        connector_config_hash: connector.connector_config_hash().to_string(),
        source_system: connector.source_system().to_string(),
        governance_domain: connector.governance_domain().to_string(),
    };
    // Enumeration scope recorded as the deletion-evidence anchor URI; the
    // corpus root is valid UTF-8 (connector construction verified it), so
    // display() is lossless here.
    let scope_uri = corpus_root.display().to_string();

    // The gate passed: the sync subsystem is operational from here on, even
    // when individual cycles fail (those surface via `detail`).
    health.fabric_ready = true;
    health.detail = None;
    publish_health(&health_slot, &health);
    info!(
        event = "scheduler.sync_operational",
        connector_name = connector.connector_name(),
        corpus_root = %corpus_root.display(),
        index_root = %index_root.display(),
        "fabric plane validated, connector configured, sync health published ready"
    );

    // Startup sweep of orphaned parse temp workspaces, before the first
    // cycle can start new workers (see the function docs for the
    // single-thread safety invariant).
    sweep_orphan_parse_temp_dirs(&index_root);

    // Parse-chain inputs threaded into every drain pass. The StorageConfig
    // is re-packaged from the exact roots main read out of config.storage
    // (see ParsePrefixContext docs) — no configuration is invented here.
    let dispatch = ParseDispatchContext {
        prefix: ParsePrefixContext {
            storage: StorageConfig {
                corpus_root,
                index_root: index_root.clone(),
            },
            docling,
        },
        registry,
        projections,
        identity,
    };

    let mut generation = startup_permit.generation();
    drop(startup_permit);
    let mut cadence = CadenceState::new();
    let mut delay = Duration::ZERO;
    loop {
        // A permit covers the full cycle, including its final storage reads and
        // health publication, so no old-generation counters survive a reset.
        let permit = match maintenance.worker_permit("scheduler", delay, generation, &shutdown) {
            Ok(Some(permit)) => permit,
            Ok(None) => break,
            Err(source) => {
                error!(
                    event = "scheduler.maintenance_wait_failed",
                    stage = "cycle",
                    error = %source,
                    "sync scheduler could not acquire storage admission"
                );
                health.fabric_ready = false;
                health.detail = Some(format!("maintenance admission failed: {source}"));
                publish_health(&health_slot, &health);
                publish_fabric_health(&fabric_slot, &FabricHealth::default());
                info!(
                    event = "scheduler.thread_stopped",
                    reason = "maintenance_wait_failed",
                    "sync scheduler thread stopped"
                );
                return;
            }
        };
        if permit.generation() != generation {
            generation = permit.generation();
            cadence = CadenceState::new();
            health = SyncHealth::startup_pending();
            health.fabric_ready = true;
            health.detail = None;
            // Rebuilding is background work; do not keep readiness at startup
            // pending until an entire fresh-corpus cycle finishes.
            publish_health(&health_slot, &health);
        }
        let cycle_started = Instant::now();
        // Spacing between cycle STARTS is the churn-measurement basis; the
        // first cycle has no predecessor, so its own duration is the only
        // measurable spacing (used below via unwrap_or).
        let spacing_ms = cadence
            .last_cycle_started
            .map(|previous| cycle_started.duration_since(previous).as_millis() as u64);
        cadence.last_cycle_started = Some(cycle_started);

        let delay_ms = match run_cycle(
            &index_root,
            &staging_root,
            &connector,
            &context,
            &scope_uri,
            &dispatch,
        ) {
            Ok(outcome) => match queue_depths(&index_root) {
                Ok(depths) => {
                    let spacing_ms = spacing_ms.unwrap_or_else(|| outcome.stats.elapsed_ms.max(1));
                    // Backpressure means undrained work: pending plus
                    // in-flight. Failed rows are excluded — they are
                    // terminal until a new detection, so counting them
                    // would latch backpressure permanently.
                    let backlog = depths.pending + depths.in_flight;
                    let (delay_ms, transition) = cadence.adapt(
                        outcome.scan_elapsed_ms,
                        outcome.changes,
                        spacing_ms,
                        backlog,
                    );

                    // Backpressure transitions are durable audit events; an
                    // emission failure is surfaced in the log and in health
                    // detail rather than stopping the scheduler.
                    let mut detail = None;
                    if let Some(entered) = transition
                        && let Err(source) = emit_backpressure_event(
                            &index_root,
                            entered,
                            connector.source_system(),
                            &depths,
                            delay_ms,
                        )
                    {
                        error!(
                            event = "scheduler.backpressure_event_failed",
                            entered,
                            error = %source,
                            "failed to append sync backpressure event"
                        );
                        detail = Some(format!("backpressure event append failed: {source}"));
                    }

                    health.detail = detail;
                    health.pending = depths.pending;
                    health.in_flight = depths.in_flight;
                    health.failed = depths.failed;
                    health.coalesced_total = depths.coalesced_total;
                    health.last_cycle = Some(outcome.stats);
                    health.cadence_ms = Some(delay_ms);
                    // Achieved freshness is measured truth (spec §9.6): the
                    // timestamp is recorded only when the cycle actually
                    // succeeded, and a clock failure loses the update
                    // visibly instead of guessing.
                    match utc_now() {
                        Ok(now) => health.last_success_at = Some(now),
                        Err(source) => error!(
                            event = "scheduler.freshness_timestamp_failed",
                            error = %source,
                            "failed to format last-success timestamp"
                        ),
                    }
                    delay_ms
                }
                Err(source) => {
                    // The cycle itself SUCCEEDED; only the post-cycle queue
                    // depth read failed. Policy: record the cycle as
                    // successful (stats and achieved freshness advance),
                    // surface the read failure as its own distinct error
                    // and in health detail, leave the depth fields at their
                    // last-known values, and back the cadence off with the
                    // error growth — conservative, because the backlog
                    // signal that would justify a faster cadence is
                    // unavailable. No cycle-failed log is emitted: that
                    // would misattribute the failure.
                    error!(
                        event = "scheduler.queue_depth_read_failed",
                        error = %source,
                        "queue depth read failed after a successful cycle; \
                         depths held at last-known values"
                    );
                    health.detail = Some(format!("queue depth read failed: {source}"));
                    health.last_cycle = Some(outcome.stats);
                    // Achieved freshness still advances: the cycle succeeded
                    // (spec §9.6 measured truth); a clock failure loses the
                    // update visibly instead of guessing.
                    match utc_now() {
                        Ok(now) => health.last_success_at = Some(now),
                        Err(source) => error!(
                            event = "scheduler.freshness_timestamp_failed",
                            error = %source,
                            "failed to format last-success timestamp"
                        ),
                    }
                    let delay_ms = cadence.back_off_after_error();
                    health.cadence_ms = Some(delay_ms);
                    delay_ms
                }
            },
            Err(source) => handle_cycle_error(&mut cadence, &mut health, cycle_started, &source),
        };
        publish_health(&health_slot, &health);

        // C10b diagnostic-only fabric counts, published every cycle by this
        // owning thread (invariant 4: the health handler computes nothing).
        // Independent of cycle success — these are backlog/fault reads that stay
        // meaningful even after a failed cycle — so a read failure is logged and
        // the fabric slot is left at its last-published value rather than
        // overwritten with a guess, and it never affects the sync summary or the
        // adaptive cadence.
        publish_cycle_fabric_health(&index_root, connector.source_system(), &fabric_slot);

        // Idle without a permit so maintenance can drain; a new generation
        // bypasses this delay and starts with fresh cadence state next cycle.
        drop(permit);
        delay = Duration::from_millis(delay_ms);
    }

    info!(
        event = "scheduler.thread_stopped",
        reason = "shutdown_requested",
        "sync scheduler thread stopped cleanly"
    );
}

/// One ready parse the CA2-P5 dry-run pass surfaced for annotation sampling: the
/// (source, parse) pair whose parse run is `ready` (freshly imported this pass,
/// or a pre-existing un-held ready run the §13.5 GateExisting guard matched).
/// The dry-run driver builds each parse's invocation plan from this pair.
#[derive(Debug, Clone)]
pub(crate) struct DryRunReadyParse {
    pub(crate) source_id: String,
    pub(crate) parse_run_id: String,
}

/// Per-pass outcome of the CA2-P5 annotation dry-run scan/parse pass, for the
/// mode's summary log. `ready` carries each source's ready parse (annotation
/// sampling targets); the counts are the drain accounting. `claimed` is every
/// queue entry the pass processed once (the pending SELECT is uncapped).
#[derive(Debug, Clone, Default)]
pub(crate) struct DryRunPassOutcome {
    /// Ready parses surfaced this pass (freshly imported or GateExisting-matched).
    pub(crate) ready: Vec<DryRunReadyParse>,
    /// Queue entries claimed and processed this pass.
    pub(crate) claimed: u64,
    /// Entries whose prefix produced a ready run (== `ready.len()`, surfaced for
    /// the summary line).
    pub(crate) parsed_ready: u64,
    /// Entries the prefix skipped (no parser, guard skip, content changed).
    pub(crate) skipped: u64,
    /// Entries with a recorded parse failure (durable failed run row).
    pub(crate) parse_failed: u64,
    /// Entries that faulted on the canonical side (infrastructure error). The
    /// pass records and counts them, then continues — the row stays in_flight for
    /// normal-cycle reclamation, exactly like the crash-replay states.
    pub(crate) faulted: u64,
}

/// CA2-P5 annotation dry-run scan/parse pass (ruling 9). Runs INLINE on the
/// caller's thread — no scheduler thread is spawned — exactly ONE full
/// scan → acquire → parse pass over the whole corpus, with the fresh-dispatch
/// chain TRUNCATED immediately after the importer returns a ready run (mechanic
/// 1): the pass does NOT build content-derived projections, mint snapshots, run
/// the activation gate, complete/fail the queue entry, or remove the consumed
/// acquisition bundle. Every claimed row is left `in_flight` and every ready run
/// is left un-held (`held_reason` NULL) — the exact precondition the NEXT normal
/// start's §13.5 GateExisting arm adopts (reclaiming the `in_flight` row,
/// rebuilding projections from canonical rows, gating WITHOUT re-invoking
/// Docling). Docling is paid once here and never redone.
///
/// The full dispatch prefix runs unchanged, so every guard that must still run
/// does: the §13.5 no-retry guard, corpus containment, and the pre-worker
/// content-identity check (all inside `parse_chain_prefix`). Queue-coupled
/// Operations behave exactly as documented (mechanic 2): `mark_operation_running`
/// runs at dispatch and such an Operation sits at `running` until the adopting
/// normal start completes it — accepted, recovery-idempotent behavior; dispatch
/// is NOT special-cased.
///
/// Returns per-pass outcomes for the mode's summary log; `Err` is a cycle-wide
/// canonical-side fault (e.g. a failed plane gate or a source-side scan failure),
/// which the mode driver surfaces as a fatal.
///
/// Deliberately takes NO `ProjectionRuntime`, `CutoverRegistry`, or
/// `ApplicationIdentity`: those are gate-continuation inputs
/// (`gate_ready_parse` — projection build, activation, snapshots), and this
/// pass truncates before all of them. `ProjectionRuntime` in particular is
/// constructible only from an initialized `InferenceRuntime`, which the
/// dry-run mode never initializes by design — the prefix-scoped
/// `ParsePrefixContext` is what makes the mode wireable without inference.
pub(crate) fn run_annotation_dry_run_pass(
    corpus_root: PathBuf,
    index_root: PathBuf,
    governance_domain: String,
    docling: DoclingConfig,
) -> Result<DryRunPassOutcome, ApiError> {
    let started = Instant::now();
    info!(
        event = "scheduler.dry_run_pass.started",
        corpus_root = %corpus_root.display(),
        index_root = %index_root.display(),
        "annotation dry-run scan/parse pass starting"
    );

    // Same fatal fabric-plane gate the scheduler thread applies before any queue
    // or acquisition work: the pass has nothing valid to drive otherwise.
    validate_fabric_plane(&index_root)?;

    let staging_root = acquisition_staging_root(&index_root);
    let connector =
        FilesystemConnector::new(corpus_root.clone(), staging_root.clone(), governance_domain)
            .map_err(|source| ApiError::InvalidCli {
                message: format!("filesystem connector configuration invalid: {source}"),
            })?;
    let context = AcquisitionContext {
        connector_name: connector.connector_name().to_string(),
        connector_version: connector.connector_version().to_string(),
        connector_config_hash: connector.connector_config_hash().to_string(),
        source_system: connector.source_system().to_string(),
        governance_domain: connector.governance_domain().to_string(),
    };
    let scope_uri = corpus_root.display().to_string();

    // Startup sweep of orphaned parser temp workspaces, safe by the single-thread
    // invariant (the pass owns the only worker on this thread), exactly as the
    // scheduler thread does before its first cycle.
    sweep_orphan_parse_temp_dirs(&index_root);

    // Prefix-scoped context only (see ParsePrefixContext's ownership boundary):
    // the pass truncates before any gate work, so it never holds projection,
    // registry, or identity handles.
    let dispatch = ParsePrefixContext {
        storage: StorageConfig {
            corpus_root,
            index_root: index_root.clone(),
        },
        docling,
    };

    // ONE full scan (which also stages new/changed items) then a single uncapped
    // drain. This mirrors `run_cycle`'s scan → enqueue → claim → import ordering,
    // but the per-entry chain truncates at the prefix and the entry is never
    // completed/failed. Autonomous full-scan detections only (no operator queue
    // coupling in this mode), so the prescreen-override path is not exercised.
    let known = acquisition::known_location_state(&index_root, connector.source_system())?;
    let scan = match connector.full_scan(&known) {
        Ok(scan) => scan,
        Err(ScanError::SourceSide {
            failure_class,
            detail,
        }) => {
            // A source-side scan failure is meaningful operational state: record it
            // durably (mirroring `run_cycle`) then surface it as the pass's fatal.
            acquisition::record_failed_acquisition(
                &index_root,
                &context,
                &scope_uri,
                failure_class,
                &detail,
            )?;
            return Err(ApiError::InternalIo {
                message: format!(
                    "annotation dry-run full scan failed on the source side: {detail}"
                ),
            });
        }
        Err(ScanError::Internal(source)) => return Err(source),
    };
    info!(
        event = "scheduler.dry_run_pass.changes_observed",
        enumerated = scan.enumerated_native_uris.len(),
        staged = scan.staged_bundle_dirs.len(),
        skipped_unchanged = scan.skipped_unchanged,
        scan_failures = scan.failures.len(),
        enumeration_complete = scan.enumeration_complete,
        elapsed_ms = scan.elapsed_ms,
        "annotation dry-run pass: full scan finished"
    );

    for bundle_dir in &scan.staged_bundle_dirs {
        match staged_bundle_native_uri(bundle_dir) {
            Ok(native_uri) => {
                enqueue_coalesced(
                    &index_root,
                    connector.source_system(),
                    &native_uri,
                    REASON_STAGED_BY_FULL_SCAN,
                    None,
                )?;
            }
            Err(detail) => {
                // Unreadable manifest: import directly so the outcome is recorded,
                // matching `run_cycle`. A direct import bypasses the queue, so it
                // contributes no ready pair to the sampling set (its parse, if any,
                // is adopted by the next normal cycle like any un-gated ready run).
                warn!(
                    event = "scheduler.dry_run_pass.staged_manifest_unreadable",
                    bundle_dir = %bundle_dir.display(),
                    detail = %detail,
                    "staged bundle manifest unreadable at enqueue; importing directly"
                );
                acquisition::import_staged_bundle(&index_root, bundle_dir)?;
            }
        }
    }

    for failure in &scan.failures {
        acquisition::record_failed_acquisition(
            &index_root,
            &context,
            &failure.native_uri,
            failure.failure_class,
            &failure.detail,
        )?;
    }

    // Drain: claim every pending (and stale in_flight) row once. The pass leaves
    // each row in_flight after processing — no complete()/fail() — so the next
    // normal cycle reclaims it. Deliberately NOT looping to quiescence: this is a
    // deliberate one-shot pass over the corpus (ruling 9).
    let entries = claim_pending(&index_root)?;
    let mut outcome = DryRunPassOutcome {
        claimed: entries.len() as u64,
        ..DryRunPassOutcome::default()
    };
    for entry in &entries {
        let bundle_dir = bundle_dir_for(&staging_root, &entry.native_uri);
        // Queue-coupled Operations do not arise in this mode (all detections are
        // autonomous), but the dispatch step is NOT special-cased (mechanic 2):
        // mark_operation_running_at_dispatch is a no-op for autonomous rows.
        if let Err(source) = mark_operation_running_at_dispatch(&index_root, &entry.id) {
            // Infrastructure fault before the prefix: record, count, and continue;
            // the row stays in_flight for normal-cycle reclamation.
            warn!(
                event = "scheduler.dry_run_pass.entry_faulted",
                entry_id = entry.id,
                native_uri = entry.native_uri,
                stage = "mark_running",
                error = %source,
                "annotation dry-run entry faulted at dispatch; left in_flight for reclamation"
            );
            outcome.faulted += 1;
            continue;
        }
        match acquisition::import_staged_bundle(&index_root, &bundle_dir) {
            Ok(import) if import.imported => {
                // Run the SHARED prefix, then TRUNCATE (mechanic 1): no gate work,
                // no complete()/fail(), no bundle removal. The row and bundle stay
                // exactly as crash-replay would leave them.
                match parse_chain_prefix(&dispatch, &index_root, entry, &import) {
                    Ok(ParseChainPrefix::Skipped) => outcome.skipped += 1,
                    Ok(ParseChainPrefix::ParseFailed) => outcome.parse_failed += 1,
                    Ok(ParseChainPrefix::GateExisting { parse_run_id }) => {
                        // A pre-existing un-held ready run: surface it for sampling.
                        // source linkage is guaranteed by imported=true.
                        if let Some(source_id) = import.source_object_id.clone() {
                            outcome.parsed_ready += 1;
                            outcome.ready.push(DryRunReadyParse {
                                source_id,
                                parse_run_id,
                            });
                        } else {
                            outcome.faulted += 1;
                        }
                    }
                    Ok(ParseChainPrefix::FreshReady {
                        parse_run_id,
                        source_id,
                        bundle_dir: _consumed_bundle,
                    }) => {
                        // Fresh ready run: surface it and DELIBERATELY leave the
                        // consumed parser bundle on disk (do not remove) so the
                        // crash-replay adoption path the next normal cycle uses
                        // sees the exact same on-disk state.
                        outcome.parsed_ready += 1;
                        outcome.ready.push(DryRunReadyParse {
                            source_id,
                            parse_run_id,
                        });
                    }
                    Err(source) => {
                        warn!(
                            event = "scheduler.dry_run_pass.entry_faulted",
                            entry_id = entry.id,
                            native_uri = entry.native_uri,
                            stage = "parse_prefix",
                            error = %source,
                            "annotation dry-run entry faulted in parse prefix; left in_flight"
                        );
                        outcome.faulted += 1;
                    }
                }
            }
            Ok(_not_imported) => {
                // The importer rejected the bundle (e.g. missing after crash mid
                // removal). Count as a fault; the row stays in_flight.
                warn!(
                    event = "scheduler.dry_run_pass.entry_faulted",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    stage = "import",
                    "annotation dry-run entry bundle not imported; left in_flight"
                );
                outcome.faulted += 1;
            }
            Err(source) => {
                warn!(
                    event = "scheduler.dry_run_pass.entry_faulted",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    stage = "import",
                    error = %source,
                    "annotation dry-run entry faulted at import; left in_flight"
                );
                outcome.faulted += 1;
            }
        }
    }

    info!(
        event = "scheduler.dry_run_pass.completed",
        claimed = outcome.claimed,
        parsed_ready = outcome.parsed_ready,
        skipped = outcome.skipped,
        parse_failed = outcome.parse_failed,
        faulted = outcome.faulted,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "annotation dry-run scan/parse pass completed; queue rows left in_flight for normal-cycle adoption"
    );
    Ok(outcome)
}

/// Record one failed cycle: log it with elapsed time, keep the subsystem
/// operational (the fabric gate already passed; the failure is visible in
/// health detail), and back the cadence off so a broken dependency is never
/// hammered. Returns the backed-off delay.
fn handle_cycle_error(
    cadence: &mut CadenceState,
    health: &mut SyncHealth,
    cycle_started: Instant,
    source: &ApiError,
) -> u64 {
    error!(
        event = "scheduler.cycle_failed",
        error = %source,
        elapsed_ms = cycle_started.elapsed().as_millis() as u64,
        "sync cycle failed; cadence backing off"
    );
    let delay_ms = cadence.back_off_after_error();
    health.detail = Some(format!("last sync cycle failed: {source}"));
    health.cadence_ms = Some(delay_ms);
    delay_ms
}

/// Counters and cadence signals of one completed cycle.
struct CycleOutcome {
    stats: SyncCycleStats,
    /// Scan duration alone — the cadence floor (a scan cannot run more
    /// often than it takes), distinct from the whole cycle's elapsed_ms.
    scan_elapsed_ms: u64,
    /// Observed change count feeding the inter-change-interval EMA: items
    /// staged (new or changed content) plus locations evidenced deleted.
    changes: u64,
}

/// One full scan/drain cycle (spec §9.6 boundary order): load known state,
/// full-scan the corpus (change observed + acquired, since staging happens
/// inside the scan), enqueue staged bundles, record scan failures durably,
/// drain the queue through the importer (imported) and the per-entry parse
/// chain (parse complete/activated — spec §9.4: one queue drives
/// acquisition → parse → gate → activation), then apply enumeration-based
/// deletion inference when the enumeration was complete. `Err` means the
/// cycle ended early: a canonical-side fault, or a source-side scan failure
/// (which is first recorded as a durable failed acquisition at scope
/// level). Per-item problems are recorded and counted, not propagated.
fn run_cycle(
    index_root: &Path,
    staging_root: &Path,
    connector: &FilesystemConnector,
    context: &AcquisitionContext,
    scope_uri: &str,
    dispatch: &ParseDispatchContext,
) -> Result<CycleOutcome, ApiError> {
    let cycle_started = Instant::now();
    debug!(event = "scheduler.cycle_started", "sync cycle starting");

    // Change-observed boundary: prescreen state, then one full scan (which
    // also stages — the acquired boundary — for new/changed items).
    let mut known = acquisition::known_location_state(index_root, connector.source_system())?;

    // Prescreen override (Option A ruling, 2026-07-16). Operator-enqueued
    // requests (`POST /sources`, `POST /sources/{id}/parses`) target content the
    // (mtime, size) prescreen may see as unchanged; left alone, the scan would
    // skip them, no bundle would be staged, and the drain would park the linked
    // Operation failed — the §34.2 force-re-parse purpose (parser rollout over
    // unchanged content) could never succeed. The override is the `native_uri`s
    // of the PENDING operation-linked rows; subtracting them from `known` makes
    // the connector treat those files as new and stage them unconditionally.
    // Autonomous full-scan entries (operation_id NULL) are excluded because they
    // are enqueued only after the scan already staged their bundle — they never
    // need an override.
    let operator_override_uris =
        pending_operator_override_uris(index_root, connector.source_system())?;
    // Capture the prescreen key set BEFORE subtraction: the drain's
    // missing-bundle policy distinguishes "URI the scan had its chance to stage"
    // (in the override set, or absent from known entirely) from an entry the scan
    // could not have seen. `known` is mutated below, so this snapshot is taken
    // first.
    let known_uris_before_override: BTreeSet<String> = known.keys().cloned().collect();
    for uri in &operator_override_uris {
        known.remove(uri);
    }
    if !operator_override_uris.is_empty() {
        // Scan-boundary diagnostic (DIAGNOSTICS-ONBOARDING boundary rule): the
        // count at info, the per-URI list at debug only — no document contents.
        info!(
            event = "scheduler.prescreen_override",
            source_system = connector.source_system(),
            override_count = operator_override_uris.len() as u64,
            reason = "operator-enqueued entries force-staged past the unchanged-prescreen",
            "operator-enqueued sync entries subtracted from the prescreen known state"
        );
        for uri in &operator_override_uris {
            tracing::debug!(
                event = "scheduler.prescreen_override.uri",
                source_system = connector.source_system(),
                native_uri = %uri,
                "prescreen override URI"
            );
        }
    }

    let scan = match connector.full_scan(&known) {
        Ok(scan) => scan,
        Err(ScanError::SourceSide {
            failure_class,
            detail,
        }) => {
            // A source-side scan failure (the corpus root itself was
            // unreadable) is meaningful operational state (spec §9.2), so
            // it leaves a durable failed AcquisitionRecord against the
            // scope URI — mirroring record_enumeration's scope-level
            // record. If recording fails, that canonical-side error wins.
            let acquisition_record_id = acquisition::record_failed_acquisition(
                index_root,
                context,
                scope_uri,
                failure_class,
                &detail,
            )?;
            // §11.2 — a source-side scan failure is LOST ACCESS, not deletion:
            // the corpus was unreadable, which asserts nothing about whether its
            // items still exist (§11.1). Mark every in-scope `current` location
            // `access_lost` so serving continues while the freshness clock stops;
            // the scope-level failed AcquisitionRecord just written anchors the
            // transition. No deactivation, no barrier.
            crate::deletion::mark_scope_access_lost(
                index_root,
                connector.source_system(),
                scope_uri,
                &acquisition_record_id,
            )?;
            // The cycle still failed: the caller backs the cadence off.
            return Err(ApiError::InternalIo {
                message: format!("full scan failed on the source side: {detail}"),
            });
        }
        // Canonical-side infrastructure fault: propagate unchanged, no
        // acquisition record (nothing was learned about the source).
        Err(ScanError::Internal(source)) => return Err(source),
    };
    debug!(
        event = "scheduler.changes_observed",
        enumerated = scan.enumerated_native_uris.len(),
        staged = scan.staged_bundle_dirs.len(),
        skipped_unchanged = scan.skipped_unchanged,
        scan_failures = scan.failures.len(),
        enumeration_complete = scan.enumeration_complete,
        elapsed_ms = scan.elapsed_ms,
        "change-observed/acquired boundary: full scan finished"
    );

    // Enqueue every staged bundle. The native URI comes from the staged
    // manifest — the bundle's self-describing identity and the simplest
    // honest source, since the scan outcome does not pair bundle dirs with
    // URIs. It is a connector claim used purely as the operational routing
    // key; the importer re-validates the whole manifest at drain time.
    let mut enqueue_failures: u64 = 0;
    let mut direct_imports: u64 = 0;
    for bundle_dir in &scan.staged_bundle_dirs {
        match staged_bundle_native_uri(bundle_dir) {
            Ok(native_uri) => {
                enqueue_coalesced(
                    index_root,
                    connector.source_system(),
                    &native_uri,
                    REASON_STAGED_BY_FULL_SCAN,
                    // The autonomous full-scan detection carries no Operation.
                    None,
                )?;
            }
            Err(detail) => {
                // Without a readable identity the bundle cannot be queued;
                // route it straight through the importer, which either
                // imports it after all (the importer reads the manifest
                // itself, so a transient read failure here can still
                // succeed there) or records the malformed bundle as a
                // durable failed acquisition and keeps it for diagnostics —
                // the single malformed-bundle mechanism, invoked at
                // detection instead of drain.
                warn!(
                    event = "scheduler.staged_manifest_unreadable",
                    bundle_dir = %bundle_dir.display(),
                    detail = %detail,
                    "staged bundle manifest unreadable at enqueue; importing directly to \
                     record the outcome"
                );
                let outcome = acquisition::import_staged_bundle(index_root, bundle_dir)?;
                if outcome.imported {
                    // The importer could read what the scheduler could not:
                    // the bundle is now canonical, folded into this cycle's
                    // imported stat as a direct (queue-bypassing) import.
                    info!(
                        event = "scheduler.staged_manifest_recovered",
                        bundle_dir = %bundle_dir.display(),
                        acquisition_record_id = outcome.acquisition_record_id,
                        "unreadable-manifest bundle imported directly after all"
                    );
                    direct_imports += 1;
                    // No queue entry exists for a direct import, so its unit
                    // of work ends at the import itself: safe to remove now.
                    remove_consumed_acquisition_bundle(bundle_dir, &outcome.acquisition_record_id);
                } else {
                    enqueue_failures += 1;
                }
            }
        }
    }

    // Every per-item scan failure becomes a durable failed
    // AcquisitionRecord (spec §9.2: unreadable sources are meaningful
    // operational state).
    for failure in &scan.failures {
        acquisition::record_failed_acquisition(
            index_root,
            context,
            &failure.native_uri,
            failure.failure_class,
            &failure.detail,
        )?;
    }

    // Drain: claim everything pending and push each entry through the
    // importer at its deterministic bundle path, then through the parse
    // chain (route → no-retry guard → worker → parse import → gate).
    let drain_started = Instant::now();
    let entries = claim_pending(index_root)?;
    let claimed = entries.len();
    let mut imported: u64 = 0;
    let mut drain_failures: u64 = 0;
    let mut parse_failures: u64 = 0;
    // complete()/fail() bookkeeping errors are handled per-entry (the
    // cycle's documented per-item contract): the entry is counted as a
    // drain failure and the loop continues. A row left in_flight by such a
    // failure is reclaimed by the next claim_pending pass and replays
    // against its still-present bundle (deletion happens only after
    // complete() succeeds), so backpressure cannot latch on it.
    for entry in &entries {
        let entry_started = Instant::now();
        let bundle_dir = bundle_dir_for(staging_root, &entry.native_uri);

        // Operation link for this entry's drain diagnostics: read best-effort so
        // the per-entry error logs below can name the queue-coupled Operation
        // that a fault may leave stranded (e.g. a complete() whose internal
        // mark_succeeded failed leaves it stuck at `running`). This is a
        // diagnostic read only — a fault reading it must NOT abort the drain, so
        // it degrades to "unknown" rather than propagating. Autonomous rows have
        // no link ("none"). The missing-bundle pre-check below keeps its own
        // `?`-propagating read because that value is on its load-bearing path.
        let drain_operation_id = match entry_operation_id(index_root, &entry.id) {
            Ok(link) => link,
            Err(source) => {
                warn!(
                    event = "scheduler.drain_entry_operation_link_unavailable",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    error = %source,
                    "could not read operation link for drain diagnostics"
                );
                None
            }
        };
        let drain_operation_id = drain_operation_id.as_deref().unwrap_or("none");

        // Missing-bundle policy for OPERATOR entries (Option A ruling,
        // 2026-07-16). An operator-enqueued row (operation_id present) whose
        // bundle dir is absent is pre-checked HERE, before the operation is
        // flipped running: the disposition depends on whether this cycle's scan
        // had its chance to stage the file.
        //
        // The pre-check is deliberately scoped to operator entries with a
        // missing bundle. Autonomous entries (operation_id None) keep their
        // existing behavior: they fall through to import_staged_bundle and its
        // rejected/Err arms — a missing autonomous bundle is the documented
        // crash-mid-removal case the importer's missing-bundle rejection parks
        // failed. Entries whose bundle exists also fall through unchanged.
        if !bundle_dir.exists() {
            let operation_id = entry_operation_id(index_root, &entry.id)?;
            if let Some(operation_id) = operation_id {
                // Eligible this cycle iff the scan had a chance to stage it: its
                // URI was force-staged (in the override set) OR was never in the
                // prescreen known state at all (so an unchanged-prescreen could
                // not have skipped it — a complete enumeration simply did not
                // find/stage it). Either way, the file is absent from the corpus
                // (or outside the corpus root): fail honestly.
                let scan_had_its_chance = operator_override_uris.contains(&entry.native_uri)
                    || !known_uris_before_override.contains(&entry.native_uri);
                if scan_had_its_chance {
                    // Honest failure (Option A): a complete-enumeration scan did
                    // not stage this file, so it is not in the corpus. fail()
                    // drives the linked Operation to `failed`.
                    let detail = "source not staged by scan enumeration; \
                                  file absent from corpus or outside corpus root";
                    warn!(
                        event = "scheduler.drain_missing_bundle",
                        entry_id = entry.id,
                        native_uri = entry.native_uri,
                        operation_id = operation_id.as_str(),
                        decision = "fail_absent_from_scan",
                        elapsed_ms = entry_started.elapsed().as_millis() as u64,
                        "operator entry has no bundle after a scan that could have staged it; \
                         parking failed"
                    );
                    if let Err(fail_error) = fail(index_root, &entry.id, detail) {
                        error!(
                            event = "scheduler.drain_entry_failed",
                            entry_id = entry.id,
                            native_uri = entry.native_uri,
                            operation_id = operation_id.as_str(),
                            error = %fail_error,
                            "missing-bundle operator entry could not be parked as failed"
                        );
                    }
                    drain_failures += 1;
                    continue;
                }
                // Mid-cycle enqueue race: this operator row was enqueued AFTER
                // this cycle computed the override set, so the scan never had a
                // chance to stage its file. Do NOT claim-and-fail it — leave it
                // pending for the next cycle, where it WILL be in the override
                // set and force-staged. claim_pending already marked it
                // in_flight; SELECT_PENDING_ENTRIES_SQL reclaims in_flight rows,
                // so simply skipping (no complete(), no fail()) leaves it
                // re-claimable next cycle rather than latching it. The operation
                // was NOT yet flipped running (this pre-check runs before
                // mark_operation_running_at_dispatch), so it stays pending too.
                info!(
                    event = "scheduler.drain_missing_bundle",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    operation_id = operation_id.as_str(),
                    decision = "defer_race_enqueued_after_scan",
                    elapsed_ms = entry_started.elapsed().as_millis() as u64,
                    "operator entry enqueued after this cycle's prescreen override; \
                     leaving pending for the next cycle"
                );
                continue;
            }
            // Autonomous entry with a missing bundle: fall through to the
            // existing import_staged_bundle path (unchanged behavior).
        }

        // Queue-coupled Operation lifecycle (Escalation 1): flip a linked
        // operation pending → running before ANY terminal path, so complete()'s
        // mark_succeeded and fail()'s mark_failed (both running-guarded) never
        // hit a still-pending row. Autonomous entries are a no-op. A failure to
        // read/flip the operation is treated like any other drain-entry fault:
        // park the row failed and keep draining the rest.
        if let Err(source) = mark_operation_running_at_dispatch(index_root, &entry.id) {
            error!(
                event = "scheduler.drain_entry_failed",
                entry_id = entry.id,
                native_uri = entry.native_uri,
                operation_id = drain_operation_id,
                error = %source,
                "queue-coupled operation could not be marked running at dispatch"
            );
            if let Err(fail_error) = fail(index_root, &entry.id, &source.to_string()) {
                error!(
                    event = "scheduler.drain_entry_failed",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    operation_id = drain_operation_id,
                    error = %fail_error,
                    "sync queue entry could not be parked as failed after a mark-running fault"
                );
            }
            drain_failures += 1;
            continue;
        }
        match acquisition::import_staged_bundle(index_root, &bundle_dir) {
            Ok(outcome) if outcome.imported => {
                // The acquisition import is durable; the parse chain runs
                // BEFORE complete(), and the consumed bundle is removed only
                // AFTER complete() succeeds. A crash anywhere in between
                // leaves the entry claimable with its bundle intact, so the
                // replay re-imports idempotently (dedup by source_hash) and
                // the no-retry guard routes the parse side (Skip /
                // GateExisting / fresh dispatch).
                match dispatch_parse_chain(dispatch, index_root, entry, &outcome) {
                    Ok(chain_outcome) => {
                        if matches!(chain_outcome, ParseChainOutcome::ParseFailed) {
                            // A recorded parse failure completes the entry
                            // (spec §13.5: no blind retry; only new content
                            // or a new parser identity re-parses) but still
                            // counts as a cycle failure for health.
                            parse_failures += 1;
                        }
                        match complete(index_root, &entry.id) {
                            Ok(()) => {
                                imported += 1;
                                // The whole unit of work (import → parse
                                // chain → completion) is finished; only now
                                // is the bundle safe to remove, because a
                                // crash replay needs it present.
                                remove_consumed_acquisition_bundle(
                                    &bundle_dir,
                                    &outcome.acquisition_record_id,
                                );
                            }
                            Err(source) => {
                                // The import itself is durable; only the
                                // queue-row removal failed, so the entry
                                // counts as a drain failure, not an import.
                                // The bundle stays for the reclaim replay.
                                error!(
                                    event = "scheduler.drain_entry_failed",
                                    entry_id = entry.id,
                                    native_uri = entry.native_uri,
                                    operation_id = drain_operation_id,
                                    error = %source,
                                    "imported sync queue entry could not be completed"
                                );
                                drain_failures += 1;
                            }
                        }
                    }
                    Err(source) => {
                        // Canonical-side infrastructure fault in the parse
                        // chain: the acquisition import stays durable, and
                        // the entry parks failed (terminal until a new
                        // detection re-pends it) with the preserved error.
                        error!(
                            event = "scheduler.drain_entry_failed",
                            entry_id = entry.id,
                            native_uri = entry.native_uri,
                            operation_id = drain_operation_id,
                            error = %source,
                            "parse chain failed after a durable acquisition import"
                        );
                        if let Err(fail_error) = fail(index_root, &entry.id, &source.to_string()) {
                            error!(
                                event = "scheduler.drain_entry_failed",
                                entry_id = entry.id,
                                native_uri = entry.native_uri,
                                operation_id = drain_operation_id,
                                error = %fail_error,
                                "parse-faulted sync queue entry could not be parked as failed"
                            );
                        }
                        drain_failures += 1;
                    }
                }
            }
            Ok(outcome) => {
                // Rejected-as-malformed: the importer already wrote the
                // failed AcquisitionRecord (the audit trail), so the queue
                // row goes to failed — not deleted — keeping the stuck
                // source operator-visible in queue depths until a new
                // detection re-pends it.
                let detail = outcome
                    .rejection_detail
                    .unwrap_or_else(|| "bundle rejected as malformed".to_string());
                if let Err(source) = fail(index_root, &entry.id, &detail) {
                    error!(
                        event = "scheduler.drain_entry_failed",
                        entry_id = entry.id,
                        native_uri = entry.native_uri,
                        operation_id = drain_operation_id,
                        error = %source,
                        "rejected sync queue entry could not be parked as failed"
                    );
                }
                drain_failures += 1;
            }
            Err(source) => {
                // Canonical-side fault on this one entry: park the row with
                // the preserved error context and keep draining the rest.
                error!(
                    event = "scheduler.drain_entry_failed",
                    entry_id = entry.id,
                    native_uri = entry.native_uri,
                    operation_id = drain_operation_id,
                    error = %source,
                    "sync queue entry import failed"
                );
                if let Err(fail_error) = fail(index_root, &entry.id, &source.to_string()) {
                    error!(
                        event = "scheduler.drain_entry_failed",
                        entry_id = entry.id,
                        native_uri = entry.native_uri,
                        operation_id = drain_operation_id,
                        error = %fail_error,
                        "failed sync queue entry could not be parked as failed"
                    );
                }
                drain_failures += 1;
            }
        }
    }
    debug!(
        event = "scheduler.imported",
        claimed,
        imported,
        drain_failures,
        parse_failures,
        elapsed_ms = drain_started.elapsed().as_millis() as u64,
        "imported boundary: queue drain and parse dispatch finished"
    );

    // Deletion inference is gated on a COMPLETE enumeration (spec §11.1): a
    // partial scan asserts nothing about absent items.
    let mut deletions: u64 = 0;
    let mut source_lifecycle_changed = false;
    if scan.enumeration_complete {
        let enumeration_record_id =
            acquisition::record_enumeration(index_root, context, scope_uri, scan.elapsed_ms)?;
        let deleted = acquisition::apply_enumeration_deletions(
            index_root,
            connector.source_system(),
            scope_uri,
            &scan.enumerated_native_uris,
            &enumeration_record_id,
        )?;
        deletions = deleted.len() as u64;

        // Source-level deletion propagation (§11.3 step 4) and reappearance
        // restore (§11.4), run ONLY after a COMPLETE enumeration (§11.1 — a
        // partial scan asserts nothing about absent items, so neither may run on
        // an incomplete scan). Propagation deactivates any source whose last
        // `current` location was just removed; reappearance restores any
        // previously deactivated source whose content came back. The identity is
        // threaded to the pre-deactivation snapshot propagation mints.
        let deactivated_pairs = crate::deletion::propagate_deletions(
            index_root,
            &dispatch.registry,
            &dispatch.projections.dense_cache,
            &dispatch.identity,
            connector.source_system(),
        )?;
        let reactivated = crate::deletion::restore_reappeared_sources(
            index_root,
            &dispatch.registry,
            &dispatch.projections.dense_cache,
            dispatch.projections.dense_dimension,
            connector.source_system(),
        )?;
        // Source transitions can finish work left by a previous cycle even
        // when this enumeration did not discover a new file or deletion.
        source_lifecycle_changed = !deactivated_pairs.is_empty() || reactivated > 0;

        // §11.3 step 4's trailing archive-verify-delete, gated on the
        // pre-deactivation snapshot each deactivation minted. Run AFTER
        // reappearance restores so a same-cycle flip-flop cannot clean a
        // just-restored source: the deactivation candidate scan
        // (SELECT_DEACTIVATION_CANDIDATES_SQL, deactivated_at IS NULL AND no
        // `current` location) and the reappearance candidate scan
        // (SELECT_REACTIVATION_CANDIDATES_SQL, deactivated_at IS NOT NULL AND a
        // `current` location exists) are disjoint within one cycle — a source
        // deactivated this cycle has NO `current` location, so nothing between
        // the two steps can add one, and it cannot also be a reappearance
        // candidate. Verified: the two sets never overlap for a source in one
        // cycle. `?` propagates a gate failure or delete fault, parking the
        // queue entry failed on the same error path as the rest of this block.
        for (source_id, active_parse_id) in &deactivated_pairs {
            crate::restore::complete_superseded_parse(
                index_root,
                source_id,
                active_parse_id,
                crate::restore::SupersededCleanupMode::Deactivation,
            )?;
        }
    }

    let stats = SyncCycleStats {
        enumerated: scan.enumerated_native_uris.len() as u64,
        staged: scan.staged_bundle_dirs.len() as u64,
        skipped: scan.skipped_unchanged,
        // Drained imports plus direct (queue-bypassing) imports of bundles
        // whose manifest was unreadable at enqueue time.
        imported: imported + direct_imports,
        // Recorded parse failures join the failure count (C5c): the entry
        // completed — the failed run row is the durable record — but the
        // cycle still surfaces that a source could not be parsed.
        failures: scan.failures.len() as u64 + enqueue_failures + drain_failures + parse_failures,
        deletions,
        elapsed_ms: cycle_started.elapsed().as_millis() as u64,
    };
    // Enumeration/bookkeeping alone is idle. Keep real work and incomplete or
    // failed scans visible at INFO without repeating empty cycles there.
    let report_activity = stats.staged > 0
        || stats.imported > 0
        || stats.failures > 0
        || stats.deletions > 0
        || source_lifecycle_changed
        || !scan.enumeration_complete;
    if report_activity {
        info!(
            event = "scheduler.cycle_completed",
            enumerated = stats.enumerated,
            staged = stats.staged,
            skipped_unchanged = stats.skipped,
            imported = stats.imported,
            failures = stats.failures,
            deletions = stats.deletions,
            scan_elapsed_ms = scan.elapsed_ms,
            elapsed_ms = stats.elapsed_ms,
            "sync cycle completed"
        );
    } else {
        debug!(
            event = "scheduler.cycle_completed",
            enumerated = stats.enumerated,
            staged = stats.staged,
            skipped_unchanged = stats.skipped,
            imported = stats.imported,
            failures = stats.failures,
            deletions = stats.deletions,
            scan_elapsed_ms = scan.elapsed_ms,
            elapsed_ms = stats.elapsed_ms,
            "sync cycle completed"
        );
    }
    // Changes = staged + deleted: both are observed state transitions the
    // cadence should track; skipped/unchanged items are not change signal.
    let changes = stats.staged + deletions;
    Ok(CycleOutcome {
        stats,
        scan_elapsed_ms: scan.elapsed_ms,
        changes,
    })
}

/// The PREFIX of the drain-loop parse chain (spec §9.4: one queue drives
/// acquisition → parse → gate → activation), stopping at the importer's
/// committed-parse boundary: route by the stored mime type, apply the §13.5
/// rule 5 no-blind-retry guard, resolve corpus containment, run the
/// content-identity check, run the routed worker, and import its staged bundle.
/// Returns WITHOUT building projections, minting snapshots, or gating — those
/// are the caller's continuation (`gate_ready_parse`). Every producer-side
/// outcome — nothing to parse, guard skip, content-changed skip, recorded parse
/// failure — is an `Ok(ParseChainPrefix)` variant; `Err` is a canonical-side
/// infrastructure fault the caller parks the entry failed with.
///
/// Shared by the normal `dispatch_parse_chain` (which continues to build/gate)
/// and the CA2-P5 dry-run pass (which truncates HERE per mechanic 1). The normal
/// path stays behavior-identical: it is exactly the former inline prefix, and
/// the gate work it used to do inline now lives in `gate_ready_parse`, called
/// with byte-equivalent arguments. Takes the prefix-scoped context ONLY — no
/// projection/gate handles — so the dry-run pass can run it without inference
/// (see ParsePrefixContext's ownership boundary).
fn parse_chain_prefix(
    dispatch: &ParsePrefixContext,
    index_root: &Path,
    entry: &SyncQueueEntry,
    import: &ImportOutcome,
) -> Result<ParseChainPrefix, ApiError> {
    let started = Instant::now();
    // imported=true guarantees this linkage per the ImportOutcome contract;
    // absence is a broken internal contract, never producer input.
    let (Some(source_id), Some(source_hash)) = (&import.source_object_id, &import.source_hash)
    else {
        return Err(ApiError::InternalIo {
            message: format!(
                "imported acquisition outcome for entry {} carries no source linkage; \
                 the import/dispatch contract is broken",
                entry.id
            ),
        });
    };
    info!(
        event = "scheduler.parse_dispatch.started",
        entry_id = entry.id,
        source_id,
        "parse dispatch starting for imported entry"
    );

    // Route by the stored column (the authoritative mime — see the SQL
    // constant), on a read-only connection dropped before any long-running
    // worker starts.
    let mime_type = {
        let connection = hot_plane::open_read(index_root)?;
        read_source_mime_type(&connection, source_id)?
    };
    let route = match mime_type.as_str() {
        MIME_TYPE_PDF => ParseRoute::Pdf,
        MIME_TYPE_PLAIN_TEXT => ParseRoute::PlainText,
        _ => {
            // Nothing-to-parse is a recorded outcome, not a failure: this
            // durable warn is its record (a health count arrives at C10b).
            warn!(
                event = "scheduler.parse_dispatch.no_parser",
                entry_id = entry.id,
                source_id,
                mime_type,
                "no parser is registered for this mime type; source stays unparsed"
            );
            return Ok(ParseChainPrefix::Skipped);
        }
    };

    // Effective parser identity for the routed worker, from the same
    // derivation the worker stamps into its bundles, so the §13.5 guard and
    // the staged manifest can never disagree.
    let profile = match route {
        ParseRoute::Pdf => pdf_worker::effective_capability_profile(&dispatch.docling)?,
        ParseRoute::PlainText => text_worker::plain_text_capability_profile()?,
    };
    info!(
        event = "scheduler.parse_dispatch.routed",
        entry_id = entry.id,
        source_id,
        mime_type,
        parser_name = profile.parser_name,
        parser_version = profile.parser_version,
        parser_config_hash = profile.parser_config_hash,
        "source routed to parser worker"
    );

    let guard = {
        let connection = hot_plane::open_read(index_root)?;
        evaluate_no_retry_guard(&connection, source_id, &profile)?
    };
    match guard {
        NoRetryGuardDecision::Skip {
            parse_run_id,
            reason,
        } => {
            info!(
                event = "scheduler.parse_dispatch.skipped",
                entry_id = entry.id,
                source_id,
                parse_run_id,
                reason,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "no-blind-retry guard matched a prior run; worker skipped"
            );
            return Ok(ParseChainPrefix::Skipped);
        }
        NoRetryGuardDecision::GateExisting { parse_run_id } => {
            // Crash-recovery idempotence: an earlier import committed this run
            // `ready` but the process died before gating it. The §13.5-conformant
            // replay is to gate the EXISTING run rather than re-parse identical
            // bytes through an identical parser. The prefix stops here; the caller
            // (`gate_ready_parse`) runs the idempotent projection (re)build, the
            // snapshots, and the gate. The dry-run pass instead records this run
            // and leaves it un-held for the NEXT normal cycle's GateExisting
            // adoption (which re-reaches exactly this arm).
            return Ok(ParseChainPrefix::GateExisting { parse_run_id });
        }
        NoRetryGuardDecision::DispatchOverStaleBuilding { stale_run_id } => {
            // With the single scheduler thread, dispatch and import run
            // inline on this same thread, so an in-flight `building` row
            // can never be observed here: any building row visible at
            // dispatch time is stale wreckage of a crashed earlier import.
            // A fresh parse dispatches anyway so one crash cannot
            // permanently block the source (stuck-building surfacing
            // arrives at C10b).
            warn!(
                event = "scheduler.parse_dispatch.stale_building",
                entry_id = entry.id,
                source_id,
                stale_parse_run_id = stale_run_id,
                "stale building parse run observed; dispatching a fresh parse anyway"
            );
        }
        NoRetryGuardDecision::Dispatch => {}
    }

    // Both routes resolve through the crate::source containment authority
    // (traversal components rejected, canonicalized symlink-resolved path
    // verified inside the corpus root, so a corrupted or foreign queue row
    // cannot point a worker outside the corpus); the PDF resolver
    // additionally enforces the .pdf extension Docling requires.
    let relative = corpus_relative_source(&dispatch.storage.corpus_root, &entry.native_uri)?;
    let resolved = match route {
        ParseRoute::Pdf => resolve_source_reference(&dispatch.storage, &relative)?,
        ParseRoute::PlainText => resolve_contained_source(&dispatch.storage, &relative)?,
    };

    // Identity check (spec §10 rule 1: identity is content): the run will be
    // bound to the source_hash acquisition computed over the STAGED bytes,
    // while the worker reads the LIVE corpus file. Without this check, a
    // file changed between the scan that staged it and this dispatch would
    // permanently bind old-hash identity to new-byte canonical content. On
    // mismatch, skip: the changed bytes are guaranteed to be re-detected,
    // re-staged, and re-parsed under their own new SourceObject by the next
    // scan, so skipping is self-healing. Deliberately narrow: an instant
    // remains between this check and the worker's own read (recorded
    // residual risk; the structural fix — parsing the acquired bytes
    // themselves — is a worker-input design change deferred by ruling).
    let live_hash = {
        let live_bytes =
            fs::read(&resolved.absolute_path).map_err(|source| ApiError::SourceResolution {
                message: format!(
                    "parse input {} is not readable for the content-identity check: {source}",
                    resolved.absolute_path.display()
                ),
            })?;
        crate::canonical::sha256_hex_bytes(&live_bytes)
    };
    if live_hash != *source_hash {
        warn!(
            event = "scheduler.parse_dispatch.content_changed",
            entry_id = entry.id,
            source_id,
            expected_source_hash = source_hash,
            observed_hash = live_hash,
            path = %resolved.absolute_path.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "corpus file changed between staging and dispatch; parse skipped pending re-detection"
        );
        return Ok(ParseChainPrefix::Skipped);
    }

    // Worker execution (spec §12.1): the worker stages an untrusted bundle
    // for succeeded and failed parses alike; `Err` here means its staging
    // workspace itself faulted. A file that vanishes after the check above
    // is the worker's recorded parse outcome.
    let bundle_dir = match route {
        ParseRoute::Pdf => pdf_worker::run_pdf_parse(
            &dispatch.docling,
            index_root,
            resolved,
            source_id,
            source_hash,
        )?,
        ParseRoute::PlainText => text_worker::run_text_parse(
            index_root,
            &resolved.absolute_path,
            source_id,
            source_hash,
        )?,
    };

    let imported_parse = import_parser_bundle(index_root, &bundle_dir, &profile)?;
    match imported_parse.status {
        ImportedParseStatus::Failed => {
            // The importer already recorded the failed run and its event;
            // the staged failure bundle stays on disk, inspectable per spec
            // §12.2. The bounded detail rides along so this terminal
            // dispatch log answers "why" without log correlation.
            info!(
                event = "scheduler.parse_dispatch.completed",
                entry_id = entry.id,
                source_id,
                parse_run_id = imported_parse.parse_run_id,
                outcome = "parse_failed",
                detail = imported_parse.rejection_detail.as_deref().unwrap_or(""),
                bundle_kept = true,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parse dispatch finished with a recorded parse failure"
            );
            Ok(ParseChainPrefix::ParseFailed)
        }
        ImportedParseStatus::Ready => {
            // The importer committed a fresh ready run. The prefix stops here; the
            // caller (`gate_ready_parse`) runs the C6 content-derived projection
            // build, the snapshots, the activation gate, and the consumed-bundle
            // removal. The dry-run pass instead truncates HERE (mechanic 1): the
            // ready row stamped `held_reason` NULL is the exact GateExisting
            // precondition the next normal cycle adopts. `source_id` and
            // `bundle_dir` ride along so the caller needs no second read.
            info!(
                event = "scheduler.parse_dispatch.imported_ready",
                entry_id = entry.id,
                source_id,
                parse_run_id = imported_parse.parse_run_id,
                unit_count = imported_parse.unit_count,
                relationship_count = imported_parse.relationship_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parse worker output imported as a ready run (pre-gate)"
            );
            Ok(ParseChainPrefix::FreshReady {
                parse_run_id: imported_parse.parse_run_id,
                source_id: source_id.clone(),
                bundle_dir,
            })
        }
    }
}

/// The normal drain-loop parse chain for one imported entry (spec §9.4): run the
/// shared prefix, then — for a ready run (freshly imported OR adopted by the
/// §13.5 GateExisting arm) — build the content-derived projections, mint the
/// pre/post snapshots, run the activation gate, and clean any superseded held
/// candidate. Every producer-side outcome returns `Ok` and the entry completes;
/// `Err` is a canonical-side infrastructure fault the caller parks the entry
/// failed with. Behavior is identical to the former inline body: the prefix is
/// the former prefix verbatim, and `gate_ready_parse` is the former gate work,
/// now shared by both ready arms.
fn dispatch_parse_chain(
    dispatch: &ParseDispatchContext,
    index_root: &Path,
    entry: &SyncQueueEntry,
    import: &ImportOutcome,
) -> Result<ParseChainOutcome, ApiError> {
    let started = Instant::now();
    // The prefix consumes only the prefix-scoped slice of the context; the gate
    // continuation below consumes the rest (projections/registry/identity).
    match parse_chain_prefix(&dispatch.prefix, index_root, entry, import)? {
        ParseChainPrefix::Skipped => Ok(ParseChainOutcome::Skipped),
        ParseChainPrefix::ParseFailed => Ok(ParseChainOutcome::ParseFailed),
        ParseChainPrefix::GateExisting { parse_run_id } => {
            // Crash-recovery idempotence: gate the pre-existing ready run without
            // re-parsing. No fresh worker ran, so there is no consumed bundle to
            // remove (`consumed_bundle: None`). The projection (re)build inside is
            // idempotent (each type is deleted-for-parse then rebuilt), so a crash
            // that died before OR after the original build replays cleanly.
            let source_id =
                import
                    .source_object_id
                    .as_deref()
                    .ok_or_else(|| ApiError::InternalIo {
                        message: format!(
                            "imported acquisition outcome for entry {} carries no source linkage \
                             at GateExisting; the import/dispatch contract is broken",
                            entry.id
                        ),
                    })?;
            let decision = gate_ready_parse(dispatch, index_root, source_id, &parse_run_id, None)?;
            info!(
                event = "scheduler.parse_dispatch.gated_existing",
                entry_id = entry.id,
                source_id,
                parse_run_id,
                decision = decision_label(&decision),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "pre-existing ready run gated (crash recovery)"
            );
            Ok(ParseChainOutcome::Gated)
        }
        ParseChainPrefix::FreshReady {
            parse_run_id,
            source_id,
            bundle_dir,
        } => {
            let decision = gate_ready_parse(
                dispatch,
                index_root,
                &source_id,
                &parse_run_id,
                Some(&bundle_dir),
            )?;
            info!(
                event = "scheduler.parse_dispatch.completed",
                entry_id = entry.id,
                source_id,
                parse_run_id,
                outcome = decision_label(&decision),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parse dispatch finished; ready run gated"
            );
            Ok(ParseChainOutcome::Gated)
        }
    }
}

/// Gate one ready parse run: build its content-derived projections (spec §22–§23,
/// runs AFTER the importer committed the run's canonical ContentUnits and BEFORE
/// the gate so a parse cannot activate without its retrieval-targeting
/// projections — see `verify_activation_prerequisites`), mint the pre-activation
/// snapshot (§30.6), run the activation gate, mint the post-activation snapshot
/// and complete the superseded predecessor ONLY when the gate activated
/// (§30.6/§31.2), and clean any superseded held candidate (ruling 1, for BOTH
/// decisions). When `consumed_bundle` is `Some`, the freshly-produced parser
/// bundle is removed after gating (a delete error is logged, never fatal — the
/// import and gate decision are already durable and a leftover promoted bundle is
/// inert). This is the exact former inline gate body, factored so the fresh-ready
/// and GateExisting arms share ONE copy (they were byte-equivalent twins).
///
/// NOT called by the CA2-P5 dry-run pass: the pass truncates before this step by
/// design (mechanic 1), leaving the ready run un-held for normal-cycle adoption.
fn gate_ready_parse(
    dispatch: &ParseDispatchContext,
    index_root: &Path,
    source_id: &str,
    parse_run_id: &str,
    consumed_bundle: Option<&Path>,
) -> Result<ActivationDecision, ApiError> {
    build_content_derived_projections(&dispatch.projections, index_root, source_id, parse_run_id)?;

    // Pre-activation snapshot (§30.6): minted immediately BEFORE the gate. The
    // gate holds the per-source cutover barrier INTERNALLY; the snapshot runs
    // OUTSIDE that hold (this call precedes gate_and_activate), preserving the
    // §31.1 brevity rule.
    crate::snapshot::pre_activation_snapshot(index_root, &dispatch.identity, parse_run_id)?;
    let decision = activation::gate_and_activate(
        index_root,
        &dispatch.registry,
        &dispatch.projections.dense_cache,
        dispatch.projections.dense_dimension,
        parse_run_id,
    )?;
    // Post-activation snapshot ONLY when the gate actually activated: a Held
    // decision changes no active state, so there is nothing new to capture over.
    // §30.6/§31.2 — this is the snapshot the deletion gate later verifies over; it
    // runs OUTSIDE the gate's internal barrier hold.
    if let ActivationDecision::Activated {
        superseded_predecessor_id,
        ..
    } = &decision
    {
        crate::snapshot::post_activation_snapshot(index_root, &dispatch.identity, parse_run_id)?;
        // §31.2 steps 3–5: complete the superseded predecessor's
        // archive-verify-delete. The gate verifies over the post_activation
        // snapshot JUST minted above (never re-taken), whose subject is the newly
        // activated candidate. A gate failure or delete fault propagates through
        // `?`, halting this source's lifecycle. Only runs when a predecessor
        // existed (None = first-time activation, nothing to supersede).
        if let Some(predecessor_id) = superseded_predecessor_id {
            crate::restore::complete_superseded_parse(
                index_root,
                source_id,
                predecessor_id,
                crate::restore::SupersededCleanupMode::ActivationSupersession {
                    activated_parse_id: parse_run_id.to_owned(),
                },
            )?;
        }
    }
    // Ruling 1: clean any held candidate the gate/hold path superseded. Runs for
    // BOTH decisions (a hold can supersede an older held one), so it is OUTSIDE
    // the Activated-only block above. Post-barrier, gated over each candidate's
    // own pre_activation snapshot.
    clean_superseded_held(index_root, source_id, &decision)?;
    // A freshly-produced bundle is consumed either way (Activated and Held are
    // both recorded outcomes). A deletion error must NOT fail the chain: the
    // import and gate decision are already durable, and a leftover promoted
    // bundle is inert because only this dispatch ever points the importer at it.
    if let Some(bundle_dir) = consumed_bundle
        && let Err(source) = fs::remove_dir_all(bundle_dir)
    {
        error!(
            event = "scheduler.parse_dispatch.bundle_delete_failed",
            source_id,
            bundle_dir = %bundle_dir.display(),
            error = %source,
            "consumed parser bundle could not be deleted; chain continues"
        );
    }
    Ok(decision)
}

/// Build the five content-derived retrieval projections for one ready parse,
/// in runtime dependency order, on ONE hot-plane write transaction (spec
/// §22–§23; C6). Runs post-import, pre-activation.
///
/// Build order and why (chunk first because every downstream channel reads
/// chunks back; view last because it is independent and archives to the store):
/// chunk → lexical → dense → multivector → view. Each type is
/// `envelope::delete_for_parse`-d immediately before its builder so a re-run
/// (crash replay) replaces rather than accumulates envelopes; the builders
/// themselves delete their own payload rows.
///
/// Transaction / atomicity: chunk→lexical→dense→multivector→view all ride ONE
/// transaction, so the parse's whole projection set commits or rolls back
/// together — activation never sees a half-built projection set. On ANY builder
/// error the transaction is rolled back (discarding every partial write AND the
/// builders' own on-tx `failed` markers), then the failure is recorded durably
/// in a SEPARATE committed transaction (see `record_projection_build_failure`)
/// so `projection.failed` survives for the operator even though the build tx
/// vanished.
///
/// Model-gate boundary (spec §1.5): the dense batch and the colbert batch each
/// hold ONE `ModelCallPermit` for the whole batch — acquired once per parse per
/// model role, never per item — and the two roles are acquired SEPARATELY and do
/// NOT overlap (the dense permit drops before the colbert permit is acquired).
/// Non-model builders (chunk, lexical, view) run with NO permit held. The gate
/// is the SAME process-global serializer AppState uses (see ProjectionRuntime).
fn build_content_derived_projections(
    runtime: &ProjectionRuntime,
    index_root: &Path,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    let started = Instant::now();
    info!(
        event = "scheduler.projection_build.started",
        source_id, parse_id, "content-derived projection build started"
    );

    let store = ArtifactStore::open(index_root)?;
    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "projection_build")?;

    // The whole build rides `tx`; `run` returns Err on the first builder failure
    // and the transaction is aborted below, so no partial projection set ever
    // commits (activation would otherwise see a torn set).
    let build = build_projection_transaction(runtime, &store, &tx, source_id, parse_id);

    match build {
        Ok(counts) => {
            hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "projection_build")?;
            info!(
                event = "scheduler.projection_build.success",
                source_id,
                parse_id,
                chunk_count = counts.chunk_count,
                dense_chunk_count = counts.dense_chunk_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "content-derived projection build committed"
            );
            Ok(())
        }
        Err(error) => {
            // Roll back the build tx: this discards every partial insert AND the
            // builders' own `failed` envelope markers (they mark failed on THIS
            // tx). To keep `projection.failed` durable for the operator, the
            // failure is re-recorded on a fresh committed transaction below.
            let error =
                hot_plane::abort_transaction(tx, TX_LOG_NAMESPACE, "projection_build", error);
            error!(
                event = "scheduler.projection_build.failure",
                source_id,
                parse_id,
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "content-derived projection build failed; build transaction rolled back"
            );
            record_projection_build_failure(&mut connection, source_id, parse_id, &error);
            Err(error)
        }
    }
}

/// Per-parse tallies of a successful projection build, for the success log only
/// (no vector values or chunk text — forbidden in logs).
struct ProjectionBuildCounts {
    chunk_count: usize,
    dense_chunk_count: usize,
}

/// The fallible body of the projection build: every builder runs on the shared
/// `tx` in runtime order, each preceded by its type's `delete_for_parse` sweep.
/// Split out so its single caller owns the commit / rollback-and-audit decision
/// (see `build_content_derived_projections`).
fn build_projection_transaction(
    runtime: &ProjectionRuntime,
    store: &ArtifactStore,
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
) -> Result<ProjectionBuildCounts, ApiError> {
    // 1. Chunk — chunks are the input the lexical and dense channels read back,
    //    so they are built first. Token counts are measured against the ColBERT
    //    tokenizer (C6b contract) via the runtime accessor.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::Chunk)?;
    let chunk_count = chunk::build_chunks(tx, source_id, parse_id, runtime.colbert.tokenizer())?;

    // 2. Lexical — FTS5 index over the chunks just built.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::LexicalDocument)?;
    lexical::build_lexical_index(tx, source_id, parse_id)?;

    // 3. Dense — per-chunk passage vectors, backend-aware gate discipline
    //    (approved design §1.5, mirrors the reranker call site in
    //    `query::rerank`). `uses_local_model_gate()` is `true` only for the local
    //    accelerator backend: THEN acquire ONE model permit held across the whole
    //    batch (dense role) and pass it as proof; the permit drops at the end of
    //    this block, BEFORE the colbert permit is acquired, so the two roles never
    //    overlap. For the HTTP backend it is `false`: no permit is acquired, so
    //    the exclusive gate is never held across the network round-trips.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::DenseVector)?;
    let dense_outcome = if runtime.dense.uses_local_model_gate() {
        let permit = acquire_model_call_gate_on(
            &runtime.gate,
            parse_id,
            DENSE_MODEL_ROLE,
            DENSE_CALL_PURPOSE,
        )?;
        let outcome = dense::build_dense_vectors(
            tx,
            source_id,
            parse_id,
            &runtime.dense,
            runtime.dense_dimension,
            Some(&permit),
        )?;
        // `permit` drops here: the dense gate release precedes the colbert
        // acquire, honoring the non-overlapping-roles gate boundary.
        outcome
    } else {
        // HTTP backend: no gate across network I/O (§1.5). The builder packs its
        // own DENSE_HTTP_BATCH_SIZE windows.
        dense::build_dense_vectors(
            tx,
            source_id,
            parse_id,
            &runtime.dense,
            runtime.dense_dimension,
            None,
        )?
    };

    // 4. Multivector — per-unit ColBERT matrices under a SEPARATE model permit
    //    (colbert role), acquired only after the dense permit released above.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::MultiVector)?;
    {
        let permit = acquire_model_call_gate_on(
            &runtime.gate,
            parse_id,
            COLBERT_MODEL_ROLE,
            COLBERT_CALL_PURPOSE,
        )?;
        multivector::build_multivectors(
            tx,
            source_id,
            parse_id,
            &runtime.colbert,
            runtime.colbert_dimension,
            &permit,
        )?;
        // colbert permit drops here.
    }

    // 5. Derived view — render + archive; independent of chunks/vectors, run
    //    last so the content-addressed ArtifactStore write is the final step.
    envelope::delete_for_parse(tx, parse_id, envelope::ProjectionType::DerivedView)?;
    view::build_derived_view(tx, store, source_id, parse_id)?;

    Ok(ProjectionBuildCounts {
        chunk_count,
        dense_chunk_count: dense_outcome.chunk_count,
    })
}

/// Record a durable `projection.failed` audit marker after the build tx rolled
/// back (FAILURE-AUDIT invariant). The build's own on-tx `failed` markers
/// vanished with the rollback, so this opens a FRESH committed transaction and
/// writes one `building`→`failed` envelope carrying the bounded failure detail,
/// so `projection.failed` survives for the operator. Best-effort: a failure to
/// record the audit is logged but never masks the original build error the
/// caller returns (the build already failed; the parse will simply not
/// activate). The generic `DerivedView` type is used purely as the audit
/// marker's carrier — the failure is per-parse, not per-channel.
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
                    projection_type: envelope::ProjectionType::DerivedView,
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
            event = "scheduler.projection_build.failure_audit_failed",
            source_id,
            parse_id,
            error = %audit_error,
            "durable projection.failed audit could not be recorded; original build error stands"
        );
    } else {
        // The original build failed, but its failure marker committed separately.
        info!(
            event = "scheduler.projection_build.failure_audit_committed",
            source_id, parse_id, "projection failure audit committed"
        );
    }
}

/// The producer provenance stamped on the durable failure-audit envelope. Names
/// the scheduler's projection-build step as a `System` producer so the
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

/// Compact log label for one gate decision; the activation module already
/// logs the full §13.3 deltas when a candidate holds.
fn decision_label(decision: &ActivationDecision) -> &'static str {
    match decision {
        ActivationDecision::Activated { .. } => "activated",
        ActivationDecision::Held { .. } => "held",
    }
}

/// The superseded held-candidate ids either decision arm carries out
/// (`ActivationDecision`, Ruling 1). Both `Activated` and `Held` supersede any
/// older held candidate of the source, so both arms feed this.
fn superseded_held_ids(decision: &ActivationDecision) -> &[String] {
    match decision {
        ActivationDecision::Activated {
            superseded_held_ids,
            ..
        }
        | ActivationDecision::Held {
            superseded_held_ids,
            ..
        } => superseded_held_ids,
    }
}

/// Ruling 1 (plan §3 C10r): complete the §31.2 archive-verify-delete of every
/// held candidate the gate/hold path just superseded. Runs AFTER
/// `gate_and_activate` returns, so the source's cutover barrier the gate held
/// internally has released — matching the predecessor cleanup's post-barrier
/// placement. Each candidate is gated over its OWN pre_activation snapshot
/// (`HeldSupersession`), which the scheduler already minted before the gate and
/// never re-takes. A gate failure or delete fault propagates through `?`,
/// halting this source's lifecycle exactly like the predecessor cleanup — the
/// queue entry parks failed on the arm's existing error path. The list is
/// normally empty or one id (§12 rule 4); a defensively larger list is cleaned
/// id by id. Safe identifiers only in the boundary logs (no tokens/contents).
fn clean_superseded_held(
    index_root: &Path,
    source_id: &str,
    decision: &ActivationDecision,
) -> Result<(), ApiError> {
    for held_id in superseded_held_ids(decision) {
        let started = Instant::now();
        info!(
            event = "scheduler.held_cleanup_started",
            source_id,
            held_parse_id = held_id.as_str(),
            "superseded held candidate cleanup starting (Ruling 1)"
        );
        if let Err(source) = crate::restore::complete_superseded_parse(
            index_root,
            source_id,
            held_id,
            crate::restore::SupersededCleanupMode::HeldSupersession,
        ) {
            error!(
                event = "scheduler.held_cleanup_failed",
                source_id,
                held_parse_id = held_id.as_str(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "superseded held candidate cleanup failed; source lifecycle halts"
            );
            return Err(source);
        }
        info!(
            event = "scheduler.held_cleanup_completed",
            source_id,
            held_parse_id = held_id.as_str(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "superseded held candidate cleaned (Ruling 1)"
        );
    }
    Ok(())
}

/// Read the stored mime type of one imported source object. The row must
/// exist — the durable import that produced `source_id` committed it — so
/// absence is a broken persisted invariant, never producer input.
fn read_source_mime_type(connection: &Connection, source_id: &str) -> Result<String, ApiError> {
    connection
        .query_row(SELECT_SOURCE_MIME_TYPE_SQL, params![source_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read mime type of source object {source_id}: {source}"),
        })?
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "source object {source_id} reported by a durable import does not exist; \
                 the import/dispatch contract is broken"
            ),
        })
}

/// Evaluate the spec §13.5 rule 5 no-blind-retry guard over every prior run
/// of one (source, parser identity/configuration) tuple. Precedence when
/// defensive drift leaves several matching runs: gate an ungated ready run
/// first (it is the anomaly needing repair — gating is idempotent state
/// convergence, while skipping would strand it), then skip on any
/// terminal/held run, then treat lone building wreckage as dispatchable.
fn evaluate_no_retry_guard(
    connection: &Connection,
    source_id: &str,
    profile: &ParserCapabilityProfile,
) -> Result<NoRetryGuardDecision, ApiError> {
    let mut statement = connection
        .prepare(SELECT_PARSE_RUNS_BY_IDENTITY_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare parse-run identity lookup: {source}"),
        })?;
    let rows = statement
        .query_map(
            params![
                source_id,
                profile.parser_name,
                profile.parser_version,
                profile.parser_config_hash
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query prior parse runs of source {source_id}: {source}"),
        })?;

    // First match per bucket wins; the query's stable ordering makes the
    // pick deterministic.
    let mut gateable: Option<String> = None;
    let mut skip: Option<(String, &'static str)> = None;
    let mut stale_building: Option<String> = None;
    for row in rows {
        let (run_id, status_text, held_reason) =
            row.map_err(|source| ApiError::StorageOperation {
                message: format!("failed to read prior parse run row: {source}"),
            })?;
        match parse_run_status_from_wire(&status_text)? {
            // ready + held waits for explicit §13.4 disposition; identical
            // input through an identical parser would only reproduce the
            // same held candidate.
            ParseRunStatus::Ready if held_reason.is_some() => {
                let _ = skip.get_or_insert((run_id, "held_awaiting_disposition"));
            }
            ParseRunStatus::Ready => {
                let _ = gateable.get_or_insert(run_id);
            }
            ParseRunStatus::Failed => {
                let _ = skip.get_or_insert((run_id, "deterministic_no_retry"));
            }
            ParseRunStatus::Active | ParseRunStatus::Archiving | ParseRunStatus::Archived => {
                let _ = skip.get_or_insert((run_id, "already_parsed"));
            }
            ParseRunStatus::Building => {
                let _ = stale_building.get_or_insert(run_id);
            }
        }
    }

    if let Some(parse_run_id) = gateable {
        return Ok(NoRetryGuardDecision::GateExisting { parse_run_id });
    }
    if let Some((parse_run_id, reason)) = skip {
        return Ok(NoRetryGuardDecision::Skip {
            parse_run_id,
            reason,
        });
    }
    if let Some(stale_run_id) = stale_building {
        return Ok(NoRetryGuardDecision::DispatchOverStaleBuilding { stale_run_id });
    }
    Ok(NoRetryGuardDecision::Dispatch)
}

/// Parse one persisted parse-run status through the model enum's serde wire
/// names, so the CHECK set, the enum, and this parse can never drift apart
/// (same single-source pattern as queue_state_from_wire).
fn parse_run_status_from_wire(status: &str) -> Result<ParseRunStatus, ApiError> {
    serde_json::from_value(Value::String(status.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!(
                "parse_runs status '{status}' is outside the schema CHECK set: {source}"
            ),
        }
    })
}

/// Remove one consumed acquisition-bundle directory after its whole unit of
/// work finished (drain: import → parse chain → queue completion; direct
/// import: the import itself). Runtime cleanup, not canonical deletion — the
/// bundle's content is already canonical (blob + rows committed). A removal
/// failure is log-surfaced but never fatal: the lingering bundle is inert
/// (staging is never enumerated by scans, the prescreen skips the unchanged
/// file, and a future change replaces the bundle atomically at the same
/// contract path).
fn remove_consumed_acquisition_bundle(bundle_dir: &Path, acquisition_record_id: &str) {
    match fs::remove_dir_all(bundle_dir) {
        Ok(()) => info!(
            event = "scheduler.acquisition_bundle_consumed",
            bundle_dir = %bundle_dir.display(),
            acquisition_record_id,
            "consumed staged acquisition bundle removed"
        ),
        Err(source) => error!(
            event = "scheduler.acquisition_bundle_cleanup_failed",
            bundle_dir = %bundle_dir.display(),
            acquisition_record_id,
            error = %source,
            "unit of work finished but consumed bundle removal failed; \
             lingering bundle is inert"
        ),
    }
}

/// Bounded example-path count in the orphan-sweep summary log, so a large
/// orphan set never bloats one log line (the count stays exact).
const SWEEP_EXAMPLE_PATH_CAP: usize = 3;

/// Startup sweep of orphaned parser temp workspaces: remove every directory
/// under the parse staging root matching the bundle writer's temp naming
/// (`bundle-*` + `.tmp`, see `crate::parse::bundle`). Safe by the
/// single-scheduler-thread design: workers run inline on this thread, so at
/// thread start no live worker can own a temp directory — every one on disk
/// is crash leftover. Promoted (non-temp) bundle directories are left
/// alone: one may be an unimported bundle from a crash, and the
/// queue/importer interplay owns those. Sweep faults are logged, never
/// terminal — temp directories are inert by the promotion naming invariant.
fn sweep_orphan_parse_temp_dirs(index_root: &Path) {
    let started = Instant::now();
    let staging_root = parse_staging_root(index_root);
    // Start boundary: the sweep enumerates and recursively removes
    // directories, which is measurably long exactly when a crash left many
    // workspaces behind — the log must show the boundary was entered even
    // if a removal wedges.
    info!(
        event = "scheduler.parse_staging_sweep_started",
        staging_root = %staging_root.display(),
        "sweeping orphaned parse temp workspaces"
    );
    let entries = match fs::read_dir(&staging_root) {
        Ok(entries) => entries,
        // No staging root yet means no worker has ever run: nothing to
        // sweep, and creating the directory is the bundle writer's job.
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            info!(
                event = "scheduler.parse_staging_swept",
                swept = 0u64,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "no parse staging root exists; nothing to sweep"
            );
            return;
        }
        Err(source) => {
            error!(
                event = "scheduler.parse_staging_sweep_failed",
                staging_root = %staging_root.display(),
                error = %source,
                "parse staging root unreadable; orphan sweep skipped"
            );
            return;
        }
    };

    let mut swept: u64 = 0;
    let mut example_paths: Vec<String> = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(source) => {
                error!(
                    event = "scheduler.parse_staging_sweep_failed",
                    staging_root = %staging_root.display(),
                    error = %source,
                    "staging entry unreadable during orphan sweep"
                );
                continue;
            }
        };
        let path = entry.path();
        // Target EXACTLY the writer's temp naming; anything else in staging
        // (promoted bundles included) is not this sweep's to touch.
        let is_temp_bundle = path.is_dir()
            && entry.file_name().to_str().is_some_and(|name| {
                name.starts_with(BUNDLE_DIR_NAME_PREFIX) && name.ends_with(BUNDLE_TEMP_DIR_SUFFIX)
            });
        if !is_temp_bundle {
            continue;
        }
        match fs::remove_dir_all(&path) {
            Ok(()) => {
                swept += 1;
                if example_paths.len() < SWEEP_EXAMPLE_PATH_CAP {
                    example_paths.push(path.display().to_string());
                }
            }
            Err(source) => error!(
                event = "scheduler.parse_staging_sweep_failed",
                temp_dir = %path.display(),
                error = %source,
                "orphaned temp bundle workspace could not be removed"
            ),
        }
    }
    info!(
        event = "scheduler.parse_staging_swept",
        swept,
        example_paths = ?example_paths,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "orphaned parse temp workspaces swept"
    );
}

/// Adaptive-cadence state (spec §9.5, knob-free): every adjustment derives
/// from observed signals — scan duration, measured churn, backlog — and is
/// individually logged with its cause and old/new values.
struct CadenceState {
    /// Next inter-cycle delay; None until the first scan establishes it.
    next_delay_ms: Option<u64>,
    /// EMA of the observed inter-change interval (spacing / changes).
    ema_inter_change_ms: Option<f64>,
    /// Start of the previous cycle: the spacing basis for churn measurement.
    last_cycle_started: Option<Instant>,
    /// Whether the last cycle ended under backpressure, for edge-triggered
    /// sync.backpressure_entered/_exited events.
    backpressure_active: bool,
}

impl CadenceState {
    /// Fresh cadence state: nothing measured yet, no backpressure.
    fn new() -> Self {
        Self {
            next_delay_ms: None,
            ema_inter_change_ms: None,
            last_cycle_started: None,
            backpressure_active: false,
        }
    }

    /// Fold one successful cycle's signals into the cadence. Returns the
    /// delay to sleep and the backpressure transition, if one occurred
    /// (Some(true) = entered, Some(false) = exited).
    fn adapt(
        &mut self,
        scan_elapsed_ms: u64,
        changes: u64,
        spacing_ms: u64,
        backlog: u64,
    ) -> (u64, Option<bool>) {
        // The scan-duration floor: a scan cannot run more often than it
        // takes (spec §9.5 source-system pushback). max(1) keeps a
        // sub-millisecond scan from busy-looping the thread.
        let floor_ms = scan_elapsed_ms.max(1);

        let mut delay_ms = match self.next_delay_ms {
            Some(current) => current,
            None => {
                // First measured signal: the cadence starts at the first
                // scan's own duration.
                info!(
                    event = "scheduler.cadence_adapted",
                    cause = "first_scan_duration",
                    new_delay_ms = floor_ms,
                    "cadence initialized to the first scan's duration"
                );
                floor_ms
            }
        };

        if changes > 0 {
            // Measured churn: this cycle's spacing carried `changes`
            // observed changes, so the inter-change interval estimate is
            // spacing/changes, EMA-smoothed. Changes only pull the delay
            // DOWN toward the scan-duration floor; slowing down is the
            // quiet path's job.
            let observed = (spacing_ms as f64 / changes as f64).max(1.0);
            let ema = match self.ema_inter_change_ms {
                Some(previous) => {
                    INTER_CHANGE_EMA_WEIGHT * observed + (1.0 - INTER_CHANGE_EMA_WEIGHT) * previous
                }
                None => observed,
            };
            self.ema_inter_change_ms = Some(ema);
            let target = (ema as u64).min(delay_ms).max(floor_ms);
            if target != delay_ms {
                info!(
                    event = "scheduler.cadence_adapted",
                    cause = "changes_observed",
                    changes,
                    ema_inter_change_ms = ema as u64,
                    old_delay_ms = delay_ms,
                    new_delay_ms = target,
                    "cadence pulled toward observed change rate"
                );
                delay_ms = target;
            }
        } else {
            // Quiet cycle: unbounded multiplicative growth, so idle
            // capacity never accelerates sampling beyond observed change
            // rates (spec §9.5). There is deliberately no cadence ceiling —
            // that would be a policy knob — and growth is self-correcting:
            // the EMA pull-down above shrinks the delay as soon as changes
            // reappear. Round upward so the 1 ms floor grows instead of
            // truncating 1.5 back to 1 forever. The f64→u64 cast saturates
            // at the representation limit without introducing a policy ceiling.
            let grown = ((delay_ms as f64) * QUIET_CYCLE_GROWTH).ceil() as u64;
            if grown != delay_ms {
                info!(
                    event = "scheduler.cadence_adapted",
                    cause = "quiet_cycle",
                    old_delay_ms = delay_ms,
                    new_delay_ms = grown,
                    "cadence grown after a cycle with no observed changes"
                );
                delay_ms = grown;
            }
        }

        let mut transition = None;
        if backlog > 0 {
            // Pipeline backpressure: undrained work at cycle end throttles
            // detection so coalescing can shed load (spec §9.5). Growth is
            // unbounded (no cadence ceiling) and the saturating f64→u64
            // cast bounds only the representation, never the policy.
            let grown = ((delay_ms as f64) * BACKPRESSURE_GROWTH) as u64;
            if grown != delay_ms {
                info!(
                    event = "scheduler.cadence_adapted",
                    cause = "backpressure",
                    backlog,
                    old_delay_ms = delay_ms,
                    new_delay_ms = grown,
                    "cadence grown under queue backpressure"
                );
                delay_ms = grown;
            }
            if !self.backpressure_active {
                self.backpressure_active = true;
                transition = Some(true);
            }
        } else if self.backpressure_active {
            self.backpressure_active = false;
            transition = Some(false);
        }

        // Floor enforcement last: no adaptation may schedule scans faster
        // than the last scan actually ran.
        if delay_ms < floor_ms {
            info!(
                event = "scheduler.cadence_adapted",
                cause = "scan_duration_floor",
                old_delay_ms = delay_ms,
                new_delay_ms = floor_ms,
                "cadence raised to the scan-duration floor"
            );
            delay_ms = floor_ms;
        }

        self.next_delay_ms = Some(delay_ms);
        (delay_ms, transition)
    }

    /// Back the cadence off after a failed cycle (spec §9.5 source-system
    /// pushback: errors throttle). Growth is multiplicative from the last
    /// delay — or the base retry delay when no scan has run yet — so the
    /// scheduler never crash-loops against a broken dependency. Growth is
    /// unbounded (no cadence ceiling; the saturating f64→u64 cast is a
    /// representation limit only) and self-corrects via the EMA pull-down
    /// once cycles succeed again.
    fn back_off_after_error(&mut self) -> u64 {
        let old_delay_ms = self.next_delay_ms.unwrap_or(ERROR_RETRY_BASE_MS);
        let new_delay_ms =
            ((old_delay_ms as f64) * ERROR_CYCLE_GROWTH).max(ERROR_RETRY_BASE_MS as f64) as u64;
        info!(
            event = "scheduler.cadence_adapted",
            cause = "cycle_error",
            old_delay_ms,
            new_delay_ms,
            "cadence backed off after a failed cycle"
        );
        self.next_delay_ms = Some(new_delay_ms);
        new_delay_ms
    }
}

/// Validate the fabric hot plane for the scheduler's lifetime: open
/// read-only and check the full schema contract. The scheduler never
/// creates, repairs, or migrates schema — a mismatch is the operator's
/// signal to run --setup-storage.
fn validate_fabric_plane(index_root: &Path) -> Result<(), ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    hot_plane::validate_fabric_schema(&connection)
}

/// Durably append one sync.backpressure_entered/_exited event with the
/// backlog and cadence facts of the transition. Single-statement write on
/// its own connection: the event is the only state change, so no explicit
/// transaction is needed.
fn emit_backpressure_event(
    index_root: &Path,
    entered: bool,
    source_system: &str,
    depths: &QueueDepths,
    cadence_ms: u64,
) -> Result<(), ApiError> {
    let event_type = if entered {
        SystemEventType::SyncBackpressureEntered
    } else {
        SystemEventType::SyncBackpressureExited
    };
    let mut payload = Map::new();
    payload.insert(
        "pendingDepth".to_owned(),
        Value::Number(depths.pending.into()),
    );
    payload.insert(
        "inFlightDepth".to_owned(),
        Value::Number(depths.in_flight.into()),
    );
    payload.insert("cadenceMs".to_owned(), Value::Number(cadence_ms.into()));

    let event = new_system_event(event_type, "source_system", source_system, Some(payload))?;
    let connection = hot_plane::open_write(index_root)?;
    append_event(&connection, &event)?;
    info!(
        event = "scheduler.backpressure_transition",
        entered,
        pending = depths.pending,
        in_flight = depths.in_flight,
        cadence_ms,
        "sync backpressure transition recorded"
    );
    Ok(())
}

/// Read the native URI of one staged bundle from its manifest. The error is
/// a local detail string (not an ApiError) because an unreadable manifest is
/// untrusted connector output, handled by routing the bundle through the
/// importer's malformed-bundle path.
fn staged_bundle_native_uri(bundle_dir: &Path) -> Result<String, String> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST_FILE_NAME);
    let bytes = fs::read(&manifest_path).map_err(|source| {
        format!(
            "unreadable {BUNDLE_MANIFEST_FILE_NAME} at {}: {source}",
            manifest_path.display()
        )
    })?;
    let manifest: AcquisitionBundleManifest = serde_json::from_slice(&bytes)
        .map_err(|source| format!("invalid {BUNDLE_MANIFEST_FILE_NAME}: {source}"))?;
    Ok(manifest.native_uri)
}

/// Select every claimable queue row (pending, plus stale in_flight — see
/// SELECT_PENDING_ENTRIES_SQL) into the typed model, on the caller's
/// transaction so claiming stays atomic with the read.
fn select_pending_entries(tx: &Transaction<'_>) -> Result<Vec<SyncQueueEntry>, ApiError> {
    let mut statement =
        tx.prepare(SELECT_PENDING_ENTRIES_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to prepare pending sync queue selection: {source}"),
            })?;
    // Raw SQL-typed tuples first; typed conversion happens outside the
    // row-mapping closure so shape violations surface as ApiErrors with
    // row identity, not as rusqlite conversion failures.
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, String>(11)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query pending sync queue entries: {source}"),
        })?;

    let mut entries = Vec::new();
    for row in rows {
        let (
            id,
            source_key,
            source_system,
            native_uri,
            detected_at,
            reason,
            state,
            attempt_count,
            last_attempt_at,
            last_error,
            coalesced_count,
            created_at,
        ) = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read pending sync queue row: {source}"),
        })?;
        entries.push(SyncQueueEntry {
            state: queue_state_from_wire(&state)?,
            attempt_count: queue_count(attempt_count, "sync_queue.attempt_count")?,
            coalesced_count: queue_count(coalesced_count, "sync_queue.coalesced_count")?,
            id,
            source_key,
            source_system,
            native_uri,
            detected_at,
            reason,
            last_attempt_at,
            last_error,
            created_at,
        });
    }
    Ok(entries)
}

/// Parse one persisted queue-state value through the model enum's serde
/// wire names, so the CHECK set, the enum, and this parse can never drift
/// apart (same single-source pattern as acquisition's enum_wire_name).
fn queue_state_from_wire(state: &str) -> Result<SyncQueueState, ApiError> {
    serde_json::from_value(Value::String(state.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!(
                "sync_queue state '{state}' is outside the schema CHECK set: {source}"
            ),
        }
    })
}

/// Convert an SQLite counter into u64, rejecting negatives as corruption
/// instead of wrapping.
fn queue_count(value: i64, what: &'static str) -> Result<u64, ApiError> {
    u64::try_from(value).map_err(|_| ApiError::StorageOperation {
        message: format!("{what} value {value} is negative; row is corrupt"),
    })
}

/// Publish one whole health snapshot into the shared slot. A poisoned lock
/// still guards a valid (stale) snapshot, and this write replaces the value
/// wholly, so poison is recovered — a panicked reader must not silence
/// health forever — and logged.
fn publish_health(slot: &Mutex<SyncHealth>, snapshot: &SyncHealth) {
    let mut guard = match slot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!(
                event = "scheduler.health_slot_poisoned",
                "sync health slot lock was poisoned; recovering and publishing"
            );
            poisoned.into_inner()
        }
    };
    *guard = snapshot.clone();
}

/// Publish one whole `FabricHealth` snapshot into the shared diagnostic slot
/// (C10b), mirroring `publish_health`'s whole-snapshot poison-recovered replace.
/// Same slot discipline: a panicked reader must not silence the fabric
/// diagnostic surface forever, so poison is recovered and the write replaces the
/// value wholly.
fn publish_fabric_health(slot: &Mutex<FabricHealth>, snapshot: &FabricHealth) {
    let mut guard = match slot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!(
                event = "scheduler.fabric_health_slot_poisoned",
                "fabric health slot lock was poisoned; recovering and publishing"
            );
            poisoned.into_inner()
        }
    };
    *guard = snapshot.clone();
}
