//! Source-level deletion lifecycle propagation (spec §11.2 access-lost, §11.3
//! deletion propagation, §11.4 reappearance). Location-level state — a location
//! marked `deleted` with its qualifying `absent_from_complete_enumeration`
//! evidence (§11.1), or refreshed back to `current` — is owned by
//! `crate::acquisition`. This module owns the SOURCE-level consequences that
//! ripple out of those location transitions:
//!
//! - §11.3: when a source's LAST `current` location is gone, deactivate the
//!   source from the queryable plane through the per-source cutover barrier
//!   (`source_objects.deactivated_at`), evict its dense plane, and record the
//!   `source.deactivated` event. The source stays searchable while any
//!   `current` location survives (§11.3 step 3), so this fires only when NO
//!   `current` location remains.
//! - §11.2: when a scope enumeration fails SOURCE-side, its `current` locations
//!   become `access_lost` (the document presumably still exists; observation
//!   was lost). Serving continues — no deactivation, no barrier — and the
//!   freshness clock stops simply because `last_seen_at` stops advancing.
//! - §11.4: when a previously deactivated source regains a `current` location
//!   with the same content, restore it from its ForensicSnapshot (no re-parse,
//!   no re-embedding) and clear `deactivated_at`.
//!
//! `deactivated_at`-vs-location-status invariant (embedded throughout): it is
//! the `source_objects.deactivated_at` flag, NOT location status, that removes
//! a source from All-scope search. The All-scope capture
//! (`query::execute::SELECT_ALL_ACTIVE_SQL`) filters ONLY
//! `active_parse_id IS NOT NULL AND deactivated_at IS NULL` and never joins
//! `source_locations`; marking every location `deleted` therefore does NOT hide
//! a source from an All-scope query. Only setting `deactivated_at` does.
//! `active_parse_id` is deliberately left intact (a reversible flag-clear), so
//! §11.4 reappearance is a flag-clear plus restore, never a re-activation.
//!
//! No-deletion-inference-from-failed-scans (§11.1, embedded at the access-lost
//! path): a failed or partial enumeration asserts NOTHING about absent items.
//! Access-lost is a reachability statement about locations that WERE current,
//! never a deletion, and never feeds §11.3 deactivation.

// The three entry points (`propagate_deletions`, `restore_reappeared_sources`,
// `mark_scope_access_lost`) are now live: the scheduler's post-drain and
// scan-failure steps in `run_cycle` dispatch all three (C9 wiring landed), so
// no module-level dead-code allow is needed.

use std::path::Path;

use rusqlite::{Transaction, params};
use serde_json::Map;
use tracing::{debug, error, info};

use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::identity::ApplicationIdentity;
use crate::model::SystemEventType;
use crate::primitives::utc_now;
use crate::projections::dense_cache::DenseCache;
use crate::restore::restore_source_from_snapshot;
use crate::snapshot::pre_deactivation_snapshot;
use crate::state::CutoverRegistry;

/// Log-event namespace passed to the shared hot-plane transaction helpers, so
/// this module's boundary logs stay attributable to deletion propagation.
const TX_LOG_NAMESPACE: &str = "deletion";

/// SystemEvent object_type for source_objects rows (mirrors
/// `acquisition::OBJECT_TYPE_SOURCE_OBJECT`; kept local so the two modules do
/// not couple through a private constant).
const OBJECT_TYPE_SOURCE_OBJECT: &str = "source_object";

/// SystemEvent object_type for source_locations rows.
const OBJECT_TYPE_SOURCE_LOCATION: &str = "source_location";

/// §11.3 candidate scan: every source that has AT LEAST ONE location of
/// `source_system` but NO `current` location, and is still active
/// (`active_parse_id IS NOT NULL`) and not yet deactivated
/// (`deactivated_at IS NULL`). A source with zero locations is never a
/// candidate — it was never place-bound, so there is no "last location gone"
/// event to propagate. Restricting to `source_system` bounds the scan to
/// exactly the sources this cycle's enumeration could have transitioned;
/// `deactivated_at`/`active_parse_id` are the source-level gate the §11.3
/// deactivation flips. Uncapped by construction — bounded by corpus size, and
/// the candidate set must be complete or a source with a genuinely lost last
/// location would silently stay searchable.
const SELECT_DEACTIVATION_CANDIDATES_SQL: &str = "
SELECT DISTINCT objects.id, objects.active_parse_id
FROM source_objects AS objects
JOIN source_locations AS scoped
  ON scoped.source_id = objects.id AND scoped.source_system = ?1
WHERE objects.deactivated_at IS NULL
  AND objects.active_parse_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM source_locations AS current_loc
    WHERE current_loc.source_id = objects.id
      AND current_loc.status = 'current'
  )
ORDER BY objects.id";

/// Marks a source deactivated (§11.3 step 4): the flag, not location status, is
/// what removes the source from All-scope search (see module invariant).
/// `active_parse_id` is left intact so the deactivation is a reversible
/// flag-set that §11.4 reappearance clears.
const DEACTIVATE_SOURCE_SQL: &str = "
UPDATE source_objects SET deactivated_at = ?2
WHERE id = ?1 AND deactivated_at IS NULL";

/// Clears a source's deactivation on §11.4 reappearance. Guarded on
/// `deactivated_at IS NOT NULL` so a concurrent transition cannot double-clear.
const REACTIVATE_SOURCE_SQL: &str = "
UPDATE source_objects SET deactivated_at = NULL
WHERE id = ?1 AND deactivated_at IS NOT NULL";

/// §11.4 candidate scan: every source of `source_system` that is deactivated
/// (`deactivated_at IS NOT NULL`) yet now has a `current` location — its
/// content reappeared and acquisition already flipped the location back to
/// `current` (`REFRESH_SOURCE_LOCATION_SQL`, same-hash re-observation). The
/// same-content constraint (§11.4 "same sourceHash") is already enforced
/// upstream: acquisition refreshes the location to `current` ONLY when the
/// re-observed content maps to the SAME `source_id`; different content rebinds
/// to a NEW SourceObject instead and never touches this row. So a deactivated
/// source with a `current` location is, by construction, the same content
/// reappearing. `active_parse_id` is carried out for the restore call.
const SELECT_REACTIVATION_CANDIDATES_SQL: &str = "
SELECT DISTINCT objects.id, objects.active_parse_id
FROM source_objects AS objects
JOIN source_locations AS locations
  ON locations.source_id = objects.id
     AND locations.source_system = ?1
     AND locations.status = 'current'
WHERE objects.deactivated_at IS NOT NULL
  AND objects.active_parse_id IS NOT NULL
ORDER BY objects.id";

/// §11.2 access-lost scan: every `current` location of `source_system` under
/// `scope_uri`. The in-scope filter is applied in Rust (path-component-aware),
/// mirroring `acquisition::enumeration_deletions_body`, because a source-side
/// scan failure asserts unreachability only for the scope it tried to
/// enumerate. Uncapped — bounded by corpus size; the set must be complete or a
/// reachable-yesterday location would silently keep advancing its freshness
/// clock.
const SELECT_CURRENT_LOCATIONS_FOR_SYSTEM_SQL: &str = "
SELECT id, native_uri, source_id FROM source_locations
WHERE source_system = ?1 AND status = 'current'
ORDER BY native_uri";

/// Transitions one location to `access_lost` (§11.2). The freshness clock stops
/// as a CONSEQUENCE, not a separate write: `last_seen_at` is deliberately left
/// untouched, so it stops advancing while the location is unobservable, and
/// query freshness reports the last verified time from it. Guarded on the
/// `current` precondition so a concurrent transition cannot regress a location.
const MARK_LOCATION_ACCESS_LOST_SQL: &str = "
UPDATE source_locations SET status = 'access_lost'
WHERE id = ?1 AND status = 'current'";

/// §11.3 deletion propagation entry, dispatched by the scheduler AFTER a
/// COMPLETE enumeration's location deletions have been applied
/// (`acquisition::apply_enumeration_deletions`). For every source of
/// `source_system` whose last `current` location is now gone, deactivate it
/// from the queryable plane. The location-level durable deletion records and
/// their `source.location_deleted` events were already written by acquisition
/// (§11.3 steps 1–2); this fn owns only §11.3 step 4 — the source-level
/// deactivation. §11.3 step 3 (a source with surviving `current` locations
/// stays searchable) needs no work here: such a source is simply not a
/// candidate.
///
/// Called only on a COMPLETE enumeration. A failed or partial scan asserts
/// nothing about absent items (§11.1), so it never reaches this entry and never
/// deactivates a source; that path routes to `mark_scope_access_lost` instead.
///
/// Returns the `(source_id, active_parse_id)` pairs it deactivated THIS call, so
/// the scheduler can drive each one's §11.3 trailing archive-verify-delete hot
/// cleanup over the pre-deactivation snapshot this fn minted (the SEAM in
/// `deactivate_one_source`). The order matches the candidate scan (ORDER BY
/// objects.id).
pub(crate) fn propagate_deletions(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    // Captured once at startup and threaded to the pre-deactivation snapshot
    // (2026-07-16 ruling — explicit identity, never a global).
    identity: &ApplicationIdentity,
    source_system: &str,
) -> Result<Vec<(String, String)>, ApiError> {
    let started = std::time::Instant::now();
    debug!(
        event = "deletion.propagation_started",
        source_system, "scanning for sources whose last current location is gone"
    );

    let candidates = load_deactivation_candidates(index_root, source_system)?;
    // The subset actually deactivated this call (each candidate that won its
    // deactivation race). Carried out so the scheduler runs the trailing hot
    // cleanup over exactly these, and only these, pre-deactivation snapshots.
    let mut deactivated: Vec<(String, String)> = Vec::new();
    for (source_id, active_parse_id) in candidates {
        deactivate_one_source(
            index_root,
            registry,
            dense_cache,
            identity,
            &source_id,
            &active_parse_id,
        )?;
        deactivated.push((source_id, active_parse_id));
    }

    if deactivated.is_empty() {
        debug!(
            event = "deletion.propagation_completed",
            source_system,
            deactivated_count = deactivated.len() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "deletion propagation finished"
        );
    } else {
        info!(
            event = "deletion.propagation_completed",
            source_system,
            deactivated_count = deactivated.len() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "deletion propagation finished"
        );
    }
    Ok(deactivated)
}

/// Read the §11.3 deactivation candidate set (see the SQL constant). Own
/// connection, own read; the returned `(source_id, active_parse_id)` pairs are
/// materialized before any deactivation writes so the per-source barrier +
/// transaction sequence below never races this cursor.
fn load_deactivation_candidates(
    index_root: &Path,
    source_system: &str,
) -> Result<Vec<(String, String)>, ApiError> {
    let connection = hot_plane::open_write(index_root)?;
    let mut statement = connection
        .prepare(SELECT_DEACTIVATION_CANDIDATES_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare deactivation-candidate listing: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to list deactivation candidates of {source_system}: {source}"),
        })?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read deactivation-candidate row: {source}"),
        })?);
    }
    Ok(candidates)
}

/// Deactivate ONE source (§11.3 step 4), mirroring the activation cutover's
/// publish-under-barrier discipline (`activation::gate_and_activate`).
///
/// Ordering, and why (§30.6 "before deactivating a source", §31.1 brevity):
///  1. Mint the pre-deactivation forensic snapshot FIRST, BEFORE the barrier is
///     acquired. The snapshot is I/O-heavy (artifact archival), and the barrier
///     rejects queries for the whole time it is held; §31.1 requires the hold
///     to last milliseconds, so the snapshot must NOT be inside it.
///  2. Acquire the per-source cutover barrier.
///  3. One IMMEDIATE transaction: set `deactivated_at` AND append the
///     `source.deactivated` event atomically, then commit — the event can never
///     claim a deactivation that did not durably happen, or vice versa.
///  4. While STILL barriered, evict the deactivated source's active dense plane
///     — the durable flag write and the in-memory eviction are one publish the
///     barrier serializes against another publish of the same source, exactly
///     as activation loads-then-evicts under its held guard.
///  5. Release (guard drop at scope end).
///
/// The barrier covers ONLY the flag write + evict — never the snapshot.
///
/// The §11.3 step-4 trailing archive-verify-delete hot cleanup is NOT invoked
/// here: it is driven by the scheduler AFTER `propagate_deletions` returns, over
/// the `(source_id, active_parse_id)` pairs this call reports up. Keeping it out
/// of this per-source barrier hold preserves §31.1 brevity (the cleanup locates
/// and verifies a snapshot, then runs a multi-table delete sweep — far longer
/// than a barrier may be held) and keeps deactivation reversible: the hot delete
/// is not, so it is dispatched by the caller, not folded into the flag write.
fn deactivate_one_source(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    identity: &ApplicationIdentity,
    source_id: &str,
    active_parse_id: &str,
) -> Result<(), ApiError> {
    let started = std::time::Instant::now();
    info!(
        event = "deletion.source_deactivating",
        source_id,
        active_parse_id,
        signal = "absent_from_complete_enumeration",
        "last current location gone; deactivating source from queryable plane"
    );

    // Step 1 — snapshot BEFORE the barrier (§30.6 / §31.1 brevity). The header
    // is retained for the boundary log so the audit trail names the snapshot the
    // deletion gate (C9d) will later verify over.
    let snapshot = pre_deactivation_snapshot(index_root, identity, source_id)?;

    // Step 2 — acquire the per-source barrier. Held ONLY across the flag write
    // (step 3 commit) and the dense-plane evict (step 4); dropped at return.
    let _barrier_guard = registry.acquire(source_id);

    // Step 3 — durable flag write + event, atomic in one IMMEDIATE transaction.
    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "deactivate_source")?;
    if let Err(source) = deactivate_source_body(&tx, source_id, &snapshot.id) {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "deactivate_source",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "deactivate_source")?;

    // Step 4 — dense-plane evict under the SAME held barrier (publish pairing).
    // Evicting an absent plane is a benign no-op inside `evict_parse`.
    dense_cache.evict_parse(active_parse_id);

    // Step 5 — barrier releases here (guard drop), logged with its hold
    // duration by the guard's Drop.
    //
    // The §11.3 step-4 archive-verify-delete hot cleanup of this source's hot
    // records is dispatched by the SCHEDULER after `propagate_deletions`
    // returns, over the pre-deactivation snapshot just minted (this fn reports
    // the (source_id, active_parse_id) pair up for that). It is deliberately
    // NOT run inside this barrier hold: it is not reversible and far exceeds the
    // §31.1 brevity budget, whereas deactivation itself is reversible (§11.4).

    info!(
        event = "deletion.source_deactivated",
        source_id,
        active_parse_id,
        snapshot_id = snapshot.id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "source deactivated from queryable plane; pre-deactivation snapshot minted"
    );
    Ok(())
}

/// Transactional body of `deactivate_one_source` step 3: set `deactivated_at`
/// and append `source.deactivated` on the SAME transaction, so the durable
/// flag and its audit event commit or roll back together. The UPDATE is guarded
/// on `deactivated_at IS NULL`; a zero-row result means the source was already
/// deactivated between the candidate read and here, which is a lost race, not a
/// success — surfaced as an explicit error so the caller does not append an
/// event for a no-op.
fn deactivate_source_body(
    tx: &Transaction<'_>,
    source_id: &str,
    snapshot_id: &str,
) -> Result<(), ApiError> {
    let now = utc_now()?;
    let changed = tx
        .execute(DEACTIVATE_SOURCE_SQL, params![source_id, now])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to deactivate source {source_id}: {source}"),
        })?;
    if changed == 0 {
        return Err(ApiError::StorageOperation {
            message: format!(
                "source {source_id} was already deactivated when propagation tried to \
                 deactivate it; no event appended"
            ),
        });
    }

    // The event payload records the pre-deactivation snapshot id so the audit
    // trail links the deactivation to the snapshot the deletion gate verifies
    // over. No contents or secrets are carried.
    let payload = Map::from_iter([
        entry("sourceObjectId", source_id),
        entry("snapshotId", snapshot_id),
        entry("reason", "last_current_location_gone"),
    ]);
    let event = new_system_event(
        SystemEventType::SourceDeactivated,
        OBJECT_TYPE_SOURCE_OBJECT,
        source_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;
    Ok(())
}

/// §11.4 reappearance entry, dispatched by the scheduler after the drain (its
/// location refreshes to `current` are durable). For every deactivated source
/// of `source_system` that now has a `current` location again — the same
/// content reappeared and acquisition already flipped the location back to
/// `current` — restore the source from its ForensicSnapshot and clear
/// `deactivated_at`.
///
/// Restore-failure policy (embedded): restore is attempted FIRST; only on its
/// success is `deactivated_at` cleared. If restore fails the error is explicit,
/// the source STAYS deactivated, and no partial flag-clear happens — a source
/// visible on the queryable plane without its restored hot records is a broken
/// publish, worse than staying deactivated until the next cycle retries.
pub(crate) fn restore_reappeared_sources(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    source_system: &str,
) -> Result<u64, ApiError> {
    let started = std::time::Instant::now();
    debug!(
        event = "deletion.reappearance_started",
        source_system, "scanning for deactivated sources whose content reappeared"
    );

    let candidates = load_reactivation_candidates(index_root, source_system)?;
    let mut reactivated: u64 = 0;
    for (source_id, active_parse_id) in candidates {
        // registry/dense_cache/dense_dimension are threaded through to the
        // restore fn so the restored parse's dense plane is published under the
        // per-source barrier (the escalated publish gap, now closed).
        restore_one_source(
            index_root,
            registry,
            dense_cache,
            dense_dimension,
            &source_id,
            &active_parse_id,
        )?;
        reactivated += 1;
    }

    if reactivated == 0 {
        debug!(
            event = "deletion.reappearance_completed",
            source_system,
            reactivated_count = reactivated,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "reappearance restore finished"
        );
    } else {
        info!(
            event = "deletion.reappearance_completed",
            source_system,
            reactivated_count = reactivated,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "reappearance restore finished"
        );
    }
    Ok(reactivated)
}

/// Read the §11.4 reactivation candidate set (see the SQL constant); own
/// connection, materialized before any restore so the per-source restore +
/// flag-clear below never races this cursor.
fn load_reactivation_candidates(
    index_root: &Path,
    source_system: &str,
) -> Result<Vec<(String, String)>, ApiError> {
    let connection = hot_plane::open_write(index_root)?;
    let mut statement = connection
        .prepare(SELECT_REACTIVATION_CANDIDATES_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare reactivation-candidate listing: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to list reactivation candidates of {source_system}: {source}"),
        })?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read reactivation-candidate row: {source}"),
        })?);
    }
    Ok(candidates)
}

/// Restore ONE reappeared source (§11.4): restore from its snapshot (no
/// re-parse, no re-embedding), THEN clear `deactivated_at` and append
/// `source.reactivated` atomically. Restore ordering is load-bearing per the
/// restore-failure policy on `restore_reappeared_sources`: the flag clears only
/// after restore succeeds.
fn restore_one_source(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    source_id: &str,
    active_parse_id: &str,
) -> Result<(), ApiError> {
    let started = std::time::Instant::now();
    info!(
        event = "deletion.reappearance_restore_attempt",
        source_id,
        active_parse_id,
        "content reappeared for a deactivated source; restoring from snapshot"
    );

    // Delegate to the shared restore→reactivate completion path (one source of
    // truth for the durable end state; §31.3 operator rollback in http.rs calls
    // the same wrapper). §11.4 reactivation carries the same_hash_reappearance
    // reason. On Err the source stays deactivated and no flag-clear happened.
    if let Err(source) = restore_and_reactivate_source(
        index_root,
        registry,
        dense_cache,
        dense_dimension,
        source_id,
        active_parse_id,
        "same_hash_reappearance",
    ) {
        error!(
            event = "deletion.reappearance_restore_failed",
            source_id,
            active_parse_id,
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "restore-and-reactivate failed; source stays deactivated, flag not cleared"
        );
        return Err(source);
    }

    info!(
        event = "deletion.reappearance_restored",
        source_id,
        active_parse_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "source restored from snapshot and reactivated onto the queryable plane"
    );
    Ok(())
}

/// Shared restore→reactivate completion path for BOTH the §11.4 autonomous
/// reappearance caller (`restore_one_source`) and the §31.3 operator
/// rollback-as-restore HTTP caller (`http::post_restore`'s detached task). This
/// is the ONE source of truth for the durable end state a restore must leave:
/// restore the hot records/indexes from the ForensicSnapshot for `parse_id`,
/// and ONLY on success clear `deactivated_at` and append `source.reactivated`
/// atomically. Both callers therefore leave identical durable state; the HTTP
/// path previously restored WITHOUT clearing the flag, stranding a `succeeded`
/// restore Operation whose source stayed gated out of All-scope queries.
///
/// Restore ordering is load-bearing: restore FIRST, flag-clear only on Ok. The
/// restore internally publishes the restored parse's dense plane under the
/// per-source cutover barrier (registry/dense_cache/dense_dimension threaded
/// in), so the flag-clear makes the source All-scope visible AFTER its dense
/// plane is loaded — ordering: durable restore commit → under-barrier dense
/// publish (inside restore) → this flag-clear. `reason` tags the reactivation
/// event for the calling context.
pub(crate) fn restore_and_reactivate_source(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    source_id: &str,
    parse_id: &str,
    reason: &str,
) -> Result<(), ApiError> {
    restore_source_from_snapshot(
        index_root,
        registry,
        dense_cache,
        dense_dimension,
        source_id,
        parse_id,
    )?;

    // Only after a successful restore: clear the flag and append the event
    // atomically, so the reactivated source becomes All-scope visible with its
    // hot records already restored.
    let mut connection = hot_plane::open_write(index_root)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "reactivate_source")?;
    if let Err(source) = reactivate_source_body(&tx, source_id, reason) {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "reactivate_source",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "reactivate_source")?;
    Ok(())
}

/// Transactional body of `restore_and_reactivate_source`: clear
/// `deactivated_at` and append `source.reactivated` on the SAME transaction.
/// The UPDATE is guarded on `deactivated_at IS NOT NULL`; a zero-row result
/// means the flag was cleared concurrently — surfaced as an explicit error so
/// no event is appended for a no-op. `reason` is the event payload's reason
/// tag: §11.4 reappearance passes `same_hash_reappearance`, the §31.3
/// operator rollback-as-restore passes its own reason.
fn reactivate_source_body(
    tx: &Transaction<'_>,
    source_id: &str,
    reason: &str,
) -> Result<(), ApiError> {
    let changed = tx
        .execute(REACTIVATE_SOURCE_SQL, params![source_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to reactivate source {source_id}: {source}"),
        })?;
    if changed == 0 {
        return Err(ApiError::StorageOperation {
            message: format!(
                "source {source_id} was no longer deactivated when reactivation tried to \
                 reactivate it; no event appended"
            ),
        });
    }

    let payload = Map::from_iter([entry("sourceObjectId", source_id), entry("reason", reason)]);
    let event = new_system_event(
        SystemEventType::SourceReactivated,
        OBJECT_TYPE_SOURCE_OBJECT,
        source_id,
        Some(payload),
    )?;
    append_event(tx, &event)?;
    Ok(())
}

/// §11.2 access-lost entry: on a SOURCE-side scan failure of `scope_uri`, set
/// every `current` location of `source_system` under that scope to
/// `access_lost` and append a `source.access_lost` event for each, atomically.
/// The document presumably still exists; only the ability to observe it was
/// lost — so serving CONTINUES (no deactivation, no barrier) and NO deletion is
/// inferred (§11.1: a failed enumeration asserts nothing about absent items).
/// The freshness clock stops as a consequence, not a separate write:
/// `last_seen_at` is left untouched and simply stops advancing.
///
/// `acquisition_record_id` is the scope-level failed AcquisitionRecord the
/// scheduler recorded for this source-side failure; it is carried into each
/// access-lost event so the transition traces back to the observing attempt.
pub(crate) fn mark_scope_access_lost(
    index_root: &Path,
    source_system: &str,
    scope_uri: &str,
    acquisition_record_id: &str,
) -> Result<u64, ApiError> {
    let started = std::time::Instant::now();
    info!(
        event = "deletion.access_lost_started",
        source_system,
        scope_uri,
        acquisition_record_id,
        "source-side scan failure; marking in-scope current locations access_lost"
    );

    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "mark_scope_access_lost",
    )?;
    let count = match access_lost_body(&tx, source_system, scope_uri, acquisition_record_id) {
        Ok(count) => count,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "mark_scope_access_lost",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "mark_scope_access_lost")?;

    info!(
        event = "deletion.access_lost_applied",
        source_system,
        scope_uri,
        access_lost_count = count,
        acquisition_record_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "access-lost transitions applied"
    );
    Ok(count)
}

/// Transactional body of `mark_scope_access_lost`: select the current
/// locations, transition each in-scope one to `access_lost`, and append its
/// `source.access_lost` event on the same transaction. Returns the count
/// transitioned.
fn access_lost_body(
    tx: &Transaction<'_>,
    source_system: &str,
    scope_uri: &str,
    acquisition_record_id: &str,
) -> Result<u64, ApiError> {
    let mut statement = tx
        .prepare(SELECT_CURRENT_LOCATIONS_FOR_SYSTEM_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare current-location listing: {source}"),
        })?;
    let rows = statement
        .query_map(params![source_system], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to list current locations of {source_system}: {source}"),
        })?;
    // Materialize before writing so the UPDATEs never race the SELECT cursor
    // over the same table (same discipline as enumeration_deletions_body).
    let mut current_locations = Vec::new();
    for row in rows {
        current_locations.push(row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read current-location row of {source_system}: {source}"),
        })?);
    }
    drop(statement);

    let mut count: u64 = 0;
    for (location_id, native_uri, source_object_id) in current_locations {
        // A source-side scan failure asserts unreachability only for the scope
        // it tried to enumerate (§11.2 is scope-local). The in-scope match is
        // path-component-aware (exact scope or under "{scope}/") and done in
        // Rust rather than SQL LIKE, which would treat % and _ in the scope as
        // wildcards — mirroring enumeration_deletions_body exactly.
        let in_scope = native_uri == scope_uri
            || native_uri
                .strip_prefix(scope_uri)
                .is_some_and(|rest| rest.starts_with('/'));
        if !in_scope {
            continue;
        }

        // Freshness clock stop is a consequence, not a write: last_seen_at is
        // deliberately left untouched, so it stops advancing while the location
        // is unobservable (§11.2). Guarded on status='current'.
        let changed = tx
            .execute(MARK_LOCATION_ACCESS_LOST_SQL, params![location_id])
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to mark source location {location_id} access_lost: {source}"
                ),
            })?;
        if changed == 0 {
            // The location left `current` between the SELECT and here; nothing
            // to transition, so no event either.
            continue;
        }

        let payload = Map::from_iter([
            entry("sourceSystem", source_system),
            entry("nativeUri", &native_uri),
            entry("sourceObjectId", &source_object_id),
            entry("acquisitionRecordId", acquisition_record_id),
        ]);
        let event = new_system_event(
            SystemEventType::SourceAccessLost,
            OBJECT_TYPE_SOURCE_LOCATION,
            &location_id,
            Some(payload),
        )?;
        append_event(tx, &event)?;

        info!(
            event = "deletion.location_access_lost",
            source_location_id = location_id,
            source_system,
            native_uri,
            source_object_id,
            acquisition_record_id,
            "location transitioned to access_lost: source-side scan failure"
        );
        count += 1;
    }
    Ok(count)
}
