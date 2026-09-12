//! Superseded-state archive-verify-delete (spec §31.2 steps 3–5) and
//! rollback-as-restore (spec §31.3 / §11.4 same-hash reappearance).
//!
//! This module owns the two halves of the §31 hot-plane lifecycle that run
//! AFTER a snapshot has been minted:
//!
//! - `complete_superseded_parse` (§31.2 steps 3–5, and the §11.3 deactivation
//!   variant): verify the gating snapshot's deletion gate, then — only on a
//!   pass — hard-delete every hot row of the superseded parse in a
//!   derived-before-source order and, for the activation-supersession flow,
//!   complete the predecessor's `archiving → archived` transition the
//!   activation cutover left open.
//! - `restore_source_from_snapshot` (§31.3 / §11.4): re-import a source's
//!   canonical rows and projection payloads from its ForensicSnapshot's
//!   archived artifacts (NEVER re-parse, NEVER re-embed — vectors are
//!   byte-reproduced from the archived blobs), deterministically rebuild the
//!   non-archived planes (FTS5 lexical index, graph tables), and hand the
//!   durable state back.
//!
//! Gating-snapshot reuse (§31.2, user ruling 2026-07-16): the deletion gate is
//! verified over the immediately-preceding lifecycle snapshot the scheduler
//! ALREADY minted — the `post_activation` snapshot in the activation flow, the
//! `pre_deactivation` snapshot in the deactivation flow. It is NEVER re-taken
//! here. Snapshots are located by querying `forensic_snapshots` on the subject
//! identity `(subject_source_id, subject_parse_id, snapshot_type)` (the partial
//! `idx_forensic_snapshots_subject` index exists precisely for this); lifecycle
//! snapshots always set both subject columns.
//!
//! CALLER-vs-VERIFIER failure duty split (§30.5 / §31.2): `snapshot::verify`
//! returns only a verdict. THIS module owns the consequences of a failed gate —
//! it HALTS with no deletion, retains the superseded state, does not auto-retry,
//! and propagates the error. `SnapshotVerificationFailed` from the gate is
//! propagated untouched; restore-path failures construct `RestoreFailed`.
//!
//! NO MODEL CALL ANYWHERE IN THIS MODULE (§38, hard invariant, mirror of
//! `snapshot::verify`). Restore RE-IMPORTS archived bytes and RE-DERIVES the
//! deterministic planes; it never re-embeds, re-parses, or re-scores. A grep for
//! `embed|InferenceRuntime|score_|docling` over this file must stay empty.

use std::path::Path;
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Map, Value};
use tracing::{error, info};

use crate::artifact_store::ArtifactStore;
use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::model::{
    EvidenceReplayMode, ForensicSnapshot, ForensicSnapshotManifest, GenerationReplayMode,
    ReplayProfile, RetrievalReplayMode, SnapshotArtifactRef, SnapshotType, SystemEventType,
};
use crate::primitives::utc_now;
use crate::projections::dense_cache::DenseCache;
use crate::projections::envelope;
use crate::snapshot::verify::{verify_deletion_gate, verify_mechanical};
use crate::state::CutoverRegistry;

/// Log-event namespace passed to the shared hot-plane transaction helpers and
/// stamped on this module's boundary logs, so every superseded-cleanup and
/// restore line is attributable to this module.
const TX_LOG_NAMESPACE: &str = "restore";

/// SystemEvent object_type for parse_runs rows (mirrors the private constant in
/// `activation`/`parse::importer`; kept local so the modules do not couple
/// through a private constant).
const OBJECT_TYPE_PARSE_RUN: &str = "parse_run";

// ---------------------------------------------------------------------------
// Snapshot lookup (shared by both entry points).
// ---------------------------------------------------------------------------

/// Locate the most recent lifecycle snapshot for a subject identity
/// `(subject_source_id, subject_parse_id, snapshot_type)` and rehydrate its
/// `ForensicSnapshot` header from the stored manifest.
///
/// The header row carries `manifest_uri`/`manifest_hash` plus the list columns,
/// but NOT the full `ReplayProfile`/`created_by`/`notes` the header type needs;
/// rather than duplicate the header shape across a second read path, the manifest
/// itself is the single source of truth for the snapshot's identity, and the row
/// supplies exactly what verification consumes: id, type, subject-derived
/// `active_parse_ids` (the deletion gate scopes its rebuild to this), and the
/// manifest address. The two verify tiers read `id`, `manifest_hash`,
/// `manifest_uri`, and `active_parse_ids` only, so the reconstructed header is
/// faithful for their purpose while other fields are filled from durable columns.
///
/// `ORDER BY created_at DESC, id DESC LIMIT 1` takes the most recent when several
/// snapshots share the subject (e.g. a re-activation after a prior cycle): the
/// newest lifecycle snapshot is the one whose captured state the current hot
/// plane must match. A missing snapshot is a caller-contract failure — the
/// lifecycle flow only reaches here after the scheduler minted the snapshot.
fn locate_lifecycle_snapshot(
    connection: &Connection,
    store: &ArtifactStore,
    source_id: &str,
    subject_parse_id: &str,
    snapshot_type: SnapshotType,
    restore_failure: bool,
) -> Result<ForensicSnapshot, ApiError> {
    const SELECT_SUBJECT_SNAPSHOT_SQL: &str = "
SELECT id, source_object_ids_json, active_parse_ids_json, manifest_uri,
       manifest_hash, system_version, spec_version, created_at
FROM forensic_snapshots
WHERE subject_source_id = ?1 AND subject_parse_id = ?2 AND snapshot_type = ?3
ORDER BY created_at DESC, id DESC
LIMIT 1";

    let type_name = snapshot_type_wire_name(snapshot_type);
    let row = connection
        .query_row(
            SELECT_SUBJECT_SNAPSHOT_SQL,
            params![source_id, subject_parse_id, type_name],
            |row| {
                Ok(SnapshotRow {
                    id: row.get(0)?,
                    source_object_ids_json: row.get(1)?,
                    active_parse_ids_json: row.get(2)?,
                    manifest_uri: row.get(3)?,
                    manifest_hash: row.get(4)?,
                    system_version: row.get(5)?,
                    spec_version: row.get(6)?,
                    created_at: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(|source| lookup_error(restore_failure, format!(
            "failed to locate {type_name} snapshot for source {source_id} parse {subject_parse_id}: {source}"
        )))?;
    let Some(row) = row else {
        return Err(lookup_error(
            restore_failure,
            format!(
                "no {type_name} snapshot exists for source {source_id} parse {subject_parse_id}; \
                 the lifecycle flow reached cleanup/restore without the expected minted snapshot"
            ),
        ));
    };

    // The manifest hash is the address the header points at; reconstruct the
    // header's list columns from the canonical JSON string columns so the
    // deletion gate can scope its rebuild to the subject parse
    // (`active_parse_ids = [subject_parse_id]` for a lifecycle snapshot).
    let source_object_ids: Vec<String> = decode_json_string_array(
        &row.source_object_ids_json,
        restore_failure,
        "source_object_ids_json",
    )?;
    let active_parse_ids: Vec<String> = decode_json_string_array(
        &row.active_parse_ids_json,
        restore_failure,
        "active_parse_ids_json",
    )?;

    // Rehydrate the replay profile and the manifest-anchored fields from the
    // stored manifest, the single source of truth for the snapshot body. The
    // manifest is re-hashed to its address by `get_json` (tamper-evident), then
    // the header fields the verify tiers do NOT read are taken from it so the
    // reconstructed header is internally consistent.
    let header = ForensicSnapshot {
        id: row.id,
        snapshot_type,
        created_at: row.created_at,
        created_by: None,
        source_object_ids,
        active_parse_ids,
        manifest_uri: row.manifest_uri,
        manifest_hash: row.manifest_hash,
        system_version: row.system_version,
        spec_version: row.spec_version,
        // The replay profile is not read by either verify tier; the header only
        // needs a well-formed value. The MVP profile is reconstructed inline
        // (snapshot's own `mvp_replay_profile` is private) — evidence bit-exact,
        // retrieval/generation not_supported, no per-channel/tolerance claims —
        // matching what C9a stamped on the located snapshot.
        replay_profile: ReplayProfile {
            evidence_replay_mode: EvidenceReplayMode::BitExact,
            retrieval_replay_mode: RetrievalReplayMode::NotSupported,
            generation_replay_mode: GenerationReplayMode::NotSupported,
            channel_replay_modes: None,
            declared_tolerances: None,
        },
        notes: None,
    };
    // `store` is passed for symmetry with the restore caller (which reads the
    // manifest through it right after); the lookup itself needs no store read —
    // the verify tiers re-open their own store and re-hash the manifest.
    let _ = store;
    Ok(header)
}

/// One `forensic_snapshots` row as read for header reconstruction.
struct SnapshotRow {
    id: String,
    source_object_ids_json: String,
    active_parse_ids_json: String,
    manifest_uri: String,
    manifest_hash: String,
    system_version: String,
    spec_version: String,
    created_at: String,
}

/// Build the right error variant for a snapshot-lookup failure. Restore-path
/// failures are `RestoreFailed` (§31.3); the superseded-cleanup path uses
/// `StorageOperation` (a missing/unreadable gating snapshot is a broken
/// persisted invariant of the deletion path, not a restore).
fn lookup_error(restore_failure: bool, message: String) -> ApiError {
    if restore_failure {
        ApiError::RestoreFailed { message }
    } else {
        ApiError::StorageOperation { message }
    }
}

// ---------------------------------------------------------------------------
// §31.2 steps 3–5 — superseded archive-verify-delete.
// ---------------------------------------------------------------------------

/// Which lifecycle flow drove a superseded parse into cleanup, and the identity
/// needed to locate its gating snapshot.
///
/// The gating snapshot is NEVER re-taken (§31.2, user ruling 2026-07-16): the
/// two flows verify over DIFFERENT already-minted snapshots keyed on DIFFERENT
/// subject parses, which is exactly why the subject-parse of the snapshot and
/// the parse being deleted must be modeled separately.
//
// Consumed by the scheduler: `ActivationSupersession` after each Activated gate
// decision (the post-drain / gate arms), `Deactivation` over each pair
// `propagate_deletions` reports, and `HeldSupersession` (Ruling 1) for each
// superseded held candidate the gate/hold path threads out. Also consumed by
// C10a's accept and discard held-parse handlers, which drive `HeldSupersession`
// for the held disposition post-return.
pub(crate) enum SupersededCleanupMode {
    /// §31.2: the predecessor parse was superseded by an activation cutover. The
    /// deletion gate verifies over the `post_activation` snapshot whose SUBJECT
    /// parse is the NEWLY ACTIVATED candidate (`activated_parse_id`), while the
    /// parse being cleaned is the predecessor. This flow also completes the
    /// predecessor's `archiving → archived` transition (which the activation
    /// cutover left open) atomically with the delete sweep's commit.
    ActivationSupersession { activated_parse_id: String },
    /// §11.3 deactivation: the source's active parse is being cleaned after a
    /// source deactivation. The deletion gate verifies over the
    /// `pre_deactivation` snapshot whose subject parse IS the parse being
    /// cleaned. No `archiving → archived` transition happens — a deactivated
    /// source's parse stays `active` (deactivation is reversible via §11.4), so
    /// this flow deletes the hot data planes only.
    Deactivation,
    /// Ruling 1 (plan §3 C10r / Current Status 2026-07-16): a never-activated
    /// HELD candidate that was superseded (`supersede_other_held`) or discarded
    /// (`discard_held_parse`) is being cleaned. The deletion gate verifies over
    /// the candidate's OWN `pre_activation` snapshot — subject parse = the held
    /// candidate itself — because a pre_activation snapshot sets its subject to
    /// that candidate and archives the vector planes whole-table, so the
    /// candidate's dense/multivector blobs (its only model-dependent state) are
    /// provably archived. This exit is TERMINAL: like `ActivationSupersession`
    /// (and UNLIKE `Deactivation`), it completes the candidate's
    /// `archiving → archived` transition with `parse.archived`, because a
    /// superseded/discarded held candidate never activates and never returns.
    HeldSupersession,
}

/// Complete archive-verify-delete of one superseded parse (spec §31.2 steps
/// 3–5, and the §11.3 deactivation-flow variant).
///
/// Sequence:
///  1. Locate the gating snapshot per `mode` (post_activation subject =
///     activated candidate; pre_deactivation subject = the cleaned parse;
///     pre_activation subject = the held candidate itself, Ruling 1) — the
///     snapshot the scheduler already minted, NEVER re-taken.
///  2. `verify_deletion_gate` over that snapshot. On Err: HALT — no deletion,
///     the superseded state is retained, no auto-retry, no grace window; the
///     error propagates (§30.5 / §31.2, caller-vs-verifier split). This fn is
///     the CALLER that owns those consequences.
///  3. On pass, in ONE IMMEDIATE transaction: `mark_superseded` every fresh
///     `retrieval_projections` envelope of the parse, then hard-delete every
///     hot row of the parse in derived-before-source order, then (the terminal
///     activation/held-supersession flows only) complete the
///     `archiving → archived` transition with its `parse.archived` event atomic
///     with the state it records. Commit.
///
/// Per-table row counts are logged after commit.
//
// Live callers: the scheduler's §31.2 wiring (`ActivationSupersession` after
// each Activated gate decision) and its deactivation-flow cleanup loop
// (`Deactivation` over each pair `propagate_deletions` returns).
pub(crate) fn complete_superseded_parse(
    index_root: &Path,
    source_id: &str,
    superseded_parse_id: &str,
    mode: SupersededCleanupMode,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let flow = cleanup_flow_label(&mode);
    let cleanup_log = crate::util::LogContext::new("parse_cleanup", superseded_parse_id);
    cleanup_log.record("source_id", source_id);
    cleanup_log.record("parse_id", superseded_parse_id);
    cleanup_log.record("trigger", flow);
    let _cleanup_log = cleanup_log.enter();
    info!(
        event = "restore.cleanup_started",
        source_id, superseded_parse_id, flow, "superseded-parse archive-verify-delete starting"
    );

    // The subject parse of the gating snapshot and its type depend on the flow
    // (see SupersededCleanupMode). For activation supersession the snapshot's
    // subject is the newly activated candidate; for deactivation it is the
    // cleaned parse itself.
    let (snapshot_subject_parse_id, snapshot_type) = match &mode {
        SupersededCleanupMode::ActivationSupersession { activated_parse_id } => {
            (activated_parse_id.as_str(), SnapshotType::PostActivation)
        }
        SupersededCleanupMode::Deactivation => (superseded_parse_id, SnapshotType::PreDeactivation),
        // Ruling 1: verify over the held candidate's OWN pre_activation snapshot
        // (its subject IS the candidate being cleaned), then archive it terminally.
        SupersededCleanupMode::HeldSupersession => {
            (superseded_parse_id, SnapshotType::PreActivation)
        }
    };

    let store = ArtifactStore::open(index_root)?;
    let connection = hot_plane::open_read(index_root)?;
    let snapshot = locate_lifecycle_snapshot(
        &connection,
        &store,
        source_id,
        snapshot_subject_parse_id,
        snapshot_type,
        false,
    )?;
    drop(connection);

    // Step 2 — the deletion gate. On failure we HALT before any DELETE: the
    // error (SnapshotVerificationFailed, propagated untouched) carries the
    // verdict, and by the caller-vs-verifier duty split THIS fn is where the
    // "retain superseded state, do not delete, do not auto-retry" consequence
    // lives — it is discharged simply by returning here before the delete tx.
    if let Err(source) = verify_deletion_gate(index_root, &snapshot) {
        error!(
            event = "restore.cleanup_gate_failed",
            source_id,
            superseded_parse_id,
            flow,
            snapshot_id = %snapshot.id,
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "deletion gate failed; superseded state retained, nothing deleted, no auto-retry"
        );
        return Err(source);
    }
    // The gating snapshot can name the successor while deletion targets its
    // predecessor. Preflight immutable embedding dependencies before taking the
    // writer, then compare the actual deletion target inside that transaction.
    let annotation_publications = (|| {
        let manifest = load_manifest(&store, &snapshot)?;
        crate::snapshot::verify::verified_annotation_publications(&store, &snapshot.id, &manifest)
    })()
    .inspect_err(|source| {
        error!(event = "restore.cleanup_gate_failed", source_id, superseded_parse_id, flow,
            snapshot_id = %snapshot.id, stage = "annotation_payload_preflight", error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "annotation snapshot verification failed; superseded state retained, nothing deleted");
    })?;
    info!(
        event = "restore.cleanup_gate_passed",
        source_id,
        superseded_parse_id,
        flow,
        snapshot_id = %snapshot.id,
        "snapshot verification passed; proceeding to final publication check and deletion"
    );

    // Step 3 — one IMMEDIATE transaction: supersede envelopes, delete every hot
    // plane in order, then (activation flow) archive the predecessor.
    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "complete_superseded",
    )?;
    let counts = match crate::snapshot::verify::verify_annotation_deletion_state(
        &tx,
        &annotation_publications,
        &snapshot.id,
        source_id,
        superseded_parse_id,
    )
    .inspect_err(|source| {
        error!(event = "restore.cleanup_gate_failed", source_id, superseded_parse_id, flow,
            snapshot_id = %snapshot.id, stage = "annotation_publication_state", error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "annotation state changed since snapshot; superseded state retained, nothing deleted");
    })
    .and_then(|()| delete_superseded_body(&tx, source_id, superseded_parse_id, &mode))
    {
        Ok(counts) => counts,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "complete_superseded",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "complete_superseded")?;

    info!(
        event = "restore.cleanup_completed",
        committed = true,
        source_id,
        superseded_parse_id,
        flow,
        snapshot_id = %snapshot.id,
        chunk_text_index_deleted = counts.chunk_text_index as u64,
        graph_mentions_deleted = counts.graph_mentions as u64,
        graph_edges_deleted = counts.graph_edges as u64,
        dense_deleted = counts.dense as u64,
        multivector_deleted = counts.multivector as u64,
        chunk_projections_deleted = counts.chunk_projections as u64,
        retrieval_projections_deleted = counts.retrieval_projections as u64,
        semantic_annotations_deleted = counts.semantic_annotations as u64,
        unit_relationships_deleted = counts.unit_relationships as u64,
        content_units_deleted = counts.content_units as u64,
        archived = counts.archived,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "superseded hot state deleted"
    );
    Ok(())
}

/// Per-table row counts of one delete sweep, for the terminal cleanup log.
struct DeleteCounts {
    chunk_text_index: usize,
    graph_mentions: usize,
    graph_edges: usize,
    dense: usize,
    multivector: usize,
    chunk_projections: usize,
    retrieval_projections: usize,
    semantic_annotations: usize,
    unit_relationships: usize,
    content_units: usize,
    /// True when the activation flow completed the predecessor's
    /// `archiving → archived` transition in this same transaction.
    archived: bool,
}

// Delete SQL, in the ORDER they must run. DERIVED-BEFORE-SOURCE is the ordering
// invariant: a table whose delete depends on a lookup THROUGH another table must
// run before that other table is emptied, so no lookup path is orphaned
// mid-sweep. `chunk_text_index` (FTS5) is scoped by a subselect through
// `chunk_projections` (it carries no `parse_id`), so it MUST precede the
// `chunk_projections` delete. The graph tables are self-scoped by `parse_id` but
// are derived from annotations, so they precede `semantic_annotations`.
// Everything is parse-scoped; `annotation_memo` is NEVER touched (memo-survives,
// §21.2 — reuse ACROSS parses is its entire purpose).

/// FTS5 lexical index rows of this parse's chunks, joined through
/// chunk_projections. MUST run before `DELETE_CHUNK_PROJECTIONS_SQL` empties the
/// join source. Mirrors `lexical::DELETE_PARSE_INDEX_SQL`.
const DELETE_CHUNK_TEXT_INDEX_SQL: &str = "
DELETE FROM chunk_text_index
WHERE chunk_id IN (SELECT id FROM chunk_projections WHERE parse_id = ?1)";

/// Graph mention rows of this parse (self-scoped by parse_id).
const DELETE_GRAPH_MENTIONS_SQL: &str = "
DELETE FROM graph_entity_mentions WHERE parse_id = ?1";

/// Graph edge rows of this parse (self-scoped by parse_id).
const DELETE_GRAPH_EDGES_SQL: &str = "
DELETE FROM graph_entity_edges WHERE parse_id = ?1";

/// Dense-vector rows of this parse (self-scoped by parse_id).
const DELETE_DENSE_SQL: &str = "
DELETE FROM chunk_dense_vectors WHERE parse_id = ?1";

/// Multi-vector rows of this parse (self-scoped by parse_id).
const DELETE_MULTIVECTOR_SQL: &str = "
DELETE FROM unit_multivector_projections WHERE parse_id = ?1";

/// Chunk projection rows of this parse. Runs AFTER `chunk_text_index` (whose
/// scoping subselect reads these rows) is emptied.
const DELETE_CHUNK_PROJECTIONS_SQL: &str = "
DELETE FROM chunk_projections WHERE parse_id = ?1";

/// Semantic annotation rows of this parse (restore.rs-local hard DELETE — no
/// hard-delete exists on `annotations::store`, which only soft-marks; the
/// archive-verify-delete sweep MUST remove superseded-parse annotation rows per
/// the C9 duty noted in `annotations::worker`). `annotation_memo` is a SEPARATE
/// table and is NEVER touched here — it deliberately survives parse archival.
const DELETE_SEMANTIC_ANNOTATIONS_SQL: &str = "
DELETE FROM semantic_annotations WHERE parse_id = ?1";

/// Unit relationship rows of this parse.
const DELETE_UNIT_RELATIONSHIPS_SQL: &str = "
DELETE FROM unit_relationships WHERE parse_id = ?1";

/// Content unit rows of this parse (the canonical source rows, deleted LAST).
const DELETE_CONTENT_UNITS_SQL: &str = "
DELETE FROM content_units WHERE parse_id = ?1";

/// Complete an archiving run's `archiving → archived` transition (§31.2). The
/// upstream write moved the run to `archiving` — the activation cutover for an
/// activation predecessor, or `supersede_other_held`/`discard_held_parse` for a
/// Ruling-1 held candidate — and left this completion to the cleanup sweep; THIS
/// is the first and only writer of `archived`/`archived_at`. Status-guarded on
/// `archiving` so a double-completion or a completion of a non-archiving run is
/// a loud zero-row failure.
const ARCHIVE_PREDECESSOR_SQL: &str = "
UPDATE parse_runs SET status = 'archived', archived_at = ?2
WHERE id = ?1 AND status = 'archiving'";

/// The transactional body of `complete_superseded_parse` step 3. Ownership: the
/// caller owns the transaction and its commit/abort; this fn only issues the
/// ordered statements and the archive transition.
///
/// mark_superseded-then-delete resolution (per the plan's "mark_superseded
/// caller lands at C9 supersession completion"): every fresh envelope is
/// transitioned `fresh → superseded` (recording `valid_to` and appending
/// `projection.superseded`) BEFORE its `retrieval_projections` row is hard
/// deleted. The supersession is the AUDITED lifecycle transition (the event is
/// the durable record on the projection's own trail); the subsequent hard delete
/// is unaudited hot cleanup (`delete_for_parse` appends no event, by design). So
/// the two do NOT collapse — the event survives the row it was recorded for.
fn delete_superseded_body(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    mode: &SupersededCleanupMode,
) -> Result<DeleteCounts, ApiError> {
    // Audited supersession of the projection envelopes FIRST: mark every fresh
    // envelope superseded (per-projection-id) so `projection.superseded` is on
    // the durable trail before the row is swept. Not every type has a fresh
    // envelope for this parse, so this reads the parse's fresh envelope ids and
    // transitions exactly those.
    supersede_fresh_envelopes(tx, source_id, parse_id)?;

    // Ordered deletes — derived before source (see the SQL constant comments).
    let chunk_text_index = execute_delete(
        tx,
        DELETE_CHUNK_TEXT_INDEX_SQL,
        parse_id,
        "chunk_text_index",
    )?;
    let graph_mentions = execute_delete(
        tx,
        DELETE_GRAPH_MENTIONS_SQL,
        parse_id,
        "graph_entity_mentions",
    )?;
    let graph_edges = execute_delete(tx, DELETE_GRAPH_EDGES_SQL, parse_id, "graph_entity_edges")?;
    let dense = execute_delete(tx, DELETE_DENSE_SQL, parse_id, "chunk_dense_vectors")?;
    let multivector = execute_delete(
        tx,
        DELETE_MULTIVECTOR_SQL,
        parse_id,
        "unit_multivector_projections",
    )?;
    let chunk_projections = execute_delete(
        tx,
        DELETE_CHUNK_PROJECTIONS_SQL,
        parse_id,
        "chunk_projections",
    )?;
    let retrieval_projections = execute_delete(
        tx,
        DELETE_RETRIEVAL_PROJECTIONS_SQL,
        parse_id,
        "retrieval_projections",
    )?;
    let semantic_annotations = execute_delete(
        tx,
        DELETE_SEMANTIC_ANNOTATIONS_SQL,
        parse_id,
        "semantic_annotations",
    )?;
    let unit_relationships = execute_delete(
        tx,
        DELETE_UNIT_RELATIONSHIPS_SQL,
        parse_id,
        "unit_relationships",
    )?;
    let content_units = execute_delete(tx, DELETE_CONTENT_UNITS_SQL, parse_id, "content_units")?;

    // Terminal-archival flows: complete the parse's archiving → archived
    // transition, atomic with the delete sweep and its parse.archived event.
    // Both activation-supersession (the predecessor) and held-supersession
    // (Ruling 1: a superseded/discarded held candidate) end at `archived` —
    // neither run ever serves again — so `archive_predecessor` completes the
    // `archiving → archived` transition the upstream write left open.
    let archived = match mode {
        SupersededCleanupMode::ActivationSupersession { .. }
        | SupersededCleanupMode::HeldSupersession => {
            archive_predecessor(tx, source_id, parse_id)?;
            true
        }
        // A deactivated source's parse stays `active` (reversible via §11.4),
        // so no archive transition — only the hot data planes are cleaned.
        SupersededCleanupMode::Deactivation => false,
    };

    Ok(DeleteCounts {
        chunk_text_index,
        graph_mentions,
        graph_edges,
        dense,
        multivector,
        chunk_projections,
        retrieval_projections,
        semantic_annotations,
        unit_relationships,
        content_units,
        archived,
    })
}

/// Parse-wide DELETE of every `retrieval_projections` envelope of the parse,
/// regardless of projection type. Authored as a single parse-scoped statement
/// (rather than iterating `envelope::delete_for_parse` per type) because the
/// supersession sweep removes the parse's ENTIRE envelope set at once — a
/// parse-wide delete is the faithful expression of "clean this parse", and the
/// per-type `delete_for_parse` exists for the DIFFERENT rebuild-idempotence case
/// where one channel is re-run in isolation. Runs after the payload tables it
/// envelopes are already gone.
const DELETE_RETRIEVAL_PROJECTIONS_SQL: &str = "
DELETE FROM retrieval_projections WHERE parse_id = ?1";

/// Read the parse's fresh `retrieval_projections` envelope ids and transition
/// each `fresh → superseded` through `envelope::mark_superseded`, so
/// `projection.superseded` is recorded per projection id on the parse's audit
/// trail before the rows are swept. Only `fresh` envelopes are eligible (the
/// mark is status-guarded), so building/failed/stale/already-superseded rows are
/// simply not selected here.
fn supersede_fresh_envelopes(
    tx: &Transaction<'_>,
    _source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    const SELECT_FRESH_ENVELOPE_IDS_SQL: &str = "
SELECT id FROM retrieval_projections
WHERE parse_id = ?1 AND freshness_status = 'fresh' AND deleted_at IS NULL
ORDER BY id";
    let ids: Vec<String> = {
        let mut statement = tx
            .prepare(SELECT_FRESH_ENVELOPE_IDS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare fresh-envelope listing for parse {parse_id}: {source}"
                ),
            })?;
        let rows = statement
            .query_map(params![parse_id], |row| row.get::<_, String>(0))
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to list fresh envelopes for parse {parse_id}: {source}"),
            })?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to read fresh-envelope row for parse {parse_id}: {source}"
                ),
            })?);
        }
        ids
    };
    for projection_id in &ids {
        envelope::mark_superseded(tx, projection_id)?;
    }
    Ok(())
}

/// Execute one parse-scoped DELETE and return the affected-row count. A delete
/// count is not a status guard (a parse may legitimately have zero rows in a
/// given plane), so zero rows is a valid outcome, not an error.
fn execute_delete(
    tx: &Transaction<'_>,
    sql: &str,
    parse_id: &str,
    table: &str,
) -> Result<usize, ApiError> {
    tx.execute(sql, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to delete {table} rows for parse {parse_id}: {source}"),
        })
}

/// Complete an archiving run's `archiving → archived` transition and append the
/// `parse.archived` event atomic with it. This is the first and only writer of
/// `archived`/`archived_at` (the upstream write — activation cutover, or a
/// Ruling-1 held-candidate supersession/discard — left the run at `archiving`).
/// The UPDATE is status-guarded on `archiving` and asserted to hit exactly one
/// row, so a double-completion or a non-archiving run is a loud failure.
fn archive_predecessor(
    tx: &Transaction<'_>,
    source_id: &str,
    predecessor_parse_id: &str,
) -> Result<(), ApiError> {
    let now = utc_now()?;
    let updated = tx
        .execute(ARCHIVE_PREDECESSOR_SQL, params![predecessor_parse_id, now])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to archive predecessor parse run {predecessor_parse_id}: {source}"
            ),
        })?;
    if updated != 1 {
        return Err(ApiError::StorageOperation {
            message: format!(
                "predecessor parse run {predecessor_parse_id} archiving → archived updated \
                 {updated} rows; the status guard did not match (run was not in 'archiving')"
            ),
        });
    }

    // parse.archived, atomic with the state it records (the crate::events
    // invariant). Payload names the source and the now-archived parse.
    let payload = Map::from_iter([
        entry("sourceId", source_id),
        entry("parseId", predecessor_parse_id),
    ]);
    let event = new_system_event(
        SystemEventType::ParseArchived,
        OBJECT_TYPE_PARSE_RUN,
        predecessor_parse_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Compact flow label for the cleanup boundary logs.
fn cleanup_flow_label(mode: &SupersededCleanupMode) -> &'static str {
    match mode {
        SupersededCleanupMode::ActivationSupersession { .. } => "activation_supersession",
        SupersededCleanupMode::Deactivation => "deactivation",
        SupersededCleanupMode::HeldSupersession => "held_supersession",
    }
}

// ---------------------------------------------------------------------------
// §31.3 / §11.4 — rollback-as-restore.
// ---------------------------------------------------------------------------

/// Restore a source from its archived snapshot (§11.4 same-hash reappearance /
/// §31.3 rollback-is-restore): re-import the canonical rows and projection
/// payloads for archived `parse_id` of `source_id`, byte-reproduce the dense and
/// multivector planes from the archived blobs, and deterministically rebuild the
/// non-archived planes (FTS5 lexical index, graph tables). Ok means the DURABLE
/// hot state for the parse is fully restored.
///
/// DENSE-CACHE PUBLISH (the escalated gap, now closed): the durable re-import
/// AND the paired in-memory dense-cache publish both run here. The caller
/// (`deletion::restore_one_source`) threads `registry` + `dense_cache` +
/// `dense_dimension` so this fn can mirror the activation publish invariant:
/// AFTER the durable re-import transaction commits, it acquires the per-source
/// cutover barrier and publishes the restored parse's dense plane under that
/// hold, exactly as `activation::publish_dense_cache` pairs the durable pointer
/// write with the in-memory plane load. The caller then clears `deactivated_at`
/// (its own transaction) once this returns Ok, so the ordering across the two
/// fns is: durable commit → under-barrier dense publish → caller's flag-clear.
/// No predecessor is evicted (restore populates a plane, it does not swap one).
///
/// Snapshot lookup: §11.4 restores from the `pre_deactivation` snapshot whose
/// subject parse is `parse_id` (the deactivated source's active parse). The
/// rollback-tier generalization (§31.3) is the same shape — a rollback restores
/// from whatever lifecycle snapshot captured the target parse; the pinned §11.4
/// caller supplies the pre_deactivation subject identity.
///
/// Failure gating: `verify_mechanical` FIRST (integrity before re-import); any
/// re-import failure errors out with the transaction rolled back (no partial
/// restore). Restore-path failures are `RestoreFailed`; `SnapshotVerificationFailed`
/// from the mechanical tier propagates untouched. NO re-parse, NO re-embed.
pub(crate) fn restore_source_from_snapshot(
    index_root: &Path,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    dense_dimension: usize,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    let started = Instant::now();
    let restore_log = crate::util::LogContext::new("source_restore", source_id);
    restore_log.record("source_id", source_id);
    restore_log.record("parse_id", parse_id);
    let _restore_log = restore_log.enter();
    info!(
        event = "restore.started",
        source_id, parse_id, "restore from ForensicSnapshot starting"
    );

    let store = ArtifactStore::open(index_root)?;
    let connection = hot_plane::open_read(index_root)?;
    // §11.4 restores from the pre_deactivation snapshot of the parse.
    let snapshot = locate_lifecycle_snapshot(
        &connection,
        &store,
        source_id,
        parse_id,
        SnapshotType::PreDeactivation,
        true,
    )?;
    drop(connection);
    info!(
        event = "restore.manifest_located",
        source_id,
        parse_id,
        snapshot_id = %snapshot.id,
        manifest_hash = %snapshot.manifest_hash,
        "restore located the subject snapshot"
    );

    // Integrity before re-import: verify_mechanical re-hashes the manifest and
    // every referenced blob. A failure here means the archived bytes are not
    // trustworthy, so restore must not proceed. SnapshotVerificationFailed
    // propagates untouched (verifier verdict, not a restore failure).
    verify_mechanical(index_root, &snapshot)?;

    // Load the manifest once for the re-import; get_json re-hashes it to its
    // address (tamper-evident) — remapped to a RestoreFailed with restore context.
    let manifest = load_manifest(&store, &snapshot)?;
    // Annotation indexes are optional post-activation state. Existing published
    // references are restored verbatim; their absence leaves discovery to the
    // projection worker and never triggers inference during restoration.
    let annotation_publications =
        crate::snapshot::verify::verified_annotation_publications(&store, &snapshot.id, &manifest)
            .inspect_err(|source| {
                error!(event = "restore.annotation_preflight_failed", source_id, parse_id,
                snapshot_id = %snapshot.id, error = %source, committed = false,
                "annotation embedding snapshot inputs failed preflight before restore writes");
            })?;
    info!(event = "restore.annotation_preflight_completed", source_id, parse_id,
        snapshot_id = %snapshot.id, snapshot_manifest_count = annotation_publications.manifest_count,
        snapshot_embedding_blob_count = annotation_publications.embedding_blob_count,
        snapshot_representation_count = annotation_publications.representation_count,
        "snapshot annotation embeddings verified without inference before restore writes");
    drop(annotation_publications);
    // Publication may lag annotation completion. Reconstruct only the graph
    // envelope captured in this manifest while preserving every annotation row.
    let graph_planes = crate::snapshot::verify::verified_graph_planes(&store, &snapshot, &manifest)
        .inspect_err(|source| {
            error!(event = "restore.graph_preflight_failed", source_id, parse_id,
                snapshot_id = %snapshot.id, error = %source, committed = false,
                "captured graph inputs failed verification before restore writes");
        })?;
    let captured_graph = graph_planes.get(parse_id);
    info!(event = "restore.graph_preflight_completed", source_id, parse_id,
        snapshot_id = %snapshot.id,
        graph_published = captured_graph.is_some(),
        projection_id = captured_graph.map(|graph| graph.projection_id.as_str()),
        "captured graph publication state verified before restore writes");
    // Reject legacy/incompatible snapshots before opening a write transaction.
    // Section vectors are immutable artifacts: restoration never calls a model.
    let section_planes =
        crate::snapshot::verify::verified_section_planes(&store, &snapshot, &manifest)?;
    let mut matching_sections = section_planes
        .iter()
        .filter(|(_, plane)| plane.source_id == source_id && plane.parse_id == parse_id);
    let section_plane = (|| {
        let plane = matching_sections.next().map(|(_, plane)| plane).ok_or_else(|| ApiError::RestoreFailed {
            message: format!("snapshot {} lacks section embeddings for parse {parse_id}; rebuild the corpus instead of restoring this pre-feature snapshot", snapshot.id),
        })?;
        if matching_sections.next().is_some() || plane.dimension != dense_dimension {
            return Err(ApiError::RestoreFailed {
                message: format!("snapshot {} has duplicate or dimension-incompatible section embeddings for parse {parse_id}; rebuild the corpus", snapshot.id),
            });
        }
        let envelopes = load_archived_jsonl(&store, &manifest, "retrieval_projections")?;
        // A historical stale/superseded envelope is valid forensic evidence but
        // cannot be published as an active retrieval representation on restore.
        for index_name in [None, Some(crate::projections::section_dense::SECTION_DENSE_INDEX_NAME)] {
            let count = envelopes.iter().filter(|record|
                record.get("parse_id").and_then(Value::as_str) == Some(parse_id)
                    && record.get("source_id").and_then(Value::as_str) == Some(source_id)
                    && record.get("projection_type").and_then(Value::as_str) == Some("dense_vector")
                    && record.get("index_name").and_then(Value::as_str) == index_name
                    && record.get("freshness_status").and_then(Value::as_str) == Some("fresh")
                    && record.get("deleted_at").is_none_or(Value::is_null)).count();
            if count != 1 {
                return Err(ApiError::RestoreFailed {
                    message: format!("snapshot {}: parse {parse_id} requires one fresh dense index {index_name:?}, found {count}; rebuild the corpus", snapshot.id),
                });
            }
        }
        Ok(plane)
    })().map_err(|error| {
        error!(event = "restore.section_preflight_failed", source_id, parse_id,
            snapshot_id = %snapshot.id, error = %error, committed = false,
            "restore rejected before section payload re-import");
        error
    })?;
    info!(event = "restore.section_preflight_completed", source_id, parse_id,
        snapshot_id = %snapshot.id, section_windows = section_plane.windows.len(),
        "section payload integrity and compatibility verified before restore writes");

    // Re-import + rebuild ride ONE IMMEDIATE transaction so a failure rolls back
    // the whole restore (no partial hot state ever commits). Blob bytes are read
    // from the store OUTSIDE... no — the store reads are pure reads with no SQL,
    // so they run inside the tx body freely; only the hot-plane writes are txn'd.
    let mut connection = hot_plane::open_write(index_root)?;
    let tx = hot_plane::begin_write_transaction(&mut connection, TX_LOG_NAMESPACE, "restore")?;
    let counts = match restore_body(&tx, &store, captured_graph, &manifest, source_id, parse_id)
        .and_then(|counts| {
            crate::projections::section_dense::validate_plane(&tx, section_plane)?;
            Ok(counts)
        }) {
        Ok(counts) => counts,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                TX_LOG_NAMESPACE,
                "restore",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "restore")?;
    // Durable restoration and in-memory publication can fail independently.
    // Record the successful commit before entering the fallible cache boundary.
    info!(
        event = "restore.committed",
        source_id,
        parse_id,
        snapshot_id = %snapshot.id,
        committed = true,
        dense_cache_published = false,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "snapshot rows restored and committed; dense cache publication pending"
    );

    // Dense-cache publish, mirroring the activation publish invariant
    // (`activation::publish_dense_cache`, activation.rs) and MUST STAY IN STEP
    // with it: the in-memory plane load is the paired half of the durable
    // re-import, so it runs AFTER the commit above and UNDER the per-source
    // cutover barrier — the barrier serializes this publish against any other
    // publish of the same source (a concurrent activation cutover). No
    // predecessor is evicted here: restore populates the restored parse's plane,
    // it does not swap out a currently-loaded one. A load failure propagates —
    // a source about to become All-scope visible (the caller clears
    // `deactivated_at` next) with no loaded dense plane is a broken publish.
    // The `connection` opened for the re-import is reused for the load read.
    let _barrier_guard = registry.acquire(source_id);
    dense_cache
        .load_parse(&connection, &store, parse_id, dense_dimension)
        .map_err(|source| {
            error!(event = "restore.publish_failed", source_id, parse_id,
            snapshot_id = %snapshot.id, committed = true, error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "snapshot rows committed but dense cache publication failed");
            source
        })?;
    // Barrier releases at guard drop (function return); the caller's flag-clear
    // runs after this returns Ok, so ordering is: durable commit → under-barrier
    // publish → (caller) flag-clear.

    info!(
        event = "restore.completed",
        source_id,
        parse_id,
        snapshot_id = %snapshot.id,
        content_units_imported = counts.content_units as u64,
        unit_relationships_imported = counts.unit_relationships as u64,
        semantic_annotations_imported = counts.semantic_annotations as u64,
        retrieval_projections_imported = counts.retrieval_projections as u64,
        chunk_projections_imported = counts.chunk_projections as u64,
        dense_imported = counts.dense as u64,
        multivector_imported = counts.multivector as u64,
        lexical_index_rows = counts.lexical_index as u64,
        graph_mentions_rebuilt = counts.graph_mentions as u64,
        graph_edges_rebuilt = counts.graph_edges as u64,
        dense_cache_published = true,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "restore completed (durable plane re-imported and dense cache published under barrier)"
    );
    Ok(())
}

/// Per-plane counts of one restore, for the terminal log.
struct RestoreCounts {
    content_units: usize,
    unit_relationships: usize,
    semantic_annotations: usize,
    retrieval_projections: usize,
    chunk_projections: usize,
    dense: usize,
    multivector: usize,
    lexical_index: usize,
    graph_mentions: usize,
    graph_edges: usize,
}

/// The transactional body of restore. Re-imports every archived plane for the
/// subject parse PRESERVING IDs (never re-derived — `ids` §16.4), then rebuilds
/// the deterministic non-archived planes. The archived JSONL records are the RAW
/// column projections C9a wrote via `read_table_as_json` (every column, keyed by
/// name), so the re-import is the exact inverse: a generic column-driven INSERT
/// that reconstructs each row verbatim. This keeps the re-import single-sourced
/// against the archive shape rather than duplicating each table's typed INSERT.
fn restore_body(
    tx: &Transaction<'_>,
    store: &ArtifactStore,
    captured_graph: Option<&crate::projections::graph::CapturedGraph>,
    manifest: &ForensicSnapshotManifest,
    source_id: &str,
    parse_id: &str,
) -> Result<RestoreCounts, ApiError> {
    // Canonical rows (content_units, unit_relationships). parse_runs is NOT
    // re-imported: neither the supersession nor the deactivation cleanup deletes
    // the parse_runs row (see the delete list), so the row still exists and, for
    // the §11.4 deactivation case, is still `active` with the source's
    // active_parse_id pointing at it — re-inserting it would be a primary-key
    // conflict. semantic_annotations is re-imported from the archived JSONL;
    // annotation_memo is never re-minted (re-mint = new ids/provenance = a
    // rebuild, not a restore).
    let content_units = reimport_plane(tx, store, manifest, "content_units", parse_id)?;
    let unit_relationships = reimport_plane(tx, store, manifest, "unit_relationships", parse_id)?;
    let semantic_annotations =
        reimport_plane(tx, store, manifest, "semantic_annotations", parse_id)?;

    // Projection envelopes + chunk payloads (metadata JSONL). The chunk rows
    // feed the deterministic FTS5 rebuild below, so they must be re-imported
    // first.
    let retrieval_projections =
        reimport_plane(tx, store, manifest, "retrieval_projections", parse_id)?;
    let chunk_projections = reimport_plane(tx, store, manifest, "chunk_projections", parse_id)?;

    // Binary planes: re-import the raw blob bytes (byte-reproduced, NEVER
    // re-embedded — §31.3/§38) back into their BLOB columns, keyed by the
    // metadata JSONL that carries each row's scalar columns plus the blob hash.
    let dense = reimport_blob_plane(
        tx,
        store,
        manifest,
        "chunk_dense_vectors_metadata",
        "chunk_dense_vectors",
        "vector_blob",
        parse_id,
    )?;
    let multivector = reimport_blob_plane(
        tx,
        store,
        manifest,
        "unit_multivector_projections_metadata",
        "unit_multivector_projections",
        "matrix_blob",
        parse_id,
    )?;

    // FTS5 uses the restored chunks. Graph payload uses only the verified
    // published envelope's exact inputs; all completed annotations were restored
    // above, including inputs the projection worker has not published yet.
    let lexical_index = rebuild_lexical_index(tx, parse_id)?;
    let (graph_mentions, graph_edges) = match captured_graph {
        Some(graph) => {
            if graph.source_id != source_id || graph.parse_id != parse_id {
                return Err(ApiError::RestoreFailed {
                    message: format!(
                        "captured graph {} does not belong to restored source {source_id}, parse {parse_id}",
                        graph.projection_id
                    ),
                });
            }
            crate::projections::graph::restore_captured_graph(tx, graph).map_err(|source| {
                ApiError::RestoreFailed {
                    message: format!(
                        "restore of captured graph {}: {source}",
                        graph.projection_id
                    ),
                }
            })?
        }
        // Absence is preserved; reconstruction cannot silently publish a graph.
        None => (0, 0),
    };

    Ok(RestoreCounts {
        content_units,
        unit_relationships,
        semantic_annotations,
        retrieval_projections,
        chunk_projections,
        dense,
        multivector,
        lexical_index,
        graph_mentions,
        graph_edges,
    })
}

/// Load the snapshot's manifest for re-import, remapping a load/verify failure
/// into a `RestoreFailed` with restore context (mechanical verification already
/// proved it addressable; this second read is the re-import's own boundary).
fn load_manifest(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
) -> Result<ForensicSnapshotManifest, ApiError> {
    let value =
        store
            .get_json(&snapshot.manifest_hash)
            .map_err(|source| ApiError::RestoreFailed {
                message: format!(
                    "restore of snapshot {}: manifest {} could not be read for re-import: {source}",
                    snapshot.id, snapshot.manifest_hash
                ),
            })?;
    serde_json::from_value(value).map_err(|source| ApiError::RestoreFailed {
        message: format!(
            "restore of snapshot {}: manifest {} is not a valid ForensicSnapshotManifest: {source}",
            snapshot.id, snapshot.manifest_hash
        ),
    })
}

/// Re-import one archived JSONL plane for the subject parse: fetch the plane's
/// archived record set by artifactType, keep only the records whose `parse_id`
/// matches (the archived set is corpus-wide; the restore is parse-scoped), and
/// INSERT each verbatim via the generic column-driven reconstructor. Returns the
/// number of rows re-imported.
fn reimport_plane(
    tx: &Transaction<'_>,
    store: &ArtifactStore,
    manifest: &ForensicSnapshotManifest,
    artifact_type: &str,
    parse_id: &str,
) -> Result<usize, ApiError> {
    let records = load_archived_jsonl(store, manifest, artifact_type)?;
    let mut imported = 0usize;
    for record in &records {
        let object = as_object(record, artifact_type)?;
        if json_str(object, "parse_id") != Some(parse_id) {
            continue;
        }
        insert_row_from_object(tx, artifact_type, object)?;
        imported += 1;
    }
    Ok(imported)
}

/// Re-import one archived BINARY plane for the subject parse. The archived
/// metadata JSONL carries each row's scalar columns plus a `<blob_column>Hash`
/// key naming the archived blob; this reconstructs the real row by fetching the
/// blob bytes (re-hash-verified by `get_bytes`) and re-inserting them into the
/// BLOB column. The vectors are byte-reproduced from the stored blobs, NEVER
/// re-embedded (§31.3/§38). Returns the number of rows re-imported.
fn reimport_blob_plane(
    tx: &Transaction<'_>,
    store: &ArtifactStore,
    manifest: &ForensicSnapshotManifest,
    metadata_artifact_type: &str,
    table: &str,
    blob_column: &str,
    parse_id: &str,
) -> Result<usize, ApiError> {
    let records = load_archived_jsonl(store, manifest, metadata_artifact_type)?;
    let hash_key = format!("{blob_column}Hash");
    let mut imported = 0usize;
    for record in &records {
        let object = as_object(record, metadata_artifact_type)?;
        if json_str(object, "parse_id") != Some(parse_id) {
            continue;
        }
        // Fetch the archived blob bytes named by the metadata record; get_bytes
        // re-hashes them to their address, so a corrupt blob fails here.
        let blob_hash = required_str(object, &hash_key, metadata_artifact_type)?;
        let blob = store
            .get_bytes(&blob_hash)
            .map_err(|source| ApiError::RestoreFailed {
                message: format!(
                    "restore of {table}: archived blob {} failed to load: {source}",
                    hash_prefix(&blob_hash)
                ),
            })?;
        insert_blob_row_from_object(tx, table, object, blob_column, &hash_key, &blob)?;
        imported += 1;
    }
    Ok(imported)
}

/// Reconstruct and execute an `INSERT INTO <table> (cols…) VALUES (…)` from one
/// archived column-projection object. The object's keys are the exact column
/// names `read_table_as_json` wrote, so the reconstruction is the inverse of the
/// archive projection — every column, verbatim, IDs preserved. Values are bound
/// by their JSON type (null/integer/real/text); the archive never contains a
/// BLOB in a JSONL projection (BLOB planes go through the blob path), so a JSON
/// array/object value is an unexpected shape surfaced as an error.
fn insert_row_from_object(
    tx: &Transaction<'_>,
    table: &str,
    object: &Map<String, Value>,
) -> Result<(), ApiError> {
    let columns: Vec<&String> = object.keys().collect();
    let values: Vec<SqlValue> = columns
        .iter()
        .map(|name| sql_value_from_json(object.get(*name).unwrap_or(&Value::Null), table, name))
        .collect::<Result<_, _>>()?;
    execute_generic_insert(tx, table, &columns, &values)
}

/// Reconstruct and execute an INSERT for a binary-plane row: every scalar column
/// verbatim EXCEPT the archived `<blob_column>Hash` key, which is replaced by the
/// real `blob_column` holding the fetched bytes. This is the exact inverse of
/// `archive_blob_plane`, which dropped the BLOB column and inserted a `…Hash`
/// key in its place.
fn insert_blob_row_from_object(
    tx: &Transaction<'_>,
    table: &str,
    object: &Map<String, Value>,
    blob_column: &str,
    hash_key: &str,
    blob: &[u8],
) -> Result<(), ApiError> {
    let mut columns: Vec<String> = Vec::with_capacity(object.len());
    let mut values: Vec<SqlValue> = Vec::with_capacity(object.len());
    for (name, value) in object {
        if name == hash_key {
            // Replace the archived hash placeholder with the real blob column.
            columns.push(blob_column.to_owned());
            values.push(SqlValue::Blob(blob.to_vec()));
            continue;
        }
        columns.push(name.clone());
        values.push(sql_value_from_json(value, table, name)?);
    }
    let column_refs: Vec<&String> = columns.iter().collect();
    execute_generic_insert(tx, table, &column_refs, &values)
}

/// Build and run one `INSERT INTO <table> (c1, c2, …) VALUES (?1, ?2, …)` with
/// positional binding. The table name is code-controlled (an archive artifactType
/// mapped to a fixed schema table, never user input), and column names come from
/// the archived projection of that table, so there is no injection surface. A
/// duplicate primary key (the row already exists) is surfaced as a RestoreFailed
/// naming the table — restore expects the plane to have been cleaned before it runs.
fn execute_generic_insert(
    tx: &Transaction<'_>,
    table: &str,
    columns: &[&String],
    values: &[SqlValue],
) -> Result<(), ApiError> {
    let column_list = columns
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=columns.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO {table} ({column_list}) VALUES ({placeholders})");
    let params: Vec<&dyn rusqlite::ToSql> =
        values.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
    tx.execute(&sql, params.as_slice())
        .map_err(|source| ApiError::RestoreFailed {
            message: format!("restore failed to re-insert a {table} row: {source}"),
        })?;
    Ok(())
}

/// One SQLite bind value reconstructed from an archived JSON column value. The
/// closed set mirrors `column_value_to_json`'s inverse: null, integer, real,
/// text, and (blob-plane only) raw bytes.
enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl rusqlite::ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef as VR};
        Ok(match self {
            SqlValue::Null => ToSqlOutput::Borrowed(VR::Null),
            SqlValue::Integer(value) => ToSqlOutput::Borrowed(VR::Integer(*value)),
            SqlValue::Real(value) => ToSqlOutput::Borrowed(VR::Real(*value)),
            SqlValue::Text(value) => ToSqlOutput::Borrowed(VR::Text(value.as_bytes())),
            SqlValue::Blob(value) => ToSqlOutput::Borrowed(VR::Blob(value.as_slice())),
        })
    }
}

/// Map one archived JSON column value to its SQLite bind value. Integers and
/// reals map back to INTEGER/REAL; a JSON array or object cannot be a scalar
/// column value in a column projection (the archive projects scalars only, list
/// columns are stored as canonical JSON STRINGS), so it is an explicit restore
/// failure rather than a silent coercion.
fn sql_value_from_json(value: &Value, table: &str, column: &str) -> Result<SqlValue, ApiError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(flag) => Ok(SqlValue::Integer(i64::from(*flag))),
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                Ok(SqlValue::Integer(int))
            } else if let Some(float) = number.as_f64() {
                Ok(SqlValue::Real(float))
            } else {
                Err(ApiError::RestoreFailed {
                    message: format!(
                        "restore of {table}.{column}: archived number is neither i64 nor f64"
                    ),
                })
            }
        }
        Value::String(text) => Ok(SqlValue::Text(text.clone())),
        Value::Array(_) | Value::Object(_) => Err(ApiError::RestoreFailed {
            message: format!(
                "restore of {table}.{column}: archived value is a JSON {} — a column projection \
                 stores only scalar columns, so this is a corrupt archived row",
                if value.is_array() { "array" } else { "object" }
            ),
        }),
    }
}

// ---------------------------------------------------------------------------
// Deterministic rebuild of the non-archived planes.
// ---------------------------------------------------------------------------

/// Deterministically rebuild the FTS5 `chunk_text_index` for the parse from its
/// just-re-imported `chunk_projections` rows. Mirrors `lexical`'s deterministic
/// producer: one `(chunk_id, targeting_text)` row per chunk, in `ORDER BY id`
/// (the FTS5 insert order is not identity-bearing, but a fixed order keeps the
/// rebuild reproducible). The lexical builder itself cannot be reused — it opens
/// an envelope and requires the whole build tx shape — so the pure index insert
/// is mirrored here and MUST STAY IN STEP with `lexical::INSERT_INDEX_ROW_SQL`.
/// No prior-index clear is needed: cleanup deleted the parse's index rows, and a
/// restore runs into an empty plane.
fn rebuild_lexical_index(tx: &Transaction<'_>, parse_id: &str) -> Result<usize, ApiError> {
    const SELECT_CHUNKS_SQL: &str = "
SELECT id, targeting_text FROM chunk_projections WHERE parse_id = ?1 ORDER BY id";
    const INSERT_INDEX_ROW_SQL: &str = "
INSERT INTO chunk_text_index (chunk_id, targeting_text) VALUES (?1, ?2)";

    let chunks: Vec<(String, String)> = {
        let mut statement =
            tx.prepare(SELECT_CHUNKS_SQL)
                .map_err(|source| ApiError::RestoreFailed {
                    message: format!(
                        "restore failed to prepare chunk read for FTS5 rebuild of parse \
                         {parse_id}: {source}"
                    ),
                })?;
        let rows = statement
            .query_map(params![parse_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|source| ApiError::RestoreFailed {
                message: format!(
                    "restore failed to read chunks for FTS5 rebuild of parse {parse_id}: {source}"
                ),
            })?;
        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row.map_err(|source| ApiError::RestoreFailed {
                message: format!(
                    "restore failed to read a chunk row for FTS5 rebuild of parse \
                     {parse_id}: {source}"
                ),
            })?);
        }
        chunks
    };
    for (chunk_id, targeting_text) in &chunks {
        tx.execute(INSERT_INDEX_ROW_SQL, params![chunk_id, targeting_text])
            .map_err(|source| ApiError::RestoreFailed {
                message: format!(
                    "restore failed to insert FTS5 index row for chunk {chunk_id}: {source}"
                ),
            })?;
    }
    Ok(chunks.len())
}

// ---------------------------------------------------------------------------
// Shared archived-record helpers.
// ---------------------------------------------------------------------------

/// Load and return the archived JSONL record set for a named plane by
/// artifactType, resolving its manifest ref across every section. An expected
/// plane absent from the manifest is a restore failure (the archive is missing a
/// plane the restore must re-import).
fn load_archived_jsonl(
    store: &ArtifactStore,
    manifest: &ForensicSnapshotManifest,
    artifact_type: &str,
) -> Result<Vec<Value>, ApiError> {
    let artifact =
        find_ref_by_type(manifest, artifact_type).ok_or_else(|| ApiError::RestoreFailed {
            message: format!("restore: manifest has no {artifact_type} artifact to re-import"),
        })?;
    store
        .get_jsonl(&artifact.hash)
        .map_err(|source| ApiError::RestoreFailed {
            message: format!(
                "restore: archived {artifact_type} ({}) failed to load: {source}",
                hash_prefix(&artifact.hash)
            ),
        })
}

/// Find the first manifest ref of a given artifactType across every section (the
/// re-import planes each contribute exactly one ref, mirroring
/// `snapshot::verify::find_ref_by_type`).
fn find_ref_by_type<'m>(
    manifest: &'m ForensicSnapshotManifest,
    artifact_type: &str,
) -> Option<&'m SnapshotArtifactRef> {
    manifest_ref_sections(manifest)
        .into_iter()
        .flatten()
        .find(|artifact| artifact.artifact_type == artifact_type)
}

/// Every referenced-artifact section of the manifest as one flat list, so
/// artifactType lookup walks all sections without repeating the field list
/// (mirror of `snapshot::manifest_ref_sections`; the optional sections
/// contribute only when present).
fn manifest_ref_sections(manifest: &ForensicSnapshotManifest) -> Vec<&[SnapshotArtifactRef]> {
    let mut sections: Vec<&[SnapshotArtifactRef]> = vec![
        &manifest.source_objects,
        &manifest.acquisition_records,
        &manifest.parse_runs,
        &manifest.canonical_parse_bundles,
        &manifest.content_units,
        &manifest.unit_relationships,
        &manifest.semantic_annotations,
        &manifest.retrieval_projections,
        &manifest.retrieval_indexes,
        &manifest.assembly_policies,
        &manifest.retrieval_profiles,
        &manifest.capability_profiles,
        &manifest.query_execution_records,
        &manifest.runtime_artifacts,
    ];
    if let Some(parser_output_bundles) = &manifest.parser_output_bundles {
        sections.push(parser_output_bundles);
    }
    if let Some(deletion_records) = &manifest.deletion_records {
        sections.push(deletion_records);
    }
    if let Some(model_artifacts) = &manifest.model_artifacts {
        sections.push(model_artifacts);
    }
    sections
}

/// Borrow a JSON record as an object or fail with the plane name.
fn as_object<'v>(record: &'v Value, plane: &str) -> Result<&'v Map<String, Value>, ApiError> {
    record.as_object().ok_or_else(|| ApiError::RestoreFailed {
        message: format!("restore: archived {plane} record is not a JSON object"),
    })
}

/// Read an object's field as `&str` if present and a string.
fn json_str<'v>(object: &'v Map<String, Value>, key: &str) -> Option<&'v str> {
    object.get(key).and_then(Value::as_str)
}

/// Read a required string field from an archived record.
fn required_str(object: &Map<String, Value>, key: &str, plane: &str) -> Result<String, ApiError> {
    json_str(object, key)
        .map(str::to_owned)
        .ok_or_else(|| ApiError::RestoreFailed {
            message: format!("restore: archived {plane} record is missing string field `{key}`"),
        })
}

/// Decode a canonical JSON string-array TEXT value into `Vec<String>`. The
/// `restore_failure` flag selects the error variant so this is shared between the
/// restore path (RestoreFailed) and the cleanup path's header reconstruction
/// (StorageOperation).
fn decode_json_string_array(
    json: &str,
    restore_failure: bool,
    field: &str,
) -> Result<Vec<String>, ApiError> {
    serde_json::from_str(json).map_err(|source| {
        let message = format!("{field} is not a JSON string array: {source}");
        if restore_failure {
            ApiError::RestoreFailed { message }
        } else {
            ApiError::StorageOperation { message }
        }
    })
}

/// First 12 hex chars of a hash for bounded logging/messages (never the full
/// digest, never content — DIAGNOSTICS forbidden-data rule).
fn hash_prefix(hash: &str) -> &str {
    let end = hash.len().min(12);
    &hash[..end]
}

/// The snake_case wire name of a `SnapshotType` for the subject-snapshot lookup
/// SQL and boundary logs (mirror of `snapshot::snapshot_type_wire_name`, private
/// there). A serialization failure is unreachable for this plain renamed enum, so
/// the exhaustive match needs no fallback.
fn snapshot_type_wire_name(snapshot_type: SnapshotType) -> &'static str {
    match snapshot_type {
        SnapshotType::Scheduled => "scheduled",
        SnapshotType::PreActivation => "pre_activation",
        SnapshotType::PostActivation => "post_activation",
        SnapshotType::PreDeactivation => "pre_deactivation",
        SnapshotType::PreDeployment => "pre_deployment",
        SnapshotType::Manual => "manual",
        SnapshotType::Incident => "incident",
    }
}
