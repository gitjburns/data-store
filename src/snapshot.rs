//! ForensicSnapshot minting (spec §30, §31.2): the lifecycle trigger entry
//! points that create a content-addressed snapshot at each moment §30.6
//! requires one. Each entry archives the manifest (§30.4) into the artifact
//! store, mints a `snap_` id, writes the `forensic_snapshots` metadata row, and
//! returns the `ForensicSnapshot` header so the caller can hand its
//! `manifestHash` to verification (§30.5).
//!
//! Handle contract (pinned by the C9s skeleton, mirrors
//! `activation::gate_and_activate`): a trigger receives `index_root` plus the
//! lifecycle subject id and opens its own artifact store and database handles.
//! One read-only transaction captures scope and every archived database record;
//! a separate write connection owns audit and metadata commits. Triggers are
//! never handed a live `Connection`/`Transaction`. Lifecycle snapshots are
//! per-subject; the parameterized `manual`/`incident` entry leaves the subject
//! columns NULL. `scheduled` and `pre_deployment` are inert (no trigger
//! constructs them).
//!
//! Two-phase minting mirrors the parse importer's caller-side bundle write
//! (`parse::importer::write_canonical_parse_bundle`): every heavy artifact is
//! written to the write-once artifact store FIRST (idempotent, outside any SQL
//! write transaction), the self-hashed manifest is written LAST, and only then does
//! the `forensic_snapshots` row plus its `snapshot.completed` event commit in
//! one IMMEDIATE hot-plane transaction. A crash between the artifact writes and
//! the row commit leaves orphaned-but-valid content-addressed blobs and no
//! metadata row — never a metadata row pointing at absent bytes.
//!
//! ARCHIVED vs REFERENCED boundary (user ruling 2026-07-16): planes that exist
//! ONLY in the hot plane and are not otherwise archived are copied into the
//! artifact store here (`archive_*`); artifacts already archived with a durable
//! uri+hash (the sealed §12.3 canonical parse bundle, the raw source bytes) are
//! REFERENCED by that existing uri+hash, never re-copied. The dense-vector and
//! multivector blobs are archived as raw bytes so a restore RE-IMPORTS them
//! (§31.3) rather than re-embedding — they are byte-reproducible only from the
//! stored blobs. FTS5 (`chunk_text_index`) and the graph planes are NOT archived
//! (C9b's deletion-gate deterministic rebuild covers them, §8.3/§30.5).
//!
//! Barrier invariant (§31.1): snapshot creation NEVER runs under a held cutover
//! barrier. These trigger fns do not acquire the barrier; callers guarantee
//! ordering — the scheduler wires pre/post-activation AROUND
//! `gate_and_activate` (whose barrier hold is internal), and C9c takes the
//! pre-deactivation snapshot BEFORE it acquires the barrier.

// The three lifecycle trigger fns are live: `pre_activation_snapshot` /
// `post_activation_snapshot` are wired around `gate_and_activate` in the
// scheduler, and `pre_deactivation_snapshot` is reached through
// `deletion::deactivate_one_source`. The `manual`/`incident` `request_snapshot`
// entry is reached through the snapshot HTTP route (`http::post_snapshots`), so
// no dead-code allow is needed anywhere in this module.

use crate::runtime::StorageContext;
use std::collections::BTreeMap;
use std::time::Instant;

use crate::sqlite::{Connection, Transaction};
use rusqlite::types::ValueRef;
use rusqlite::{OptionalExtension, params};
use serde_json::{Map, Value};
use tracing::{error, info};

use crate::artifact_store::{ArtifactRef, ArtifactStore};
use crate::canonical;
use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::identity;
use crate::identity::ApplicationIdentity;
use crate::model::{
    EvidenceReplayMode, ForensicSnapshot, ForensicSnapshotManifest, GenerationReplayMode,
    MANIFEST_FORMAT_VERSION, ReplayProfile, RetrievalReplayMode, SnapshotArtifactRef, SnapshotType,
    SystemEventType,
};
use crate::primitives::utc_now;

pub(crate) mod verify;

/// Manifest payload identity shared by archival, verification, and restoration.
pub(crate) const SECTION_DENSE_PAYLOAD_TYPE: &str = "section_dense_payload";

/// Log-event namespace for this module's boundary logs and the namespace passed
/// to the shared hot-plane transaction helpers, so every snapshot log line and
/// transaction boundary is attributable to snapshot minting.
const TX_LOG_NAMESPACE: &str = "snapshot";

/// SystemEvent object_type for `forensic_snapshots` rows. The snapshot id is the
/// object id; the type names the object class in the audit trail.
const OBJECT_TYPE_SNAPSHOT: &str = "forensic_snapshot";

/// Insert the `forensic_snapshots` metadata row (schema §30/§32): the queryable
/// header pointing at the archived manifest. Column order matches the DDL in
/// `sql/fabric/schema.sql`.
const INSERT_FORENSIC_SNAPSHOT_SQL: &str = "
INSERT INTO forensic_snapshots (
  id, snapshot_type, subject_source_id, subject_parse_id,
  source_object_ids_json, active_parse_ids_json,
  manifest_uri, manifest_hash, system_version, spec_version, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// The source's current active-parse pointer (§14): the subject parse of a
/// pre-deactivation snapshot is whatever parse is active for the source now.
const SELECT_ACTIVE_PARSE_ID_SQL: &str = "
SELECT active_parse_id FROM source_objects WHERE id = ?1";

/// The run→source binding (§12, immutable): pre/post-activation snapshots derive
/// their subject source from the candidate `parse_run_id` exactly as the
/// activation gate does.
const SELECT_PARSE_RUN_SOURCE_SQL: &str = "
SELECT source_id FROM parse_runs WHERE id = ?1";

/// Mint the `pre_activation` snapshot (§30.6 "Before activating a ParseRun").
/// Called by the scheduler immediately before `activation::gate_and_activate`
/// for the parse under `parse_run_id`; derives the subject source from the run
/// exactly as the gate does. The subject parse is the candidate itself, so the
/// snapshot captures the source's PRE-activation active set plus the candidate's
/// full artifact set. Returns the header carrying the `manifestHash` that
/// mechanical verification (§30.5) checks.
pub(crate) fn pre_activation_snapshot(
    index_root: &StorageContext,
    identity: &ApplicationIdentity,
    parse_run_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): the scheduler calls this BEFORE
    // gate_and_activate acquires the source barrier, so no barrier is held here.
    mint(
        index_root,
        identity,
        SnapshotType::PreActivation,
        |connection| {
            let source_id = lookup_source_of_parse(connection, parse_run_id)?;
            Ok(lifecycle_scope(
                SnapshotType::PreActivation,
                &source_id,
                parse_run_id,
            ))
        },
    )
}

/// Mint the `post_activation` snapshot (§30.6 / §31.2 step 3), created AFTER a
/// successful cutover for `parse_run_id`. This is the snapshot the §31.2
/// deletion gate verifies over before superseded state is removed (user ruling
/// 2026-07-16: no separate pre-deletion snapshot). The candidate is now the
/// source's active parse, so the captured active set already reflects the
/// cutover.
pub(crate) fn post_activation_snapshot(
    index_root: &StorageContext,
    identity: &ApplicationIdentity,
    parse_run_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): the scheduler calls this AFTER
    // gate_and_activate returns and its internal barrier has released.
    mint(
        index_root,
        identity,
        SnapshotType::PostActivation,
        |connection| {
            let source_id = lookup_source_of_parse(connection, parse_run_id)?;
            Ok(lifecycle_scope(
                SnapshotType::PostActivation,
                &source_id,
                parse_run_id,
            ))
        },
    )
}

/// Mint the `pre_deactivation` snapshot (§30.6 "Before deactivating a source").
/// Deactivation is source-scoped, so the subject is `source_id` and the subject
/// parse is the source's CURRENT active parse (the state deactivation is about
/// to retire). Called by C9c before it acquires the cutover barrier. A source
/// with no active parse is a caller-contract error: there is nothing to
/// snapshot before deactivating.
pub(crate) fn pre_deactivation_snapshot(
    index_root: &StorageContext,
    identity: &ApplicationIdentity,
    source_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): C9c calls this BEFORE it acquires the source
    // barrier for the deactivation cutover.
    mint(
        index_root,
        identity,
        SnapshotType::PreDeactivation,
        |connection| {
            let active_parse_id = lookup_active_parse(connection, source_id)?.ok_or_else(|| {
                ApiError::BadRequest {
                    message: format!(
                        "cannot mint pre_deactivation snapshot for source {source_id}: \
                 the source has no active parse to capture"
                    ),
                }
            })?;
            Ok(lifecycle_scope(
                SnapshotType::PreDeactivation,
                source_id,
                &active_parse_id,
            ))
        },
    )
}

/// Mint a `manual` or `incident` snapshot on request (§30.6 "On manual or
/// incident request"). Corpus-wide: it captures the full active set of every
/// source, so the subject columns stay NULL (per the schema and §30.3 — only
/// lifecycle snapshots carry a subject). `created_by`/`notes` thread operator
/// attribution and free-text context from the request. Exposed through the
/// manual/incident snapshot HTTP route in `crate::http` (`post_snapshots`).
pub(crate) fn request_snapshot(
    index_root: &StorageContext,
    identity: &ApplicationIdentity,
    snapshot_type: SnapshotType,
    created_by: Option<&str>,
    notes: Option<&str>,
) -> Result<ForensicSnapshot, ApiError> {
    if !matches!(snapshot_type, SnapshotType::Manual | SnapshotType::Incident) {
        // Guard the closed enum: only manual/incident are operator-requestable.
        // pre/post-activation and pre_deactivation are lifecycle-triggered;
        // scheduled/pre_deployment are inert. A wrong type here is a caller bug.
        return Err(ApiError::BadRequest {
            message: format!(
                "request_snapshot only mints manual or incident snapshots, not {}",
                snapshot_type_wire_name(snapshot_type)
            ),
        });
    }

    mint(index_root, identity, snapshot_type, |connection| {
        // These scope reads and manifest archival share the transaction owned by mint.
        let sources = load_all_source_ids(connection)?;
        let active_parses = load_all_active_parse_ids(connection)?;
        Ok(SnapshotScope {
            snapshot_type,
            subject_source_id: None,
            subject_parse_id: None,
            source_object_ids: sources,
            active_parse_ids: active_parses,
            created_by: created_by.map(str::to_owned),
            notes: notes.map(str::to_owned),
        })
    })
}

/// Describe one per-source lifecycle snapshot from its captured read view. The
/// subject source and parse are set (lifecycle snapshots always set both
/// subject columns); the captured scope is exactly that source and that parse,
/// which is the artifact set the deletion gate and restore resolve by
/// `(subject_source_id, subject_parse_id, snapshot_type)`.
fn lifecycle_scope(snapshot_type: SnapshotType, source_id: &str, parse_id: &str) -> SnapshotScope {
    SnapshotScope {
        snapshot_type,
        subject_source_id: Some(source_id.to_owned()),
        subject_parse_id: Some(parse_id.to_owned()),
        source_object_ids: vec![source_id.to_owned()],
        active_parse_ids: vec![parse_id.to_owned()],
        created_by: None,
        notes: None,
    }
}

/// The resolved scope of one snapshot: which sources/parses it covers and its
/// subject/attribution metadata. Trigger callbacks resolve it within mint's read
/// transaction, so scope and exported database records describe the same state.
struct SnapshotScope {
    snapshot_type: SnapshotType,
    subject_source_id: Option<String>,
    subject_parse_id: Option<String>,
    source_object_ids: Vec<String>,
    active_parse_ids: Vec<String>,
    created_by: Option<String>,
    notes: Option<String>,
}

impl SnapshotScope {
    /// Derive the row filter every archived plane applies from the subject
    /// columns. Lifecycle snapshots set both subjects and archive the whole
    /// subject SOURCE (every parse it currently holds, not only the subject
    /// parse); manual/incident snapshots set neither and archive the corpus.
    /// One subject without the other is a caller-contract error, not a silent
    /// widening to the corpus.
    ///
    /// Source scope, not parse scope, is deliberate: the activation-supersession
    /// deletion gate verifies over the SUCCESSOR's `post_activation` snapshot and
    /// then deletes the PREDECESSOR's rows (`restore::SupersededCleanupMode`).
    /// The predecessor's final state, including annotations committed after its
    /// own activation, is archived only because that snapshot still contains
    /// every parse of the source at mint time.
    fn plane_scope(&self) -> Result<PlaneScope<'_>, ApiError> {
        match (&self.subject_source_id, &self.subject_parse_id) {
            (Some(source_id), Some(_)) => Ok(PlaneScope::Source { source_id }),
            (None, None) => Ok(PlaneScope::Corpus),
            _ => Err(ApiError::StorageOperation {
                message: format!(
                    "snapshot scope for {} names a subject source or parse without the other",
                    snapshot_type_wire_name(self.snapshot_type)
                ),
            }),
        }
    }
}

/// Which rows a snapshot's plane reads select. The scope drives the SQL so the
/// archived planes can never disagree with the subject the header declares.
#[derive(Clone, Copy)]
enum PlaneScope<'a> {
    /// Every row of every plane (operator-requested manual/incident snapshots).
    Corpus,
    /// One source with every parse it holds (every lifecycle snapshot).
    Source { source_id: &'a str },
}

/// One `column = value` equality applied to a plane read.
struct Condition<'a> {
    column: &'static str,
    value: &'a str,
}

impl<'a> PlaneScope<'a> {
    /// The equality condition that confines `table` to this scope. Every
    /// archived table carries the source id under one of three column names;
    /// the readers narrow further to their subject parse by `parse_id`.
    fn condition(self, table: &str) -> Option<Condition<'a>> {
        let PlaneScope::Source { source_id } = self else {
            return None;
        };
        let column = match table {
            "source_objects" => "id",
            "acquisition_records" => "source_object_id",
            _ => "source_id",
        };
        Some(Condition {
            column,
            value: source_id,
        })
    }
}

/// Compose `SELECT * FROM <table> WHERE … <order_by>` from the scope condition
/// and an optional fixed predicate, returning the SQL and its bound parameters
/// in order. Table, column, and predicate text are code-controlled constants;
/// only the scope value is bound, so there is no injection surface.
fn scoped_select<'a>(
    table: &str,
    scope: PlaneScope<'a>,
    fixed_predicate: Option<&str>,
    order_by: &str,
) -> (String, Vec<&'a str>) {
    let mut predicates = Vec::new();
    let mut parameters = Vec::new();
    if let Some(condition) = scope.condition(table) {
        parameters.push(condition.value);
        predicates.push(format!("{} = ?{}", condition.column, parameters.len()));
    }
    if let Some(predicate) = fixed_predicate {
        predicates.push(predicate.to_owned());
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {} ", predicates.join(" AND "))
    };
    (
        format!("SELECT * FROM {table} {where_clause}{order_by}"),
        parameters,
    )
}

/// Archive one consistent SQLite view before publishing its metadata. Scope
/// resolution and every manifest read share a read-only transaction; independent
/// write transactions retain started/failed audit events and atomically publish
/// the final metadata row with its completed event after all artifacts exist.
fn mint(
    index_root: &StorageContext,
    identity: &ApplicationIdentity,
    snapshot_type: SnapshotType,
    resolve_scope: impl FnOnce(&Connection) -> Result<SnapshotScope, ApiError>,
) -> Result<ForensicSnapshot, ApiError> {
    let started = Instant::now();
    let snapshot_id = crate::ids::new_forensic_snapshot_id()?;
    let snapshot_log = crate::util::LogContext::new("snapshot", &snapshot_id);
    snapshot_log.record("trigger", snapshot_type_wire_name(snapshot_type));
    let _snapshot_log = snapshot_log.enter();
    let created_at = utc_now()?;
    let type_name = snapshot_type_wire_name(snapshot_type);

    info!(
        event = "snapshot.started",
        snapshot_id,
        snapshot_type = type_name,
        "forensic snapshot minting started"
    );
    let mut connection = hot_plane::open_write(index_root).inspect_err(|source| {
        error!(
            event = "snapshot.failed",
            snapshot_id,
            snapshot_type = type_name,
            stage = "open_write",
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "forensic snapshot failed opening its audit connection"
        );
    })?;
    // Durable started-boundary event on its own small transaction: the audit
    // trail records that a snapshot began even if archival later crashes. It is
    // NOT part of the completion transaction — a started event with no
    // completed event is exactly the recoverable "began but did not finish"
    // signal an operator needs.
    if let Err(source) = emit_started_event(&mut connection, &snapshot_id, &created_at, type_name) {
        error!(
            event = "snapshot.failed",
            snapshot_id,
            snapshot_type = type_name,
            stage = "started_event",
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "forensic snapshot failed before archival"
        );
        return Err(source);
    }

    // The first scope SELECT pins the WAL view for every later plane and artifact
    // reference. A separate read-only connection cannot accidentally publish
    // metadata or audit writes into that captured view. Writers remain available;
    // checkpoints retain WAL history until this export releases its connection.
    let mut archive_stage = "open_artifact_store";
    let captured = (|| {
        let store = ArtifactStore::open(index_root)?;
        archive_stage = "open_read";
        let mut read_connection = hot_plane::open_read(index_root)?;
        archive_stage = "begin_read_transaction";
        let tx = hot_plane::begin_read_transaction(
            &mut read_connection,
            TX_LOG_NAMESPACE,
            "archive_snapshot",
        )?;
        let read_started = Instant::now();
        archive_stage = "capture_scope";
        let result = resolve_scope(&tx).and_then(|scope| {
            if let Some(source_id) = &scope.subject_source_id {
                snapshot_log.record("source_id", source_id.as_str());
            }
            if let Some(parse_id) = &scope.subject_parse_id {
                snapshot_log.record("parse_id", parse_id.as_str());
            }
            info!(
                event = "snapshot.scope_captured",
                subject_source_id = scope.subject_source_id.as_deref().unwrap_or("none"),
                subject_parse_id = scope.subject_parse_id.as_deref().unwrap_or("none"),
                source_count = scope.source_object_ids.len() as u64,
                active_parse_count = scope.active_parse_ids.len() as u64,
                snapshot_id,
                "snapshot scope captured; archiving its consistent database view"
            );
            archive_stage = "archive";
            build_and_archive_manifest(&tx, &store, identity, &snapshot_id, &created_at, &scope)
                .map(|(manifest_ref, counts)| (scope, manifest_ref, counts))
        });
        // No database writes occur on this connection. Drop both handles on
        // success and failure before any final audit/metadata write, releasing
        // this export's WAL pin even when archival returned an error.
        drop(tx);
        drop(read_connection);
        info!(
            event = "snapshot.read_snapshot_released",
            snapshot_id,
            archive_succeeded = result.is_ok(),
            snapshot_held_ms = read_started.elapsed().as_millis() as u64,
            "snapshot read transaction and connection dropped"
        );
        result
    })();
    let (scope, manifest_ref, counts) = match captured {
        Ok(result) => result,
        Err(source) => {
            return Err(fail_snapshot(
                &mut connection,
                &snapshot_id,
                type_name,
                archive_stage,
                started,
                source,
            ));
        }
    };

    let header = ForensicSnapshot {
        id: snapshot_id.clone(),
        snapshot_type: scope.snapshot_type,
        created_at: created_at.clone(),
        created_by: scope.created_by.clone(),
        source_object_ids: scope.source_object_ids.clone(),
        active_parse_ids: scope.active_parse_ids.clone(),
        manifest_uri: manifest_ref.uri.clone(),
        manifest_hash: manifest_ref.hash.clone(),
        system_version: identity::system_version().to_owned(),
        spec_version: identity::SPEC_VERSION.to_owned(),
        replay_profile: mvp_replay_profile(),
        notes: scope.notes.clone(),
    };

    // Phase 2: commit the metadata row + completed event atomically. The
    // subject columns come from the scope (lifecycle snapshots always set both;
    // manual/incident leave both NULL) — they are a table concern, not carried
    // on the returned header.
    if let Err(source) = commit_snapshot_row(
        &mut connection,
        &header,
        scope.subject_source_id.as_deref(),
        scope.subject_parse_id.as_deref(),
    ) {
        return Err(fail_snapshot(
            &mut connection,
            &snapshot_id,
            type_name,
            "commit",
            started,
            source,
        ));
    }

    info!(
        event = "snapshot.completed",
        committed = true,
        snapshot_id,
        snapshot_type = type_name,
        manifest_uri = header.manifest_uri,
        manifest_hash = header.manifest_hash,
        source_objects = counts.source_objects as u64,
        acquisition_records = counts.acquisition_records as u64,
        parse_runs = counts.parse_runs as u64,
        content_units = counts.content_units as u64,
        unit_relationships = counts.unit_relationships as u64,
        semantic_annotations = counts.semantic_annotations as u64,
        retrieval_projections = counts.retrieval_projections as u64,
        chunk_projections = counts.chunk_projections as u64,
        dense_blobs = counts.dense_blobs as u64,
        section_dense_payloads = counts.section_dense_payloads as u64,
        annotation_manifests = counts.annotation_manifests as u64,
        annotation_embedding_blobs = counts.annotation_embedding_blobs as u64,
        multivector_blobs = counts.multivector_blobs as u64,
        deletion_evidence_rows = counts.deletion_evidence_rows as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "forensic snapshot completed"
    );
    Ok(header)
}

/// Emit the `snapshot.failed` event on its own transaction, log the failure
/// boundary with elapsed time, and hand the original error back. The failure
/// event rides its own tx because the completion tx never opened (or rolled
/// back), yet the audit trail must still record that this snapshot terminated
/// in failure. A failure to record the failure event is logged but does not
/// mask the underlying error.
fn fail_snapshot(
    connection: &mut Connection,
    snapshot_id: &str,
    type_name: &str,
    stage: &'static str,
    started: Instant,
    source: ApiError,
) -> ApiError {
    error!(
        event = "snapshot.failed",
        snapshot_id,
        snapshot_type = type_name,
        stage,
        error = %source,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "forensic snapshot failed"
    );
    if let Err(event_error) = emit_failed_event(connection, snapshot_id, type_name, &source) {
        error!(
            event = "snapshot.failed_event_unrecorded",
            snapshot_id,
            error = %event_error,
            "could not append snapshot.failed audit event"
        );
    }
    source
}

/// Per-plane cardinalities of what a snapshot actually archived, surfaced on the
/// `snapshot.completed` log so an operator can see each plane's size without
/// fetching the manifest. JSONL planes carry their archived record count; the
/// two blob planes carry the number of vector rows folded into their single
/// plane blob. Counts only — no contents.
struct ArchivedCounts {
    source_objects: usize,
    acquisition_records: usize,
    parse_runs: usize,
    content_units: usize,
    unit_relationships: usize,
    semantic_annotations: usize,
    retrieval_projections: usize,
    chunk_projections: usize,
    dense_blobs: usize,
    section_dense_payloads: usize,
    annotation_manifests: usize,
    annotation_embedding_blobs: usize,
    multivector_blobs: usize,
    deletion_evidence_rows: usize,
}

/// Read the archived record count a JSONL ref carries in its `recordCount`
/// metadata (written by `jsonl_ref`, the single writer of that field), for the
/// per-plane counts surfaced on `snapshot.completed`. A ref without the field is
/// counted as zero rather than failing the mint — the count is diagnostic, not a
/// correctness gate.
fn jsonl_record_count(artifact: &SnapshotArtifactRef) -> usize {
    artifact
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("recordCount"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize
}

/// Build the §30.4 manifest — archiving every hot-only plane and referencing
/// every already-archived artifact — self-hash it, and write it LAST to the
/// artifact store. Every referenced blob is proven present via `exists()`
/// before the manifest seals (acceptance §3): a manifest never references bytes
/// that are not in the store. Returns the sealed manifest ref plus the per-plane
/// archived counts for the `snapshot.completed` log. The caller-owned read
/// transaction also resolved scope, preventing mixed annotation/projection states.
fn build_and_archive_manifest(
    connection: &Transaction<'_>,
    store: &ArtifactStore,
    identity: &ApplicationIdentity,
    snapshot_id: &str,
    created_at: &str,
    snapshot_scope: &SnapshotScope,
) -> Result<(ArtifactRef, ArchivedCounts), ApiError> {
    // Every plane read below is confined to the subject source (or the whole
    // corpus for manual/incident snapshots), so archived volume scales with one
    // source rather than the corpus.
    let scope = snapshot_scope.plane_scope()?;

    // --- Hot relational planes archived as canonical JSONL record-set
    // projections (§30.2 "or its exact artifact projection"). Each plane is one
    // ordered record set; order is fixed by the query so the archived bytes are
    // reproducible.
    let source_objects = archive_plane(store, connection, "source_objects", scope)?;
    let acquisition_records = archive_plane(store, connection, "acquisition_records", scope)?;
    let parse_runs = archive_plane(store, connection, "parse_runs", scope)?;
    let content_units = archive_plane(store, connection, "content_units", scope)?;
    let unit_relationships = archive_plane(store, connection, "unit_relationships", scope)?;
    let semantic_annotations = archive_plane(store, connection, "semantic_annotations", scope)?;

    // Retrieval projection metadata + chunk payloads archived as JSONL; the
    // binary vector planes are archived below as raw content-addressed blobs.
    // The envelope rows are read once and shared by the three reference passes
    // instead of re-reading the table per pass.
    let mut retrieval_projections = Vec::new();
    let envelope_records = read_table_as_json(
        connection,
        "retrieval_projections",
        scope,
        None,
        ORDER_BY_ID,
    )?;
    let stored_envelopes = store.put_jsonl(&envelope_records)?;
    retrieval_projections.push(jsonl_ref(
        "retrieval_projections",
        &stored_envelopes,
        envelope_records.len(),
    ));
    retrieval_projections.push(archive_plane(
        store,
        connection,
        "chunk_projections",
        scope,
    )?);
    retrieval_projections.extend(reference_chunk_policies(&envelope_records, store)?);

    // --- Dense/multivector blobs archived as raw bytes so restore RE-IMPORTS
    // them (§31.3) rather than re-embedding: they are byte-reproducible ONLY
    // from the stored blobs. Each plane is one archived blob; each row
    // contributes one metadata record (its scalar columns) carrying the row's
    // byte range within that blob.
    let mut retrieval_indexes = Vec::new();
    let dense = archive_blob_plane(
        store,
        connection,
        scope,
        "chunk_dense_vectors",
        "chunk_id",
        "vector_blob",
        "dense_vector_blob",
    )?;
    retrieval_projections.push(dense.metadata);
    retrieval_indexes.extend(dense.plane);
    let dense_blob_count = dense.row_count;
    let multivector = archive_blob_plane(
        store,
        connection,
        scope,
        "unit_multivector_projections",
        "id",
        "matrix_blob",
        "multivector_blob",
    )?;
    retrieval_projections.push(multivector.metadata);
    retrieval_indexes.extend(multivector.plane);
    let multivector_blob_count = multivector.row_count;
    let section_payloads = reference_section_dense_payloads(&envelope_records, connection, store)?;
    let section_dense_payloads = section_payloads.len();
    retrieval_indexes.extend(section_payloads);
    retrieval_indexes.extend(reference_annotation_payloads(&envelope_records, store)?);

    // --- Sealed policies/profiles serialized at snapshot time (§30.4
    // assemblyPolicies/retrievalProfiles/capabilityProfiles). These are
    // validated runtime settings; their resolved values
    // (self-hash-sealed documents) are put_json'd here so the snapshot pins the
    // exact policy/profile identity that governed the captured state.
    let assembly_policies = vec![put_json_ref(
        store,
        &json_value_of(&store.settings().assembly_policy)?,
        "assembly_policy",
        created_at,
    )?];
    let retrieval_profiles = vec![put_json_ref(
        store,
        &json_value_of(&store.settings().retrieval_profile)?,
        "retrieval_profile",
        created_at,
    )?];
    // Required-annotation-set policy is serialized alongside the assembly policy
    // and retrieval profile (§30.2 "required-annotation-set policy"); it shares
    // the capabilityProfiles slot with the capability-profile hash references
    // below since the manifest has no dedicated section for it and both are
    // "governing profile" artifacts.
    let mut capability_profiles = vec![put_json_ref(
        store,
        &json_value_of(crate::annotations::policy::active_policy()?)?,
        "required_annotation_set_policy",
        created_at,
    )?];
    // Parser/connector capability profiles are persisted ONLY as hashes on
    // parse_runs.capability_profile_hash and
    // acquisition_records.connector_config_hash — the full profile documents are
    // not stored rows. Per the ruling we reference the hashes (the parse_runs /
    // acquisition_records plane archives above already carry them) rather than
    // re-deriving the documents; this ref records that provenance explicitly.
    capability_profiles.push(capability_profile_hash_reference(created_at));

    // --- Deletion evidence for the interval (§30.2 "Deletion evidence
    // records"). Deletion evidence lives on source_locations.deletion_evidence_json;
    // the archived record set is the source_locations rows carrying evidence.
    let deletion_records = archive_deletion_evidence(store, connection, scope)?;

    // --- Runtime artifacts (§30.2 "Application identity"): the COMPLETE
    // replay-environment identity (§30.7) — systemVersion, specVersion,
    // buildFeatures, AND the aggregate configurationHash. The configuration hash
    // now arrives explicitly threaded (`identity`, captured once at startup per
    // the 2026-07-16 ruling), so the recorded identity is whole rather than
    // config-blind.
    let runtime_artifacts = vec![archive_application_identity(store, identity, created_at)?];

    let mut manifest = ForensicSnapshotManifest {
        snapshot_id: snapshot_id.to_owned(),
        created_at: created_at.to_owned(),
        format_version: Some(MANIFEST_FORMAT_VERSION),
        source_objects: vec![source_objects],
        acquisition_records: vec![acquisition_records],
        parse_runs: vec![parse_runs],
        // Sealed §12.3 canonical parse bundles are REFERENCED by their existing
        // parse_runs.artifact_bundle_uri/_hash, never re-archived.
        canonical_parse_bundles: reference_canonical_parse_bundles(connection, scope)?,
        // Parser output bundles are staging-only today (spec-optional §30.4);
        // absent, not empty.
        parser_output_bundles: None,
        content_units: vec![content_units],
        unit_relationships: vec![unit_relationships],
        semantic_annotations: vec![semantic_annotations],
        retrieval_projections,
        retrieval_indexes,
        assembly_policies,
        retrieval_profiles,
        capability_profiles,
        // QER-tier deferral: no query execution records are associated at MVP.
        query_execution_records: Vec::new(),
        deletion_records,
        runtime_artifacts,
        // No stable immutable model-weight references are reachable from the
        // pinned signatures (they live on ServiceConfig.models paths); the
        // section is spec-optional, so it is absent rather than fabricated.
        model_artifacts: None,
        manifest_hash: String::new(),
    };

    // Every referenced blob must be present before the manifest seals: a
    // manifest that references absent bytes would pass its own self-hash yet
    // fail mechanical verification. Prove presence now, at the write boundary,
    // where the failure is attributable to the missing artifact.
    verify_all_refs_present(store, &manifest)?;
    // Exact input and nested-payload validation runs against the just-archived
    // database view before the manifest can be sealed as a completed snapshot.
    let annotation_publications =
        verify::verified_annotation_publications(store, snapshot_id, &manifest)?;

    // manifestHash covers the manifest body WITHOUT its own hash field — a
    // record cannot contain its own hash (§30.4, §16.2). The shared self-hash
    // helper removes the placeholder before hashing and fails loudly if a
    // struct rename ever makes the field disappear.
    manifest.manifest_hash =
        canonical::canonical_sha256_hex_without_field(&manifest, "manifestHash")?;

    // Per-plane archived counts for the completion log, read back from the
    // sealed refs (JSONL planes carry `recordCount`; the two blob planes were
    // counted from their blob-ref vectors above). Diagnostic only.
    let counts = ArchivedCounts {
        source_objects: manifest
            .source_objects
            .first()
            .map_or(0, jsonl_record_count),
        acquisition_records: manifest
            .acquisition_records
            .first()
            .map_or(0, jsonl_record_count),
        parse_runs: manifest.parse_runs.first().map_or(0, jsonl_record_count),
        content_units: manifest.content_units.first().map_or(0, jsonl_record_count),
        unit_relationships: manifest
            .unit_relationships
            .first()
            .map_or(0, jsonl_record_count),
        semantic_annotations: manifest
            .semantic_annotations
            .first()
            .map_or(0, jsonl_record_count),
        retrieval_projections: retrieval_projection_count(&manifest, "retrieval_projections"),
        chunk_projections: retrieval_projection_count(&manifest, "chunk_projections"),
        dense_blobs: dense_blob_count,
        section_dense_payloads,
        annotation_manifests: annotation_publications.manifest_count,
        annotation_embedding_blobs: annotation_publications.embedding_blob_count,
        multivector_blobs: multivector_blob_count,
        deletion_evidence_rows: manifest
            .deletion_records
            .as_ref()
            .and_then(|refs| refs.first())
            .map_or(0, jsonl_record_count),
    };

    // The manifest is the LAST artifact written (mirrors the parse bundle
    // manifest): every artifact it references is already durable. `put_json`
    // canonicalizes the value, so the stored bytes match the self-hash input.
    let manifest_value = json_value_of(&manifest)?;
    let manifest_ref = store.put_json(&manifest_value)?;
    Ok((manifest_ref, counts))
}

/// Pin construction descriptors separately so restore can authenticate the original
/// chunk semantics after operators change indexing limits.
fn reference_chunk_policies(
    envelope_records: &[Value],
    store: &ArtifactStore,
) -> Result<Vec<SnapshotArtifactRef>, ApiError> {
    let mut refs = Vec::new();
    for record in envelope_records {
        if record.get("projection_type").and_then(Value::as_str) != Some("chunk") {
            continue;
        }
        let Some(uri) = record.get("payload_uri").and_then(Value::as_str) else {
            continue;
        };
        let policy: crate::projections::ChunkerConfig = store.with_verified_reader(
            uri,
            Some(store.limits().resources.max_json_cell_bytes as u64),
            |reader| {
                serde_json::from_reader(reader).map_err(|source| ApiError::StorageOperation {
                    message: format!("decode chunk construction descriptor {uri}: {source}"),
                })
            },
        )?;
        policy.validate()?;
        let stored = store.reference_for_uri(uri)?;
        refs.push(SnapshotArtifactRef {
            artifact_type: crate::projections::CHUNK_CONFIG_PAYLOAD_TYPE.to_owned(),
            uri: stored.uri,
            hash: stored.hash,
            format: Some("json".to_owned()),
            created_at: record
                .get("created_at")
                .and_then(Value::as_str)
                .map(str::to_owned),
            metadata: Some(BTreeMap::from([(
                "projectionId".to_owned(),
                record["id"].clone(),
            )])),
        });
    }
    Ok(refs)
}

/// Pin immutable section payloads explicitly: envelope URIs alone are not a
/// manifest dependency and would otherwise escape completeness/hash checks.
fn reference_section_dense_payloads(
    envelope_records: &[Value],
    connection: &Connection,
    store: &ArtifactStore,
) -> Result<Vec<SnapshotArtifactRef>, ApiError> {
    use crate::projections::section_dense::{SECTION_DENSE_INDEX_NAME, SectionDensePlane};
    let mut refs = Vec::new();
    for record in envelope_records {
        if record.get("index_name").and_then(Value::as_str) != Some(SECTION_DENSE_INDEX_NAME) {
            continue;
        }
        let Some(uri) = record.get("payload_uri").and_then(Value::as_str) else {
            // A building/failed attempt has no completed representation to archive.
            if matches!(
                record.get("freshness_status").and_then(Value::as_str),
                Some("building" | "failed")
            ) {
                continue;
            }
            return Err(ApiError::StorageOperation {
                message: "section dense projection has no payload URI".to_owned(),
            });
        };
        let stored = store.reference_for_uri(uri)?;
        let plane: SectionDensePlane = serde_json::from_slice(&store.get_bytes(&stored.hash)?)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("invalid section dense payload {}: {source}", stored.hash),
            })?;
        let plane = crate::projections::section_dense::read_section_payload(
            store,
            uri,
            &plane.source_id,
            &plane.parse_id,
            plane.dimension,
        )?;
        crate::projections::section_dense::validate_plane(connection, &plane)?;
        if record.get("source_id").and_then(Value::as_str) != Some(plane.source_id.as_str())
            || record.get("parse_id").and_then(Value::as_str) != Some(plane.parse_id.as_str())
        {
            return Err(ApiError::StorageOperation {
                message: "section dense envelope/payload ownership mismatch".to_owned(),
            });
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("projectionId".to_owned(), record["id"].clone());
        metadata.insert("sourceId".to_owned(), Value::String(plane.source_id));
        metadata.insert("parseId".to_owned(), Value::String(plane.parse_id));
        metadata.insert("windowCount".to_owned(), Value::from(plane.windows.len()));
        metadata.insert("dimension".to_owned(), Value::from(plane.dimension));
        metadata.insert("policyHash".to_owned(), Value::String(plane.policy_hash));
        refs.push(SnapshotArtifactRef {
            artifact_type: SECTION_DENSE_PAYLOAD_TYPE.to_owned(),
            uri: stored.uri,
            hash: stored.hash,
            format: Some("json".to_owned()),
            created_at: record
                .get("created_at")
                .and_then(Value::as_str)
                .map(str::to_owned),
            metadata: Some(metadata),
        });
    }
    Ok(refs)
}

/// Pin every completed annotation manifest and its nested immutable embedding
/// blobs. Paired envelopes share one manifest; repeated model payloads are
/// referenced once by content hash without losing their per-manifest shapes.
fn reference_annotation_payloads(
    envelope_records: &[Value],
    store: &ArtifactStore,
) -> Result<Vec<SnapshotArtifactRef>, ApiError> {
    use crate::projections::annotation::{
        INDEX_NAME, PAYLOAD_TYPE, VECTOR_PAYLOAD_TYPE, read_manifest,
    };
    let mut refs = BTreeMap::new();
    for record in envelope_records {
        if record.get("index_name").and_then(Value::as_str) != Some(INDEX_NAME) {
            continue;
        }
        let Some(uri) = record.get("payload_uri").and_then(Value::as_str) else {
            if matches!(
                record.get("freshness_status").and_then(Value::as_str),
                Some("building" | "failed")
            ) {
                continue;
            }
            return Err(ApiError::StorageOperation {
                message: "completed annotation projection has no payload URI".to_owned(),
            });
        };
        let stored = store.reference_for_uri(uri)?;
        if refs.contains_key(&(PAYLOAD_TYPE, stored.hash.clone())) {
            continue;
        }
        let publication = read_manifest(store, uri)?;
        for representation in &publication.representations {
            for embedding in [&representation.dense, &representation.colbert] {
                let key = (VECTOR_PAYLOAD_TYPE, embedding.artifact.hash.clone());
                if refs.contains_key(&key) {
                    continue;
                }
                let actual = store.reference_for_uri(&embedding.artifact.uri)?;
                if actual.hash != embedding.artifact.hash
                    || actual.size_bytes != embedding.artifact.size_bytes
                {
                    return Err(ApiError::StorageOperation {
                        message: format!(
                            "annotation embedding {} differs from its manifest reference",
                            embedding.artifact.hash
                        ),
                    });
                }
                refs.insert(
                    key,
                    SnapshotArtifactRef {
                        artifact_type: VECTOR_PAYLOAD_TYPE.to_owned(),
                        uri: actual.uri,
                        hash: actual.hash,
                        format: Some("f32_le".to_owned()),
                        created_at: None,
                        metadata: None,
                    },
                );
            }
        }
        refs.insert(
            (PAYLOAD_TYPE, stored.hash.clone()),
            SnapshotArtifactRef {
                artifact_type: PAYLOAD_TYPE.to_owned(),
                uri: stored.uri,
                hash: stored.hash,
                format: Some("json".to_owned()),
                created_at: None,
                metadata: None,
            },
        );
    }
    Ok(refs.into_values().collect())
}

/// The archived record count of the `retrieval_projections` section's JSONL ref
/// of a given artifact type. That section holds both the `retrieval_projections`
/// and `chunk_projections` metadata refs; each is looked up by artifactType so
/// the completion log reports each plane distinctly. A missing ref counts as
/// zero (diagnostic, not a correctness gate).
fn retrieval_projection_count(manifest: &ForensicSnapshotManifest, artifact_type: &str) -> usize {
    manifest
        .retrieval_projections
        .iter()
        .find(|artifact| artifact.artifact_type == artifact_type)
        .map_or(0, jsonl_record_count)
}

/// `ORDER BY id` — the deterministic row order for every plane archived by
/// primary key. Fixed here so the archived JSONL bytes are reproducible.
const ORDER_BY_ID: &str = "ORDER BY id";

/// Archive the in-scope rows of one hot-plane table as a canonical JSONL record
/// set, ordered by primary key, and return its `SnapshotArtifactRef` using the
/// table name as the artifact type. This is the "exact artifact projection" of
/// hot relational state (§30.2): lossless (every column captured) and
/// deterministic (fixed column names, fixed row order), so the archived bytes
/// re-hash stably.
fn archive_plane(
    store: &ArtifactStore,
    connection: &Connection,
    table: &str,
    scope: PlaneScope<'_>,
) -> Result<SnapshotArtifactRef, ApiError> {
    let records = read_table_as_json(connection, table, scope, None, ORDER_BY_ID)?;
    let stored = store.put_jsonl(&records)?;
    Ok(jsonl_ref(table, &stored, records.len()))
}

/// Read the in-scope rows of `table` as JSON objects. The SQL comes from
/// `scoped_select` (code-controlled table, column, and predicate text; only the
/// scope value is bound); column values are read generically via `ValueRef` so
/// this one reader serves every archived plane. The statement is fully
/// consumed and dropped before this returns, so the SQL execution budget covers
/// only the row reads.
fn read_table_as_json(
    connection: &Connection,
    table: &str,
    scope: PlaneScope<'_>,
    fixed_predicate: Option<&str>,
    order_by: &str,
) -> Result<Vec<Value>, ApiError> {
    let (sql, parameters) = scoped_select(table, scope, fixed_predicate, order_by);
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare snapshot read of {table}: {source}"),
        })?;
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let mut rows = statement
        .query(rusqlite::params_from_iter(parameters))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query {table} for snapshot: {source}"),
        })?;

    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read a {table} row for snapshot: {source}"),
    })? {
        let mut object = Map::new();
        for (index, name) in column_names.iter().enumerate() {
            object.insert(name.clone(), column_value_to_json(row, index, table, name)?);
        }
        records.push(Value::Object(object));
    }
    Ok(records)
}

/// Convert one SQLite column value to canonical JSON. BLOB columns are the
/// carrier of the dense/multivector planes, which are archived as raw bytes
/// elsewhere; a BLOB reaching THIS generic reader means a plane not intended to
/// be JSON-projected has a binary column, so it is an explicit error rather than
/// a lossy encoding. Integers and reals map to JSON numbers; a non-finite real
/// (NaN/Inf) is rejected because canonical JSON forbids it (§16.2).
fn column_value_to_json(
    row: &rusqlite::Row<'_>,
    index: usize,
    table: &str,
    column: &str,
) -> Result<Value, ApiError> {
    let value_ref = row
        .get_ref(index)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read {table}.{column} for snapshot: {source}"),
        })?;
    match value_ref {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(value) => Ok(Value::from(value)),
        ValueRef::Real(value) => {
            // Canonical JSON has no NaN/Infinity; a non-finite stored REAL is
            // corrupt content, surfaced explicitly instead of silently dropped.
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .ok_or_else(|| ApiError::StorageOperation {
                    message: format!(
                        "{table}.{column} holds a non-finite REAL that cannot be \
                         canonically serialized for snapshot archival"
                    ),
                })
        }
        ValueRef::Text(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "{table}.{column} is not valid UTF-8 for snapshot archival: {source}"
                ),
            })?;
            Ok(Value::String(text.to_owned()))
        }
        ValueRef::Blob(_) => Err(ApiError::StorageOperation {
            message: format!(
                "{table}.{column} is a BLOB and cannot be JSON-projected; binary \
                 planes are archived as raw content-addressed bytes, not JSONL"
            ),
        }),
    }
}

/// One archived binary plane: its metadata JSONL ref, the single plane blob ref
/// (absent when the plane had no in-scope rows, so the manifest never points at
/// an empty blob), and the number of rows folded into that blob.
struct BlobPlaneArchive {
    metadata: SnapshotArtifactRef,
    plane: Option<SnapshotArtifactRef>,
    row_count: usize,
}

/// Archive a binary plane (dense vectors, multivector matrices) in the
/// `MANIFEST_FORMAT_VERSION` layout: every in-scope row's binary payload is
/// concatenated, in primary-key order, into ONE content-addressed blob, and each
/// row's scalar columns become one metadata record carrying the row's byte
/// range within that blob. Blobs are archived as bytes so restore RE-IMPORTS
/// them (§31.3) without re-embedding — they are byte-reproducible only this way.
///
/// Rows are read completely and the statement dropped BEFORE any artifact
/// write: the SQL execution budget covers open row iteration, so filesystem
/// work between rows would be charged to SQLite and could interrupt the read.
/// The whole plane is therefore held in memory between the read and the write.
fn archive_blob_plane(
    store: &ArtifactStore,
    connection: &Connection,
    scope: PlaneScope<'_>,
    table: &str,
    key_column: &str,
    blob_column: &str,
    blob_artifact_type: &str,
) -> Result<BlobPlaneArchive, ApiError> {
    // Order by primary key for a reproducible metadata record set.
    let (sql, parameters) = scoped_select(table, scope, None, &format!("ORDER BY {key_column}"));
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare snapshot read of {table}: {source}"),
        })?;
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let blob_index = column_names
        .iter()
        .position(|name| name == blob_column)
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!("{table} has no {blob_column} column to archive as bytes"),
        })?;

    let mut rows = statement
        .query(rusqlite::params_from_iter(parameters))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query {table} for snapshot: {source}"),
        })?;

    // Phase 1 (under the SQL budget): scalar columns and payload bytes, no I/O.
    let mut metadata_records = Vec::new();
    let mut plane_bytes: Vec<u8> = Vec::new();
    while let Some(row) = rows.next().map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read a {table} row for snapshot: {source}"),
    })? {
        let blob = row
            .get_ref(blob_index)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to read {table}.{blob_column} for snapshot: {source}"),
            })?;
        let blob_bytes = match blob {
            ValueRef::Blob(bytes) => bytes,
            _ => {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "{table}.{blob_column} is not a BLOB; the binary plane archival \
                         path requires a BLOB payload column"
                    ),
                });
            }
        };
        let offset = plane_bytes.len() as u64;
        let length = blob_bytes.len() as u64;
        plane_bytes.extend_from_slice(blob_bytes);

        // Build the scalar metadata record, replacing the binary column with the
        // row's byte range in the plane blob so the record set stays JSON and the
        // row's identity → bytes mapping is preserved.
        let mut object = Map::new();
        for (index, name) in column_names.iter().enumerate() {
            if index == blob_index {
                object.insert(format!("{blob_column}Offset"), Value::from(offset));
                object.insert(format!("{blob_column}Length"), Value::from(length));
                continue;
            }
            object.insert(name.clone(), column_value_to_json(row, index, table, name)?);
        }
        metadata_records.push(Value::Object(object));
    }
    drop(rows);
    drop(statement);

    // Phase 2 (outside any statement): one plane blob, then the metadata set.
    let row_count = metadata_records.len();
    let plane = if row_count == 0 {
        None
    } else {
        let plane_ref = store.put_bytes(&plane_bytes)?;
        Some(SnapshotArtifactRef {
            artifact_type: blob_artifact_type.to_owned(),
            uri: plane_ref.uri,
            format: Some("bytes".to_owned()),
            hash: plane_ref.hash,
            created_at: None,
            metadata: Some(BTreeMap::from([(
                "rowCount".to_owned(),
                Value::from(row_count as u64),
            )])),
        })
    };
    let stored = store.put_jsonl(&metadata_records)?;
    let metadata = jsonl_ref(&format!("{table}_metadata"), &stored, row_count);
    Ok(BlobPlaneArchive {
        metadata,
        plane,
        row_count,
    })
}

/// Archive the deletion-evidence record set (§30.2). Deletion evidence lives on
/// `source_locations.deletion_evidence_json`; the archived set is exactly the
/// source_locations rows that carry evidence, so absence produces `None` (the
/// manifest field is spec-optional) rather than an empty archived blob.
fn archive_deletion_evidence(
    store: &ArtifactStore,
    connection: &Connection,
    scope: PlaneScope<'_>,
) -> Result<Option<Vec<SnapshotArtifactRef>>, ApiError> {
    let records = read_table_as_json(
        connection,
        "source_locations",
        scope,
        Some("deletion_evidence_json IS NOT NULL"),
        ORDER_BY_ID,
    )?;
    if records.is_empty() {
        return Ok(None);
    }
    let stored = store.put_jsonl(&records)?;
    Ok(Some(vec![jsonl_ref(
        "deletion_evidence",
        &stored,
        records.len(),
    )]))
}

/// Reference the sealed §12.3 canonical parse bundles by their EXISTING
/// artifact_bundle_uri/_hash (never re-archived — write-once already stored
/// them at parse time). One ref per parse_run that has a bundle; ordering by id
/// keeps the reference list deterministic.
fn reference_canonical_parse_bundles(
    connection: &Connection,
    scope: PlaneScope<'_>,
) -> Result<Vec<SnapshotArtifactRef>, ApiError> {
    // The bundle columns are read from the same scoped parse_runs rows the
    // parse_runs plane archives, so referenced bundles and archived runs agree.
    let (sql, parameters) = scoped_select(
        "parse_runs",
        scope,
        Some("artifact_bundle_uri IS NOT NULL AND artifact_bundle_hash IS NOT NULL"),
        ORDER_BY_ID,
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare canonical parse bundle reference read: {source}"),
        })?;
    let refs = statement
        .query_map(rusqlite::params_from_iter(parameters), |row| {
            Ok(SnapshotArtifactRef {
                artifact_type: "canonical_parse_bundle".to_owned(),
                // Named access: the scoped SELECT projects every column.
                uri: row.get::<_, String>("artifact_bundle_uri")?,
                format: Some("json".to_owned()),
                hash: row.get::<_, String>("artifact_bundle_hash")?,
                created_at: None,
                metadata: None,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read canonical parse bundle references: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to collect canonical parse bundle references: {source}"),
        })?;
    Ok(refs)
}

/// Serialize the COMPLETE §30.7 replay-environment application identity and
/// archive it as one canonical JSON artifact: systemVersion, specVersion,
/// buildFeatures, AND the aggregate configurationHash. Every field comes from
/// the threaded `ApplicationIdentity` captured once at startup (2026-07-16
/// ruling — no global, no per-trigger `&ServiceConfig`), so the recorded
/// identity now pins the full code-and-config half of the replay environment
/// rather than leaving the configuration hash absent.
fn archive_application_identity(
    store: &ArtifactStore,
    identity: &ApplicationIdentity,
    created_at: &str,
) -> Result<SnapshotArtifactRef, ApiError> {
    // Serialized from the captured identity so the archived object is the single
    // source of truth for the recorded §30.2 identity shape (camelCase field
    // names come from the struct's serde rename).
    let identity_value = json_value_of(identity)?;
    put_json_ref(store, &identity_value, "application_identity", created_at)
}

/// A capability-profile reference recording that the parser/connector capability
/// profiles are pinned by hash (on the archived parse_runs.capability_profile_hash
/// and acquisition_records.connector_config_hash columns), not by an archived
/// document — a deliberate reference-by-hash per the 2026-07-16 ruling. Carries
/// no blob: the hashes it points at live inside the already-archived plane
/// projections, so this ref is a provenance marker with `metadata` explaining
/// the indirection.
fn capability_profile_hash_reference(created_at: &str) -> SnapshotArtifactRef {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "pinnedBy".to_owned(),
        Value::String(
            "parse_runs.capability_profile_hash and \
             acquisition_records.connector_config_hash in the archived plane projections"
                .to_owned(),
        ),
    );
    SnapshotArtifactRef {
        artifact_type: "capability_profile_hash_reference".to_owned(),
        // No standalone blob: the referenced hashes live in the archived plane
        // records, so uri/hash carry the marker's own sentinel rather than a
        // blob address. Verification skips refs of this artifact type (see
        // `verify_all_refs_present`), because there is no independent blob.
        uri: String::new(),
        format: None,
        hash: String::new(),
        created_at: Some(created_at.to_owned()),
        metadata: Some(metadata),
    }
}

/// Store a canonical JSON value as an artifact and wrap the store ref as a typed
/// manifest ref with `format: "json"`. Shared by the policy/profile/identity
/// archival sites so they all record the same format tag.
fn put_json_ref(
    store: &ArtifactStore,
    value: &Value,
    artifact_type: &str,
    created_at: &str,
) -> Result<SnapshotArtifactRef, ApiError> {
    let stored = store.put_json(value)?;
    Ok(SnapshotArtifactRef {
        artifact_type: artifact_type.to_owned(),
        uri: stored.uri,
        format: Some("json".to_owned()),
        hash: stored.hash,
        created_at: Some(created_at.to_owned()),
        metadata: None,
    })
}

/// Wrap an artifact-store ref for a JSONL record set as a typed manifest ref,
/// recording the archived record count in `metadata` so an auditor can see a
/// plane's cardinality without fetching the blob.
fn jsonl_ref(
    artifact_type: &str,
    stored: &ArtifactRef,
    record_count: usize,
) -> SnapshotArtifactRef {
    let mut metadata = BTreeMap::new();
    metadata.insert("recordCount".to_owned(), Value::from(record_count as u64));
    SnapshotArtifactRef {
        artifact_type: artifact_type.to_owned(),
        uri: stored.uri.clone(),
        format: Some("jsonl".to_owned()),
        hash: stored.hash.clone(),
        created_at: None,
        metadata: Some(metadata),
    }
}

/// The MVP replay profile stamped on every snapshot (§30.3 / §29.1). Evidence
/// replay is bit-exact (recorded evidence replays byte-for-byte, Guarantee 2);
/// retrieval and generation are `not_supported` — the probe/tolerance machinery
/// (§29.4) and QER tier do not exist at MVP, and the system must never claim a
/// mode it cannot demonstrate. `record_replay` is NEVER emitted here (it is
/// undemonstrable until the QER tier). `channelReplayModes`/`declaredTolerances`
/// are absent: no per-channel or rank-stable claim is made.
fn mvp_replay_profile() -> ReplayProfile {
    ReplayProfile {
        evidence_replay_mode: EvidenceReplayMode::BitExact,
        retrieval_replay_mode: RetrievalReplayMode::NotSupported,
        generation_replay_mode: GenerationReplayMode::NotSupported,
        channel_replay_modes: None,
        declared_tolerances: None,
    }
}

/// Prove every artifact the manifest references is present in the store before
/// the manifest seals (acceptance §3). A missing blob here is a snapshot-
/// incompleteness failure surfaced as `StorageOperation` with the offending
/// artifact identity. Refs with an empty hash are hash-only provenance markers
/// (the capability-profile-hash reference) with no independent blob and are
/// skipped — their referenced hashes are verified as part of the plane
/// projections that carry them.
fn verify_all_refs_present(
    store: &ArtifactStore,
    manifest: &ForensicSnapshotManifest,
) -> Result<(), ApiError> {
    for refs in manifest_ref_sections(manifest) {
        for artifact in refs {
            if artifact.hash.is_empty() {
                continue;
            }
            if !store.exists(&artifact.hash) {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "snapshot manifest references artifact {} ({}) that is not present \
                         in the store; snapshot is incomplete",
                        artifact.hash, artifact.artifact_type
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Every referenced-artifact section of the manifest, as one flat iterator, so
/// presence verification walks all sections without repeating the field list.
/// The optional sections contribute only when present.
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

/// Emit the `snapshot.started` audit event on its own IMMEDIATE transaction, so
/// it commits independently of the (later, possibly-failing) completion tx. See
/// `mint` for why the start boundary is durable on its own.
fn emit_started_event(
    connection: &mut Connection,
    snapshot_id: &str,
    created_at: &str,
    type_name: &str,
) -> Result<(), ApiError> {
    let event = new_system_event(
        SystemEventType::SnapshotStarted,
        OBJECT_TYPE_SNAPSHOT,
        snapshot_id,
        Some(
            [
                entry("snapshotType", type_name),
                entry("createdAt", created_at),
            ]
            .into_iter()
            .collect(),
        ),
    )?;
    append_event_in_tx(connection, "emit_started_event", &event)
}

/// Emit the `snapshot.failed` audit event on its own transaction, recording the
/// terminal failure of a snapshot whose completion tx never committed.
fn emit_failed_event(
    connection: &mut Connection,
    snapshot_id: &str,
    type_name: &str,
    source: &ApiError,
) -> Result<(), ApiError> {
    let event = new_system_event(
        SystemEventType::SnapshotFailed,
        OBJECT_TYPE_SNAPSHOT,
        snapshot_id,
        Some(
            [
                entry("snapshotType", type_name),
                entry("error", &source.to_string()),
            ]
            .into_iter()
            .collect(),
        ),
    )?;
    append_event_in_tx(connection, "emit_failed_event", &event)
}

/// Append one event inside its own IMMEDIATE transaction — the started/failed
/// boundary events that must commit independently of the completion tx. The
/// completion event does NOT use this: it commits ON the metadata-row tx (see
/// `commit_snapshot_row`) so the row and its `snapshot.completed` event are one
/// atomic unit per the `crate::events` invariant.
fn append_event_in_tx(
    connection: &mut Connection,
    operation: &'static str,
    event: &crate::model::SystemEvent,
) -> Result<(), ApiError> {
    let tx = hot_plane::begin_write_transaction(connection, TX_LOG_NAMESPACE, operation)?;
    if let Err(source) = append_event(&tx, event) {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            operation,
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, operation)
}

/// Commit the `forensic_snapshots` metadata row and the `snapshot.completed`
/// event in ONE IMMEDIATE transaction (the `crate::events` atomicity invariant):
/// the queryable header and its completion audit event become durable together
/// or not at all, so the audit trail can never claim a completed snapshot whose
/// row did not commit.
fn commit_snapshot_row(
    connection: &mut Connection,
    header: &ForensicSnapshot,
    subject_source_id: Option<&str>,
    subject_parse_id: Option<&str>,
) -> Result<(), ApiError> {
    let tx = hot_plane::begin_write_transaction(connection, TX_LOG_NAMESPACE, "commit_snapshot")?;
    if let Err(source) = snapshot_row_body(&tx, header, subject_source_id, subject_parse_id) {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "commit_snapshot",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "commit_snapshot")
}

/// The row + completed-event body of the metadata-commit transaction. The list
/// columns are canonical JSON string arrays (the repo list-column convention);
/// the completed event carries the manifest hash so an audit can jump from the
/// event to the archived manifest.
fn snapshot_row_body(
    tx: &Transaction<'_>,
    header: &ForensicSnapshot,
    subject_source_id: Option<&str>,
    subject_parse_id: Option<&str>,
) -> Result<(), ApiError> {
    let source_ids_json = canonical_json_string_of(&header.source_object_ids)?;
    let active_ids_json = canonical_json_string_of(&header.active_parse_ids)?;
    let type_name = snapshot_type_wire_name(header.snapshot_type);

    tx.execute(
        INSERT_FORENSIC_SNAPSHOT_SQL,
        params![
            header.id,
            type_name,
            subject_source_id,
            subject_parse_id,
            source_ids_json,
            active_ids_json,
            header.manifest_uri,
            header.manifest_hash,
            header.system_version,
            header.spec_version,
            header.created_at,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert forensic_snapshots row {}: {source}",
            header.id
        ),
    })?;

    let event = new_system_event(
        SystemEventType::SnapshotCompleted,
        OBJECT_TYPE_SNAPSHOT,
        &header.id,
        Some(
            [
                entry("snapshotType", type_name),
                entry("manifestUri", &header.manifest_uri),
                entry("manifestHash", &header.manifest_hash),
            ]
            .into_iter()
            .collect(),
        ),
    )?;
    append_event(tx, &event)
}

/// Look up the immutable source of a candidate parse (§12), the pre/post-
/// activation subject-source derivation. A parse_run id with no row is a
/// caller-contract `BadRequest`: the trigger was handed a parse that does not
/// exist.
fn lookup_source_of_parse(connection: &Connection, parse_run_id: &str) -> Result<String, ApiError> {
    connection
        .query_row(SELECT_PARSE_RUN_SOURCE_SQL, params![parse_run_id], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to look up source of parse {parse_run_id}: {source}"),
        })?
        .ok_or_else(|| ApiError::BadRequest {
            message: format!("cannot snapshot parse {parse_run_id}: no such parse run"),
        })
}

/// Look up a source's current active parse pointer (§14): `None` when the source
/// has no active parse. A missing source row is `None` too — the caller decides
/// whether that is an error for its trigger.
fn lookup_active_parse(
    connection: &Connection,
    source_id: &str,
) -> Result<Option<String>, ApiError> {
    connection
        .query_row(SELECT_ACTIVE_PARSE_ID_SQL, params![source_id], |row| {
            row.get::<_, Option<String>>(0)
        })
        .optional()
        .map(Option::flatten)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to look up active parse of source {source_id}: {source}"),
        })
}

/// Every source id in the corpus, ascending, for a corpus-wide snapshot's scope.
fn load_all_source_ids(connection: &Connection) -> Result<Vec<String>, ApiError> {
    load_id_column(
        connection,
        "SELECT id FROM source_objects ORDER BY id",
        "source ids",
    )
}

/// Every currently-active parse id in the corpus, ascending, for a corpus-wide
/// snapshot's active-parse scope.
fn load_all_active_parse_ids(connection: &Connection) -> Result<Vec<String>, ApiError> {
    load_id_column(
        connection,
        "SELECT active_parse_id FROM source_objects \
         WHERE active_parse_id IS NOT NULL ORDER BY active_parse_id",
        "active parse ids",
    )
}

/// Read a single TEXT id column into a Vec, sharing the prepare/query/collect
/// boilerplate between the corpus-wide scope reads.
fn load_id_column(connection: &Connection, sql: &str, what: &str) -> Result<Vec<String>, ApiError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare snapshot {what} read: {source}"),
        })?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query snapshot {what}: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to collect snapshot {what}: {source}"),
        })?;
    Ok(ids)
}

/// Convert a `Serialize` shape to a `serde_json::Value` for storage via
/// `put_json` (which then canonicalizes). A serialization failure is a loud
/// `InternalIo`, mirroring the importer/acquisition `json_value_of` helpers.
fn json_value_of<T: serde::Serialize>(value: &T) -> Result<Value, ApiError> {
    serde_json::to_value(value).map_err(|source| ApiError::InternalIo {
        message: format!("value is not representable as JSON for snapshot archival: {source}"),
    })
}

/// Canonical JSON TEXT of a `Serialize` shape for a hot list column
/// (`source_object_ids_json`, `active_parse_ids_json`) — the repo list-column
/// convention. Mirrors the private `canonical_json_string_of` helpers in
/// `parse::importer` and `acquisition`; canonical bytes are valid UTF-8 by
/// construction (§16.2), so the error arm keeps the panic-free policy.
fn canonical_json_string_of<T: serde::Serialize>(value: &T) -> Result<String, ApiError> {
    let bytes = canonical::canonical_json_bytes_of(value)?;
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical snapshot list-column bytes are not UTF-8: {source}"),
    })
}

/// The snake_case wire name of a `SnapshotType` (the value persisted in
/// `forensic_snapshots.snapshot_type` and logged), recovered from the enum's
/// serde rename so the persisted name never drifts from the wire schema. A
/// serialization failure is unreachable for this plain renamed enum, so it maps
/// to a loud `InternalIo` rather than a silent fallback.
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
