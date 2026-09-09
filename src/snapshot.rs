//! ForensicSnapshot minting (spec §30, §31.2): the lifecycle trigger entry
//! points that create a content-addressed snapshot at each moment §30.6
//! requires one. Each entry archives the manifest (§30.4) into the artifact
//! store, mints a `snap_` id, writes the `forensic_snapshots` metadata row, and
//! returns the `ForensicSnapshot` header so the caller can hand its
//! `manifestHash` to verification (§30.5).
//!
//! Handle contract (pinned by the C9s skeleton, mirrors
//! `activation::gate_and_activate`): a trigger receives `index_root` plus the
//! lifecycle subject id, opens its own write connection via
//! `hot_plane::open_write` and its own `ArtifactStore::open(index_root)`, and is
//! never handed a live `Connection`/`Transaction`. Lifecycle snapshots are
//! per-subject; the parameterized `manual`/`incident` entry leaves the subject
//! columns NULL. `scheduled` and `pre_deployment` are inert (no trigger
//! constructs them).
//!
//! Two-phase minting mirrors the parse importer's caller-side bundle write
//! (`parse::importer::write_canonical_parse_bundle`): every heavy artifact is
//! written to the write-once artifact store FIRST (idempotent, outside any SQL
//! transaction), the self-hashed manifest is written LAST, and only then does
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

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params};
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
    ReplayProfile, RetrievalReplayMode, SnapshotArtifactRef, SnapshotType, SystemEventType,
};
use crate::primitives::utc_now;

pub(crate) mod verify;

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
    index_root: &Path,
    identity: &ApplicationIdentity,
    parse_run_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): the scheduler calls this BEFORE
    // gate_and_activate acquires the source barrier, so no barrier is held here.
    let mut connection = hot_plane::open_write(index_root)?;
    let store = ArtifactStore::open(index_root)?;
    let source_id = lookup_source_of_parse(&connection, parse_run_id)?;
    mint_lifecycle_snapshot(
        &mut connection,
        &store,
        identity,
        SnapshotType::PreActivation,
        &source_id,
        parse_run_id,
    )
}

/// Mint the `post_activation` snapshot (§30.6 / §31.2 step 3), created AFTER a
/// successful cutover for `parse_run_id`. This is the snapshot the §31.2
/// deletion gate verifies over before superseded state is removed (user ruling
/// 2026-07-16: no separate pre-deletion snapshot). The candidate is now the
/// source's active parse, so the captured active set already reflects the
/// cutover.
pub(crate) fn post_activation_snapshot(
    index_root: &Path,
    identity: &ApplicationIdentity,
    parse_run_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): the scheduler calls this AFTER
    // gate_and_activate returns and its internal barrier has released.
    let mut connection = hot_plane::open_write(index_root)?;
    let store = ArtifactStore::open(index_root)?;
    let source_id = lookup_source_of_parse(&connection, parse_run_id)?;
    mint_lifecycle_snapshot(
        &mut connection,
        &store,
        identity,
        SnapshotType::PostActivation,
        &source_id,
        parse_run_id,
    )
}

/// Mint the `pre_deactivation` snapshot (§30.6 "Before deactivating a source").
/// Deactivation is source-scoped, so the subject is `source_id` and the subject
/// parse is the source's CURRENT active parse (the state deactivation is about
/// to retire). Called by C9c before it acquires the cutover barrier. A source
/// with no active parse is a caller-contract error: there is nothing to
/// snapshot before deactivating.
pub(crate) fn pre_deactivation_snapshot(
    index_root: &Path,
    identity: &ApplicationIdentity,
    source_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    // Barrier boundary (§31.1): C9c calls this BEFORE it acquires the source
    // barrier for the deactivation cutover.
    let mut connection = hot_plane::open_write(index_root)?;
    let store = ArtifactStore::open(index_root)?;
    let active_parse_id =
        lookup_active_parse(&connection, source_id)?.ok_or_else(|| ApiError::BadRequest {
            message: format!(
                "cannot mint pre_deactivation snapshot for source {source_id}: \
                 the source has no active parse to capture"
            ),
        })?;
    mint_lifecycle_snapshot(
        &mut connection,
        &store,
        identity,
        SnapshotType::PreDeactivation,
        source_id,
        &active_parse_id,
    )
}

/// Mint a `manual` or `incident` snapshot on request (§30.6 "On manual or
/// incident request"). Corpus-wide: it captures the full active set of every
/// source, so the subject columns stay NULL (per the schema and §30.3 — only
/// lifecycle snapshots carry a subject). `created_by`/`notes` thread operator
/// attribution and free-text context from the request. Exposed through the
/// manual/incident snapshot HTTP route in `crate::http` (`post_snapshots`).
pub(crate) fn request_snapshot(
    index_root: &Path,
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

    let mut connection = hot_plane::open_write(index_root)?;
    let store = ArtifactStore::open(index_root)?;

    // Corpus-wide scope: every source and its active parse.
    let sources = load_all_source_ids(&connection)?;
    let active_parses = load_all_active_parse_ids(&connection)?;
    let scope = SnapshotScope {
        snapshot_type,
        subject_source_id: None,
        subject_parse_id: None,
        source_object_ids: sources,
        active_parse_ids: active_parses,
        created_by: created_by.map(str::to_owned),
        notes: notes.map(str::to_owned),
    };
    mint(&mut connection, &store, identity, &scope)
}

/// Mint one per-source lifecycle snapshot. The subject source and subject parse
/// are set (schema partial index invariant: lifecycle snapshots ALWAYS set both
/// subject columns); the captured scope is exactly that source and that parse,
/// which is the artifact set the deletion gate and restore resolve by
/// `(subject_source_id, subject_parse_id, snapshot_type)`.
fn mint_lifecycle_snapshot(
    connection: &mut Connection,
    store: &ArtifactStore,
    identity: &ApplicationIdentity,
    snapshot_type: SnapshotType,
    source_id: &str,
    parse_id: &str,
) -> Result<ForensicSnapshot, ApiError> {
    let scope = SnapshotScope {
        snapshot_type,
        subject_source_id: Some(source_id.to_owned()),
        subject_parse_id: Some(parse_id.to_owned()),
        source_object_ids: vec![source_id.to_owned()],
        active_parse_ids: vec![parse_id.to_owned()],
        created_by: None,
        notes: None,
    };
    mint(connection, store, identity, &scope)
}

/// The resolved scope of one snapshot: which sources/parses it covers and its
/// subject/attribution metadata. Built by the trigger entry points and consumed
/// by `mint`, so the two-phase minting body is independent of trigger kind.
struct SnapshotScope {
    snapshot_type: SnapshotType,
    subject_source_id: Option<String>,
    subject_parse_id: Option<String>,
    source_object_ids: Vec<String>,
    active_parse_ids: Vec<String>,
    created_by: Option<String>,
    notes: Option<String>,
}

/// The shared two-phase minting body (§30.1–30.4). Phase 1 archives every heavy
/// artifact and writes the self-hashed manifest to the write-once store (no SQL
/// transaction — the store is idempotent). Phase 2 verifies every referenced
/// blob still `exists()` and commits the metadata row plus its
/// `snapshot.completed` event in one IMMEDIATE hot-plane transaction. The
/// `snapshot.started` boundary is emitted on its own tiny transaction first, so
/// a crash during the (potentially long) artifact-archival phase still leaves
/// durable evidence that this snapshot began — matching the diagnostics
/// standard's start-boundary rule.
fn mint(
    connection: &mut Connection,
    store: &ArtifactStore,
    identity: &ApplicationIdentity,
    scope: &SnapshotScope,
) -> Result<ForensicSnapshot, ApiError> {
    let started = Instant::now();
    let snapshot_id = crate::ids::new_forensic_snapshot_id()?;
    let snapshot_log = crate::util::LogContext::new("snapshot", &snapshot_id);
    if let Some(source_id) = &scope.subject_source_id {
        snapshot_log.record("source_id", source_id.as_str());
    }
    if let Some(parse_id) = &scope.subject_parse_id {
        snapshot_log.record("parse_id", parse_id.as_str());
    }
    snapshot_log.record("trigger", snapshot_type_wire_name(scope.snapshot_type));
    let _snapshot_log = snapshot_log.enter();
    let created_at = utc_now()?;
    let type_name = snapshot_type_wire_name(scope.snapshot_type);

    info!(
        event = "snapshot.started",
        snapshot_id,
        snapshot_type = type_name,
        subject_source_id = scope.subject_source_id.as_deref().unwrap_or("none"),
        subject_parse_id = scope.subject_parse_id.as_deref().unwrap_or("none"),
        source_count = scope.source_object_ids.len() as u64,
        active_parse_count = scope.active_parse_ids.len() as u64,
        "forensic snapshot minting started"
    );
    // Durable started-boundary event on its own small transaction: the audit
    // trail records that a snapshot began even if archival later crashes. It is
    // NOT part of the completion transaction — a started event with no
    // completed event is exactly the recoverable "began but did not finish"
    // signal an operator needs.
    if let Err(source) = emit_started_event(connection, &snapshot_id, &created_at, type_name) {
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

    // Phase 1: archive every heavy artifact and build the self-hashed manifest.
    let (manifest_ref, counts) =
        match build_and_archive_manifest(connection, store, identity, &snapshot_id, &created_at) {
            Ok(result) => result,
            Err(source) => {
                return Err(fail_snapshot(
                    connection,
                    &snapshot_id,
                    type_name,
                    "archive",
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
        connection,
        &header,
        scope.subject_source_id.as_deref(),
        scope.subject_parse_id.as_deref(),
    ) {
        return Err(fail_snapshot(
            connection,
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
/// two blob planes carry their archived blob count (one blob per row). Counts
/// only — no contents.
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
/// archived counts for the `snapshot.completed` log.
fn build_and_archive_manifest(
    connection: &Connection,
    store: &ArtifactStore,
    identity: &ApplicationIdentity,
    snapshot_id: &str,
    created_at: &str,
) -> Result<(ArtifactRef, ArchivedCounts), ApiError> {
    // --- Hot relational planes archived as canonical JSONL record-set
    // projections (§30.2 "or its exact artifact projection"). Each plane is one
    // ordered record set; order is fixed by the query so the archived bytes are
    // reproducible.
    let source_objects = archive_plane(store, connection, "source_objects", ORDER_BY_ID)?;
    let acquisition_records = archive_plane(store, connection, "acquisition_records", ORDER_BY_ID)?;
    let parse_runs = archive_plane(store, connection, "parse_runs", ORDER_BY_ID)?;
    let content_units = archive_plane(store, connection, "content_units", ORDER_BY_ID)?;
    let unit_relationships = archive_plane(store, connection, "unit_relationships", ORDER_BY_ID)?;
    let semantic_annotations =
        archive_plane(store, connection, "semantic_annotations", ORDER_BY_ID)?;

    // Retrieval projection metadata + chunk payloads archived as JSONL; the
    // binary vector planes are archived below as raw content-addressed blobs.
    let mut retrieval_projections = Vec::new();
    retrieval_projections.push(archive_plane_ref(
        store,
        connection,
        "retrieval_projections",
        ORDER_BY_ID,
        "retrieval_projections",
    )?);
    retrieval_projections.push(archive_plane_ref(
        store,
        connection,
        "chunk_projections",
        ORDER_BY_ID,
        "chunk_projections",
    )?);

    // --- Dense/multivector blobs archived as raw bytes so restore RE-IMPORTS
    // them (§31.3) rather than re-embedding: they are byte-reproducible ONLY
    // from the stored blobs. Each row contributes one metadata record (its
    // scalar columns) referencing the archived blob's hash, and one blob ref.
    let (dense_meta, mut retrieval_indexes) = archive_blob_plane(
        store,
        connection,
        "chunk_dense_vectors",
        "chunk_id",
        "vector_blob",
        "dense_vector_blob",
    )?;
    retrieval_projections.push(dense_meta);
    // One archived blob per dense-vector row; captured before the multivector
    // blobs are folded into the same section.
    let dense_blob_count = retrieval_indexes.len();
    let (multivector_meta, multivector_blobs) = archive_blob_plane(
        store,
        connection,
        "unit_multivector_projections",
        "id",
        "matrix_blob",
        "multivector_blob",
    )?;
    retrieval_projections.push(multivector_meta);
    let multivector_blob_count = multivector_blobs.len();
    retrieval_indexes.extend(multivector_blobs);

    // --- Sealed policies/profiles serialized at snapshot time (§30.4
    // assemblyPolicies/retrievalProfiles/capabilityProfiles). These are
    // OnceLock constants never otherwise archived; their resolved values
    // (self-hash-sealed documents) are put_json'd here so the snapshot pins the
    // exact policy/profile identity that governed the captured state.
    let assembly_policies = vec![put_json_ref(
        store,
        &json_value_of(crate::assembly::policy::active_policy()?)?,
        "assembly_policy",
        created_at,
    )?];
    let retrieval_profiles = vec![put_json_ref(
        store,
        &json_value_of(crate::query::profile::active_profile()?)?,
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
    let deletion_records = archive_deletion_evidence(store, connection)?;

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
        source_objects: vec![source_objects],
        acquisition_records: vec![acquisition_records],
        parse_runs: vec![parse_runs],
        // Sealed §12.3 canonical parse bundles are REFERENCED by their existing
        // parse_runs.artifact_bundle_uri/_hash, never re-archived.
        canonical_parse_bundles: reference_canonical_parse_bundles(connection)?,
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

/// Archive one hot-plane table as a canonical JSONL record set and return its
/// `SnapshotArtifactRef`, using the table name as the artifact type. Thin
/// wrapper over `archive_plane_ref` for the common case where the artifact type
/// equals the table name.
fn archive_plane(
    store: &ArtifactStore,
    connection: &Connection,
    table: &str,
    order_by: &str,
) -> Result<SnapshotArtifactRef, ApiError> {
    archive_plane_ref(store, connection, table, order_by, table)
}

/// Read every row of `table` (in `order_by` order) as a canonical JSON object
/// keyed by column name, archive the ordered set as one JSONL blob, and return
/// a typed manifest ref. This is the "exact artifact projection" of hot
/// relational state (§30.2): lossless (every column captured) and deterministic
/// (fixed column names, fixed row order), so the archived bytes re-hash stably.
fn archive_plane_ref(
    store: &ArtifactStore,
    connection: &Connection,
    table: &str,
    order_by: &str,
    artifact_type: &str,
) -> Result<SnapshotArtifactRef, ApiError> {
    let records = read_table_as_json(connection, table, order_by)?;
    let stored = store.put_jsonl(&records)?;
    Ok(jsonl_ref(artifact_type, &stored, records.len()))
}

/// Read every row of `table` as a JSON object. The SQL is a bare
/// `SELECT * FROM <table> <order_by>` built from a code-controlled table name
/// (never user input), so there is no injection surface; column values are read
/// generically via `ValueRef` so this one reader serves every archived plane.
fn read_table_as_json(
    connection: &Connection,
    table: &str,
    order_by: &str,
) -> Result<Vec<Value>, ApiError> {
    let sql = format!("SELECT * FROM {table} {order_by}");
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
        .query([])
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

/// Archive a binary plane (dense vectors, multivector matrices): each row's
/// binary payload column is stored as a raw content-addressed blob (`put_bytes`)
/// and referenced by hash; the row's scalar columns become one metadata record
/// in a JSONL set that carries the blob hash. Returns the metadata ref plus one
/// blob ref per row. Blobs are archived as bytes so restore RE-IMPORTS them
/// (§31.3) without re-embedding — they are byte-reproducible only this way.
fn archive_blob_plane(
    store: &ArtifactStore,
    connection: &Connection,
    table: &str,
    key_column: &str,
    blob_column: &str,
    blob_artifact_type: &str,
) -> Result<(SnapshotArtifactRef, Vec<SnapshotArtifactRef>), ApiError> {
    // Order by primary key for a reproducible metadata record set.
    let sql = format!("SELECT * FROM {table} ORDER BY {key_column}");
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
        .query([])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query {table} for snapshot: {source}"),
        })?;

    let mut metadata_records = Vec::new();
    let mut blob_refs = Vec::new();
    while let Some(row) = rows.next().map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read a {table} row for snapshot: {source}"),
    })? {
        // Archive the raw blob bytes content-addressed.
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
        let blob_ref = store.put_bytes(blob_bytes)?;
        blob_refs.push(SnapshotArtifactRef {
            artifact_type: blob_artifact_type.to_owned(),
            uri: blob_ref.uri.clone(),
            format: Some("bytes".to_owned()),
            hash: blob_ref.hash.clone(),
            created_at: None,
            metadata: None,
        });

        // Build the scalar metadata record, replacing the binary column with a
        // reference to its archived blob hash so the record set stays JSON and
        // the row's identity → blob mapping is preserved.
        let mut object = Map::new();
        for (index, name) in column_names.iter().enumerate() {
            if index == blob_index {
                object.insert(
                    format!("{blob_column}Hash"),
                    Value::String(blob_ref.hash.clone()),
                );
                continue;
            }
            object.insert(name.clone(), column_value_to_json(row, index, table, name)?);
        }
        metadata_records.push(Value::Object(object));
    }

    let stored = store.put_jsonl(&metadata_records)?;
    let metadata_ref = jsonl_ref(
        &format!("{table}_metadata"),
        &stored,
        metadata_records.len(),
    );
    Ok((metadata_ref, blob_refs))
}

/// Archive the deletion-evidence record set (§30.2). Deletion evidence lives on
/// `source_locations.deletion_evidence_json`; the archived set is exactly the
/// source_locations rows that carry evidence, so absence produces `None` (the
/// manifest field is spec-optional) rather than an empty archived blob.
fn archive_deletion_evidence(
    store: &ArtifactStore,
    connection: &Connection,
) -> Result<Option<Vec<SnapshotArtifactRef>>, ApiError> {
    let records = read_table_as_json(
        connection,
        "source_locations",
        "WHERE deletion_evidence_json IS NOT NULL ORDER BY id",
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
) -> Result<Vec<SnapshotArtifactRef>, ApiError> {
    const SELECT_BUNDLES_SQL: &str = "
SELECT artifact_bundle_uri, artifact_bundle_hash
FROM parse_runs
WHERE artifact_bundle_uri IS NOT NULL AND artifact_bundle_hash IS NOT NULL
ORDER BY id";
    let mut statement =
        connection
            .prepare(SELECT_BUNDLES_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare canonical parse bundle reference read: {source}"
                ),
            })?;
    let refs = statement
        .query_map([], |row| {
            Ok(SnapshotArtifactRef {
                artifact_type: "canonical_parse_bundle".to_owned(),
                uri: row.get::<_, String>(0)?,
                format: Some("json".to_owned()),
                hash: row.get::<_, String>(1)?,
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
    tx: &rusqlite::Transaction<'_>,
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
