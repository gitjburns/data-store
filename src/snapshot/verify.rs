//! ForensicSnapshot verification tiers (spec §30.5). Two of the three tiers
//! run unattended inside the lifecycle:
//!
//! - Mechanical (every snapshot): manifest completeness plus hash verification
//!   of every referenced artifact.
//! - Deletion gate (before superseded hot state is removed, §31.2 step 4):
//!   mechanical verification PLUS index-rebuild verification (indexes rebuild
//!   deterministically from referenced artifacts).
//!
//! (The scheduled restore-drill tier is a full clean-workspace restore, driven
//! elsewhere; it is NOT built here — QER-tier deferral.) A verification failure
//! halts the affected source's lifecycle only and never silently proceeds to
//! deletion.
//!
//! Lookup contract (pinned by C9s): the caller passes the resolved
//! `&ForensicSnapshot` header — the trigger fn in `super` already returned it
//! and the lifecycle flow holds it, or C9d looks it up — so verification does
//! not re-query `forensic_snapshots` by identity. The manifest is reached via
//! the header's `manifest_uri`/`manifest_hash`; the artifact store via
//! `ArtifactStore::open(index_root)`; the hot plane via this module's own
//! read-only connection (`hot_plane::open_read`).
//!
//! CALLER-vs-VERIFIER FAILURE DUTY SPLIT (§30.5 / §31.2). These functions
//! decide ONLY whether a snapshot verifies: a failure returns
//! `Err(ApiError::SnapshotVerificationFailed)`. Everything the spec attaches to
//! a failure BEYOND the verdict — retaining superseded hot state, refusing to
//! delete, never auto-retrying past a failed gate, and surfacing the halt in
//! health (§10b) — is the CALLER's duty (C9d, the deletion flow). This module
//! never deletes, never retries, and never mutates lifecycle state.
//!
//! NO MODEL CALL ANYWHERE IN THIS MODULE (§38, hard invariant). The deletion
//! gate's rebuild check RE-IMPORTS archived bytes and RE-DERIVES from archived
//! rows; it never re-embeds, re-parses, or re-scores. Dense/multivector vectors
//! are byte-reproducible only from their stored blobs (§31.3), so they are
//! decoded via `primitives::codec` and compared, not regenerated. A grep for
//! `embed|InferenceRuntime|score_` over this file must stay empty.

// Both tiers are live: `src/restore.rs` imports this module (restore.rs:59) and
// calls `verify_deletion_gate` (restore.rs:314) and `verify_mechanical`
// (restore.rs:751), and every private helper is transitively reachable from
// them — so no module-level dead-code allow is needed.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use rusqlite::{Connection, params};
use serde_json::{Map, Value};
use tracing::{error, info};

use crate::artifact_store::ArtifactStore;
use crate::error::ApiError;
use crate::hot_plane;
use crate::model::{ForensicSnapshot, ForensicSnapshotManifest, SnapshotArtifactRef};
use crate::projections::ChunkerConfig;
use crate::projections::graph::normalize_entity_name;

/// Log-event namespace for this module's verification boundary logs, so every
/// verify line is attributable to snapshot verification.
const VERIFY_LOG_NAMESPACE: &str = "snapshot.verify";

/// Artifact type of the capability-profile hash-marker ref C9a emits. It has NO
/// independent blob (uri/hash are empty sentinels); its referenced hashes live
/// inside the archived plane projections. Named here so the mechanical tier can
/// account for it deliberately rather than tripping over an empty hash. Must
/// stay in step with `snapshot::capability_profile_hash_reference`.
const CAPABILITY_PROFILE_HASH_MARKER_TYPE: &str = "capability_profile_hash_reference";

/// Mechanical verification (§30.5, every snapshot): confirm the archived
/// manifest is complete and that every referenced artifact's stored bytes hash
/// to its recorded hash. `snapshot` is the resolved header (carrying
/// `manifestUri`/`manifestHash`); `index_root` locates the artifact store. Ok
/// means the snapshot is mechanically sound; Err carries the first failing
/// artifact's context so the lifecycle halt is diagnosable.
pub(crate) fn verify_mechanical(
    index_root: &Path,
    snapshot: &ForensicSnapshot,
) -> Result<(), ApiError> {
    let store = ArtifactStore::open(index_root)?;
    let started = Instant::now();
    info!(
        event = "snapshot.verify.mechanical_started",
        tier = "mechanical",
        snapshot_id = %snapshot.id,
        manifest_hash = %snapshot.manifest_hash,
        "mechanical snapshot verification started"
    );

    let manifest = match load_and_check_manifest(&store, snapshot) {
        Ok(manifest) => manifest,
        Err(source) => return Err(log_tier_failure("mechanical", snapshot, started, source)),
    };

    let counts = match verify_all_refs_hash(&store, snapshot, &manifest) {
        Ok(counts) => counts,
        Err(source) => return Err(log_tier_failure("mechanical", snapshot, started, source)),
    };

    info!(
        event = "snapshot.verify.mechanical_succeeded",
        tier = "mechanical",
        snapshot_id = %snapshot.id,
        sections_checked = counts.sections_checked as u64,
        blob_refs_hashed = counts.blob_refs_hashed as u64,
        marker_refs_skipped = counts.marker_refs_skipped as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "mechanical snapshot verification succeeded"
    );
    Ok(())
}

/// Deletion-gate verification (§30.5 / §31.2 step 4): mechanical verification
/// plus deterministic index-rebuild verification over the subject snapshot,
/// run before superseded hot state is deleted. Called by C9d in the deletion
/// flow with the resolved subject `snapshot` (the `post_activation` snapshot in
/// the activation flow, `pre_deactivation` in the deactivation flow — never
/// re-taken). Ok clears the deletion gate; Err halts before deletion and — by
/// the caller's duty — retains superseded state.
pub(crate) fn verify_deletion_gate(
    index_root: &Path,
    snapshot: &ForensicSnapshot,
) -> Result<(), ApiError> {
    // Mechanical is the floor: a deletion gate that skipped it could delete over
    // a corrupt manifest. Its own logs cover the mechanical boundary; this tier
    // adds the rebuild boundary on top.
    verify_mechanical(index_root, snapshot)?;

    let store = ArtifactStore::open(index_root)?;
    let connection = hot_plane::open_read(index_root)?;
    let started = Instant::now();

    // The subject parse the rebuild check is scoped to. Lifecycle snapshots set
    // `active_parse_ids = [subject_parse_id]` (see
    // `snapshot::mint_lifecycle_snapshot`); the deletion gate only ever runs
    // over a lifecycle snapshot, so exactly one active parse is expected. A
    // corpus-wide or empty scope reaching the gate is a caller-contract error.
    let subject_parse_id = subject_parse_of(snapshot)?;
    info!(
        event = "snapshot.verify.deletion_gate_started",
        tier = "deletion_gate",
        snapshot_id = %snapshot.id,
        subject_parse_id = %subject_parse_id,
        "deletion-gate rebuild verification started"
    );

    let manifest = load_and_check_manifest(&store, snapshot)?;

    // Each sub-check compares an archived artifact against the live hot plane,
    // returns the count of rows it compared, and returns Err on the first
    // disagreement. Order is cheapest-first so a common failure surfaces without
    // decoding every vector.
    let chunks_compared =
        match verify_chunk_plane(&store, &connection, snapshot, &manifest, &subject_parse_id) {
            Ok(count) => count,
            Err(source) => {
                return Err(log_tier_failure("deletion_gate", snapshot, started, source));
            }
        };
    let dense_compared =
        match verify_dense_plane(&store, &connection, snapshot, &manifest, &subject_parse_id) {
            Ok(count) => count,
            Err(source) => {
                return Err(log_tier_failure("deletion_gate", snapshot, started, source));
            }
        };
    let multivector_compared =
        match verify_multivector_plane(&store, &connection, snapshot, &manifest, &subject_parse_id)
        {
            Ok(count) => count,
            Err(source) => {
                return Err(log_tier_failure("deletion_gate", snapshot, started, source));
            }
        };
    let (graph_mentions_compared, graph_edges_compared) =
        match verify_graph_plane(&store, &connection, snapshot, &manifest, &subject_parse_id) {
            Ok(counts) => counts,
            Err(source) => {
                return Err(log_tier_failure("deletion_gate", snapshot, started, source));
            }
        };
    // Lexical is verified transitively through the chunk plane (see
    // `verify_lexical_plane`), which only re-checks the deterministic producer
    // identity — the FTS5 virtual table's internals cannot be diffed.
    if let Err(source) = verify_lexical_plane(snapshot) {
        return Err(log_tier_failure("deletion_gate", snapshot, started, source));
    }

    let counts = RebuildCounts {
        chunks_compared,
        dense_compared,
        multivector_compared,
        graph_mentions_compared,
        graph_edges_compared,
    };
    info!(
        event = "snapshot.verify.deletion_gate_succeeded",
        tier = "deletion_gate",
        snapshot_id = %snapshot.id,
        subject_parse_id = %subject_parse_id,
        chunks_compared = counts.chunks_compared as u64,
        dense_compared = counts.dense_compared as u64,
        multivector_compared = counts.multivector_compared as u64,
        graph_mentions_compared = counts.graph_mentions_compared as u64,
        graph_edges_compared = counts.graph_edges_compared as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "deletion-gate rebuild verification succeeded"
    );
    Ok(())
}

/// Counts recorded for the mechanical-tier success log: how much was actually
/// checked, so an operator can confirm the tier did real work rather than
/// short-circuiting an empty manifest.
struct MechanicalCounts {
    sections_checked: usize,
    blob_refs_hashed: usize,
    marker_refs_skipped: usize,
}

/// Counts recorded for the deletion-gate success log: the rows each rebuild
/// sub-check actually compared, so an operator can confirm the gate did real
/// rebuild work rather than clearing over an empty archive. The lexical plane is
/// a documented no-op (`verify_lexical_plane`) and contributes no count.
struct RebuildCounts {
    chunks_compared: usize,
    dense_compared: usize,
    multivector_compared: usize,
    graph_mentions_compared: usize,
    graph_edges_compared: usize,
}

/// Fetch the manifest at the header's `manifest_hash` and confirm its recorded
/// self-hash matches the header (§30.4). Two integrity gates fire here: the
/// artifact store's `get_json` re-hashes the stored bytes against the requested
/// address (so tamper of the manifest file is caught), and the manifest's own
/// `manifestHash` field is recomputed via the SAME derivation C9a used
/// (`canonical_sha256_hex_without_field` over `manifestHash`) and compared to
/// the stored value — so a manifest whose body was altered but whose file was
/// re-addressed still fails.
fn load_and_check_manifest(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
) -> Result<ForensicSnapshotManifest, ApiError> {
    // get_json re-hashes the bytes to their address; a corrupt manifest file
    // surfaces as a StorageOperation corruption error, remapped here with tier
    // context so the failure reads as a verification failure, not a bare I/O.
    let manifest_value = store.get_json(&snapshot.manifest_hash).map_err(|source| {
        verification_failure(format!(
            "snapshot {}: manifest at hash {} could not be read/verified from the store: {source}",
            snapshot.id, snapshot.manifest_hash
        ))
    })?;
    let manifest: ForensicSnapshotManifest =
        serde_json::from_value(manifest_value).map_err(|source| {
            verification_failure(format!(
                "snapshot {}: manifest at hash {} is not a valid ForensicSnapshotManifest: {source}",
                snapshot.id, snapshot.manifest_hash
            ))
        })?;

    // Recompute the manifest self-hash over the body WITHOUT its own hash field,
    // exactly as C9a sealed it, and compare against the recorded value. This is
    // independent of the store's byte-address check: it proves the manifest body
    // the verifier parsed is the body C9a hashed.
    let recomputed =
        crate::canonical::canonical_sha256_hex_without_field(&manifest, "manifestHash")?;
    if recomputed != manifest.manifest_hash {
        return Err(verification_failure(format!(
            "snapshot {}: manifest self-hash mismatch (recorded {}, recomputed {})",
            snapshot.id,
            hash_prefix(&manifest.manifest_hash),
            hash_prefix(&recomputed)
        )));
    }
    // The recorded self-hash must also be the address the header points at, or
    // the header references a different manifest than the one it names.
    if manifest.manifest_hash != snapshot.manifest_hash {
        return Err(verification_failure(format!(
            "snapshot {}: header manifest_hash {} does not match the manifest's own \
             manifestHash {}",
            snapshot.id,
            hash_prefix(&snapshot.manifest_hash),
            hash_prefix(&manifest.manifest_hash)
        )));
    }
    Ok(manifest)
}

/// Iterate EVERY manifest section in the explicit-battery style of
/// `hot_plane::validate_fabric_schema` (a static section list, each named and
/// checked, first failure surfaced with its section) and re-hash every
/// blob-backed ref via `store.get_bytes`, which re-hashes the bytes against the
/// address internally — so a corrupt blob surfaces as its corruption error,
/// remapped here into a named verification failure carrying section +
/// artifactType + expected-hash-prefix context.
///
/// MARKER-REF POLICY (deliberate, not a silent skip). C9a pins the
/// parser/connector capability profiles by a HASH MARKER ref with an empty
/// `uri`/`hash` and no independent blob — the referenced hashes live inside the
/// already-hashed plane projections (parse_runs / acquisition_records). Such a
/// ref carries no bytes to re-hash, so this battery: (1) requires the marker's
/// artifactType to be exactly the known marker type, (2) requires its hash to
/// be the empty sentinel, and (3) skips it with a named, logged reason. Any
/// OTHER ref with an empty hash is rejected — a blob-backed ref is NEVER
/// silently skipped.
fn verify_all_refs_hash(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
) -> Result<MechanicalCounts, ApiError> {
    let sections = manifest_ref_sections(manifest);
    let mut blob_refs_hashed = 0usize;
    let mut marker_refs_skipped = 0usize;

    for (section_name, refs) in &sections {
        for artifact in *refs {
            if artifact.hash.is_empty() {
                // Empty hash ⇒ must be the known capability-profile hash marker,
                // or the manifest references bytes it did not store.
                if artifact.artifact_type == CAPABILITY_PROFILE_HASH_MARKER_TYPE {
                    marker_refs_skipped += 1;
                    info!(
                        event = "snapshot.verify.marker_ref_skipped",
                        snapshot_id = %snapshot.id,
                        section = *section_name,
                        artifact_type = %artifact.artifact_type,
                        "hash-marker ref has no independent blob; its hashes are verified \
                         inside the archived plane projections"
                    );
                    continue;
                }
                return Err(verification_failure(format!(
                    "snapshot {}: section {} ref of type {} has an empty hash but is not the \
                     known capability-profile hash marker; a blob-backed ref must never be \
                     skipped",
                    snapshot.id, section_name, artifact.artifact_type
                )));
            }
            // Blob-backed ref: get_bytes re-hashes the stored bytes to their
            // address, so corruption or absence surfaces here. Remap into a
            // verification failure naming the section and artifactType.
            store.get_bytes(&artifact.hash).map_err(|source| {
                verification_failure(format!(
                    "snapshot {}: section {} artifact {} ({}) failed hash verification: {source}",
                    snapshot.id,
                    section_name,
                    hash_prefix(&artifact.hash),
                    artifact.artifact_type
                ))
            })?;
            blob_refs_hashed += 1;
        }
    }

    Ok(MechanicalCounts {
        sections_checked: sections.len(),
        blob_refs_hashed,
        marker_refs_skipped,
    })
}

/// Every manifest section as (name, refs) pairs, so the mechanical battery walks
/// all of them without repeating the field list and names each in its logs.
/// Mirrors `snapshot::manifest_ref_sections` (the write-side presence check);
/// the two MUST list the same sections — a section added there but not here
/// would go unverified. Optional sections contribute only when present.
fn manifest_ref_sections(
    manifest: &ForensicSnapshotManifest,
) -> Vec<(&'static str, &[SnapshotArtifactRef])> {
    let mut sections: Vec<(&'static str, &[SnapshotArtifactRef])> = vec![
        ("source_objects", &manifest.source_objects),
        ("acquisition_records", &manifest.acquisition_records),
        ("parse_runs", &manifest.parse_runs),
        ("canonical_parse_bundles", &manifest.canonical_parse_bundles),
        ("content_units", &manifest.content_units),
        ("unit_relationships", &manifest.unit_relationships),
        ("semantic_annotations", &manifest.semantic_annotations),
        ("retrieval_projections", &manifest.retrieval_projections),
        ("retrieval_indexes", &manifest.retrieval_indexes),
        ("assembly_policies", &manifest.assembly_policies),
        ("retrieval_profiles", &manifest.retrieval_profiles),
        ("capability_profiles", &manifest.capability_profiles),
        ("query_execution_records", &manifest.query_execution_records),
        ("runtime_artifacts", &manifest.runtime_artifacts),
    ];
    if let Some(parser_output_bundles) = &manifest.parser_output_bundles {
        sections.push(("parser_output_bundles", parser_output_bundles));
    }
    if let Some(deletion_records) = &manifest.deletion_records {
        sections.push(("deletion_records", deletion_records));
    }
    if let Some(model_artifacts) = &manifest.model_artifacts {
        sections.push(("model_artifacts", model_artifacts));
    }
    sections
}

// ---------------------------------------------------------------------------
// Deletion-gate rebuild sub-checks. Each RE-IMPORTS or RE-DERIVES from archived
// artifacts and compares against the live hot plane; none invokes a model.
// ---------------------------------------------------------------------------

/// CHUNK PLANE (§30.5 deterministic-rebuild). Compares the archived
/// `chunk_projections` JSONL record set against the live hot `chunk_projections`
/// rows for the subject parse, bounded to what `chunk.rs` DETERMINISTICALLY
/// derives: row identity/count, `input_unit_ids`, `targeting_text`, and the
/// `chunker_config_hash`. It additionally requires every archived chunk's
/// `chunker_config_hash` to equal the banked `ChunkerConfig::active().config_hash()`
/// — the single source of the §22 chunker identity.
///
/// DELIBERATELY NOT CHECKED: this does NOT re-run the chunker's text splitter.
/// `chunk.rs::split_units_into_chunks` requires a caller-supplied ColBERT
/// `Tokenizer` (a model runtime handle), and §38 forbids any model call here.
/// The deterministic-rebuild guarantee is instead established by (a) proving the
/// archived and live chunk sets agree byte-for-byte on the deterministic
/// columns, and (b) pinning the producing config identity to the banked hash —
/// together these show the captured chunks are the ones the banked deterministic
/// producer would yield, without re-running the tokenizer-bearing split.
fn verify_chunk_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<usize, ApiError> {
    let archived = load_archived_jsonl(store, snapshot, manifest, "chunk_projections")?;

    // Banked config identity: every archived chunk must carry this exact hash.
    let banked_config_hash = ChunkerConfig::active().config_hash()?;

    // Index the archived chunk records for this parse by their deterministic
    // comparison key. Only the subject parse's chunks are in scope (the archived
    // set is corpus-wide; the hot comparison is parse-scoped).
    let mut archived_chunks: BTreeMap<String, ChunkComparison> = BTreeMap::new();
    for record in &archived {
        let object = as_object(record, snapshot, "chunk_projections")?;
        if json_str(object, "parse_id") != Some(subject_parse_id) {
            continue;
        }
        let comparison = chunk_comparison(object, snapshot)?;
        if comparison.chunker_config_hash != banked_config_hash {
            return Err(verification_failure(format!(
                "snapshot {}: archived chunk {} has chunker_config_hash {} but the banked \
                 ChunkerConfig hashes to {}; the captured chunks were not produced by the \
                 current deterministic chunker identity",
                snapshot.id,
                comparison.id,
                hash_prefix(&comparison.chunker_config_hash),
                hash_prefix(&banked_config_hash)
            )));
        }
        archived_chunks.insert(comparison.id.clone(), comparison);
    }

    // Live hot rows for the same parse, keyed identically.
    let hot_chunks = load_hot_chunks(connection, subject_parse_id)?;

    // On success both sides share every key, so either cardinality is the count
    // of chunks compared.
    let compared = archived_chunks.len();
    compare_keyed_sets(
        snapshot,
        "chunk_projections",
        &archived_chunks,
        &hot_chunks,
        |id, archived, hot| {
            if archived != hot {
                return Err(verification_failure(format!(
                    "snapshot {}: chunk {} archived/hot mismatch on a deterministic field \
                     (input_unit_ids, targeting_text, or chunker_config_hash)",
                    snapshot.id, id
                )));
            }
            Ok(())
        },
    )?;
    Ok(compared)
}

/// DENSE PLANE (§30.5). RE-IMPORTS the archived dense-vector blobs and compares
/// them byte-and-value against the live `chunk_dense_vectors` rows for the
/// subject parse — never re-embeds (§38). The archived
/// `chunk_dense_vectors_metadata` JSONL carries each row's scalar columns plus
/// the `vector_blobHash` pointing at the archived blob; the live row carries the
/// blob bytes. Both are decoded via `primitives::codec::decode_vector_blob`
/// under the row's recorded dimension and the decoded vectors are compared, and
/// the raw blob bytes are compared directly.
fn verify_dense_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<usize, ApiError> {
    let metadata = load_archived_jsonl(store, snapshot, manifest, "chunk_dense_vectors_metadata")?;

    // Counts only the subject-parse rows actually compared (the corpus-wide
    // archive is filtered to the subject parse below).
    let mut compared = 0usize;
    for record in &metadata {
        let object = as_object(record, snapshot, "chunk_dense_vectors_metadata")?;
        if json_str(object, "parse_id") != Some(subject_parse_id) {
            continue;
        }
        let chunk_id = required_str(object, "chunk_id", snapshot, "chunk_dense_vectors_metadata")?;
        let dimension = required_u64(
            object,
            "dimension",
            snapshot,
            "chunk_dense_vectors_metadata",
        )? as usize;
        let blob_hash = required_str(
            object,
            "vector_blobHash",
            snapshot,
            "chunk_dense_vectors_metadata",
        )?;

        // Archived blob bytes (re-hash-verified by get_bytes) and the live row's
        // blob bytes for the same chunk.
        let archived_blob = store.get_bytes(&blob_hash).map_err(|source| {
            verification_failure(format!(
                "snapshot {}: dense vector blob {} for chunk {} failed to load: {source}",
                snapshot.id,
                hash_prefix(&blob_hash),
                chunk_id
            ))
        })?;
        let (hot_dimension, hot_blob) = load_hot_dense_row(connection, snapshot, &chunk_id)?;

        // Byte compare first (cheapest exact check), then decode both under the
        // recorded dimension and value-compare — decoding also validates the
        // vectors, so a corrupt archived or live blob surfaces here.
        if hot_dimension != dimension {
            return Err(verification_failure(format!(
                "snapshot {}: dense vector for chunk {} archived dimension {} != hot dimension {}",
                snapshot.id, chunk_id, dimension, hot_dimension
            )));
        }
        if archived_blob != hot_blob {
            return Err(verification_failure(format!(
                "snapshot {}: dense vector bytes for chunk {} differ between archive and hot plane",
                snapshot.id, chunk_id
            )));
        }
        let archived_vector = crate::primitives::codec::decode_vector_blob(
            &chunk_id,
            &archived_blob,
            dimension,
            dimension,
        )
        .map_err(|message| decode_failure(snapshot, "dense vector", &chunk_id, &message))?;
        let hot_vector = crate::primitives::codec::decode_vector_blob(
            &chunk_id,
            &hot_blob,
            hot_dimension,
            dimension,
        )
        .map_err(|message| decode_failure(snapshot, "dense vector", &chunk_id, &message))?;
        if archived_vector != hot_vector {
            return Err(verification_failure(format!(
                "snapshot {}: dense vector values for chunk {} differ after decode",
                snapshot.id, chunk_id
            )));
        }
        compared += 1;
    }
    Ok(compared)
}

/// MULTIVECTOR PLANE (§30.5). RE-IMPORTS the archived ColBERT matrix blobs and
/// compares them byte-and-value against the live `unit_multivector_projections`
/// rows for the subject parse — never re-embeds (§38). Same re-import shape as
/// the dense plane, decoded via
/// `primitives::codec::decode_colbert_document_vector_blob` under the row's
/// recorded (token_count, dimension).
fn verify_multivector_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<usize, ApiError> {
    let metadata = load_archived_jsonl(
        store,
        snapshot,
        manifest,
        "unit_multivector_projections_metadata",
    )?;

    // Counts only the subject-parse rows actually compared (the corpus-wide
    // archive is filtered to the subject parse below).
    let mut compared = 0usize;
    for record in &metadata {
        let object = as_object(record, snapshot, "unit_multivector_projections_metadata")?;
        if json_str(object, "parse_id") != Some(subject_parse_id) {
            continue;
        }
        let row_id = required_str(
            object,
            "id",
            snapshot,
            "unit_multivector_projections_metadata",
        )?;
        let unit_id = required_str(
            object,
            "unit_id",
            snapshot,
            "unit_multivector_projections_metadata",
        )?;
        let token_count = required_u64(
            object,
            "token_count",
            snapshot,
            "unit_multivector_projections_metadata",
        )? as usize;
        let dimension = required_u64(
            object,
            "dimension",
            snapshot,
            "unit_multivector_projections_metadata",
        )? as usize;
        let blob_hash = required_str(
            object,
            "matrix_blobHash",
            snapshot,
            "unit_multivector_projections_metadata",
        )?;

        let archived_blob = store.get_bytes(&blob_hash).map_err(|source| {
            verification_failure(format!(
                "snapshot {}: multivector blob {} for row {} failed to load: {source}",
                snapshot.id,
                hash_prefix(&blob_hash),
                row_id
            ))
        })?;
        let (hot_token_count, hot_dimension, hot_blob) =
            load_hot_multivector_row(connection, snapshot, &row_id)?;

        if (hot_token_count, hot_dimension) != (token_count, dimension) {
            return Err(verification_failure(format!(
                "snapshot {}: multivector row {} archived shape ({token_count},{dimension}) != \
                 hot shape ({hot_token_count},{hot_dimension})",
                snapshot.id, row_id
            )));
        }
        if archived_blob != hot_blob {
            return Err(verification_failure(format!(
                "snapshot {}: multivector bytes for row {} differ between archive and hot plane",
                snapshot.id, row_id
            )));
        }
        // Decode both (validates the matrices too) and value-compare.
        let archived_matrix = crate::primitives::codec::decode_colbert_document_vector_blob(
            &unit_id,
            &archived_blob,
            token_count,
            dimension,
            dimension,
        )
        .map_err(|message| decode_failure(snapshot, "multivector", &row_id, &message))?;
        let hot_matrix = crate::primitives::codec::decode_colbert_document_vector_blob(
            &unit_id,
            &hot_blob,
            hot_token_count,
            hot_dimension,
            dimension,
        )
        .map_err(|message| decode_failure(snapshot, "multivector", &row_id, &message))?;
        if archived_matrix != hot_matrix {
            return Err(verification_failure(format!(
                "snapshot {}: multivector values for row {} differ after decode",
                snapshot.id, row_id
            )));
        }
        compared += 1;
    }
    Ok(compared)
}

/// GRAPH PLANE (§30.5). RE-DERIVES entity mentions and directional edges from
/// the ARCHIVED `semantic_annotations` rows using the SAME pure derivation
/// `graph.rs` uses, and compares against the live `graph_entity_mentions` /
/// `graph_entity_edges` tables for the subject parse. `normalize_entity_name`
/// is imported from `graph.rs` so the node identity — and the edge's
/// relation_type, which the builder also stores as the NORMALIZED predicate —
/// is byte-identical to the build-time key; the mention accumulation and edge
/// derivation are MIRRORED
/// here (the builder's `accumulate_mentions` / `derive_edges` are private) and
/// MUST STAY IN STEP with `graph.rs` — a change to that derivation must be
/// reflected here or this gate will spuriously fail. No model is invoked.
///
/// The archived rows are the RAW column projection C9a wrote via
/// `read_table_as_json` (columns `annotation_type`, `body_json`,
/// `target_unit_ids_json`, `freshness_status`, `deleted_at`), NOT the
/// `SemanticAnnotation` model serialization, so this reads those columns
/// directly. The build reads annotations via `fresh_for_active_parse`, so the
/// re-derivation applies the SAME filter: subject parse, `annotation_type` in
/// {entity, relation}, `freshness_status == 'fresh'`, `deleted_at` absent.
fn verify_graph_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<(usize, usize), ApiError> {
    let archived = load_archived_jsonl(store, snapshot, manifest, "semantic_annotations")?;

    let mut expected_mentions: BTreeMap<String, MentionExpectation> = BTreeMap::new();
    let mut expected_edges: BTreeSet<EdgeKey> = BTreeSet::new();

    for record in &archived {
        let object = as_object(record, snapshot, "semantic_annotations")?;
        // Mirror fresh_for_active_parse: subject parse, fresh, not deleted.
        if json_str(object, "parse_id") != Some(subject_parse_id) {
            continue;
        }
        if json_str(object, "freshness_status") != Some("fresh") {
            continue;
        }
        if !json_is_null_or_absent(object, "deleted_at") {
            continue;
        }
        let annotation_type = json_str(object, "annotation_type").unwrap_or("");
        match annotation_type {
            "entity" => accumulate_expected_mention(object, snapshot, &mut expected_mentions)?,
            "relation" => {
                expected_edges.insert(derive_expected_edge(object, snapshot)?);
            }
            // Every other annotation type is irrelevant to the graph channel
            // (mirror of graph.rs's entity/relation partition).
            _ => {}
        }
    }

    // Compare against the live mention rows: one row per (parse, normalized
    // name), unit_ids the deduplicated deterministic set. On success both sides
    // share every key, so the expected count is the mentions compared.
    let mentions_compared = expected_mentions.len();
    let hot_mentions = load_hot_mentions(connection, subject_parse_id)?;
    compare_keyed_sets(
        snapshot,
        "graph_entity_mentions",
        &expected_mentions
            .iter()
            .map(|(name, expectation)| (name.clone(), expectation.units.clone()))
            .collect(),
        &hot_mentions,
        |name, expected_units, hot_units| {
            if expected_units != hot_units {
                return Err(verification_failure(format!(
                    "snapshot {}: graph mention for normalized entity name {:?} has archived-derived \
                     unit set differing from the hot plane",
                    snapshot.id,
                    bounded_name(name)
                )));
            }
            Ok(())
        },
    )?;

    // Compare against the live edge rows as a set of edge keys. The builder
    // stores one row per relation annotation in its natural subject→object
    // direction, so the comparison is over the multiset-as-set of edge keys.
    let hot_edges = load_hot_edges(connection, subject_parse_id)?;
    if expected_edges != hot_edges {
        return Err(verification_failure(format!(
            "snapshot {}: graph edge set re-derived from archived relation annotations differs \
             from the hot graph_entity_edges for parse {subject_parse_id} (archived-derived {} \
             edges, hot {} edges)",
            snapshot.id,
            expected_edges.len(),
            hot_edges.len()
        )));
    }
    // Equal sets, so either cardinality is the edges compared.
    Ok((mentions_compared, expected_edges.len()))
}

/// LEXICAL PLANE (§30.5). The FTS5 `chunk_text_index` is NOT archived, so there
/// is nothing to re-import and no way to diff a virtual table's internal
/// posting lists. What the §30.5 "deterministic rebuild artifacts" verification
/// requires here is that the index's DETERMINISTIC SOURCE verifies and its
/// producer is deterministic: `chunk_text_index` is rebuilt purely from
/// `chunk_projections` (schema.sql §22/§36) in a fixed order, and those chunk
/// rows are already verified by `verify_chunk_plane`.
///
/// CHECKED: the chunk plane the index derives from verified (transitively, via
/// the ordering of sub-checks in `verify_deletion_gate`), and the lexical
/// producer is deterministic (no model, no config — a pure FTS5 insert per
/// chunk). DELIBERATELY NOT CHECKED: the FTS5 table's internal posting/index
/// state — it is a virtual table with no diffable payload, and re-populating it
/// would require a scratch database this unattended gate does not build. Since
/// the source rows + deterministic-producer identity are the entirety of what
/// determines the rebuilt index, checking them IS the §30.5 verification for
/// this plane. This function therefore holds only the assertion boundary and
/// its comment; it takes the snapshot for a uniform sub-check signature.
fn verify_lexical_plane(_snapshot: &ForensicSnapshot) -> Result<(), ApiError> {
    // No independent artifact to verify: the check is discharged by
    // verify_chunk_plane (source rows) plus the deterministic-producer contract
    // documented above. Kept as an explicit, commented no-op so the lexical
    // plane is a visible, accounted-for tier rather than a silent omission.
    Ok(())
}

// ---------------------------------------------------------------------------
// Chunk comparison shapes and readers.
// ---------------------------------------------------------------------------

/// The deterministic comparison projection of one chunk row (archived or hot):
/// the fields `chunk.rs` derives deterministically. `PartialEq` is the whole
/// archived-vs-hot equality the chunk sub-check rests on.
#[derive(Debug, PartialEq, Eq)]
struct ChunkComparison {
    id: String,
    input_unit_ids: Vec<String>,
    targeting_text: String,
    chunker_config_hash: String,
}

/// Project one archived `chunk_projections` JSON record into its deterministic
/// comparison shape. `input_unit_ids_json` is the stored canonical JSON string
/// array; it is decoded so the comparison is over the id list, not its textual
/// encoding.
fn chunk_comparison(
    object: &Map<String, Value>,
    snapshot: &ForensicSnapshot,
) -> Result<ChunkComparison, ApiError> {
    let id = required_str(object, "id", snapshot, "chunk_projections")?;
    let input_unit_ids = decode_json_string_array(
        required_str(object, "input_unit_ids_json", snapshot, "chunk_projections")?.as_str(),
        snapshot,
        "chunk_projections.input_unit_ids_json",
    )?;
    let targeting_text = required_str(object, "targeting_text", snapshot, "chunk_projections")?;
    let chunker_config_hash =
        required_str(object, "chunker_config_hash", snapshot, "chunk_projections")?;
    Ok(ChunkComparison {
        id,
        input_unit_ids,
        targeting_text,
        chunker_config_hash,
    })
}

/// Ordered SELECT of the subject parse's live chunk rows, projected into the
/// same deterministic comparison shape as the archived records so the two sets
/// compare directly. Column set matches `chunk_projections` (schema.sql §22).
fn load_hot_chunks(
    connection: &Connection,
    parse_id: &str,
) -> Result<BTreeMap<String, ChunkComparison>, ApiError> {
    const SQL: &str = "SELECT id, input_unit_ids_json, targeting_text, chunker_config_hash \
                       FROM chunk_projections WHERE parse_id = ?1 ORDER BY id";
    let mut statement = prepare(connection, SQL, "hot chunk_projections")?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|source| query_failure("hot chunk_projections", source))?;

    let mut chunks = BTreeMap::new();
    for row in rows {
        let (id, input_unit_ids_json, targeting_text, chunker_config_hash) =
            row.map_err(|source| query_failure("hot chunk_projections row", source))?;
        let input_unit_ids = decode_json_string_array_untied(
            &input_unit_ids_json,
            "hot chunk_projections.input_unit_ids_json",
        )?;
        chunks.insert(
            id.clone(),
            ChunkComparison {
                id,
                input_unit_ids,
                targeting_text,
                chunker_config_hash,
            },
        );
    }
    Ok(chunks)
}

/// Load one live dense-vector row's (dimension, blob) for a chunk. A missing row
/// is a verification failure: the archive references a chunk the hot plane no
/// longer has, so the deterministic rebuild cannot be confirmed.
fn load_hot_dense_row(
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    chunk_id: &str,
) -> Result<(usize, Vec<u8>), ApiError> {
    const SQL: &str = "SELECT dimension, vector_blob FROM chunk_dense_vectors WHERE chunk_id = ?1";
    let row = connection
        .query_row(SQL, params![chunk_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(|source| match source {
            rusqlite::Error::QueryReturnedNoRows => verification_failure(format!(
                "snapshot {}: dense vector for chunk {} is archived but absent from the hot plane",
                snapshot.id, chunk_id
            )),
            other => query_failure("hot chunk_dense_vectors", other),
        })?;
    Ok((row.0 as usize, row.1))
}

/// Load one live multivector row's (token_count, dimension, blob) by row id. A
/// missing row is a verification failure for the same reason as the dense case.
fn load_hot_multivector_row(
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    row_id: &str,
) -> Result<(usize, usize, Vec<u8>), ApiError> {
    const SQL: &str = "SELECT token_count, dimension, matrix_blob \
                       FROM unit_multivector_projections WHERE id = ?1";
    let row = connection
        .query_row(SQL, params![row_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .map_err(|source| match source {
            rusqlite::Error::QueryReturnedNoRows => verification_failure(format!(
                "snapshot {}: multivector row {} is archived but absent from the hot plane",
                snapshot.id, row_id
            )),
            other => query_failure("hot unit_multivector_projections", other),
        })?;
    Ok((row.0 as usize, row.1 as usize, row.2))
}

// ---------------------------------------------------------------------------
// Graph comparison shapes and readers.
// ---------------------------------------------------------------------------

/// One re-derived entity mention: the accumulated, deduplicated, deterministically
/// ordered target unit set for a normalized name. Mirrors
/// `graph.rs::MentionAccumulator` but keeps ONLY the unit set — the hot
/// comparison is over unit sets, and entityType is metadata, never identity (D9),
/// so it is intentionally excluded from the comparison.
struct MentionExpectation {
    units: Vec<String>,
}

/// One re-derived directional edge key. Mirrors `graph.rs::DerivedEdge`'s
/// comparison-relevant fields; the target unit list is included so an edge whose
/// endpoints match but whose supporting units differ is still caught.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeKey {
    from_normalized_name: String,
    to_normalized_name: String,
    relation_type: String,
    target_unit_ids: Vec<String>,
}

/// Accumulate one archived entity annotation into the expected mention set,
/// mirroring `graph.rs::accumulate_mentions`: read the body's `name`, normalize
/// it to node identity, and fold the annotation's target units into that name's
/// deduplicated set. entityType is metadata and is NOT accumulated. A corrupt
/// body (missing string `name`) is a verification failure, matching the
/// builder's honest-reflection policy.
fn accumulate_expected_mention(
    object: &Map<String, Value>,
    snapshot: &ForensicSnapshot,
    mentions: &mut BTreeMap<String, MentionExpectation>,
) -> Result<(), ApiError> {
    let body = annotation_body(object, snapshot)?;
    let raw_name = body.get("name").and_then(Value::as_str).ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: archived entity annotation has no string `name` body field",
            snapshot.id
        ))
    })?;
    let normalized = normalize_entity_name(raw_name);
    let target_unit_ids = archived_target_unit_ids(object, snapshot)?;

    // BTreeSet gives dedup + deterministic order in one step, exactly as the
    // builder does, so the flushed unit list is byte-identical.
    let entry = mentions
        .entry(normalized)
        .or_insert_with(|| MentionExpectation { units: Vec::new() });
    let mut set: BTreeSet<String> = entry.units.iter().cloned().collect();
    for unit_id in target_unit_ids {
        set.insert(unit_id);
    }
    entry.units = set.into_iter().collect();
    Ok(())
}

/// Re-derive one directional edge from an archived relation annotation,
/// mirroring `graph.rs::derive_edges`: read the `{subject, predicate, object}`
/// body, normalize subject/object to node identities, and take the NORMALIZED
/// predicate (same `normalize_entity_name` scheme the builder applies) as
/// relation_type with the annotation's target units as supporting units. A
/// corrupt body is a verification failure.
fn derive_expected_edge(
    object: &Map<String, Value>,
    snapshot: &ForensicSnapshot,
) -> Result<EdgeKey, ApiError> {
    let body = annotation_body(object, snapshot)?;
    let field = |name: &str| -> Result<String, ApiError> {
        body.get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: archived relation annotation has no string `{name}` body field",
                    snapshot.id
                ))
            })
    };
    let subject = field("subject")?;
    let predicate = field("predicate")?;
    let object_name = field("object")?;
    Ok(EdgeKey {
        from_normalized_name: normalize_entity_name(&subject),
        to_normalized_name: normalize_entity_name(&object_name),
        // Mirror of the builder: the live plane stores the NORMALIZED predicate
        // as relation_type (`graph.rs::derive_edges`), so the re-derivation must
        // normalize identically or the deletion gate would spuriously fail.
        relation_type: normalize_entity_name(&predicate),
        target_unit_ids: archived_target_unit_ids(object, snapshot)?,
    })
}

/// Decode the `body_json` TEXT column of an archived annotation row into a JSON
/// object. The archived value is the raw hot column (a JSON string), so it is
/// parsed here; a NULL/absent or non-object body is a verification failure (the
/// graph builder only runs over fresh rows, whose body is present).
fn annotation_body(
    object: &Map<String, Value>,
    snapshot: &ForensicSnapshot,
) -> Result<Map<String, Value>, ApiError> {
    let body_text = json_str(object, "body_json").ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: archived fresh annotation has a NULL/absent body_json",
            snapshot.id
        ))
    })?;
    let parsed: Value = serde_json::from_str(body_text).map_err(|source| {
        verification_failure(format!(
            "snapshot {}: archived annotation body_json is not valid JSON: {source}",
            snapshot.id
        ))
    })?;
    match parsed {
        Value::Object(map) => Ok(map),
        _ => Err(verification_failure(format!(
            "snapshot {}: archived annotation body_json is not a JSON object",
            snapshot.id
        ))),
    }
}

/// Decode the `target_unit_ids_json` TEXT column of an archived annotation row.
fn archived_target_unit_ids(
    object: &Map<String, Value>,
    snapshot: &ForensicSnapshot,
) -> Result<Vec<String>, ApiError> {
    let json = required_str(
        object,
        "target_unit_ids_json",
        snapshot,
        "semantic_annotations",
    )?;
    decode_json_string_array(&json, snapshot, "semantic_annotations.target_unit_ids_json")
}

/// Load the subject parse's live entity-mention rows as (normalized_name →
/// deterministically ordered unit ids), matching the archived-derived shape.
fn load_hot_mentions(
    connection: &Connection,
    parse_id: &str,
) -> Result<BTreeMap<String, Vec<String>>, ApiError> {
    const SQL: &str = "SELECT normalized_name, unit_ids_json FROM graph_entity_mentions \
                       WHERE parse_id = ?1 ORDER BY normalized_name";
    let mut statement = prepare(connection, SQL, "hot graph_entity_mentions")?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| query_failure("hot graph_entity_mentions", source))?;

    let mut mentions = BTreeMap::new();
    for row in rows {
        let (normalized_name, unit_ids_json) =
            row.map_err(|source| query_failure("hot graph_entity_mentions row", source))?;
        let unit_ids = decode_json_string_array_untied(
            &unit_ids_json,
            "hot graph_entity_mentions.unit_ids_json",
        )?;
        mentions.insert(normalized_name, unit_ids);
    }
    Ok(mentions)
}

/// Load the subject parse's live edge rows as a set of edge keys, matching the
/// archived-derived shape.
fn load_hot_edges(connection: &Connection, parse_id: &str) -> Result<BTreeSet<EdgeKey>, ApiError> {
    const SQL: &str = "SELECT from_normalized_name, to_normalized_name, relation_type, \
                       target_unit_ids_json FROM graph_entity_edges WHERE parse_id = ?1";
    let mut statement = prepare(connection, SQL, "hot graph_entity_edges")?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|source| query_failure("hot graph_entity_edges", source))?;

    let mut edges = BTreeSet::new();
    for row in rows {
        let (from_normalized_name, to_normalized_name, relation_type, target_unit_ids_json) =
            row.map_err(|source| query_failure("hot graph_entity_edges row", source))?;
        let target_unit_ids = decode_json_string_array_untied(
            &target_unit_ids_json,
            "hot graph_entity_edges.target_unit_ids_json",
        )?;
        edges.insert(EdgeKey {
            from_normalized_name,
            to_normalized_name,
            relation_type,
            target_unit_ids,
        });
    }
    Ok(edges)
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// The subject parse a lifecycle snapshot's deletion gate is scoped to. Derived
/// from the header's `active_parse_ids` (lifecycle snapshots set it to
/// `[subject_parse_id]`; the header carries no standalone subject_parse column).
/// Exactly one active parse is required — a corpus-wide (many) or empty scope is
/// not a lifecycle snapshot and must never reach the deletion gate.
fn subject_parse_of(snapshot: &ForensicSnapshot) -> Result<String, ApiError> {
    match snapshot.active_parse_ids.as_slice() {
        [parse_id] => Ok(parse_id.clone()),
        other => Err(verification_failure(format!(
            "snapshot {}: deletion gate requires a single-subject lifecycle snapshot, but the \
             header carries {} active parse ids",
            snapshot.id,
            other.len()
        ))),
    }
}

/// Load and return the archived JSONL record set for a named plane, resolving
/// the plane's manifest ref by artifactType. A section expected by the gate but
/// absent from the manifest is a verification failure (the archive is missing a
/// plane the gate must rebuild-check).
fn load_archived_jsonl(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    artifact_type: &str,
) -> Result<Vec<Value>, ApiError> {
    let artifact = find_ref_by_type(manifest, artifact_type).ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: manifest has no {artifact_type} artifact to rebuild-check against",
            snapshot.id
        ))
    })?;
    store.get_jsonl(&artifact.hash).map_err(|source| {
        verification_failure(format!(
            "snapshot {}: archived {artifact_type} ({}) failed to load: {source}",
            snapshot.id,
            hash_prefix(&artifact.hash)
        ))
    })
}

/// Find the first manifest ref of a given artifactType across every section.
/// The rebuild-check planes (chunk/dense/multivector metadata,
/// semantic_annotations) each contribute exactly one ref, so first-match is the
/// intended one.
fn find_ref_by_type<'m>(
    manifest: &'m ForensicSnapshotManifest,
    artifact_type: &str,
) -> Option<&'m SnapshotArtifactRef> {
    manifest_ref_sections(manifest)
        .into_iter()
        .flat_map(|(_, refs)| refs.iter())
        .find(|artifact| artifact.artifact_type == artifact_type)
}

/// Compare two keyed sets (archived-derived vs hot) and run `on_match` for each
/// shared key. A key present on only one side is a verification failure naming
/// the side and key; `on_match` handles the per-key value comparison. Shared by
/// the chunk and graph-mention sub-checks so the presence/comparison plumbing is
/// written once.
fn compare_keyed_sets<V>(
    snapshot: &ForensicSnapshot,
    plane: &str,
    archived: &BTreeMap<String, V>,
    hot: &BTreeMap<String, V>,
    on_match: impl Fn(&str, &V, &V) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    for (key, archived_value) in archived {
        match hot.get(key) {
            Some(hot_value) => on_match(key, archived_value, hot_value)?,
            None => {
                return Err(verification_failure(format!(
                    "snapshot {}: {plane} key {:?} is archived but absent from the hot plane",
                    snapshot.id,
                    bounded_name(key)
                )));
            }
        }
    }
    for key in hot.keys() {
        if !archived.contains_key(key) {
            return Err(verification_failure(format!(
                "snapshot {}: {plane} key {:?} exists in the hot plane but not in the archive",
                snapshot.id,
                bounded_name(key)
            )));
        }
    }
    Ok(())
}

/// Borrow a JSON record as an object or fail with a plane-named verification
/// error — the archived JSONL records are always objects (column projections).
fn as_object<'v>(
    record: &'v Value,
    snapshot: &ForensicSnapshot,
    plane: &str,
) -> Result<&'v Map<String, Value>, ApiError> {
    record.as_object().ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: archived {plane} record is not a JSON object",
            snapshot.id
        ))
    })
}

/// Read an object's field as `&str` if present and a string, else `None`.
fn json_str<'v>(object: &'v Map<String, Value>, key: &str) -> Option<&'v str> {
    object.get(key).and_then(Value::as_str)
}

/// True when a field is absent or explicitly JSON null (the archived shape of a
/// NULL column) — used to mirror the builder's `deleted_at IS NULL` filter.
fn json_is_null_or_absent(object: &Map<String, Value>, key: &str) -> bool {
    match object.get(key) {
        None => true,
        Some(value) => value.is_null(),
    }
}

/// Read a required string field from an archived record, failing with plane +
/// field context when it is missing or non-string.
fn required_str(
    object: &Map<String, Value>,
    key: &str,
    snapshot: &ForensicSnapshot,
    plane: &str,
) -> Result<String, ApiError> {
    json_str(object, key).map(str::to_owned).ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: archived {plane} record is missing string field `{key}`",
            snapshot.id
        ))
    })
}

/// Read a required unsigned-integer field from an archived record. The archived
/// column projection stores integers as JSON numbers (§16.2), so a non-integer
/// or negative value is corrupt and fails here.
fn required_u64(
    object: &Map<String, Value>,
    key: &str,
    snapshot: &ForensicSnapshot,
    plane: &str,
) -> Result<u64, ApiError> {
    object.get(key).and_then(Value::as_u64).ok_or_else(|| {
        verification_failure(format!(
            "snapshot {}: archived {plane} record field `{key}` is not a non-negative integer",
            snapshot.id
        ))
    })
}

/// Decode a canonical JSON string-array TEXT value (the repo list-column
/// convention) into `Vec<String>`, with snapshot + field context on failure.
fn decode_json_string_array(
    json: &str,
    snapshot: &ForensicSnapshot,
    field: &str,
) -> Result<Vec<String>, ApiError> {
    serde_json::from_str(json).map_err(|source| {
        verification_failure(format!(
            "snapshot {}: {field} is not a JSON string array: {source}",
            snapshot.id
        ))
    })
}

/// Decode a canonical JSON string-array TEXT value with only field context (no
/// snapshot in scope) — for the hot-plane readers, whose failures are hot-plane
/// integrity errors rather than archive-vs-hot verdicts.
fn decode_json_string_array_untied(json: &str, field: &str) -> Result<Vec<String>, ApiError> {
    serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
        message: format!("{field} is not a JSON string array: {source}"),
    })
}

/// Prepare a hot-plane statement, mapping the prepare error to a hot-plane
/// storage error (a failed prepare is an integrity problem, not a verdict).
fn prepare<'c>(
    connection: &'c Connection,
    sql: &str,
    what: &str,
) -> Result<rusqlite::Statement<'c>, ApiError> {
    connection
        .prepare(sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare {what} read for snapshot verification: {source}"),
        })
}

/// Wrap a hot-plane query error as a StorageOperation error naming the read.
fn query_failure(what: &str, source: rusqlite::Error) -> ApiError {
    ApiError::StorageOperation {
        message: format!("failed to read {what} for snapshot verification: {source}"),
    }
}

/// Wrap a codec decode failure as a named verification failure. A decode error
/// on either the archived or the hot bytes means the deterministic rebuild
/// cannot be confirmed for that row.
fn decode_failure(snapshot: &ForensicSnapshot, plane: &str, row: &str, message: &str) -> ApiError {
    verification_failure(format!(
        "snapshot {}: {plane} for {row} failed to decode during rebuild verification: {message}",
        snapshot.id
    ))
}

/// Build the module's single verification-failure error. Centralized so every
/// tier and sub-check produces the SAME `ApiError` variant (§30.5 failure
/// semantics: a verification failure is one error kind the caller can gate on),
/// never a mix of StorageOperation and verification errors for real
/// disagreements.
fn verification_failure(message: String) -> ApiError {
    ApiError::SnapshotVerificationFailed { message }
}

/// Log a tier failure at its boundary with elapsed time and the failure detail,
/// then return the error unchanged. The error message already carries the
/// bounded section/artifactType/hash-prefix context (never full contents,
/// vectors, or secrets — DIAGNOSTICS forbidden-data rule).
fn log_tier_failure(
    tier: &str,
    snapshot: &ForensicSnapshot,
    started: Instant,
    source: ApiError,
) -> ApiError {
    error!(
        event = "snapshot.verify.failed",
        namespace = VERIFY_LOG_NAMESPACE,
        tier,
        snapshot_id = %snapshot.id,
        error = %source,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "snapshot verification failed"
    );
    source
}

/// First 12 hex characters of a hash for bounded logging/messages — enough to
/// disambiguate in practice, never the full digest, and never content.
fn hash_prefix(hash: &str) -> &str {
    let end = hash.len().min(12);
    &hash[..end]
}

/// Bound a normalized entity name / key for logging so a failure message never
/// carries an unbounded (or content-revealing) name. Entity names are already
/// derived identities, but the DIAGNOSTICS rule against logging document
/// contents is honored by capping length.
fn bounded_name(name: &str) -> String {
    const MAX: usize = 64;
    if name.len() <= MAX {
        name.to_owned()
    } else {
        format!("{}…", &name[..MAX])
    }
}
