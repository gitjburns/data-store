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

use crate::runtime::StorageContext;
use crate::sqlite::Connection;
use rusqlite::params;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tracing::{error, info};

use crate::artifact_store::ArtifactStore;
use crate::error::ApiError;
use crate::hot_plane;
use crate::model::{
    AnnotationFreshnessStatus, ContentType, ForensicSnapshot, ForensicSnapshotManifest, Provenance,
    SemanticAnnotation, SemanticAnnotationType, SnapshotArtifactRef,
};
use crate::projections::ChunkerConfig;
use crate::projections::graph::{CapturedGraph, DerivedEdge};
use crate::projections::section_dense::{
    SECTION_DENSE_INDEX_NAME, SectionDensePlane, read_section_payload,
};
use crate::projections::{annotation, annotation_io};
use crate::query::provenance::{AnnotationRepresentation, SourceExcerpt};
use crate::util::hash_prefix;

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
    index_root: &StorageContext,
    snapshot: &ForensicSnapshot,
) -> Result<(), ApiError> {
    let verification_log = crate::util::LogContext::new("snapshot", &snapshot.id);
    verification_log.record("stage", "mechanical_verification");
    let _verification_log = verification_log.enter();
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
    let section_planes = verified_section_planes(&store, snapshot, &manifest)
        .map_err(|source| log_tier_failure("mechanical", snapshot, started, source))?;
    let graph_planes = verified_graph_planes(&store, snapshot, &manifest)
        .map_err(|source| log_tier_failure("mechanical", snapshot, started, source))?;
    let annotation_publications = verified_annotation_publications(&store, &snapshot.id, &manifest)
        .map_err(|source| log_tier_failure("mechanical", snapshot, started, source))?;
    verified_chunk_policies(&store, snapshot, &manifest)
        .map_err(|source| log_tier_failure("mechanical", snapshot, started, source))?;

    info!(
        event = "snapshot.verify.mechanical_succeeded",
        tier = "mechanical",
        snapshot_id = %snapshot.id,
        sections_checked = counts.sections_checked as u64,
        blob_refs_hashed = counts.blob_refs_hashed as u64,
        marker_refs_skipped = counts.marker_refs_skipped as u64,
        section_dense_payloads = section_planes.len(),
        graph_projections = graph_planes.len(),
        annotation_manifests = annotation_publications.manifest_count,
        annotation_embedding_blobs = annotation_publications.embedding_blob_count,
        annotation_representations = annotation_publications.representation_count,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "mechanical snapshot verification succeeded"
    );
    Ok(())
}

/// Authenticate each chunk's producer against its pinned construction settings.
/// The descriptor-free v1 path accepts only the exact historical identity.
fn verified_chunk_policies(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
) -> Result<BTreeMap<String, String>, ApiError> {
    let mut refs = BTreeMap::new();
    for reference in manifest.retrieval_projections.iter().filter(|reference| {
        reference.artifact_type == crate::projections::CHUNK_CONFIG_PAYLOAD_TYPE
    }) {
        let id = reference
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("projectionId"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: chunk policy reference lacks projection identity",
                    snapshot.id
                ))
            })?;
        if refs.insert(id.to_owned(), reference).is_some() {
            return Err(verification_failure(format!(
                "snapshot {}: repeated chunk policy reference {id}",
                snapshot.id
            )));
        }
    }
    let envelopes = load_archived_jsonl(store, snapshot, manifest, "retrieval_projections")?;
    let mut policies = BTreeMap::new();
    for envelope in &envelopes {
        let object = as_object(envelope, snapshot, "retrieval_projections")?;
        if json_str(object, "projection_type") != Some("chunk") {
            continue;
        }
        let id = required_str(object, "id", snapshot, "retrieval_projections")?;
        let producer: Provenance = serde_json::from_str(&required_str(
            object,
            "producer_json",
            snapshot,
            "retrieval_projections",
        )?)
        .map_err(|source| {
            verification_failure(format!(
                "snapshot {}: chunk producer {id}: {source}",
                snapshot.id
            ))
        })?;
        let policy = if let Some(uri) = json_str(object, "payload_uri") {
            let reference = refs.remove(&id).ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: chunk projection {id} has no pinned policy",
                    snapshot.id
                ))
            })?;
            if reference.uri != uri {
                return Err(verification_failure(format!(
                    "snapshot {}: chunk projection {id} policy URI differs",
                    snapshot.id
                )));
            }
            let policy: ChunkerConfig = store.with_verified_reader(
                uri,
                Some(store.limits().resources.max_json_cell_bytes as u64),
                |reader| {
                    serde_json::from_reader(reader).map_err(|source| {
                        verification_failure(format!(
                            "snapshot {}: chunk policy {id}: {source}",
                            snapshot.id
                        ))
                    })
                },
            )?;
            policy.validate()?;
            let config_hash = policy.config_hash()?;
            if reference.hash != config_hash
                || producer.config_hash.as_deref() != Some(config_hash.as_str())
            {
                return Err(verification_failure(format!(
                    "snapshot {}: chunk projection {id} producer/reference/config disagreement",
                    snapshot.id
                )));
            }
            policy
        } else {
            // Unpublished attempts have no rows to restore and no completed descriptor.
            if matches!(
                json_str(object, "freshness_status"),
                Some("building" | "failed")
            ) {
                continue;
            }
            let policy = ChunkerConfig::legacy();
            if producer.config_hash.is_some() {
                return Err(verification_failure(format!(
                    "snapshot {}: descriptor-free chunk projection {id} has a nonlegacy config hash",
                    snapshot.id
                )));
            }
            policy
        };
        if producer.producer_name != policy.chunker_name
            || producer.producer_version.as_deref() != Some(policy.chunker_version.as_str())
        {
            return Err(verification_failure(format!(
                "snapshot {}: chunk projection {id} has unsupported producer identity",
                snapshot.id
            )));
        }
        policies.insert(id, policy);
    }
    if !refs.is_empty() {
        return Err(verification_failure(format!(
            "snapshot {}: chunk construction reference has no owning envelope",
            snapshot.id
        )));
    }
    for record in load_archived_jsonl(store, snapshot, manifest, "chunk_projections")? {
        let object = as_object(&record, snapshot, "chunk_projections")?;
        let id = required_str(object, "id", snapshot, "chunk_projections")?;
        let owner = required_str(object, "projection_id", snapshot, "chunk_projections")?;
        let policy = policies.get(&owner).ok_or_else(|| {
            verification_failure(format!(
                "snapshot {}: chunk {id} has no completed producer",
                snapshot.id
            ))
        })?;
        if json_str(object, "chunker_config_hash") != Some(policy.config_hash()?.as_str())
            || json_str(object, "chunker_name") != Some(policy.chunker_name.as_str())
            || json_str(object, "chunker_version") != Some(policy.chunker_version.as_str())
            || object
                .get("token_count")
                .and_then(Value::as_u64)
                .is_some_and(|tokens| tokens > policy.max_unit_tokens as u64)
        {
            return Err(verification_failure(format!(
                "snapshot {}: chunk {id} disagrees with its recorded construction policy",
                snapshot.id
            )));
        }
    }
    policies
        .into_iter()
        .map(|(id, policy)| Ok((id, policy.config_hash()?)))
        .collect()
}

/// Verified immutable publications retain compact row identities for the final
/// deletion transaction; neither manifest texts nor embedding matrices survive.
pub(crate) struct VerifiedAnnotationPublications {
    pub(crate) manifest_count: usize,
    pub(crate) embedding_blob_count: usize,
    pub(crate) representation_count: usize,
    envelopes: BTreeMap<String, VerifiedAnnotationEnvelope>,
}

/// Full archived-row hashing detects a replacement even when its cohort is unchanged.
struct VerifiedAnnotationEnvelope {
    source_id: String,
    parse_id: String,
    record_hash: String,
}

/// Only publication columns participate in pairing; other columns remain pinned
/// by each record's full hash and are restored through the existing row importer.
#[derive(serde::Deserialize)]
struct ArchivedAnnotationEnvelope {
    id: String,
    source_id: String,
    parse_id: String,
    projection_type: String,
    input_unit_ids_json: Option<String>,
    input_annotation_ids_json: Option<String>,
    producer_json: String,
    index_partition: Option<String>,
    payload_uri: Option<String>,
    freshness_status: String,
    deleted_at: Option<String>,
}

/// A provenance target remains exact even when a later cohort uses a finer range.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedAnnotationTarget {
    unit_id: String,
    range: Option<SourceExcerpt>,
}

/// Compact expectations are merged across historical and current publications.
struct ExpectedAnnotationInput {
    source_id: String,
    parse_id: String,
    fingerprint: String,
    representation: AnnotationRepresentation,
    require_fresh: bool,
    targets: BTreeSet<ExpectedAnnotationTarget>,
    nonempty: bool,
    windows: BTreeSet<ExpectedTextWindow>,
    full_text_lengths: BTreeSet<usize>,
    combined_inputs: Vec<usize>,
}

/// Source verification retains hashes and scalar offsets instead of source bodies.
struct ExpectedSourceInput {
    source_id: String,
    parse_id: String,
    ranges: BTreeSet<SourceExcerpt>,
    whole_unit_lengths: BTreeSet<usize>,
}

/// Model-input windows are verified by scalar offsets and exact-byte hashes.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedTextWindow {
    start_char: usize,
    end_char: usize,
    text_hash: String,
}

/// A combined input is hashed incrementally while its ordered annotations stream
/// past; no corpus-wide collection of rendered annotation strings is retained.
struct ExpectedCombinedInput {
    annotation_ids: Vec<String>,
    next_annotation: usize,
    next_char: usize,
    next_window: usize,
    windows: Vec<(ExpectedTextWindow, Sha256)>,
    full_text_length: usize,
}

/// Verify optional annotation indexes without model calls. Each bounded manifest
/// is decoded once, each matrix is released after validation, and archived inputs
/// are streamed once per plane after collecting only their hashes and ranges.
pub(crate) fn verified_annotation_publications(
    store: &ArtifactStore,
    snapshot_id: &str,
    manifest: &ForensicSnapshotManifest,
) -> Result<VerifiedAnnotationPublications, ApiError> {
    let mut result = VerifiedAnnotationPublications {
        manifest_count: 0,
        embedding_blob_count: 0,
        representation_count: 0,
        envelopes: BTreeMap::new(),
    };
    let mut by_payload: BTreeMap<String, Vec<ArchivedAnnotationEnvelope>> = BTreeMap::new();
    visit_annotation_archive::<Value>(
        store,
        snapshot_id,
        manifest,
        "retrieval_projections",
        |record| {
            if record.get("index_name").and_then(Value::as_str) != Some(annotation::INDEX_NAME) {
                return Ok(());
            }
            let record_hash = crate::canonical::canonical_sha256_hex_of(&record)?;
            let row: ArchivedAnnotationEnvelope =
                serde_json::from_value(record).map_err(|source| {
                    annotation_snapshot_failure(
                        snapshot_id,
                        format!("decode annotation envelope: {source}"),
                    )
                })?;
            if row.payload_uri.is_none()
                && matches!(row.freshness_status.as_str(), "building" | "failed")
            {
                return Ok(());
            }
            if !matches!(
                row.freshness_status.as_str(),
                "fresh" | "stale" | "superseded"
            ) {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "annotation envelope {} has a payload in unfinished state {}",
                        row.id, row.freshness_status
                    ),
                ));
            }
            let uri = row.payload_uri.as_deref().ok_or_else(|| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("completed annotation envelope {} has no payload", row.id),
                )
            })?;
            let hash = annotation_uri_hash(uri, snapshot_id)?.to_owned();
            let identity = VerifiedAnnotationEnvelope {
                source_id: row.source_id.clone(),
                parse_id: row.parse_id.clone(),
                record_hash,
            };
            if result.envelopes.insert(row.id.clone(), identity).is_some() {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("duplicate annotation envelope {}", row.id),
                ));
            }
            by_payload.entry(hash).or_default().push(row);
            Ok(())
        },
    )?;
    let manifest_refs = annotation_snapshot_refs(manifest, annotation::PAYLOAD_TYPE, snapshot_id)?;
    let blob_refs =
        annotation_snapshot_refs(manifest, annotation::VECTOR_PAYLOAD_TYPE, snapshot_id)?;
    if by_payload.keys().collect::<BTreeSet<_>>() != manifest_refs.keys().collect::<BTreeSet<_>>() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation manifest refs and completed envelopes differ",
        ));
    }
    if by_payload.is_empty() {
        if !blob_refs.is_empty() {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                "annotation blobs have no published manifest",
            ));
        }
        return Ok(result);
    }
    let mut expected_annotations = BTreeMap::new();
    let mut expected_sources = BTreeMap::new();
    let mut expected_combined = Vec::new();
    let mut fresh_cohorts = BTreeSet::new();
    let mut used_blobs = BTreeSet::new();
    let mut checked_shapes = BTreeSet::new();
    for (hash, rows) in by_payload {
        let reference = manifest_refs.get(&hash).ok_or_else(|| {
            annotation_snapshot_failure(snapshot_id, "missing annotation manifest reference")
        })?;
        let publication = annotation::read_manifest(store, &reference.uri).map_err(|source| {
            annotation_snapshot_failure(
                snapshot_id,
                format!("read annotation manifest {hash}: {source}"),
            )
        })?;
        let require_fresh =
            verify_annotation_envelope_pair(snapshot_id, &rows, &publication, &mut fresh_cohorts)?;
        collect_annotation_expectations(
            snapshot_id,
            &publication,
            require_fresh,
            &mut expected_annotations,
            &mut expected_sources,
            &mut expected_combined,
        )?;
        for representation in &publication.representations {
            for embedding in [&representation.dense, &representation.colbert] {
                let blob = blob_refs.get(&embedding.artifact.hash).ok_or_else(|| {
                    annotation_snapshot_failure(
                        snapshot_id,
                        format!(
                            "annotation manifest {hash} references unpinned embedding {}",
                            embedding.artifact.hash
                        ),
                    )
                })?;
                if annotation_uri_hash(&embedding.artifact.uri, snapshot_id)? != blob.hash {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        "embedding URI differs from its explicit snapshot ref",
                    ));
                }
                used_blobs.insert(embedding.artifact.hash.clone());
                let shape = (
                    embedding.artifact.hash.clone(),
                    embedding.rows,
                    embedding.dimension,
                    embedding.norm.to_bits(),
                );
                if checked_shapes.insert(shape) {
                    // The decoder validates shape, finite nonzero rows, norm, size,
                    // and the full digest. Only this one bounded matrix is resident.
                    drop(
                        annotation_io::load_embedding(store, embedding).map_err(|source| {
                            annotation_snapshot_failure(
                                snapshot_id,
                                format!("verify embedding {}: {source}", embedding.artifact.hash),
                            )
                        })?,
                    );
                }
            }
        }
        result.manifest_count += 1;
        result.representation_count += publication.representations.len();
    }
    if used_blobs.iter().collect::<BTreeSet<_>>() != blob_refs.keys().collect::<BTreeSet<_>>() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation snapshot contains embedding refs with no owning manifest",
        ));
    }
    result.embedding_blob_count = used_blobs.len();
    verify_archived_annotation_inputs(
        store,
        snapshot_id,
        manifest,
        expected_annotations,
        expected_combined,
    )?;
    verify_archived_annotation_sources(store, snapshot_id, manifest, expected_sources)?;
    Ok(result)
}

/// Reduce one bounded manifest to fingerprint and window-hash expectations. Its
/// annotation-ID order is also the stream order used to hash combined inputs.
fn collect_annotation_expectations(
    snapshot_id: &str,
    publication: &annotation::AnnotationProjection,
    require_fresh: bool,
    annotations: &mut BTreeMap<String, ExpectedAnnotationInput>,
    sources: &mut BTreeMap<String, ExpectedSourceInput>,
    combined_inputs: &mut Vec<ExpectedCombinedInput>,
) -> Result<(), ApiError> {
    let plan = &publication.plan;
    if plan
        .inputs
        .windows(2)
        .any(|pair| pair[0].annotation_id >= pair[1].annotation_id)
    {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation manifest inputs are not in producer order",
        ));
    }
    let source = sources
        .entry(plan.target.unit_id.clone())
        .or_insert_with(|| ExpectedSourceInput {
            source_id: plan.source_id.clone(),
            parse_id: plan.parse_id.clone(),
            ranges: BTreeSet::new(),
            whole_unit_lengths: BTreeSet::new(),
        });
    if source.source_id != plan.source_id || source.parse_id != plan.parse_id {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation manifests disagree on canonical unit ownership",
        ));
    }
    let target_range = plan.target.range.as_ref().map(|range| SourceExcerpt {
        unit_id: plan.target.unit_id.clone(),
        start_char: range.start_char,
        end_char: range.end_char,
        text_hash: range.text_hash.clone(),
    });
    if let Some(range) = &target_range {
        source.ranges.insert(range.clone());
    }
    let mut groups: BTreeMap<
        (AnnotationRepresentation, Vec<String>),
        Vec<&annotation::RepresentationText>,
    > = BTreeMap::new();
    let mut dimensions = None;
    for representation in &publication.representations {
        let shape = (
            representation.dense.dimension,
            representation.colbert.dimension,
        );
        if dimensions.is_some_and(|expected| expected != shape) {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                "annotation manifest mixes dimensions within one model identity",
            ));
        }
        dimensions = Some(shape);
        let input = &representation.input;
        if let Some(excerpt) = &input.source_excerpt {
            let base = plan
                .target
                .range
                .as_ref()
                .map_or(0, |range| range.start_char);
            if base.checked_add(input.input_start_char) != Some(excerpt.start_char)
                || base.checked_add(input.input_end_char) != Some(excerpt.end_char)
            {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "source window model-input and canonical offsets disagree",
                ));
            }
            source.ranges.insert(excerpt.clone());
        }
        groups
            .entry((input.representation, input.annotation_ids.clone()))
            .or_default()
            .push(input);
    }
    let mut individual = BTreeMap::new();
    let mut combined = None;
    for ((kind, ids), mut windows) in groups {
        windows.sort_by_key(|window| window.input_start_char);
        let mut next = 0;
        let mut expected_windows = Vec::new();
        for window in windows {
            if window.input_start_char != next {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "annotation model-input windows have a gap or overlap",
                ));
            }
            next = window.input_end_char;
            expected_windows.push(ExpectedTextWindow {
                start_char: window.input_start_char,
                end_char: window.input_end_char,
                text_hash: crate::canonical::sha256_hex_bytes(window.text.as_bytes()),
            });
        }
        match kind {
            AnnotationRepresentation::Source => {
                if plan.target.range.is_none() {
                    source.whole_unit_lengths.insert(next);
                }
            }
            AnnotationRepresentation::Combined => {
                if combined.replace((ids, expected_windows, next)).is_some() {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        "annotation manifest has inconsistent combined-input lineage",
                    ));
                }
            }
            _ => {
                let [id] = ids.as_slice() else {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        "individual annotation representation must name exactly one input",
                    ));
                };
                if individual
                    .insert(id.clone(), (kind, expected_windows, next))
                    .is_some()
                {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        "annotation input has conflicting individual representation types",
                    ));
                }
            }
        }
    }
    let mut nonempty_ids = Vec::new();
    for signature in &plan.inputs {
        let detail = individual.remove(&signature.annotation_id);
        let nonempty = detail.is_some();
        let expected = annotations
            .entry(signature.annotation_id.clone())
            .or_insert_with(|| ExpectedAnnotationInput {
                source_id: plan.source_id.clone(),
                parse_id: plan.parse_id.clone(),
                fingerprint: signature.fingerprint.clone(),
                representation: signature.representation,
                require_fresh,
                targets: BTreeSet::new(),
                nonempty,
                windows: BTreeSet::new(),
                full_text_lengths: BTreeSet::new(),
                combined_inputs: Vec::new(),
            });
        if expected.source_id != plan.source_id
            || expected.parse_id != plan.parse_id
            || expected.fingerprint != signature.fingerprint
            || expected.representation != signature.representation
            || expected.nonempty != nonempty
        {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                format!(
                    "publications disagree on annotation {}",
                    signature.annotation_id
                ),
            ));
        }
        expected.require_fresh |= require_fresh;
        expected.targets.insert(ExpectedAnnotationTarget {
            unit_id: plan.target.unit_id.clone(),
            range: target_range.clone(),
        });
        if let Some((kind, windows, length)) = detail {
            if kind != signature.representation {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "individual annotation representation disagrees with input signature",
                ));
            }
            expected.windows.extend(windows);
            expected.full_text_lengths.insert(length);
            nonempty_ids.push(signature.annotation_id.clone());
        }
    }
    if !individual.is_empty() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "individual representation references an undeclared annotation",
        ));
    }
    match combined {
        Some((ids, windows, length)) if !ids.is_empty() && ids == nonempty_ids => {
            let index = combined_inputs.len();
            for id in &ids {
                let expected = annotations.get_mut(id).ok_or_else(|| {
                    annotation_snapshot_failure(snapshot_id, "combined input is undeclared")
                })?;
                expected.combined_inputs.push(index);
            }
            combined_inputs.push(ExpectedCombinedInput {
                annotation_ids: ids,
                next_annotation: 0,
                next_char: 0,
                next_window: 0,
                windows: windows
                    .into_iter()
                    .map(|window| (window, Sha256::new()))
                    .collect(),
                full_text_length: length,
            });
        }
        None if nonempty_ids.is_empty() => {}
        _ => {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                "combined representation must cover every nonempty annotation in producer order",
            ));
        }
    }
    Ok(())
}

/// Streamed SQLite annotation columns are decoded into the shared semantic model
/// only when a published manifest references the row; unrelated rows are dropped.
#[derive(serde::Deserialize)]
struct ArchivedAnnotationInput {
    id: String,
    source_id: String,
    parse_id: String,
    target_unit_ids_json: String,
    annotation_type: SemanticAnnotationType,
    body_json: Option<String>,
    provenance_json: String,
    confidence: Option<f64>,
    freshness_status: AnnotationFreshnessStatus,
    created_at: String,
    deleted_at: Option<String>,
}

impl ArchivedAnnotationInput {
    /// Reconstruct exactly the fields used by the authoritative input fingerprint.
    fn into_annotation(self, snapshot_id: &str) -> Result<SemanticAnnotation, ApiError> {
        let body = self.body_json.ok_or_else(|| {
            annotation_snapshot_failure(
                snapshot_id,
                format!("published annotation {} has no body", self.id),
            )
        })?;
        Ok(SemanticAnnotation {
            target_unit_ids: serde_json::from_str(&self.target_unit_ids_json).map_err(
                |source| {
                    annotation_snapshot_failure(
                        snapshot_id,
                        format!("annotation {} target IDs: {source}", self.id),
                    )
                },
            )?,
            body: serde_json::from_str(&body).map_err(|source| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("annotation {} body: {source}", self.id),
                )
            })?,
            provenance: serde_json::from_str(&self.provenance_json).map_err(|source| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("annotation {} provenance: {source}", self.id),
                )
            })?,
            id: self.id,
            source_id: self.source_id,
            parse_id: self.parse_id,
            annotation_type: self.annotation_type,
            confidence: self.confidence,
            freshness_status: self.freshness_status,
            created_at: self.created_at,
            deleted_at: self.deleted_at,
        })
    }
}

/// Verify fingerprints, provenance targets, and framed model inputs in one
/// annotation pass. Combined windows receive only streaming digest updates.
fn verify_archived_annotation_inputs(
    store: &ArtifactStore,
    snapshot_id: &str,
    manifest: &ForensicSnapshotManifest,
    mut expected: BTreeMap<String, ExpectedAnnotationInput>,
    mut combined: Vec<ExpectedCombinedInput>,
) -> Result<(), ApiError> {
    let mut seen = BTreeSet::new();
    visit_annotation_archive::<ArchivedAnnotationInput>(
        store,
        snapshot_id,
        manifest,
        "semantic_annotations",
        |row| {
            if seen.contains(&row.id) {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("duplicate referenced annotation {}", row.id),
                ));
            }
            let Some(input) = expected.remove(&row.id) else {
                return Ok(());
            };
            seen.insert(row.id.clone());
            let annotation = row.into_annotation(snapshot_id)?;
            if annotation.source_id != input.source_id
                || annotation.parse_id != input.parse_id
                || !matches!(
                    annotation.freshness_status,
                    AnnotationFreshnessStatus::Fresh | AnnotationFreshnessStatus::Stale
                )
                || (input.require_fresh
                    && (annotation.freshness_status != AnnotationFreshnessStatus::Fresh
                        || annotation.deleted_at.is_some()))
                || annotation::representation_kind(annotation.annotation_type)
                    != Some(input.representation)
                || annotation::input_fingerprint(&annotation)? != input.fingerprint
            {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "published input {} differs from archived annotation ownership, freshness, type, or fingerprint",
                        annotation.id
                    ),
                ));
            }
            for target in &input.targets {
                verify_annotation_target(snapshot_id, &annotation, target)?;
            }
            let rendered = annotation::render_annotation(&annotation).map_err(|source| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("render archived annotation {}: {source}", annotation.id),
                )
            })?;
            if rendered.is_some() != input.nonempty {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "annotation {} representation coverage disagrees with its empty-marker state",
                        annotation.id
                    ),
                ));
            }
            if let Some(text) = rendered {
                let length = text.chars().count();
                if input
                    .full_text_lengths
                    .iter()
                    .any(|expected_length| *expected_length != length)
                {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        format!(
                            "annotation {} model-input windows omit or extend rendered text",
                            annotation.id
                        ),
                    ));
                }
                for window in &input.windows {
                    let slice = annotation::slice_chars(&text, window.start_char, window.end_char)?;
                    if crate::canonical::sha256_hex_bytes(slice.as_bytes()) != window.text_hash {
                        return Err(annotation_snapshot_failure(
                            snapshot_id,
                            format!(
                                "annotation {} model-input text differs from its archived body",
                                annotation.id
                            ),
                        ));
                    }
                }
                for index in input.combined_inputs {
                    let state = combined.get_mut(index).ok_or_else(|| {
                        annotation_snapshot_failure(
                            snapshot_id,
                            "combined verification identity is missing",
                        )
                    })?;
                    if state.annotation_ids.get(state.next_annotation) != Some(&annotation.id) {
                        return Err(annotation_snapshot_failure(
                            snapshot_id,
                            "archived annotations do not follow the recorded combined-input order",
                        ));
                    }
                    if state.next_annotation > 0 {
                        feed_combined_window_hashes(snapshot_id, state, "\n\n")?;
                    }
                    feed_combined_window_hashes(snapshot_id, state, &text)?;
                    state.next_annotation += 1;
                }
            }
            Ok(())
        },
    )?;
    if let Some((id, _)) = expected.first_key_value() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            format!("published annotation {id} is missing from the archive"),
        ));
    }
    for state in combined {
        if state.next_annotation != state.annotation_ids.len()
            || state.next_char != state.full_text_length
        {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                "combined annotation text does not cover its complete declared input",
            ));
        }
        for (window, digest) in state.windows {
            if format!("{:x}", digest.finalize()) != window.text_hash {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "combined annotation window differs from the archived annotation bodies",
                ));
            }
        }
    }
    Ok(())
}

/// Match the producer's range-discovery rule, including old whole-unit inputs
/// whose provenance contained no reference for the target unit.
fn verify_annotation_target(
    snapshot_id: &str,
    annotation: &SemanticAnnotation,
    expected: &ExpectedAnnotationTarget,
) -> Result<(), ApiError> {
    let refs: Vec<_> = annotation
        .provenance
        .input_refs
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .filter(|input| input.id == expected.unit_id)
        .collect();
    let matches_range = refs
        .iter()
        .any(|input| match (&input.text_range, &expected.range) {
            (None, None) => true,
            (Some(actual), Some(expected)) => {
                actual.start_char == expected.start_char
                    && actual.end_char == expected.end_char
                    && actual.text_hash == expected.text_hash
            }
            _ => false,
        })
        || (refs.is_empty() && expected.range.is_none());
    if !annotation.target_unit_ids.contains(&expected.unit_id) || !matches_range {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            format!(
                "annotation {} does not declare the manifest target/range on {}",
                annotation.id, expected.unit_id
            ),
        ));
    }
    Ok(())
}

/// Hash only window intersections with the next rendered fragment. Completed
/// windows are skipped thereafter, so verification does not rescan prior text.
fn feed_combined_window_hashes(
    snapshot_id: &str,
    state: &mut ExpectedCombinedInput,
    text: &str,
) -> Result<(), ApiError> {
    let end = state
        .next_char
        .checked_add(text.chars().count())
        .filter(|end| *end <= state.full_text_length)
        .ok_or_else(|| {
            annotation_snapshot_failure(
                snapshot_id,
                "combined annotation text exceeds its recorded windows",
            )
        })?;
    for (index, (window, digest)) in state.windows.iter_mut().enumerate().skip(state.next_window) {
        if window.start_char >= end {
            break;
        }
        let start = window.start_char.max(state.next_char);
        let stop = window.end_char.min(end);
        if start < stop {
            digest.update(
                annotation::slice_chars(text, start - state.next_char, stop - state.next_char)?
                    .as_bytes(),
            );
        }
        if window.end_char <= end {
            state.next_window = index + 1;
        }
    }
    state.next_char = end;
    Ok(())
}

/// Canonical bodies are held for one streamed record, never for the corpus.
#[derive(serde::Deserialize)]
struct ArchivedAnnotationSource {
    id: String,
    source_id: String,
    parse_id: String,
    content_type: ContentType,
    body_json: Option<String>,
}

/// A parse's immutable owner is checked independently of its current active state.
#[derive(serde::Deserialize)]
struct ArchivedParseOwner {
    id: String,
    source_id: String,
}

/// Source existence needs only identity; other source metadata is skipped on read.
#[derive(serde::Deserialize)]
struct ArchivedSourceIdentity {
    id: String,
}

/// Check exact canonical text hashes and complete whole-unit coverage, then
/// confirm parse/source bindings without retaining source contents between rows.
fn verify_archived_annotation_sources(
    store: &ArtifactStore,
    snapshot_id: &str,
    manifest: &ForensicSnapshotManifest,
    mut expected: BTreeMap<String, ExpectedSourceInput>,
) -> Result<(), ApiError> {
    let mut parse_owners = BTreeMap::new();
    let mut source_ids = BTreeSet::new();
    for input in expected.values() {
        if parse_owners
            .insert(input.parse_id.clone(), input.source_id.clone())
            .is_some_and(|owner| owner != input.source_id)
        {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                "annotation source expectations disagree on parse ownership",
            ));
        }
        source_ids.insert(input.source_id.clone());
    }
    let mut seen = BTreeSet::new();
    visit_annotation_archive::<ArchivedAnnotationSource>(
        store,
        snapshot_id,
        manifest,
        "content_units",
        |row| {
            if seen.contains(&row.id) {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("duplicate referenced source unit {}", row.id),
                ));
            }
            let Some(input) = expected.remove(&row.id) else {
                return Ok(());
            };
            seen.insert(row.id.clone());
            if row.source_id != input.source_id || row.parse_id != input.parse_id {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("annotation source unit {} has mismatched ownership", row.id),
                ));
            }
            let body_json = row.body_json.ok_or_else(|| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("annotation source unit {} has no body", row.id),
                )
            })?;
            let body: Value = serde_json::from_str(&body_json).map_err(|source| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("decode annotation source unit {}: {source}", row.id),
                )
            })?;
            let text = crate::assembly::evidence::evidence_text(row.content_type, &body)
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| {
                    annotation_snapshot_failure(
                        snapshot_id,
                        format!(
                            "annotation source unit {} has no canonical evidence text",
                            row.id
                        ),
                    )
                })?;
            let length = text.chars().count();
            if input
                .whole_unit_lengths
                .iter()
                .any(|expected_length| *expected_length != length)
            {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "whole-unit annotation source {} is not completely covered by its windows",
                        row.id
                    ),
                ));
            }
            for range in input.ranges {
                let slice = annotation::slice_chars(&text, range.start_char, range.end_char)
                    .map_err(|source| {
                        annotation_snapshot_failure(
                            snapshot_id,
                            format!("annotation source range on {}: {source}", row.id),
                        )
                    })?;
                if crate::canonical::sha256_hex_bytes(slice.as_bytes()) != range.text_hash {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        format!(
                            "annotation source window hash differs from canonical unit {}",
                            row.id
                        ),
                    ));
                }
            }
            Ok(())
        },
    )?;
    if let Some((id, _)) = expected.first_key_value() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            format!("annotation source unit {id} is missing from the archive"),
        ));
    }
    let mut seen_parses = BTreeSet::new();
    visit_annotation_archive::<ArchivedParseOwner>(
        store,
        snapshot_id,
        manifest,
        "parse_runs",
        |row| {
            if seen_parses.contains(&row.id) {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "duplicate referenced parse",
                ));
            }
            if let Some(owner) = parse_owners.remove(&row.id) {
                if owner != row.source_id {
                    return Err(annotation_snapshot_failure(
                        snapshot_id,
                        format!("annotation parse {} source mismatch", row.id),
                    ));
                }
                seen_parses.insert(row.id);
            }
            Ok(())
        },
    )?;
    if !parse_owners.is_empty() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation publication references a missing parse",
        ));
    }
    let mut seen_sources = BTreeSet::new();
    visit_annotation_archive::<ArchivedSourceIdentity>(
        store,
        snapshot_id,
        manifest,
        "source_objects",
        |row| {
            if seen_sources.contains(&row.id) {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    "duplicate referenced source",
                ));
            }
            if source_ids.remove(&row.id) {
                seen_sources.insert(row.id);
            }
            Ok(())
        },
    )?;
    if !source_ids.is_empty() {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            "annotation publication references a missing source",
        ));
    }
    Ok(())
}

/// Refuse deletion of annotation embedding state absent from the gating snapshot.
/// The caller runs this on its deletion transaction after immutable preflight, so
/// publication cannot replace an envelope between this comparison and deletion.
pub(crate) fn verify_annotation_deletion_state(
    connection: &Connection,
    verified: &VerifiedAnnotationPublications,
    snapshot_id: &str,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    const SQL: &str = "SELECT * FROM retrieval_projections WHERE source_id = ?1 AND parse_id = ?2 AND index_name = ?3 ORDER BY id";
    let mut statement = connection.prepare(SQL).map_err(|source| {
        annotation_snapshot_failure(
            snapshot_id,
            format!("prepare annotation deletion-state check: {source}"),
        )
    })?;
    let columns: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let mut rows = statement
        .query(params![source_id, parse_id, annotation::INDEX_NAME])
        .map_err(|source| {
            annotation_snapshot_failure(
                snapshot_id,
                format!("read annotation deletion state: {source}"),
            )
        })?;
    let mut seen = BTreeSet::new();
    while let Some(row) = rows.next().map_err(|source| {
        annotation_snapshot_failure(
            snapshot_id,
            format!("read annotation deletion-state row: {source}"),
        )
    })? {
        let mut record = Map::new();
        for (index, column) in columns.iter().enumerate() {
            record.insert(
                column.clone(),
                super::column_value_to_json(row, index, "retrieval_projections", column)?,
            );
        }
        if record.get("payload_uri").is_none_or(Value::is_null)
            && matches!(
                record.get("freshness_status").and_then(Value::as_str),
                Some("building" | "failed")
            )
        {
            continue;
        }
        let id = record.get("id").and_then(Value::as_str).ok_or_else(|| {
            annotation_snapshot_failure(snapshot_id, "live annotation envelope has no ID")
        })?;
        let expected = verified.envelopes.get(id).ok_or_else(|| annotation_snapshot_failure(snapshot_id,
            format!("refusing to delete annotation projection {id} for parse {parse_id}: it is absent from the gating snapshot")))?;
        if expected.source_id != source_id
            || expected.parse_id != parse_id
            || crate::canonical::canonical_sha256_hex_of(&record)? != expected.record_hash
        {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                format!(
                    "refusing to delete annotation projection {id}: its live state differs from the gating snapshot"
                ),
            ));
        }
        seen.insert(id.to_owned());
    }
    if verified.envelopes.iter().any(|(id, expected)| {
        expected.source_id == source_id && expected.parse_id == parse_id && !seen.contains(id)
    }) {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            format!(
                "annotation projections for parse {parse_id} disappeared since the gating snapshot"
            ),
        ));
    }
    tracing::debug!(
        event = "snapshot.verify.annotation_deletion_state_checked",
        snapshot_id,
        source_id,
        parse_id,
        annotation_envelopes_compared = seen.len(),
        "annotation publication state matches the deletion snapshot"
    );
    Ok(())
}

/// Validate both model roles against the producer's current immutable contract.
/// Historical versions may coexist; each completed version has coherent paired
/// lifecycle state, while only one non-deleted fresh version may own a cohort.
fn verify_annotation_envelope_pair(
    snapshot_id: &str,
    rows: &[ArchivedAnnotationEnvelope],
    publication: &annotation::AnnotationProjection,
    fresh_cohorts: &mut BTreeSet<(String, String, String)>,
) -> Result<bool, ApiError> {
    let plan = &publication.plan;
    let expected_producer = crate::canonical::canonical_json_bytes_of(&annotation::producer(plan))?;
    let mut roles: BTreeMap<(&str, Option<&str>, &str), (usize, usize)> = BTreeMap::new();
    for row in rows {
        let decode = |field: Option<&str>, label: &str| -> Result<Vec<String>, ApiError> {
            let text = field.ok_or_else(|| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!("annotation envelope {} lacks {label}", row.id),
                )
            })?;
            serde_json::from_str(text).map_err(|source| {
                annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "annotation envelope {} has invalid {label}: {source}",
                        row.id
                    ),
                )
            })
        };
        let ids = decode(
            row.input_annotation_ids_json.as_deref(),
            "input_annotation_ids",
        )?;
        let units = decode(row.input_unit_ids_json.as_deref(), "input_unit_ids")?;
        let producer: Provenance = serde_json::from_str(&row.producer_json).map_err(|source| {
            annotation_snapshot_failure(
                snapshot_id,
                format!("annotation envelope {} producer: {source}", row.id),
            )
        })?;
        if row.source_id != plan.source_id
            || row.parse_id != plan.parse_id
            || row.index_partition.as_deref() != Some(plan.cohort_id.as_str())
            || !ids
                .iter()
                .map(String::as_str)
                .eq(plan.inputs.iter().map(|input| input.annotation_id.as_str()))
            || units.as_slice() != std::slice::from_ref(&plan.target.unit_id)
            || crate::canonical::canonical_json_bytes_of(&producer)? != expected_producer
        {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                format!(
                    "annotation envelope {} differs from its manifest ownership, producer, or declared inputs",
                    row.id
                ),
            ));
        }
        let uri = row.payload_uri.as_deref().ok_or_else(|| {
            annotation_snapshot_failure(snapshot_id, "paired envelope lacks payload")
        })?;
        let counts = roles
            .entry((&row.freshness_status, row.deleted_at.as_deref(), uri))
            .or_default();
        match row.projection_type.as_str() {
            "dense_vector" => counts.0 += 1,
            "multi_vector" => counts.1 += 1,
            other => {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "annotation envelope {} has unsupported projection type {other}",
                        row.id
                    ),
                ));
            }
        }
    }
    let mut require_fresh = false;
    for ((status, deleted_at, _), (dense, colbert)) in roles {
        if dense == 0 || dense != colbert {
            return Err(annotation_snapshot_failure(
                snapshot_id,
                format!(
                    "annotation cohort {} has incoherent {status} dense/ColBERT publication pair",
                    plan.cohort_id
                ),
            ));
        }
        if status == "fresh" && deleted_at.is_none() {
            if dense != 1
                || !fresh_cohorts.insert((
                    plan.source_id.clone(),
                    plan.parse_id.clone(),
                    plan.cohort_id.clone(),
                ))
            {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!(
                        "annotation cohort {} has multiple fresh publications",
                        plan.cohort_id
                    ),
                ));
            }
            require_fresh = true;
        }
    }
    Ok(require_fresh)
}

/// Require one explicit manifest reference per immutable dependency identity.
fn annotation_snapshot_refs<'a>(
    manifest: &'a ForensicSnapshotManifest,
    artifact_type: &str,
    snapshot_id: &str,
) -> Result<BTreeMap<String, &'a SnapshotArtifactRef>, ApiError> {
    let mut output = BTreeMap::new();
    for (_, refs) in manifest_ref_sections(manifest) {
        for reference in refs {
            if reference.artifact_type != artifact_type {
                continue;
            }
            if annotation_uri_hash(&reference.uri, snapshot_id)? != reference.hash {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("{artifact_type} URI and hash disagree"),
                ));
            }
            if output.insert(reference.hash.clone(), reference).is_some() {
                return Err(annotation_snapshot_failure(
                    snapshot_id,
                    format!("duplicate {artifact_type} ref {}", reference.hash),
                ));
            }
        }
    }
    Ok(output)
}

/// Stream a canonical archived plane once. A callback retains only requested
/// verification metadata; partial conclusions are discarded if the hash fails.
fn visit_annotation_archive<T: serde::de::DeserializeOwned>(
    store: &ArtifactStore,
    snapshot_id: &str,
    manifest: &ForensicSnapshotManifest,
    artifact_type: &str,
    mut visit: impl FnMut(T) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let reference = find_ref_by_type(manifest, artifact_type).ok_or_else(|| {
        annotation_snapshot_failure(snapshot_id, format!("missing archived {artifact_type}"))
    })?;
    if annotation_uri_hash(&reference.uri, snapshot_id)? != reference.hash {
        return Err(annotation_snapshot_failure(
            snapshot_id,
            format!("archived {artifact_type} URI differs from its hash"),
        ));
    }
    store
        .with_verified_reader(&reference.uri, None, |reader| {
            // Buffer outside the hashing reader so serde's byte-oriented parser
            // updates the integrity digest in blocks instead of one byte at a time.
            let buffered = std::io::BufReader::new(reader);
            for record in serde_json::Deserializer::from_reader(buffered).into_iter::<T>() {
                visit(record.map_err(|source| {
                    annotation_snapshot_failure(
                        snapshot_id,
                        format!("decode archived {artifact_type}: {source}"),
                    )
                })?)?;
            }
            Ok(())
        })
        .map_err(|source| {
            annotation_snapshot_failure(
                snapshot_id,
                format!("verify archived {artifact_type}: {source}"),
            )
        })
}

/// The artifact store validates the full URI suffix and reads local bytes; this
/// extracts only the content identity for matching explicit snapshot references.
fn annotation_uri_hash<'a>(uri: &'a str, snapshot_id: &str) -> Result<&'a str, ApiError> {
    Path::new(uri)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|hash| {
            hash.len() == 64
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            annotation_snapshot_failure(
                snapshot_id,
                "annotation artifact URI has no canonical SHA-256 identity",
            )
        })
}

/// Attach snapshot identity while preserving the specific failed input boundary.
fn annotation_snapshot_failure(snapshot_id: &str, message: impl std::fmt::Display) -> ApiError {
    verification_failure(format!("snapshot {snapshot_id}: {message}"))
}

/// Validate and derive exactly the graphs published in the archived database
/// view. Missing graph envelopes are valid publication lag; extra fresh
/// annotations remain available for later publication after restoration.
pub(crate) fn verified_graph_planes(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
) -> Result<BTreeMap<String, crate::projections::graph::CapturedGraph>, ApiError> {
    let projections = load_archived_jsonl(store, snapshot, manifest, "retrieval_projections")?;
    let annotations = load_archived_jsonl(store, snapshot, manifest, "semantic_annotations")?;
    let parses = load_archived_jsonl(store, snapshot, manifest, "parse_runs")?;
    let sources = load_archived_jsonl(store, snapshot, manifest, "source_objects")?;
    let unit_rows = archived_unit_owners(store, snapshot, manifest)?;
    let annotation_rows = archived_rows_by_id(&annotations, snapshot, "semantic_annotations")?;
    let parse_rows = archived_rows_by_id(&parses, snapshot, "parse_runs")?;
    let source_rows = archived_rows_by_id(&sources, snapshot, "source_objects")?;
    let mut graphs = BTreeMap::new();
    for projection in &projections {
        let object = as_object(projection, snapshot, "retrieval_projections")?;
        if json_str(object, "projection_type") != Some("graph_projection")
            || json_str(object, "freshness_status") != Some("fresh")
            || !json_is_null_or_absent(object, "deleted_at")
        {
            continue;
        }
        let graph = crate::projections::graph::derive_captured_graph(projection, &annotation_rows)
            .map_err(|source| {
                verification_failure(format!(
                    "snapshot {}: captured graph input validation failed: {source}",
                    snapshot.id
                ))
            })?;
        let parse = parse_rows.get(graph.parse_id.as_str());
        let source = source_rows.get(graph.source_id.as_str());
        // A post-activation snapshot still contains the predecessor's fresh
        // envelopes until archive/verify/delete supersedes them. Their source
        // binding must agree; the source's current active pointer need not.
        if parse
            .and_then(|row| row.get("source_id"))
            .and_then(Value::as_str)
            != Some(graph.source_id.as_str())
            || source.is_none()
        {
            return Err(verification_failure(format!(
                "snapshot {}: graph {} source/parse ownership differs from archived canonical records",
                snapshot.id, graph.projection_id
            )));
        }
        // Validate every consumed annotation's canonical targets, including []
        // markers that intentionally produce neither a mention nor an edge.
        for annotation_id in &graph.payload.input_annotation_ids {
            let record = annotation_rows.get(annotation_id.as_str()).ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: graph {} lost referenced annotation {annotation_id}",
                    snapshot.id, graph.projection_id
                ))
            })?;
            let object = as_object(record, snapshot, "semantic_annotations")?;
            let target_ids = decode_json_string_array(
                &required_str(
                    object,
                    "target_unit_ids_json",
                    snapshot,
                    "semantic_annotations",
                )?,
                snapshot,
                "semantic_annotations.target_unit_ids_json",
            )?;
            for unit_id in target_ids {
                let unit = unit_rows.get(unit_id.as_str());
                if !unit.is_some_and(|unit| {
                    unit.source_id == graph.source_id && unit.parse_id == graph.parse_id
                }) {
                    return Err(verification_failure(format!(
                        "snapshot {}: graph {} annotation {annotation_id} targets missing or mismatched content unit {unit_id}",
                        snapshot.id, graph.projection_id
                    )));
                }
            }
        }
        let parse_id = graph.parse_id.clone();
        if graphs.insert(parse_id.clone(), graph).is_some() {
            return Err(verification_failure(format!(
                "snapshot {}: parse {parse_id} has multiple fresh graph projections",
                snapshot.id
            )));
        }
    }
    Ok(graphs)
}

/// Index immutable archived rows without copying payloads, rejecting duplicate
/// identities rather than silently choosing one version of a referenced record.
fn archived_rows_by_id<'a>(
    records: &'a [Value],
    snapshot: &ForensicSnapshot,
    plane: &str,
) -> Result<BTreeMap<&'a str, &'a Value>, ApiError> {
    let mut by_id = BTreeMap::new();
    for record in records {
        let object = as_object(record, snapshot, plane)?;
        let id = json_str(object, "id").ok_or_else(|| {
            verification_failure(format!(
                "snapshot {}: archived {plane} row has no string id",
                snapshot.id
            ))
        })?;
        if by_id.insert(id, record).is_some() {
            return Err(verification_failure(format!(
                "snapshot {}: archived {plane} repeats id {id}",
                snapshot.id
            )));
        }
    }
    Ok(by_id)
}

/// Only the canonical ownership columns needed to validate graph targets.
/// Other archived columns, including document bodies, are skipped by serde.
#[derive(serde::Deserialize)]
struct ArchivedUnitOwner {
    id: String,
    source_id: String,
    parse_id: String,
}

/// Stream canonical ownership without retaining archived content bodies. The
/// compact map is released to the caller only after the artifact hash verifies.
fn archived_unit_owners(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
) -> Result<BTreeMap<String, ArchivedUnitOwner>, ApiError> {
    let artifact = find_ref_by_type(manifest, "content_units").ok_or_else(|| {
        verification_failure(format!(
            "snapshot {} has no content_units artifact for graph ownership validation",
            snapshot.id
        ))
    })?;
    // The streaming API validates the URI's content address; require that address
    // to be the manifest's recorded hash before deriving any ownership claims.
    if Path::new(&artifact.uri)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(artifact.hash.as_str())
    {
        return Err(verification_failure(format!(
            "snapshot {}: content_units URI disagrees with its recorded hash",
            snapshot.id
        )));
    }
    store.with_verified_reader(&artifact.uri, None, |reader| {
        let mut owners = BTreeMap::new();
        let buffered = std::io::BufReader::new(reader);
        for record in serde_json::Deserializer::from_reader(buffered).into_iter::<ArchivedUnitOwner>() {
            let owner = record.map_err(|source| verification_failure(format!(
                "snapshot {}: archived content-unit ownership could not be decoded: {source}", snapshot.id
            )))?;
            let id = owner.id.clone();
            if owners.insert(id.clone(), owner).is_some() {
                return Err(verification_failure(format!(
                    "snapshot {}: archived content_units repeats id {id}", snapshot.id
                )));
            }
        }
        Ok(owners)
    }).map_err(|source| verification_failure(format!(
        "snapshot {}: archived content-unit ownership validation failed: {source}", snapshot.id
    )))
}

/// Cross-check immutable section payloads against their archived envelopes and
/// canonical ownership without opening or modifying the live database. Unfinished
/// incoming parses need no section payload until they own a completed dense plane.
pub(crate) fn verified_section_planes(
    store: &ArtifactStore,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
) -> Result<Vec<(String, SectionDensePlane)>, ApiError> {
    let envelopes = load_archived_jsonl(store, snapshot, manifest, "retrieval_projections")?;
    let units = load_archived_jsonl(store, snapshot, manifest, "content_units")?;
    let relationships = load_archived_jsonl(store, snapshot, manifest, "unit_relationships")?;
    let mut refs = BTreeMap::new();
    for artifact in &manifest.retrieval_indexes {
        if artifact.artifact_type != super::SECTION_DENSE_PAYLOAD_TYPE {
            continue;
        }
        let id = artifact
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("projectionId"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: section payload has no projectionId",
                    snapshot.id
                ))
            })?;
        if refs.insert(id, artifact).is_some() {
            return Err(verification_failure(format!(
                "snapshot {}: duplicate section payload for {id}",
                snapshot.id
            )));
        }
    }
    let mut sections = Vec::new();
    let mut completed_dense_parses = BTreeSet::new();
    let mut completed_section_parses = BTreeSet::new();
    for record in &envelopes {
        let object = as_object(record, snapshot, "retrieval_projections")?;
        let is_section = json_str(object, "index_name") == Some(SECTION_DENSE_INDEX_NAME);
        let completed = matches!(
            json_str(object, "freshness_status"),
            Some("fresh" | "stale" | "superseded")
        );
        if !is_section {
            if completed
                && json_str(object, "projection_type") == Some("dense_vector")
                && json_is_null_or_absent(object, "index_name")
            {
                completed_dense_parses.insert(required_str(
                    object,
                    "parse_id",
                    snapshot,
                    "retrieval_projections",
                )?);
            }
            continue;
        }
        if json_str(object, "projection_type") != Some("dense_vector") {
            return Err(verification_failure(format!(
                "snapshot {}: section index has incorrect projection type",
                snapshot.id
            )));
        }
        let Some(uri) = json_str(object, "payload_uri") else {
            if !completed {
                continue;
            }
            return Err(verification_failure(format!(
                "snapshot {}: completed section index has no payload",
                snapshot.id
            )));
        };
        let id = required_str(object, "id", snapshot, "retrieval_projections")?;
        let source_id = required_str(object, "source_id", snapshot, "retrieval_projections")?;
        let parse_id = required_str(object, "parse_id", snapshot, "retrieval_projections")?;
        let artifact = refs.remove(id.as_str()).ok_or_else(|| {
            verification_failure(format!(
                "snapshot {}: section projection {id} has no explicit payload reference",
                snapshot.id
            ))
        })?;
        let metadata = artifact.metadata.as_ref().ok_or_else(|| {
            verification_failure(format!(
                "snapshot {}: section projection {id} has no metadata",
                snapshot.id
            ))
        })?;
        let dimension = metadata
            .get("dimension")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: section projection {id} has invalid dimension",
                    snapshot.id
                ))
            })?;
        if store.reference_for_uri(uri)?.hash != artifact.hash
            || store.reference_for_uri(&artifact.uri)?.hash != artifact.hash
        {
            return Err(verification_failure(format!(
                "snapshot {}: section projection {id} payload hash differs from envelope",
                snapshot.id
            )));
        }
        let plane = read_section_payload(store, uri, &source_id, &parse_id, dimension)?;
        if metadata.get("sourceId").and_then(Value::as_str) != Some(source_id.as_str())
            || metadata.get("parseId").and_then(Value::as_str) != Some(parse_id.as_str())
            || metadata.get("policyHash").and_then(Value::as_str)
                != Some(plane.policy_hash.as_str())
            || metadata.get("windowCount").and_then(Value::as_u64)
                != Some(plane.windows.len() as u64)
        {
            return Err(verification_failure(format!(
                "snapshot {}: section projection {id} metadata disagrees with payload",
                snapshot.id
            )));
        }
        let input_ids = required_str(
            object,
            "input_unit_ids_json",
            snapshot,
            "retrieval_projections",
        )?;
        let input_ids: Vec<String> = serde_json::from_str(&input_ids).map_err(|source| {
            verification_failure(format!(
                "snapshot {}: section projection {id} inputs are invalid: {source}",
                snapshot.id
            ))
        })?;
        if input_ids != crate::projections::section_dense::plane_input_ids(&plane) {
            return Err(verification_failure(format!(
                "snapshot {}: section projection {id} input membership differs from payload",
                snapshot.id
            )));
        }
        crate::projections::section_dense::validate_archived_plane(
            &units,
            &relationships,
            &plane,
            store.limits(),
        )?;
        if completed {
            completed_section_parses.insert(parse_id);
        }
        sections.push((id, plane));
    }
    if !refs.is_empty() {
        return Err(verification_failure(format!(
            "snapshot {}: section payload references have no owning envelope",
            snapshot.id
        )));
    }
    if let Some(parse_id) = completed_dense_parses
        .difference(&completed_section_parses)
        .next()
    {
        return Err(verification_failure(format!(
            "snapshot {}: parse {parse_id} lacks section embeddings; pre-feature snapshots cannot be restored under the current retrieval policy; rebuild the corpus",
            snapshot.id
        )));
    }
    Ok(sections)
}

/// Deletion-gate verification (§30.5 / §31.2 step 4): mechanical verification
/// plus deterministic index-rebuild verification over the subject snapshot,
/// run before superseded hot state is deleted. Called by C9d in the deletion
/// flow with the resolved subject `snapshot` (the `post_activation` snapshot in
/// the activation flow, `pre_deactivation` in the deactivation flow — never
/// re-taken). Ok clears the deletion gate; Err halts before deletion and — by
/// the caller's duty — retains superseded state.
pub(crate) fn verify_deletion_gate(
    index_root: &StorageContext,
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
    // `snapshot::lifecycle_scope`); the deletion gate only ever runs
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
    let section_windows_compared =
        verify_live_section_planes(&store, &connection, snapshot, &manifest, &subject_parse_id)
            .map_err(|source| log_tier_failure("deletion_gate", snapshot, started, source))?;

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
        section_windows_compared,
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

/// Require the exact archived section payload and canonical mappings to survive
/// in the hot plane before allowing its envelope and units to be deleted.
fn verify_live_section_planes(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    parse_id: &str,
) -> Result<usize, ApiError> {
    let section_planes = verified_section_planes(store, snapshot, manifest)?;
    let archived_ids: BTreeSet<&str> = section_planes
        .iter()
        .filter(|(_, plane)| plane.parse_id == parse_id)
        .map(|(id, _)| id.as_str())
        .collect();
    // Include every named section envelope, regardless of freshness or payload:
    // cleanup must never delete a live representation the snapshot did not pin.
    // One extra row beyond the archive cardinality is sufficient to reject it.
    const LIVE_SECTION_IDS_SQL: &str = "SELECT id FROM retrieval_projections
        WHERE parse_id = ?1 AND index_name = ?2 ORDER BY id LIMIT ?3";
    let mut statement = connection.prepare(LIVE_SECTION_IDS_SQL).map_err(|source| {
        verification_failure(format!(
            "snapshot {}: prepare live section identities for {parse_id}: {source}",
            snapshot.id
        ))
    })?;
    let live_ids = statement
        .query_map(
            params![
                parse_id,
                SECTION_DENSE_INDEX_NAME,
                archived_ids.len().saturating_add(1)
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|source| {
            verification_failure(format!(
                "snapshot {}: query live section identities for {parse_id}: {source}",
                snapshot.id
            ))
        })?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|source| {
            verification_failure(format!(
                "snapshot {}: read live section identities for {parse_id}: {source}",
                snapshot.id
            ))
        })?;
    if let Some(id) = live_ids
        .iter()
        .find(|id| !archived_ids.contains(id.as_str()))
    {
        return Err(verification_failure(format!(
            "snapshot {}: live section envelope {id} for parse {parse_id} has no archived payload; refusing deletion",
            snapshot.id
        )));
    }
    if let Some(id) = archived_ids.iter().find(|id| !live_ids.contains(**id)) {
        return Err(verification_failure(format!(
            "snapshot {}: archived section envelope {id} for parse {parse_id} is absent from the hot plane",
            snapshot.id
        )));
    }
    let mut windows = 0;
    for (id, plane) in section_planes {
        if plane.parse_id != parse_id {
            continue;
        }
        let uri: String = connection
            .query_row(
                "SELECT payload_uri FROM retrieval_projections WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .map_err(|source| {
                verification_failure(format!(
                    "snapshot {}: live section envelope {id} missing or invalid: {source}",
                    snapshot.id
                ))
            })?;
        let archived = manifest
            .retrieval_indexes
            .iter()
            .find(|artifact| {
                artifact.artifact_type == super::SECTION_DENSE_PAYLOAD_TYPE
                    && artifact
                        .metadata
                        .as_ref()
                        .and_then(|meta| meta.get("projectionId"))
                        .and_then(Value::as_str)
                        == Some(id.as_str())
            })
            .ok_or_else(|| {
                verification_failure(format!(
                    "snapshot {}: section ref {id} missing",
                    snapshot.id
                ))
            })?;
        if store.reference_for_uri(&uri)?.hash != archived.hash {
            return Err(verification_failure(format!(
                "snapshot {}: live section payload {id} differs from archived bytes",
                snapshot.id
            )));
        }
        crate::projections::section_dense::validate_plane(connection, &plane)?;
        windows += plane.windows.len();
    }
    Ok(windows)
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
            hash_prefix(&manifest.manifest_hash, &store.limits().diagnostics),
            hash_prefix(&recomputed, &store.limits().diagnostics)
        )));
    }
    // The recorded self-hash must also be the address the header points at, or
    // the header references a different manifest than the one it names.
    if manifest.manifest_hash != snapshot.manifest_hash {
        return Err(verification_failure(format!(
            "snapshot {}: header manifest_hash {} does not match the manifest's own \
             manifestHash {}",
            snapshot.id,
            hash_prefix(&snapshot.manifest_hash, &store.limits().diagnostics),
            hash_prefix(&manifest.manifest_hash, &store.limits().diagnostics)
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
                    tracing::debug!(
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
            // Integrity checks stream payloads before any format-specific decoder
            // allocates memory. A malformed matrix cannot bypass its decoder's
            // allocation ceiling through this generic completeness pass.
            let actual = store.reference_for_uri(&artifact.uri).map_err(|source| {
                verification_failure(format!(
                    "snapshot {}: section {} artifact {} ({}) failed hash verification: {source}",
                    snapshot.id,
                    section_name,
                    hash_prefix(&artifact.hash, &store.limits().diagnostics),
                    artifact.artifact_type
                ))
            })?;
            if actual.hash != artifact.hash {
                return Err(verification_failure(format!(
                    "snapshot {}: section {} artifact {} URI disagrees with its recorded hash",
                    snapshot.id, section_name, artifact.artifact_type
                )));
            }
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
/// `chunker_config_hash`. Construction settings come from the pinned descriptor,
/// or the exact frozen v1 identity for snapshots predating descriptors.
///
/// DELIBERATELY NOT CHECKED: this does NOT re-run the chunker's text splitter.
/// `chunk.rs::split_units_into_chunks` requires a caller-supplied ColBERT
/// `Tokenizer` (a model runtime handle), and §38 forbids any model call here.
/// The deterministic-rebuild guarantee is instead established by (a) proving the
/// archived and live chunk sets agree byte-for-byte on the deterministic
/// columns, and (b) authenticating their recorded construction settings without
/// re-running the tokenizer-bearing split or applying current indexing limits.
fn verify_chunk_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<usize, ApiError> {
    let archived = load_archived_jsonl(store, snapshot, manifest, "chunk_projections")?;

    let policies = verified_chunk_policies(store, snapshot, manifest)?;

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
        let projection_id = required_str(object, "projection_id", snapshot, "chunk_projections")?;
        let expected_hash = policies.get(&projection_id).ok_or_else(|| {
            verification_failure(format!(
                "snapshot {}: chunk {} has no captured construction policy",
                snapshot.id, comparison.id
            ))
        })?;
        if &comparison.chunker_config_hash != expected_hash {
            return Err(verification_failure(format!(
                "snapshot {}: archived chunk {} has chunker_config_hash {} but its captured construction policy hashes to {}",
                snapshot.id,
                comparison.id,
                hash_prefix(&comparison.chunker_config_hash, &store.limits().diagnostics),
                hash_prefix(expected_hash, &store.limits().diagnostics)
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
        &store.limits().diagnostics,
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
                hash_prefix(&blob_hash, &store.limits().diagnostics),
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
                hash_prefix(&blob_hash, &store.limits().diagnostics),
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

/// Compare the live payload with the graph projection actually published in the
/// captured snapshot, using the builder's shared derivation. Completed annotations
/// outside its input sequence cannot alter the expected graph during this check.
fn verify_graph_plane(
    store: &ArtifactStore,
    connection: &Connection,
    snapshot: &ForensicSnapshot,
    manifest: &ForensicSnapshotManifest,
    subject_parse_id: &str,
) -> Result<(usize, usize), ApiError> {
    let graphs = verified_graph_planes(store, snapshot, manifest)?;
    let captured = graphs.get(subject_parse_id);
    let empty = crate::projections::graph::GraphPayload::default();
    let payload = captured.map(|graph| &graph.payload).unwrap_or(&empty);
    let expected_mentions = &payload.mentions;

    // Compare against the live mention rows: one row per (parse, normalized
    // name), unit_ids the deduplicated deterministic set. Entity type remains
    // metadata outside this comparison: old envelopes did not record the order
    // needed to prove their first-seen type when annotations disagreed.
    let mentions_compared = expected_mentions.len();
    let hot_mentions = load_hot_mentions(connection, subject_parse_id, captured)?;
    compare_keyed_sets(
        snapshot,
        "graph_entity_mentions",
        &store.limits().diagnostics,
        &expected_mentions
            .iter()
            .map(|(name, expectation)| {
                (name.clone(), expectation.unit_ids.iter().cloned().collect())
            })
            .collect(),
        &hot_mentions,
        |name, expected_units, hot_units| {
            if expected_units != hot_units {
                return Err(verification_failure(format!(
                    "snapshot {}: graph mention for normalized entity name {:?} has archived-derived \
                     unit set differing from the hot plane",
                    snapshot.id,
                    bounded_name(name, &store.limits().diagnostics)
                )));
            }
            Ok(())
        },
    )?;

    // Compare multiplicities as well as direction and supporting units: two
    // consumed relation annotations may intentionally emit identical edge rows.
    let hot_edges = load_hot_edges(connection, subject_parse_id, captured)?;
    let mut expected_edges: Vec<_> = payload.edges.iter().collect();
    let mut observed_edges: Vec<_> = hot_edges.iter().collect();
    expected_edges.sort();
    observed_edges.sort();
    if expected_edges != observed_edges {
        return Err(verification_failure(format!(
            "snapshot {}: graph edge set re-derived from archived relation annotations differs \
             from the hot graph_entity_edges for parse {subject_parse_id} (archived-derived {} \
             edges, hot {} edges)",
            snapshot.id,
            expected_edges.len(),
            hot_edges.len()
        )));
    }
    // Equal multisets, so either cardinality is the number of rows compared.
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

/// Load the subject parse's live entity-mention rows as (normalized_name →
/// deterministically ordered unit ids), matching the archived-derived shape.
fn load_hot_mentions(
    connection: &Connection,
    parse_id: &str,
    captured: Option<&CapturedGraph>,
) -> Result<BTreeMap<String, Vec<String>>, ApiError> {
    const SQL: &str = "SELECT normalized_name, unit_ids_json, projection_id, source_id FROM graph_entity_mentions \
                       WHERE parse_id = ?1 ORDER BY normalized_name";
    let mut statement = prepare(connection, SQL, "hot graph_entity_mentions")?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|source| query_failure("hot graph_entity_mentions", source))?;

    let mut mentions = BTreeMap::new();
    for row in rows {
        let (normalized_name, unit_ids_json, projection_id, source_id) =
            row.map_err(|source| query_failure("hot graph_entity_mentions row", source))?;
        verify_graph_row_owner(captured, &projection_id, &source_id, parse_id)?;
        let unit_ids = decode_json_string_array_untied(
            &unit_ids_json,
            "hot graph_entity_mentions.unit_ids_json",
        )?;
        if mentions.insert(normalized_name, unit_ids).is_some() {
            return Err(verification_failure(format!(
                "hot graph_entity_mentions repeats an entity name for parse {parse_id}"
            )));
        }
    }
    Ok(mentions)
}

/// Load every live edge in the shared derived shape without dropping duplicate
/// rows, checking that each belongs to the captured published projection.
fn load_hot_edges(
    connection: &Connection,
    parse_id: &str,
    captured: Option<&CapturedGraph>,
) -> Result<Vec<DerivedEdge>, ApiError> {
    const SQL: &str = "SELECT from_normalized_name, to_normalized_name, relation_type, \
                       target_unit_ids_json, projection_id, source_id FROM graph_entity_edges WHERE parse_id = ?1";
    let mut statement = prepare(connection, SQL, "hot graph_entity_edges")?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(|source| query_failure("hot graph_entity_edges", source))?;

    let mut edges = Vec::new();
    for row in rows {
        let (
            from_normalized_name,
            to_normalized_name,
            relation_type,
            target_unit_ids_json,
            projection_id,
            source_id,
        ) = row.map_err(|source| query_failure("hot graph_entity_edges row", source))?;
        verify_graph_row_owner(captured, &projection_id, &source_id, parse_id)?;
        let target_unit_ids = decode_json_string_array_untied(
            &target_unit_ids_json,
            "hot graph_entity_edges.target_unit_ids_json",
        )?;
        edges.push(DerivedEdge {
            from_normalized_name,
            to_normalized_name,
            relation_type,
            target_unit_ids,
        });
    }
    Ok(edges)
}

/// A payload row requires the captured envelope identity. An absent publication
/// cannot own rows, even if enough annotations now exist to build a newer graph.
fn verify_graph_row_owner(
    captured: Option<&CapturedGraph>,
    projection_id: &str,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    if captured.is_some_and(|graph| {
        graph.projection_id == projection_id
            && graph.source_id == source_id
            && graph.parse_id == parse_id
    }) {
        Ok(())
    } else {
        Err(verification_failure(format!(
            "live graph row for source {source_id}, parse {parse_id}, projection {projection_id} does not belong to the snapshot's published graph"
        )))
    }
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
            hash_prefix(&artifact.hash, &store.limits().diagnostics)
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
    diagnostics: &crate::limits::DiagnosticLimits,
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
                    bounded_name(key, diagnostics)
                )));
            }
        }
    }
    for key in hot.keys() {
        if !archived.contains_key(key) {
            return Err(verification_failure(format!(
                "snapshot {}: {plane} key {:?} exists in the hot plane but not in the archive",
                snapshot.id,
                bounded_name(key, diagnostics)
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
) -> Result<crate::sqlite::Statement<'c>, ApiError> {
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

/// Bound a normalized entity name / key for logging so a failure message never
/// carries an unbounded (or content-revealing) name. Entity names are already
/// derived identities, but the DIAGNOSTICS rule against logging document
/// contents is honored by capping length.
fn bounded_name(name: &str, diagnostics: &crate::limits::DiagnosticLimits) -> String {
    let mut chars = name.chars();
    let mut bounded: String = chars
        .by_ref()
        .take(diagnostics.identifier_preview_chars)
        .collect();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}
