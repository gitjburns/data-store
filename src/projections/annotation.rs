//! Annotation-derived retrieval records retain exact source identity and immutable model inputs.
//!
//! Cohorts are keyed by context window (PLAN-grains Section 2, Phase 6): an
//! annotation joins the cohort of every window whose fragments intersect one of
//! its attributed `inputRefs`; the cohort's source representation is the
//! window's canonical text, and its coverage is the window's fragments.

use std::collections::{BTreeMap, BTreeSet};

use crate::sqlite::Connection;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    annotations::store,
    artifact_store::{ArtifactRef, ArtifactStore},
    canonical::{canonical_json_bytes_of, canonical_sha256_hex_of, sha256_hex_bytes},
    error::ApiError,
    inference::ColbertBackend,
    model::provenance::ProvenanceTextRange,
    model::{Provenance, SemanticAnnotation, SemanticAnnotationType},
    query::provenance::{AnnotationRepresentation, SourceExcerpt, SourceFragment},
};

use super::{
    annotation_io::{EmbeddingRef, validate_ref},
    chunk::{Fragment, join_text, ordered_unit_ids},
    section_dense::{
        CONSTRUCTION_VERSION as CONTEXT_WINDOW_CONSTRUCTION_VERSION, load_section_dense_reference,
        visit_section_dense,
    },
};

pub(crate) const INDEX_NAME: &str = "annotation_retrieval_v1";
pub(crate) const PAYLOAD_TYPE: &str = "annotation_retrieval_manifest";
pub(crate) const VECTOR_PAYLOAD_TYPE: &str = "annotation_embedding_blob";
const FORMAT_VERSION: u32 = 3;
const FORMAT_POLICY: &str = "annotation_retrieval_v1;entity_name_type;relation_triple;summary_text;combined_by_window;source_by_context_window;complete_colbert_windows;fragments_scalar_offsets;canonical_source_separate";
const PUBLICATIONS_SQL: &str = "WITH scoped AS (
 SELECT *, length(CAST(producer_json AS BLOB))
   + coalesce(length(CAST(input_annotation_ids_json AS BLOB)), 0)
   + coalesce(length(CAST(input_unit_ids_json AS BLOB)), 0) AS metadata_bytes
 FROM retrieval_projections WHERE source_id = ?1 AND parse_id = ?2 AND index_name = ?3
 AND freshness_status = 'fresh' AND deleted_at IS NULL)
 SELECT id, index_partition, payload_uri,
 CASE WHEN metadata_bytes <= ?5 THEN producer_json END,
 CASE WHEN metadata_bytes <= ?5 THEN input_annotation_ids_json END,
 CASE WHEN metadata_bytes <= ?5 THEN input_unit_ids_json END, projection_type
 FROM scoped ORDER BY index_partition, projection_type, id LIMIT ?4";
// Guard raw fields before JSON construction, then guard encoded bytes before
// rusqlite copies the value into Rust. A row with NULL output is an oversize
// error; an absent row remains the distinct missing/stale input outcome.
const INPUT_SQL: &str = "WITH raw_input AS (
 SELECT *, coalesce(length(CAST(body_json AS BLOB)), 0)
  + length(CAST(provenance_json AS BLOB)) + length(CAST(target_unit_ids_json AS BLOB))
  + length(CAST(id AS BLOB)) + length(CAST(source_id AS BLOB)) + length(CAST(parse_id AS BLOB))
  + length(CAST(annotation_type AS BLOB)) + length(CAST(freshness_status AS BLOB))
  + length(CAST(created_at AS BLOB)) AS input_bytes
 FROM semantic_annotations WHERE id = ?1 AND source_id = ?2 AND parse_id = ?3
 AND freshness_status = 'fresh' AND deleted_at IS NULL),
 encoded AS (SELECT input_bytes, CASE WHEN input_bytes <= ?4 THEN json_object(
 'id', id, 'sourceId', source_id, 'parseId', parse_id,
 'targetUnitIds', json(target_unit_ids_json), 'annotationType', annotation_type,
 'body', json(body_json), 'provenance', json(provenance_json),
 'confidence', confidence, 'freshnessStatus', freshness_status,
 'createdAt', created_at, 'deletedAt', deleted_at) END AS annotation_json FROM raw_input)
 SELECT input_bytes, CASE WHEN length(CAST(annotation_json AS BLOB)) <= ?4 THEN annotation_json END FROM encoded";
// One member fine chunk of the cohort window; the text cell is guarded before
// rusqlite copies it, so an oversized member is an explicit error.
const CHUNK_SQL: &str = "SELECT fragments_json,
 CASE WHEN length(CAST(targeting_text AS BLOB)) <= ?4 THEN targeting_text END
 FROM chunk_projections WHERE id = ?1 AND source_id = ?2 AND parse_id = ?3";

/// Small discovery identity; bodies are fetched only for a cohort requiring publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InputSignature {
    pub(crate) annotation_id: String,
    pub(crate) fingerprint: String,
    pub(crate) representation: AnnotationRepresentation,
}

/// The context window a cohort is keyed by: its identity, member fine chunks in
/// chunk order, fragments (scalar offsets, end exclusive), section path, and
/// the hash of its canonical text (member texts joined by one blank line).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WindowTarget {
    pub(crate) window_id: String,
    pub(crate) chunk_ids: Vec<String>,
    pub(crate) fragments: Vec<Fragment>,
    pub(crate) section_path: Vec<String>,
    pub(crate) text_hash: String,
}

/// The complete declared input set for one window publication, including empty markers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CohortPlan {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) cohort_id: String,
    pub(crate) input_hash: String,
    pub(crate) model_identity: String,
    pub(crate) target: WindowTarget,
    pub(crate) inputs: Vec<InputSignature>,
    pub(crate) construction: AnnotationConstruction,
}

/// Construction policy is archived so configuration changes cannot reinterpret old windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AnnotationConstruction {
    pub(crate) version: u32,
    /// ColBERT content cap each representation text is partitioned under.
    pub(crate) window_max_tokens: u32,
    /// The context-window construction the cohort window was built under.
    pub(crate) context_window_version: u32,
}

/// Where one fragment's slice lies in the window's canonical text, so a
/// ColBERT partition of that text maps back to exact fragment ranges.
struct FragmentSpan {
    fragment: Fragment,
    start_char: usize,
    end_char: usize,
}

/// The cohort window's canonical text rebuilt from its member chunk rows, with
/// the layout of every fragment inside it.
struct WindowText {
    canonical: String,
    spans: Vec<FragmentSpan>,
}

/// A complete bounded model input, with source coordinates only for canonical source text.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RepresentationText {
    pub(crate) id: String,
    pub(crate) representation: AnnotationRepresentation,
    pub(crate) annotation_ids: Vec<String>,
    pub(crate) text: String,
    /// Coordinates in the complete framed model input preserve repeated windows.
    pub(crate) input_start_char: usize,
    pub(crate) input_end_char: usize,
    pub(crate) source_excerpt: Option<SourceExcerpt>,
}

/// Both model forms publish together, so a searchable record is also ready for MaxSim.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EmbeddedRepresentation {
    pub(crate) input: RepresentationText,
    pub(crate) dense: EmbeddingRef,
    pub(crate) colbert: EmbeddingRef,
}

/// An immutable excerpt version; query readers select its envelope in their WAL snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AnnotationProjection {
    pub(crate) format_version: u32,
    pub(crate) plan: CohortPlan,
    pub(crate) representations: Vec<EmbeddedRepresentation>,
}

/// Prepared source and annotation text contains no database or model-runtime handles.
pub(crate) struct PreparedProjection {
    pub(crate) plan: CohortPlan,
    pub(crate) texts: Vec<RepresentationText>,
}

/// Discover input identities without retaining annotation bodies across a source
/// scan. The parse's context windows are streamed once from the captured
/// section-dense artifact; only window metadata (no vectors, no text) is
/// retained while annotations are visited. No windows means no cohorts this
/// cycle: the section plane is built first and discovery runs again.
pub(crate) fn plan_for_parse(
    conn: &Connection,
    store: &ArtifactStore,
    source_id: &str,
    parse_id: &str,
    dense_dimension: usize,
    model_identity: &str,
) -> Result<Vec<CohortPlan>, ApiError> {
    let limits = &conn.limits().resources;
    let Some(reference) = load_section_dense_reference(conn, store, parse_id, dense_dimension)?
    else {
        return Ok(Vec::new());
    };
    if reference.source_id != source_id {
        return Err(failure(format!(
            "section windows of parse {parse_id} belong to source {}, not {source_id}",
            reference.source_id
        )));
    }
    let mut windows: Vec<WindowTarget> = Vec::new();
    let mut windows_by_unit: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    visit_section_dense(conn, store, &reference, |window| {
        let index = windows.len();
        for unit_id in &window.input_unit_ids {
            windows_by_unit
                .entry(unit_id.clone())
                .or_default()
                .push(index);
        }
        // The visitor lends each streamed window; the plan owns its target.
        windows.push(WindowTarget {
            window_id: window.window_id.clone(),
            chunk_ids: window.chunk_ids.clone(),
            fragments: window.fragments.clone(),
            section_path: window.section_path.clone(),
            text_hash: sha256_hex_bytes(window.targeting_text.as_bytes()),
        });
        Ok(())
    })?;
    let mut cohorts: BTreeMap<String, CohortPlan> = BTreeMap::new();
    store::visit_fresh_for_active_parse(
        conn,
        source_id,
        Some(limits.max_json_cell_bytes as u64),
        |annotation| {
            if annotation.parse_id != parse_id {
                return Err(failure("active parse changed during annotation discovery"));
            }
            let Some(representation) = representation_kind(annotation.annotation_type) else {
                return Ok(());
            };
            let fingerprint = input_fingerprint(&annotation)?;
            let mut members = BTreeSet::new();
            for (unit_id, range) in attributed_refs(&annotation) {
                for index in windows_by_unit.get(unit_id).into_iter().flatten() {
                    if intersects(&windows[*index].fragments, unit_id, range) {
                        members.insert(*index);
                    }
                }
            }
            for index in members {
                let window = &windows[index];
                let cohort_id = cohort_id(source_id, parse_id, &window.window_id)?;
                if !cohorts.contains_key(&cohort_id)
                    && cohorts.len() >= limits.max_cohorts_per_parse
                {
                    return Err(failure(format!(
                        "resource limit: parse {parse_id} exceeds {} annotation cohorts",
                        limits.max_cohorts_per_parse
                    )));
                }
                let cohort = cohorts
                    .entry(cohort_id.clone())
                    .or_insert_with(|| CohortPlan {
                        source_id: source_id.to_owned(),
                        parse_id: parse_id.to_owned(),
                        cohort_id,
                        input_hash: String::new(),
                        model_identity: model_identity.to_owned(),
                        // The discovery index outlives no cohort; each plan owns its window.
                        target: window.clone(),
                        inputs: Vec::new(),
                        construction: AnnotationConstruction {
                            version: FORMAT_VERSION,
                            window_max_tokens: conn.limits().indexing.colbert_max_tokens,
                            context_window_version: CONTEXT_WINDOW_CONSTRUCTION_VERSION,
                        },
                    });
                if !cohort
                    .inputs
                    .iter()
                    .any(|input| input.annotation_id == annotation.id)
                {
                    if cohort.inputs.len() >= limits.max_annotations_per_cohort {
                        return Err(failure(
                            "resource limit: annotation cohort input ceiling exceeded",
                        ));
                    }
                    cohort.inputs.push(InputSignature {
                        annotation_id: annotation.id.clone(),
                        fingerprint: fingerprint.clone(),
                        representation,
                    });
                }
            }
            Ok(())
        },
    )?;
    for cohort in cohorts.values_mut() {
        cohort
            .inputs
            .sort_by(|left, right| left.annotation_id.cmp(&right.annotation_id));
        cohort.input_hash = plan_hash(cohort)?;
    }
    Ok(cohorts.into_values().collect())
}

/// Load one cohort under the caller's read snapshot before making any model call.
pub(crate) fn prepare(
    conn: &Connection,
    plan: &CohortPlan,
    colbert: &ColbertBackend,
) -> Result<Option<PreparedProjection>, ApiError> {
    let max_manifest_bytes = conn.limits().resources.max_manifest_bytes;
    let Some(annotations) = current_inputs(conn, plan)? else {
        return Ok(None);
    };
    let source = window_text(conn, plan)?;
    let mut texts = Vec::new();
    append_windows(
        &mut texts,
        colbert,
        plan,
        AnnotationRepresentation::Source,
        &[],
        &source.canonical,
        Some(&source.spans),
        max_manifest_bytes,
    )?;
    let mut combined = String::new();
    let mut nonempty_ids = Vec::new();
    for annotation in &annotations {
        let Some(text) = render_annotation(annotation)? else {
            continue;
        };
        let kind = representation_kind(annotation.annotation_type)
            .ok_or_else(|| failure("unsupported annotation input type"))?;
        append_windows(
            &mut texts,
            colbert,
            plan,
            kind,
            std::slice::from_ref(&annotation.id),
            &text,
            None,
            max_manifest_bytes,
        )?;
        if !combined.is_empty() {
            combined.push_str("\n\n");
        }
        combined.push_str(&text);
        nonempty_ids.push(annotation.id.clone());
        if combined.len() > max_manifest_bytes {
            return Err(failure(
                "resource limit: combined annotation text exceeds publication ceiling",
            ));
        }
    }
    if !combined.is_empty() {
        append_windows(
            &mut texts,
            colbert,
            plan,
            AnnotationRepresentation::Combined,
            &nonempty_ids,
            &combined,
            None,
            max_manifest_bytes,
        )?;
    }
    if texts.is_empty() {
        return Err(failure("annotation cohort has no canonical source text"));
    }
    if canonical_json_bytes_of(&texts)?.len() > max_manifest_bytes {
        return Err(failure(
            "resource limit: prepared annotation representations exceed publication ceiling",
        ));
    }
    Ok(Some(PreparedProjection {
        plan: plan.clone(),
        texts,
    }))
}

/// Recheck only declared inputs; later independent annotation commits do not invalidate this version.
pub(crate) fn current_inputs(
    conn: &Connection,
    plan: &CohortPlan,
) -> Result<Option<Vec<SemanticAnnotation>>, ApiError> {
    let max_input_bytes = conn.limits().resources.max_json_cell_bytes;
    let max_manifest_bytes = conn.limits().resources.max_manifest_bytes as u64;
    if plan.inputs.len() > conn.limits().resources.max_annotations_per_cohort {
        return Err(failure(
            "resource limit: declared annotation cohort exceeds configured membership limit",
        ));
    }
    let mut inputs = Vec::new();
    let mut retained_bytes = 0_u64;
    let mut statement = conn
        .prepare(INPUT_SQL)
        .map_err(|source| failure(format!("prepare annotation input read: {source}")))?;
    for input in &plan.inputs {
        let row: Option<(i64, Option<String>)> = statement
            .query_row(
                params![
                    input.annotation_id,
                    plan.source_id,
                    plan.parse_id,
                    max_input_bytes
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|source| {
                failure(format!("read annotation {}: {source}", input.annotation_id))
            })?;
        let Some((input_bytes, raw)) = row else {
            return Ok(None);
        };
        let raw = raw.ok_or_else(|| failure(format!("resource limit: annotation {} raw fields or encoded JSON exceed {max_input_bytes} bytes (raw fields: {input_bytes} bytes)", input.annotation_id)))?;
        retained_bytes = retained_bytes.saturating_add(raw.len() as u64);
        if retained_bytes > max_manifest_bytes {
            return Err(failure(
                "resource limit: annotation inputs exceed publication ceiling",
            ));
        }
        let annotation: SemanticAnnotation = serde_json::from_str(&raw).map_err(|source| {
            failure(format!(
                "decode annotation {}: {source}",
                input.annotation_id
            ))
        })?;
        if input_fingerprint(&annotation)? != input.fingerprint {
            return Err(failure(format!(
                "fresh annotation {} differs from its published input fingerprint",
                input.annotation_id
            )));
        }
        // Membership invariant: a declared input's attributed refs intersect the
        // cohort window. The fingerprint covers provenance, so a fresh input
        // that fails this was never a member; the plan is not this window's.
        if !intersects_window(&annotation, &plan.target.fragments) {
            return Err(failure(format!(
                "annotation {} does not intersect cohort window {}",
                input.annotation_id, plan.target.window_id
            )));
        }
        inputs.push(annotation);
    }
    Ok(Some(inputs))
}

/// Archive a fully embedded publication after checking its self-consistent model/source contract.
pub(crate) fn archive(
    store: &ArtifactStore,
    plan: CohortPlan,
    representations: Vec<EmbeddedRepresentation>,
) -> Result<ArtifactRef, ApiError> {
    let projection = AnnotationProjection {
        format_version: FORMAT_VERSION,
        plan,
        representations,
    };
    validate_manifest(&projection)?;
    let bytes = canonical_json_bytes_of(&projection)?;
    if bytes.len() > store.limits().resources.max_manifest_bytes {
        return Err(failure(
            "resource limit: annotation manifest exceeds publication ceiling",
        ));
    }
    store.put_bytes(&bytes)
}

/// Read exactly one bounded cohort, completing integrity verification before returning it.
pub(crate) fn read_manifest(
    store: &ArtifactStore,
    uri: &str,
) -> Result<AnnotationProjection, ApiError> {
    let projection: AnnotationProjection = store.with_verified_reader(
        uri,
        Some(store.limits().resources.max_manifest_bytes as u64),
        |reader| {
            serde_json::from_reader(reader)
                .map_err(|source| failure(format!("decode annotation manifest {uri}: {source}")))
        },
    )?;
    validate_manifest(&projection)?;
    if projection.plan.inputs.len() > store.limits().resources.max_annotations_per_cohort {
        return Err(failure(
            "resource limit: archived annotation cohort exceeds configured membership limit",
        ));
    }
    Ok(projection)
}

/// Projection metadata pins declared inputs; model configuration is captured
/// separately in the manifest. Unit refs carry no text range: the exact
/// fragments are the plan target, archived with the manifest.
pub(crate) fn producer(plan: &CohortPlan) -> Provenance {
    Provenance {
        producer_type: crate::model::ProducerType::System,
        producer_name: INDEX_NAME.to_owned(),
        producer_version: Some(FORMAT_VERSION.to_string()),
        config_hash: Some(plan.input_hash.clone()),
        model_name: None,
        model_version: None,
        prompt_hash: Some(sha256_hex_bytes(FORMAT_POLICY.as_bytes())),
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(
            ordered_unit_ids(&plan.target.fragments)
                .into_iter()
                .map(|unit_id| crate::model::ProvenanceInputRef {
                    object_type: crate::model::ProvenanceObjectType::ContentUnit,
                    id: unit_id,
                    text_range: None,
                })
                .chain(
                    plan.inputs
                        .iter()
                        .map(|input| crate::model::ProvenanceInputRef {
                            object_type: crate::model::ProvenanceObjectType::SemanticAnnotation,
                            id: input.annotation_id.clone(),
                            text_range: None,
                        }),
                )
                .collect(),
        ),
    }
}

/// A published cohort selected inside the caller's SQL snapshot; no vectors are resident here.
#[derive(Debug, Clone)]
pub(crate) struct PublishedCohort {
    pub(crate) projection_id: String,
    pub(crate) cohort_id: String,
    pub(crate) input_hash: String,
    pub(crate) payload_uri: String,
    /// A compact digest retains both decoded lineage lists and complete producer identity.
    pub(crate) lineage_hash: String,
}

/// One bounded row waits only for its adjacent role in the ordered inventory.
struct PublicationRow {
    id: String,
    cohort_id: Option<String>,
    uri: Option<String>,
    producer: Option<String>,
    annotation_ids: Option<String>,
    unit_ids: Option<String>,
    role: String,
}

/// Scan both model roles so an orphan ColBERT half cannot disappear behind a
/// dense-only query. The caller's read snapshot covers this entire ordered scan.
pub(crate) fn published_for_parse(
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
) -> Result<Vec<PublishedCohort>, ApiError> {
    let max_cohorts = conn.limits().resources.max_cohorts_per_parse;
    let mut statement = conn
        .prepare(PUBLICATIONS_SQL)
        .map_err(|source| failure(format!("prepare annotation publications: {source}")))?;
    let rows = statement
        .query_map(
            params![
                source_id,
                parse_id,
                INDEX_NAME,
                max_cohorts * 2 + 1,
                conn.limits().resources.max_json_cell_bytes
            ],
            |row| {
                Ok(PublicationRow {
                    id: row.get(0)?,
                    cohort_id: row.get(1)?,
                    uri: row.get(2)?,
                    producer: row.get(3)?,
                    annotation_ids: row.get(4)?,
                    unit_ids: row.get(5)?,
                    role: row.get(6)?,
                })
            },
        )
        .map_err(|source| failure(format!("read annotation publications: {source}")))?;
    let mut output = Vec::new();
    let mut pending: Option<PublicationRow> = None;
    for row in rows {
        let row =
            row.map_err(|source| failure(format!("decode annotation publication: {source}")))?;
        if output.len() >= max_cohorts {
            return Err(failure(
                "resource limit: annotation publication inventory exceeds its cohort limit",
            ));
        }
        // Dense sorts before ColBERT within a cohort. A second dense row or a
        // ColBERT row without a pending dense row exposes duplicate/torn state.
        match row.role.as_str() {
            "dense_vector" => {
                if let Some(previous) = &pending {
                    return Err(failure(format!(
                        "annotation publication {} is duplicated or has no ColBERT partner",
                        previous.id
                    )));
                }
                pending = Some(row);
            }
            "multi_vector" => {
                let dense = pending.take().ok_or_else(|| {
                    failure(format!(
                        "annotation ColBERT publication {} has no dense partner",
                        row.id
                    ))
                })?;
                output.push(validate_publication_pair(dense, row)?);
            }
            other => {
                return Err(failure(format!(
                    "annotation publication {} has unsupported role {other}",
                    row.id
                )));
            }
        }
    }
    if let Some(row) = pending {
        return Err(failure(format!(
            "annotation dense publication {} has no ColBERT partner",
            row.id
        )));
    }
    Ok(output)
}

/// Require the two rows to agree on both canonical and annotation lineage before
/// exposing their immutable manifest as one usable publication.
fn validate_publication_pair(
    dense: PublicationRow,
    colbert: PublicationRow,
) -> Result<PublishedCohort, ApiError> {
    if dense.cohort_id != colbert.cohort_id
        || dense.uri != colbert.uri
        || dense.producer != colbert.producer
        || dense.annotation_ids != colbert.annotation_ids
        || dense.unit_ids != colbert.unit_ids
    {
        return Err(failure(format!(
            "annotation publications {} and {} disagree on cohort, payload, producer, or input lineage",
            dense.id, colbert.id
        )));
    }
    let cohort_id = dense
        .cohort_id
        .ok_or_else(|| failure("annotation publication lacks a cohort identity"))?;
    let payload_uri = dense
        .uri
        .ok_or_else(|| failure("annotation publication lacks a payload"))?;
    let raw_producer = dense.producer.ok_or_else(|| {
        failure(format!(
            "resource limit: annotation publication {} metadata exceeds configured JSON byte limit",
            dense.id
        ))
    })?;
    let raw_annotations = dense
        .annotation_ids
        .ok_or_else(|| failure("annotation publication lacks annotation lineage"))?;
    let raw_units = dense
        .unit_ids
        .ok_or_else(|| failure("annotation publication lacks canonical-unit lineage"))?;
    let annotation_ids: Vec<String> = serde_json::from_str(&raw_annotations).map_err(|source| {
        failure(format!(
            "annotation publication {} input IDs: {source}",
            dense.id
        ))
    })?;
    let unit_ids: Vec<String> = serde_json::from_str(&raw_units).map_err(|source| {
        failure(format!(
            "annotation publication {} unit IDs: {source}",
            dense.id
        ))
    })?;
    if annotation_ids.is_empty()
        || annotation_ids.iter().collect::<BTreeSet<_>>().len() != annotation_ids.len()
        || unit_ids.is_empty()
        || unit_ids.iter().collect::<BTreeSet<_>>().len() != unit_ids.len()
    {
        return Err(failure(format!(
            "annotation publication {} has invalid annotation/canonical lineage membership",
            dense.id
        )));
    }
    let producer: Provenance = serde_json::from_str(&raw_producer)
        .map_err(|source| failure(format!("decode annotation publication producer: {source}")))?;
    let lineage_hash = canonical_sha256_hex_of(&(&producer, &annotation_ids, &unit_ids))?;
    Ok(PublishedCohort {
        projection_id: dense.id,
        cohort_id,
        payload_uri,
        lineage_hash,
        input_hash: producer
            .config_hash
            .ok_or_else(|| failure("annotation publication lacks an input hash"))?,
    })
}

/// Bind a manifest or freshly discovered plan to the captured pair's exact
/// lineage, retaining only hashes of its potentially large JSON metadata.
pub(crate) fn validate_publication_lineage(
    publication: &PublishedCohort,
    plan: &CohortPlan,
) -> Result<(), ApiError> {
    let annotation_ids: Vec<&str> = plan
        .inputs
        .iter()
        .map(|input| input.annotation_id.as_str())
        .collect();
    let unit_ids = ordered_unit_ids(&plan.target.fragments);
    let expected = canonical_sha256_hex_of(&(producer(plan), annotation_ids, unit_ids))?;
    if publication.cohort_id != plan.cohort_id
        || publication.input_hash != plan.input_hash
        || publication.lineage_hash != expected
    {
        return Err(failure(format!(
            "annotation publication {} plan disagrees with its captured envelope lineage",
            publication.projection_id
        )));
    }
    Ok(())
}

/// Validate manifest identities and source coverage without the embedding
/// provider or the database. Input-membership checks that need annotation
/// bodies run in `current_inputs`.
pub(crate) fn validate_manifest(projection: &AnnotationProjection) -> Result<(), ApiError> {
    let plan = &projection.plan;
    let policy = &plan.construction;
    let valid_policy = projection.format_version == FORMAT_VERSION
        && policy.version == FORMAT_VERSION
        && policy.window_max_tokens > 0
        && policy.context_window_version == CONTEXT_WINDOW_CONSTRUCTION_VERSION;
    if !valid_policy
        || plan.model_identity.is_empty()
        || plan.source_id.is_empty()
        || plan.parse_id.is_empty()
        || plan.inputs.is_empty()
        || plan.target.window_id.is_empty()
        || plan.target.chunk_ids.is_empty()
        || !valid_fragments(&wire_fragments(&plan.target.fragments))
        || plan.cohort_id != cohort_id(&plan.source_id, &plan.parse_id, &plan.target.window_id)?
        || plan.input_hash != plan_hash(plan)?
        || projection.representations.is_empty()
    {
        return Err(failure(
            "annotation manifest identity or input set is invalid",
        ));
    }
    let inputs: BTreeSet<&str> = plan
        .inputs
        .iter()
        .map(|input| input.annotation_id.as_str())
        .collect();
    if inputs.len() != plan.inputs.len() {
        return Err(failure("annotation manifest repeats input IDs"));
    }
    let mut ids = BTreeSet::new();
    let mut source_windows = Vec::new();
    for representation in &projection.representations {
        let input = &representation.input;
        if !ids.insert(&input.id)
            || input.text.is_empty()
            || input.input_end_char <= input.input_start_char
            || input.input_end_char - input.input_start_char != input.text.chars().count()
            || input.id
                != representation_id(
                    plan,
                    input.representation,
                    &input.annotation_ids,
                    &input.text,
                    input.source_excerpt.as_ref(),
                    input.input_start_char,
                    input.input_end_char,
                )?
            || input
                .annotation_ids
                .iter()
                .any(|id| !inputs.contains(id.as_str()))
        {
            return Err(failure(
                "annotation representation identity or lineage is invalid",
            ));
        }
        validate_ref(&representation.dense)?;
        validate_ref(&representation.colbert)?;
        if representation.dense.rows != 1 {
            return Err(failure("dense annotation representation is not one vector"));
        }
        match (&input.source_excerpt, input.representation) {
            (Some(excerpt), AnnotationRepresentation::Source) => {
                if !valid_fragments(&excerpt.fragments)
                    || excerpt.text_hash != sha256_hex_bytes(input.text.as_bytes())
                    || !input.annotation_ids.is_empty()
                {
                    return Err(failure("canonical annotation source window is invalid"));
                }
                source_windows.push(input);
            }
            (None, kind)
                if kind != AnnotationRepresentation::Source && !input.annotation_ids.is_empty() => {
            }
            _ => {
                return Err(failure(
                    "source coordinates and annotation representation kind disagree",
                ));
            }
        }
    }
    // Coverage invariant: the source windows partition the cohort window's
    // canonical text from offset 0 without gap or overlap, their texts compose
    // exactly that text, and their fragments compose exactly the window's
    // fragments. Adjacent same-unit ranges are coalesced on both sides because
    // a ColBERT boundary may split one fragment and a window may already hold a
    // unit split across two member chunks.
    source_windows.sort_by_key(|window| window.input_start_char);
    let mut next = 0;
    let mut canonical = String::new();
    let mut covered = Vec::new();
    for window in &source_windows {
        if window.input_start_char != next {
            return Err(failure("annotation source windows have a gap or overlap"));
        }
        next = window.input_end_char;
        canonical.push_str(&window.text);
        if let Some(excerpt) = &window.source_excerpt {
            covered.extend(excerpt.fragments.iter().cloned());
        }
    }
    if source_windows.is_empty()
        || sha256_hex_bytes(canonical.as_bytes()) != plan.target.text_hash
        || coalesce_fragments(&covered)
            != coalesce_fragments(&wire_fragments(&plan.target.fragments))
    {
        return Err(failure(
            "annotation source windows do not cover their cohort window",
        ));
    }
    Ok(())
}

/// The wire form of grain fragments; the two types are the same record.
fn wire_fragments(fragments: &[Fragment]) -> Vec<SourceFragment> {
    fragments
        .iter()
        .map(|fragment| SourceFragment {
            unit_id: fragment.unit_id.clone(),
            start_char: fragment.start_char,
            end_char: fragment.end_char,
        })
        .collect()
}

/// Every range is nonempty with end exclusive; an empty list covers nothing.
fn valid_fragments(fragments: &[SourceFragment]) -> bool {
    !fragments.is_empty()
        && fragments
            .iter()
            .all(|fragment| fragment.start_char < fragment.end_char)
}

/// Merge adjacent fragments of one unit whose ranges abut, in order, so two
/// fragment lists covering the same ranges compare equal regardless of where
/// either was split.
fn coalesce_fragments(fragments: &[SourceFragment]) -> Vec<SourceFragment> {
    let mut merged: Vec<SourceFragment> = Vec::new();
    for fragment in fragments {
        if let Some(last) = merged.last_mut()
            && last.unit_id == fragment.unit_id
            && last.end_char == fragment.start_char
        {
            last.end_char = fragment.end_char;
        } else {
            merged.push(fragment.clone());
        }
    }
    merged
}

/// The attributed refs of one annotation: each target unit with the ranges its
/// `inputRefs` name on that unit, or `None` when it names the whole unit
/// (records without extraction offsets keep their declared whole-unit target).
fn attributed_refs(annotation: &SemanticAnnotation) -> Vec<(&str, Option<&ProvenanceTextRange>)> {
    let mut refs = Vec::new();
    for unit_id in annotation.target_unit_ids.iter().collect::<BTreeSet<_>>() {
        let before = refs.len();
        refs.extend(
            annotation
                .provenance
                .input_refs
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter(|input| input.id == *unit_id)
                .map(|input| (unit_id.as_str(), input.text_range.as_ref())),
        );
        if refs.len() == before {
            refs.push((unit_id.as_str(), None));
        }
    }
    refs
}

/// Intersection rule: a ref intersects a window when some fragment cites the
/// same unit and, if the ref has a range, the scalar ranges overlap
/// (`start < other.end && other.start < end`); a ref without a range is the
/// whole unit and intersects any fragment of that unit.
fn intersects(fragments: &[Fragment], unit_id: &str, range: Option<&ProvenanceTextRange>) -> bool {
    fragments.iter().any(|fragment| {
        fragment.unit_id == unit_id
            && range.is_none_or(|range| {
                range.start_char < fragment.end_char && fragment.start_char < range.end_char
            })
    })
}

/// Whether every attributed ref names an extraction range; false for records
/// produced before attribution recorded offsets, which target whole units.
pub(crate) fn has_exact_ranges(annotation: &SemanticAnnotation) -> bool {
    attributed_refs(annotation)
        .iter()
        .all(|(_, range)| range.is_some())
}

/// Whether any attributed ref of the annotation intersects the window.
pub(crate) fn intersects_window(annotation: &SemanticAnnotation, fragments: &[Fragment]) -> bool {
    attributed_refs(annotation)
        .into_iter()
        .any(|(unit_id, range)| intersects(fragments, unit_id, range))
}

/// Rebuild the cohort window's canonical text from its member chunk rows in
/// the caller's snapshot and lay out every fragment inside it. Layout is the
/// chunker's: fragments of one member are joined by one tab (a table row's
/// cells), members by one blank line. The rebuilt fragments must equal the
/// plan's and the text must hash to the plan's, or the window has changed.
fn window_text(conn: &Connection, plan: &CohortPlan) -> Result<WindowText, ApiError> {
    let max_source_bytes = conn.limits().resources.max_source_body_bytes;
    let mut statement = conn
        .prepare(CHUNK_SQL)
        .map_err(|source| failure(format!("prepare cohort window member read: {source}")))?;
    let mut canonical = String::new();
    let mut spans = Vec::new();
    for chunk_id in &plan.target.chunk_ids {
        let (fragments_json, text): (String, Option<String>) = statement
            .query_row(
                params![chunk_id, plan.source_id, plan.parse_id, max_source_bytes],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|source| {
                failure(format!(
                    "read cohort window member {chunk_id} in {}: {source}",
                    plan.parse_id
                ))
            })?;
        let text = text.ok_or_else(|| {
            failure(format!(
                "resource limit: cohort window member {chunk_id} exceeds {max_source_bytes} bytes"
            ))
        })?;
        let fragments: Vec<Fragment> = serde_json::from_str(&fragments_json)
            .map_err(|source| failure(format!("decode fragments of {chunk_id}: {source}")))?;
        if !valid_fragments(&wire_fragments(&fragments)) {
            return Err(failure(format!("member {chunk_id} has no valid fragments")));
        }
        let base = if canonical.is_empty() {
            0
        } else {
            canonical.chars().count() + 2
        };
        let mut offset = base;
        for (index, fragment) in fragments.into_iter().enumerate() {
            if index > 0 {
                offset += 1;
            }
            let end_char = offset + (fragment.end_char - fragment.start_char);
            spans.push(FragmentSpan {
                fragment,
                start_char: offset,
                end_char,
            });
            offset = end_char;
        }
        if offset - base != text.chars().count() {
            return Err(failure(format!(
                "member {chunk_id} text length disagrees with its fragment layout"
            )));
        }
        canonical = if canonical.is_empty() {
            text
        } else {
            join_text(&canonical, &text)
        };
        if canonical.len() > max_source_bytes {
            return Err(failure(format!(
                "resource limit: cohort window {} exceeds {max_source_bytes} bytes",
                plan.target.window_id
            )));
        }
    }
    let rebuilt: Vec<&Fragment> = spans.iter().map(|span| &span.fragment).collect();
    if rebuilt.len() != plan.target.fragments.len()
        || rebuilt
            .iter()
            .zip(&plan.target.fragments)
            .any(|(left, right)| *left != right)
        || sha256_hex_bytes(canonical.as_bytes()) != plan.target.text_hash
    {
        return Err(failure(format!(
            "cohort window {} differs from its member chunks",
            plan.target.window_id
        )));
    }
    Ok(WindowText { canonical, spans })
}

/// The fragments a ColBERT partition `[start, end)` of the canonical text
/// displays, clipped to the partition. Whitespace of a join that the boundary
/// cut through belongs to no fragment; the partition text still hashes whole.
fn clip_spans(
    spans: &[FragmentSpan],
    start: usize,
    end: usize,
) -> Result<Vec<SourceFragment>, ApiError> {
    let mut fragments = Vec::new();
    for span in spans {
        let from = span.start_char.max(start);
        let to = span.end_char.min(end);
        if from < to {
            fragments.push(SourceFragment {
                unit_id: span.fragment.unit_id.clone(),
                start_char: span.fragment.start_char + (from - span.start_char),
                end_char: span.fragment.start_char + (to - span.start_char),
            });
        }
    }
    if fragments.is_empty() {
        return Err(failure("source window displays no fragment text"));
    }
    Ok(fragments)
}

/// Translate Unicode-scalar coordinates to UTF-8 boundaries without altering source bytes.
pub(crate) fn slice_chars(text: &str, start: usize, end: usize) -> Result<&str, ApiError> {
    if start >= end {
        return Err(failure("source excerpt has an empty or reversed range"));
    }
    let mut boundaries = text
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()));
    let start_byte = boundaries
        .nth(start)
        .ok_or_else(|| failure("source excerpt starts beyond its unit"))?;
    let end_byte = boundaries
        .nth(end - start - 1)
        .ok_or_else(|| failure("source excerpt ends beyond its unit"))?;
    Ok(&text[start_byte..end_byte])
}

/// Frame each stored annotation faithfully; empty coverage records do not become search text.
pub(crate) fn render_annotation(
    annotation: &SemanticAnnotation,
) -> Result<Option<String>, ApiError> {
    if annotation.body.as_array().is_some_and(Vec::is_empty) {
        return Ok(None);
    }
    let field = |name: &str| {
        annotation
            .body
            .get(name)
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| {
                failure(format!(
                    "annotation {} has no nonempty {name}",
                    annotation.id
                ))
            })
    };
    let text = match annotation.annotation_type {
        SemanticAnnotationType::Entity => {
            format!("Entity: {}\nType: {}", field("name")?, field("entityType")?)
        }
        SemanticAnnotationType::Relation => format!(
            "Subject: {}\nRelationship: {}\nObject: {}",
            field("subject")?,
            field("predicate")?,
            field("object")?
        ),
        SemanticAnnotationType::Summary => format!("Summary: {}", field("text")?),
        _ => return Ok(None),
    };
    Ok(Some(text))
}

/// All annotation producers share this conversion; deferred annotation kinds remain unindexed.
pub(crate) fn representation_kind(
    kind: SemanticAnnotationType,
) -> Option<AnnotationRepresentation> {
    match kind {
        SemanticAnnotationType::Entity => Some(AnnotationRepresentation::Entity),
        SemanticAnnotationType::Relation => Some(AnnotationRepresentation::Relation),
        SemanticAnnotationType::Summary => Some(AnnotationRepresentation::Summary),
        _ => None,
    }
}

/// Split model inputs independently of corpus units and preserve every source coordinate.
// Explicit arguments keep canonical offsets, model framing, and current admission
// separate from the immutable construction policy archived in the plan.
#[allow(clippy::too_many_arguments)]
fn append_windows(
    texts: &mut Vec<RepresentationText>,
    colbert: &ColbertBackend,
    plan: &CohortPlan,
    representation: AnnotationRepresentation,
    annotation_ids: &[String],
    text: &str,
    source_spans: Option<&[FragmentSpan]>,
    max_manifest_bytes: usize,
) -> Result<(), ApiError> {
    let mut retained_bytes: usize = texts.iter().map(retained_text_bytes).sum();
    // Plans carry their own construction limit, so changed settings require new identities.
    let window_tokens = plan.construction.window_max_tokens as usize;
    for window in colbert.document_windows(text, window_tokens)? {
        // Bound retained input records before cloning repeated combined-input IDs.
        let added = window
            .text
            .len()
            .saturating_add(
                annotation_ids
                    .iter()
                    .map(|id| id.len() + std::mem::size_of::<String>())
                    .sum::<usize>(),
            )
            .saturating_add(512);
        retained_bytes = retained_bytes.saturating_add(added);
        if retained_bytes > max_manifest_bytes {
            return Err(failure(format!(
                "resource limit: {} window working set exceeds publication ceiling",
                representation.label()
            )));
        }
        let source_excerpt = match source_spans {
            Some(spans) => Some(SourceExcerpt {
                fragments: clip_spans(spans, window.start_char, window.end_char)?,
                text_hash: sha256_hex_bytes(window.text.as_bytes()),
            }),
            None => None,
        };
        let id = representation_id(
            plan,
            representation,
            annotation_ids,
            &window.text,
            source_excerpt.as_ref(),
            window.start_char,
            window.end_char,
        )?;
        texts.push(RepresentationText {
            id,
            representation,
            annotation_ids: annotation_ids.to_vec(),
            text: window.text,
            input_start_char: window.start_char,
            input_end_char: window.end_char,
            source_excerpt,
        });
    }
    Ok(())
}

/// Account for owned variable-size buffers while constructing one bounded publication.
fn retained_text_bytes(input: &RepresentationText) -> usize {
    input
        .text
        .len()
        .saturating_add(
            input
                .annotation_ids
                .iter()
                .map(|id| id.len() + std::mem::size_of::<String>())
                .sum::<usize>(),
        )
        .saturating_add(512)
}

/// Model identity, rendering policy, and the window's membership participate in
/// freshness: a rebuilt section plane that reuses a window id with different
/// members must republish, so the target is hashed here, not only its id.
fn plan_hash(plan: &CohortPlan) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(
        FORMAT_POLICY,
        &plan.construction,
        &plan.model_identity,
        &plan.cohort_id,
        &plan.target,
        &plan.inputs,
    ))
}

/// Cohort identity is the window's; annotation type is excluded so completed
/// types enrich one shared window.
fn cohort_id(source_id: &str, parse_id: &str, window_id: &str) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(INDEX_NAME, source_id, parse_id, window_id))
}

/// Exact targeting text bytes remain identity-bearing even when Unicode forms differ.
fn representation_id(
    plan: &CohortPlan,
    kind: AnnotationRepresentation,
    ids: &[String],
    text: &str,
    excerpt: Option<&SourceExcerpt>,
    start: usize,
    end: usize,
) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(
        &plan.construction,
        &plan.cohort_id,
        &plan.model_identity,
        kind,
        ids,
        sha256_hex_bytes(text.as_bytes()),
        excerpt,
        start,
        end,
    ))
}

/// Detect changes to a declared annotation input without retaining its body during discovery.
pub(crate) fn input_fingerprint(annotation: &SemanticAnnotation) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(
        &annotation.id,
        annotation.annotation_type,
        &annotation.target_unit_ids,
        &annotation.body,
        &annotation.provenance,
    ))
}

/// Attribute internal validation failures through the service's storage error contract.
fn failure(message: impl Into<String>) -> ApiError {
    ApiError::StorageOperation {
        message: message.into(),
    }
}
