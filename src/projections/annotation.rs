//! Annotation-derived retrieval records retain exact source identity and immutable model inputs.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    annotations::store,
    artifact_store::{ArtifactRef, ArtifactStore},
    assembly::evidence::evidence_text,
    canonical::{canonical_json_bytes_of, canonical_sha256_hex_of, sha256_hex_bytes},
    error::ApiError,
    inference::ColbertBackend,
    model::provenance::ProvenanceTextRange,
    model::{ContentType, Provenance, SemanticAnnotation, SemanticAnnotationType},
    query::provenance::{AnnotationRepresentation, SourceExcerpt},
};

use super::annotation_io::{EmbeddingRef, MAX_MANIFEST_BYTES, validate_ref};

pub(crate) const INDEX_NAME: &str = "annotation_retrieval_v1";
pub(crate) const PAYLOAD_TYPE: &str = "annotation_retrieval_manifest";
pub(crate) const VECTOR_PAYLOAD_TYPE: &str = "annotation_embedding_blob";
const FORMAT_VERSION: u32 = 1;
const MAX_SOURCE_BYTES: usize = 1_048_576;
const MAX_INPUTS: usize = 4096;
const MAX_COHORTS: usize = 100_000;
const FORMAT_POLICY: &str = "annotation_retrieval_v1;entity_name_type;relation_triple;summary_text;combined_by_excerpt;complete_colbert_windows;unicode_scalar_ranges;canonical_source_separate";
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
const SOURCE_SQL: &str = "SELECT content_type,
 CASE WHEN length(CAST(body_json AS BLOB)) <= ?4 THEN body_json END
 FROM content_units WHERE id = ?1 AND source_id = ?2 AND parse_id = ?3";

/// Small discovery identity; bodies are fetched only for a cohort requiring publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InputSignature {
    pub(crate) annotation_id: String,
    pub(crate) fingerprint: String,
    pub(crate) representation: AnnotationRepresentation,
}

/// One canonical target. Missing ranges explicitly preserve historical whole-unit targeting.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InputTarget {
    pub(crate) unit_id: String,
    pub(crate) range: Option<ProvenanceTextRange>,
}

/// The complete declared input set for one excerpt publication, including empty markers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CohortPlan {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) cohort_id: String,
    pub(crate) input_hash: String,
    pub(crate) model_identity: String,
    pub(crate) target: InputTarget,
    pub(crate) inputs: Vec<InputSignature>,
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

/// Discover input identities without retaining annotation bodies across a source scan.
pub(crate) fn plan_for_parse(
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
    model_identity: &str,
) -> Result<Vec<CohortPlan>, ApiError> {
    let mut cohorts: BTreeMap<String, CohortPlan> = BTreeMap::new();
    store::visit_fresh_for_active_parse(conn, source_id, Some(MAX_MANIFEST_BYTES), |annotation| {
        if annotation.parse_id != parse_id {
            return Err(failure("active parse changed during annotation discovery"));
        }
        let Some(representation) = representation_kind(annotation.annotation_type) else {
            return Ok(());
        };
        let fingerprint = input_fingerprint(&annotation)?;
        for unit_id in annotation.target_unit_ids.iter().collect::<BTreeSet<_>>() {
            let ranges: Vec<Option<ProvenanceTextRange>> = annotation
                .provenance
                .input_refs
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter(|input| input.id == *unit_id)
                .map(|input| input.text_range.clone())
                .collect();
            // Old producers did not record extraction offsets. Keep their declared
            // whole-unit target instead of pretending the annotation names a precise slice.
            let ranges = if ranges.is_empty() {
                vec![None]
            } else {
                ranges
            };
            for range in ranges {
                let target = InputTarget {
                    unit_id: unit_id.clone(),
                    range,
                };
                let cohort_id = cohort_id(source_id, parse_id, &target)?;
                if !cohorts.contains_key(&cohort_id) && cohorts.len() >= MAX_COHORTS {
                    return Err(failure(format!(
                        "parse {parse_id} exceeds {MAX_COHORTS} annotation cohorts"
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
                        target,
                        inputs: Vec::new(),
                    });
                if !cohort
                    .inputs
                    .iter()
                    .any(|input| input.annotation_id == annotation.id)
                {
                    if cohort.inputs.len() >= MAX_INPUTS {
                        return Err(failure("annotation cohort input ceiling exceeded"));
                    }
                    cohort.inputs.push(InputSignature {
                        annotation_id: annotation.id.clone(),
                        fingerprint: fingerprint.clone(),
                        representation,
                    });
                }
            }
        }
        Ok(())
    })?;
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
    let Some(annotations) = current_inputs(conn, plan)? else {
        return Ok(None);
    };
    let source = source_text(conn, &plan.source_id, &plan.parse_id, &plan.target.unit_id)?;
    let (source_slice, start_char) = match &plan.target.range {
        Some(range) => {
            let text = slice_chars(&source, range.start_char, range.end_char)?;
            if sha256_hex_bytes(text.as_bytes()) != range.text_hash {
                return Err(failure(
                    "annotation source range hash differs from canonical text",
                ));
            }
            (text, range.start_char)
        }
        None => (source.as_str(), 0),
    };
    let mut texts = Vec::new();
    append_windows(
        &mut texts,
        colbert,
        plan,
        AnnotationRepresentation::Source,
        &[],
        source_slice,
        Some(start_char),
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
        )?;
        if !combined.is_empty() {
            combined.push_str("\n\n");
        }
        combined.push_str(&text);
        nonempty_ids.push(annotation.id.clone());
        if combined.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(failure(
                "combined annotation text exceeds publication ceiling",
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
        )?;
    }
    if texts.is_empty() {
        return Err(failure("annotation cohort has no canonical source text"));
    }
    if canonical_json_bytes_of(&texts)?.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(failure(
            "prepared annotation representations exceed publication ceiling",
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
                    MAX_MANIFEST_BYTES
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
        let raw = raw.ok_or_else(|| failure(format!("annotation {} raw fields or encoded JSON exceed {MAX_MANIFEST_BYTES} bytes (raw fields: {input_bytes} bytes)", input.annotation_id)))?;
        retained_bytes = retained_bytes.saturating_add(raw.len() as u64);
        if retained_bytes > MAX_MANIFEST_BYTES {
            return Err(failure("annotation inputs exceed publication ceiling"));
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
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(failure("annotation manifest exceeds publication ceiling"));
    }
    store.put_bytes(&bytes)
}

/// Read exactly one bounded cohort, completing integrity verification before returning it.
pub(crate) fn read_manifest(
    store: &ArtifactStore,
    uri: &str,
) -> Result<AnnotationProjection, ApiError> {
    let projection: AnnotationProjection =
        store.with_verified_reader(uri, Some(MAX_MANIFEST_BYTES), |reader| {
            serde_json::from_reader(reader)
                .map_err(|source| failure(format!("decode annotation manifest {uri}: {source}")))
        })?;
    validate_manifest(&projection)?;
    Ok(projection)
}

/// Projection metadata pins declared inputs; model configuration is captured separately in the manifest.
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
            std::iter::once(crate::model::ProvenanceInputRef {
                object_type: crate::model::ProvenanceObjectType::ContentUnit,
                id: plan.target.unit_id.clone(),
                text_range: plan.target.range.clone(),
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
    let mut statement = conn
        .prepare(PUBLICATIONS_SQL)
        .map_err(|source| failure(format!("prepare annotation publications: {source}")))?;
    let rows = statement
        .query_map(
            params![
                source_id,
                parse_id,
                INDEX_NAME,
                MAX_COHORTS * 2 + 1,
                MAX_MANIFEST_BYTES
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
        if output.len() >= MAX_COHORTS {
            return Err(failure(
                "annotation publication inventory exceeds its cohort limit",
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
            "annotation publication {} metadata exceeds {MAX_MANIFEST_BYTES} bytes",
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
        || unit_ids.len() != 1
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
    let unit_ids = [plan.target.unit_id.as_str()];
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

/// Validate manifest identities without requiring the embedding provider to be available.
pub(crate) fn validate_manifest(projection: &AnnotationProjection) -> Result<(), ApiError> {
    let plan = &projection.plan;
    if projection.format_version != FORMAT_VERSION
        || plan.model_identity.is_empty()
        || plan.source_id.is_empty()
        || plan.parse_id.is_empty()
        || plan.inputs.is_empty()
        || plan.inputs.len() > MAX_INPUTS
        || plan.cohort_id != cohort_id(&plan.source_id, &plan.parse_id, &plan.target)?
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
                if excerpt.unit_id != plan.target.unit_id
                    || excerpt.end_char <= excerpt.start_char
                    || excerpt.end_char - excerpt.start_char != input.text.chars().count()
                    || excerpt.text_hash != sha256_hex_bytes(input.text.as_bytes())
                    || !input.annotation_ids.is_empty()
                {
                    return Err(failure("canonical annotation source window is invalid"));
                }
                source_windows.push(excerpt);
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
    source_windows.sort_by_key(|window| window.start_char);
    let mut next = plan
        .target
        .range
        .as_ref()
        .map_or(0, |range| range.start_char);
    for window in &source_windows {
        if window.start_char != next {
            return Err(failure("annotation source windows have a gap or overlap"));
        }
        next = window.end_char;
    }
    if source_windows.is_empty()
        || plan
            .target
            .range
            .as_ref()
            .is_some_and(|range| next != range.end_char)
    {
        return Err(failure(
            "annotation source windows do not cover their declared target",
        ));
    }
    Ok(())
}

/// Resolve the canonical evidence field and reject oversized or foreign source data.
pub(crate) fn source_text(
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
    unit_id: &str,
) -> Result<String, ApiError> {
    let (kind, body): (String, Option<String>) = conn
        .query_row(
            SOURCE_SQL,
            params![unit_id, source_id, parse_id, MAX_SOURCE_BYTES],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|source| {
            failure(format!(
                "read canonical source {unit_id} in {parse_id}: {source}"
            ))
        })?;
    let body = body.ok_or_else(|| {
        failure(format!(
            "canonical source {unit_id} exceeds {MAX_SOURCE_BYTES} bytes"
        ))
    })?;
    let kind: ContentType = serde_json::from_value(Value::String(kind))
        .map_err(|source| failure(format!("decode source type {unit_id}: {source}")))?;
    let body: Value = serde_json::from_str(&body)
        .map_err(|source| failure(format!("decode source body {unit_id}: {source}")))?;
    evidence_text(kind, &body)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| {
            failure(format!(
                "annotation target {unit_id} has no canonical evidence text"
            ))
        })
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
fn append_windows(
    texts: &mut Vec<RepresentationText>,
    colbert: &ColbertBackend,
    plan: &CohortPlan,
    representation: AnnotationRepresentation,
    annotation_ids: &[String],
    text: &str,
    source_start: Option<usize>,
) -> Result<(), ApiError> {
    let mut retained_bytes: usize = texts.iter().map(retained_text_bytes).sum();
    for window in colbert.document_windows(text, super::MAX_UNIT_TOKENS as usize)? {
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
        if retained_bytes as u64 > MAX_MANIFEST_BYTES {
            return Err(failure(format!(
                "{} window working set exceeds publication ceiling",
                representation.label()
            )));
        }
        let source_excerpt = source_start.map(|start| SourceExcerpt {
            unit_id: plan.target.unit_id.clone(),
            start_char: start + window.start_char,
            end_char: start + window.end_char,
            text_hash: sha256_hex_bytes(window.text.as_bytes()),
        });
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

/// Model identity and rendering policy participate in freshness without changing annotations.
fn plan_hash(plan: &CohortPlan) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(
        FORMAT_POLICY,
        &plan.model_identity,
        &plan.cohort_id,
        &plan.inputs,
    ))
}

/// Cohort identity excludes annotation type so completed types can enrich one shared excerpt.
fn cohort_id(source_id: &str, parse_id: &str, target: &InputTarget) -> Result<String, ApiError> {
    canonical_sha256_hex_of(&(INDEX_NAME, source_id, parse_id, target))
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
