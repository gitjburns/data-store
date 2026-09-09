//! Section context vectors are immutable targeting artifacts, never canonical evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::artifact_store::ArtifactStore;
use crate::assembly::evidence::evidence_text;
use crate::canonical::canonical_json_bytes_of;
use crate::config::{DenseBackendKind, DenseModelConfig};
use crate::error::ApiError;
use crate::inference::DenseEmbeddingBackend;
use crate::model::{ContentType, ProducerType, Provenance};
use crate::primitives::sha256_hex;
use crate::primitives::validate::validate_vector;
use crate::sections::read_section;
use crate::state::ModelCallPermit;

use super::envelope::{self, NewProjection, ProjectionType};

/// Distinguishes archived section vectors from the fine passage dense plane.
pub(crate) const SECTION_DENSE_INDEX_NAME: &str = "section_dense_v1";
const MAX_WINDOW_TOKENS: usize = 2048;
const MAX_PARSE_UNITS: usize = 100_000;
const MAX_CELL_BYTES: usize = 1_048_576;
const MAX_ENVELOPE_BYTES: usize = 16_777_216;
const WINDOW_POLICY: &str = "section_dense_v1;nearest_logical_section;canonical_sequence;heading_path_slash;paragraph_separator_double_newline;utf8_prefix_split;max_tokens=2048;special_tokens=true;no_truncation;no_padding;exclude_header_footer;retain_short_text";
const UNITS_SQL: &str = "
SELECT id, source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?2 THEN body_json END
FROM content_units WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id LIMIT ?3";
const ENVELOPE_SQL: &str = "
SELECT source_id, projection_type, freshness_status,
       CASE WHEN length(CAST(payload_uri AS BLOB)) <= ?3 THEN payload_uri END,
       CASE WHEN length(CAST(input_unit_ids_json AS BLOB)) <= ?3 THEN input_unit_ids_json END,
       deleted_at
FROM retrieval_projections WHERE parse_id = ?1 AND index_name = ?2 LIMIT 2";

/// Self-contained input, model identity, and vectors needed to restore without inference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionDensePlane {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) dimension: usize,
    pub(crate) policy_hash: String,
    pub(crate) tokenizer_hash: String,
    pub(crate) model_backend: String,
    pub(crate) model_name: String,
    pub(crate) model_pooling: String,
    pub(crate) windows: Vec<SectionDenseWindow>,
}

/// Exact heading-prefixed model input and its canonical targets, bounded at build time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionDenseWindow {
    pub(crate) window_id: String,
    pub(crate) section_id: Option<String>,
    pub(crate) section_path: Vec<String>,
    pub(crate) input_unit_ids: Vec<String>,
    pub(crate) targeting_text: String,
    pub(crate) token_count: usize,
    pub(crate) vector: Vec<f32>,
    pub(crate) norm: f32,
    /// UTF-8 byte ranges permit lossless verification when an oversized leaf is split.
    pub(crate) fragments: Vec<SectionDenseFragment>,
}

/// A canonical leaf excerpt; offsets address evidence_text, not the raw body JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionDenseFragment {
    pub(crate) unit_id: String,
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
}

/// Canonical leaves retain source order while section grouping owns targeting context.
struct Leaf {
    id: String,
    text: String,
    section_id: Option<String>,
    section_path: Vec<String>,
}

/// Archive the complete section plane before making its envelope fresh. The caller
/// owns the transaction, projection replacement, and local inference gate lifetime.
// These explicit inputs preserve storage, model, and caller-owned gate boundaries.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_section_dense(
    tx: &Transaction<'_>,
    store: &ArtifactStore,
    source_id: &str,
    parse_id: &str,
    backend: &DenseEmbeddingBackend,
    config: &DenseModelConfig,
    tokenizer: &Tokenizer,
    permit: Option<&ModelCallPermit>,
) -> Result<usize, ApiError> {
    let started = Instant::now();
    info!(
        event = "section_dense.build.started",
        source_id,
        parse_id,
        max_tokens = MAX_WINDOW_TOKENS,
        "building section context vectors"
    );
    let result = (|| {
        if backend.uses_local_model_gate() != permit.is_some() {
            return Err(failure(format!(
                "section dense model gate does not match backend for {parse_id}"
            )));
        }
        // Runtime tokenizers may truncate at the ColBERT model limit. This independent
        // CPU-only copy counts the full section input and never silently clips it.
        let mut counter = tokenizer.clone();
        counter.with_truncation(None).map_err(|source| {
            failure(format!(
                "disable section tokenizer truncation for {parse_id}: {source}"
            ))
        })?;
        counter.with_padding(None);
        let tokenizer_json = counter
            .to_string(false)
            .map_err(|source| failure(format!("serialize section tokenizer identity: {source}")))?;
        let tokenizer_value: Value = serde_json::from_str(&tokenizer_json)
            .map_err(|source| failure(format!("decode section tokenizer identity: {source}")))?;
        let leaves = read_leaves(tx, source_id, parse_id)?;
        let windows = build_windows(&leaves, parse_id, &counter)?;
        let (model_backend, model_name) = match config.backend {
            DenseBackendKind::Local => ("local", config.local_path()?.display().to_string()),
            DenseBackendKind::Http => (
                "http",
                config.model.clone().ok_or_else(|| {
                    failure("section dense HTTP model name is missing".to_owned())
                })?,
            ),
        };
        let mut plane = SectionDensePlane {
            source_id: source_id.to_owned(),
            parse_id: parse_id.to_owned(),
            dimension: config.dimension as usize,
            policy_hash: policy_hash(),
            tokenizer_hash: sha256_hex(&canonical_json_bytes_of(&tokenizer_value)?),
            model_backend: model_backend.to_owned(),
            model_name,
            model_pooling: config.pooling.clone(),
            windows,
        };
        info!(event = "section_dense.windows.ready", source_id, parse_id, unit_count = leaves.len(), window_count = plane.windows.len(), policy_hash = %plane.policy_hash, elapsed_ms = started.elapsed().as_millis() as u64, "section inputs constructed");
        let projection = envelope::insert_building(tx, &new_projection(&plane))?;
        let built = (|| {
            info!(
                event = "section_dense.embedding.started",
                source_id,
                parse_id,
                window_count = plane.windows.len(),
                "embedding section context inputs"
            );
            let texts: Vec<&str> = plane
                .windows
                .iter()
                .map(|window| window.targeting_text.as_str())
                .collect();
            let vectors = super::dense::embed_texts(backend, &texts, parse_id)?;
            if vectors.len() != plane.windows.len() {
                return Err(failure(format!(
                    "section embedding count for {parse_id}: {} returned for {} windows",
                    vectors.len(),
                    plane.windows.len()
                )));
            }
            for (window, vector) in plane.windows.iter_mut().zip(vectors) {
                let checked = validate_vector(window.window_id.clone(), vector, plane.dimension)
                    .map_err(failure)?;
                window.vector = checked.vector;
                window.norm = checked.norm;
            }
            info!(
                event = "section_dense.embedding.completed",
                source_id,
                parse_id,
                window_count = plane.windows.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "section vectors validated"
            );
            validate_payload(&plane, source_id, parse_id, plane.dimension)?;
            validate_leaves(&leaves, &plane)?;
            let bytes = canonical_json_bytes_of(&plane)?;
            info!(
                event = "section_dense.archive.started",
                source_id,
                parse_id,
                bytes = bytes.len(),
                "archiving section plane"
            );
            let artifact = store.put_bytes(&bytes)?;
            envelope::complete_fresh(tx, &projection, Some(&artifact.uri))?;
            info!(event = "section_dense.build.completed", source_id, parse_id, window_count = plane.windows.len(), artifact_hash = %artifact.hash, persistence = "pending_commit", elapsed_ms = started.elapsed().as_millis() as u64, "section plane archived and envelope fresh pending owner commit");
            Ok(plane.windows.len())
        })();
        if let Err(source) = &built {
            envelope::mark_failed(tx, &projection, &source.to_string()).map_err(|marker| {
                failure(format!(
                    "{source}; section envelope failure marker: {marker}"
                ))
            })?;
        }
        built
    })();
    result.inspect_err(|source| error!(event = "section_dense.build.failed", source_id, parse_id, error = %source, elapsed_ms = started.elapsed().as_millis() as u64, "section dense build failed"))
}

/// Return None only for a legacy parse without a section envelope; damaged or
/// unfinished planes must never masquerade as an empty, valid representation.
pub(crate) fn load_section_dense(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    expected_dimension: usize,
) -> Result<Option<SectionDensePlane>, ApiError> {
    let mut statement = conn
        .prepare(ENVELOPE_SQL)
        .map_err(|source| failure(format!("prepare section envelope for {parse_id}: {source}")))?;
    let envelopes = statement
        .query_map(
            params![parse_id, SECTION_DENSE_INDEX_NAME, MAX_ENVELOPE_BYTES],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .map_err(|source| failure(format!("read section envelope for {parse_id}: {source}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| failure(format!("decode section envelope for {parse_id}: {source}")))?;
    if envelopes.len() > 1 {
        return Err(failure(format!(
            "duplicate section dense envelopes for {parse_id}"
        )));
    }
    let Some((source_id, kind, status, uri, inputs, deleted_at)) = envelopes.into_iter().next()
    else {
        return Ok(None);
    };
    if kind != "dense_vector" || status != "fresh" || deleted_at.is_some() {
        return Err(failure(format!(
            "section envelope for {parse_id} is incomplete or incompatible: type={kind}, status={status}"
        )));
    }
    let source_id = source_id
        .ok_or_else(|| failure(format!("section envelope source missing for {parse_id}")))?;
    let uri = uri.ok_or_else(|| {
        failure(format!(
            "section payload URI missing or exceeds {MAX_ENVELOPE_BYTES} bytes for {parse_id}"
        ))
    })?;
    let plane = read_section_payload(store, &uri, &source_id, parse_id, expected_dimension)?;
    let inputs = inputs.ok_or_else(|| {
        failure(format!(
            "section envelope inputs missing or exceed {MAX_ENVELOPE_BYTES} bytes for {parse_id}"
        ))
    })?;
    let inputs: Vec<String> = serde_json::from_str(&inputs)
        .map_err(|source| failure(format!("section envelope inputs for {parse_id}: {source}")))?;
    if inputs != plane_input_ids(&plane) {
        return Err(failure(format!(
            "section envelope input membership differs from payload for {parse_id}"
        )));
    }
    validate_plane(conn, &plane)?;
    Ok(Some(plane))
}

/// Verify immutable artifact integrity and shape without requiring model inference.
/// Snapshot readers additionally call validate_plane against captured canonical rows.
pub(crate) fn read_section_payload(
    store: &ArtifactStore,
    uri: &str,
    source_id: &str,
    parse_id: &str,
    expected_dimension: usize,
) -> Result<SectionDensePlane, ApiError> {
    let bytes = store.get_bytes_by_uri(uri)?;
    let plane: SectionDensePlane = serde_json::from_slice(&bytes).map_err(|source| {
        failure(format!(
            "decode section payload {uri} for {parse_id}: {source}"
        ))
    })?;
    validate_payload(&plane, source_id, parse_id, expected_dimension)?;
    Ok(plane)
}

/// Confirm every eligible leaf is represented exactly once, possibly split across
/// windows, and each stored input is reconstructible from the canonical parse.
pub(crate) fn validate_plane(conn: &Connection, plane: &SectionDensePlane) -> Result<(), ApiError> {
    let leaves = read_leaves(conn, &plane.source_id, &plane.parse_id)?;
    validate_leaves(&leaves, plane)
}

/// Verify snapshot section inputs against archived SQL rows before any restore
/// writes. The same completeness checker is used after canonical rows are imported.
pub(crate) fn validate_archived_plane(
    units: &[Value],
    relationships: &[Value],
    plane: &SectionDensePlane,
) -> Result<(), ApiError> {
    let mut nodes = BTreeMap::new();
    let mut order = Vec::new();
    for unit in units.iter().filter(|unit| {
        unit.get("parse_id").and_then(Value::as_str) == Some(plane.parse_id.as_str())
    }) {
        let id = archived_string(unit, "id")?;
        if archived_string(unit, "source_id")? != plane.source_id {
            return Err(failure(format!(
                "archived section leaf {id} has a different source from {}",
                plane.source_id
            )));
        }
        let kind: ContentType = serde_json::from_value(Value::String(
            archived_string(unit, "content_type")?.to_owned(),
        ))
        .map_err(|source| failure(format!("archived section leaf type {id}: {source}")))?;
        let raw_body = archived_string(unit, "body_json")?;
        if raw_body.len() > MAX_CELL_BYTES {
            return Err(failure(format!(
                "archived section unit {id} exceeds {MAX_CELL_BYTES} body bytes"
            )));
        }
        let body: Value = serde_json::from_str(raw_body)
            .map_err(|source| failure(format!("archived section unit body {id}: {source}")))?;
        let sequence = match unit.get("sequence_index") {
            Some(Value::Null) | None => None,
            Some(value) => Some(
                value
                    .as_u64()
                    .ok_or_else(|| failure(format!("invalid archived sequence index for {id}")))?,
            ),
        };
        if nodes.insert(id, (kind, body)).is_some() {
            return Err(failure(format!("duplicate archived section unit {id}")));
        }
        order.push((sequence.is_none(), sequence, id));
    }
    if nodes.len() > MAX_PARSE_UNITS {
        return Err(failure(format!(
            "archived section parse {} exceeds {MAX_PARSE_UNITS} units",
            plane.parse_id
        )));
    }
    order.sort_unstable();
    let mut parents: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for relation in relationships.iter().filter(|relation| {
        relation.get("parse_id").and_then(Value::as_str) == Some(plane.parse_id.as_str())
    }) {
        if !matches!(
            archived_string(relation, "relationship_type")?,
            "logically_contains" | "contains"
        ) {
            continue;
        }
        let from = archived_string(relation, "from_unit_id")?;
        let to = archived_string(relation, "to_unit_id")?;
        if nodes
            .get(from)
            .is_some_and(|(kind, _)| *kind != ContentType::Page)
        {
            parents.entry(to).or_default().insert(from);
        }
    }
    let mut leaves = Vec::new();
    for (_, _, id) in order {
        let (kind, body) = nodes
            .get(id)
            .ok_or_else(|| failure(format!("archived section unit {id} disappeared")))?;
        let Some(text) = eligible_text(*kind, body) else {
            continue;
        };
        let (section_id, section_path) = archived_section(id, &plane.parse_id, &nodes, &parents)?;
        leaves.push(Leaf {
            id: id.to_owned(),
            text,
            section_id,
            section_path,
        });
    }
    validate_leaves(&leaves, plane)
}

/// Resolve the archived equivalent of read_section without opening a scratch DB.
/// Parent sets mirror SQL DISTINCT and omit physical page containment identically.
fn archived_section(
    unit_id: &str,
    parse_id: &str,
    nodes: &BTreeMap<&str, (ContentType, Value)>,
    parents: &BTreeMap<&str, BTreeSet<&str>>,
) -> Result<(Option<String>, Vec<String>), ApiError> {
    let mut current = unit_id;
    let mut visited = BTreeSet::from([current]);
    for _ in 0..crate::assembly::model::MAX_PASSAGE_UNITS {
        let Some(links) = parents.get(current) else {
            return Ok((None, Vec::new()));
        };
        if links.len() > 1 {
            return Err(failure(format!(
                "ambiguous archived logical parent of {current} in {parse_id}"
            )));
        }
        let Some(id) = links.first().copied() else {
            return Ok((None, Vec::new()));
        };
        if !visited.insert(id) {
            return Err(failure(format!(
                "archived logical containment cycle at {id} in {parse_id}"
            )));
        }
        let (kind, body) = nodes.get(id).ok_or_else(|| {
            failure(format!(
                "missing archived section ancestor {id} in {parse_id}"
            ))
        })?;
        if *kind == ContentType::TextSection {
            let path = crate::sections::section_path(body, parse_id, id)?;
            return Ok((Some(id.to_owned()), path));
        }
        current = id;
    }
    Err(failure(format!(
        "archived logical section ancestry of {unit_id} in {parse_id} exceeds depth limit"
    )))
}

/// SQL row snapshots retain snake_case column names and raw body_json strings.
fn archived_string<'a>(row: &'a Value, field: &str) -> Result<&'a str, ApiError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| failure(format!("archived section row has missing/invalid {field}")))
}

/// Reject partial, foreign, or invalid vector payloads before they reach the cache.
fn validate_payload(
    plane: &SectionDensePlane,
    source_id: &str,
    parse_id: &str,
    dimension: usize,
) -> Result<(), ApiError> {
    if plane.source_id != source_id
        || plane.parse_id != parse_id
        || plane.dimension != dimension
        || dimension == 0
        || plane.policy_hash != policy_hash()
        || plane.tokenizer_hash.len() != 64
        || !plane
            .tokenizer_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !matches!(plane.model_backend.as_str(), "local" | "http")
        || plane.model_name.is_empty()
        || plane.model_pooling.is_empty()
    {
        return Err(failure(format!(
            "section plane identity/policy/model mismatch for {parse_id}"
        )));
    }
    let mut ids = BTreeSet::new();
    for (index, window) in plane.windows.iter().enumerate() {
        // The shared validator owns its input; retain the archived vector while
        // checking its norm with exactly the same arithmetic as fresh embeddings.
        let checked = validate_vector(window.window_id.clone(), window.vector.clone(), dimension)
            .map_err(failure)?;
        if !ids.insert(&window.window_id)
            || window.window_id != window_id(parse_id, index)
            || window.token_count == 0
            || window.token_count > MAX_WINDOW_TOKENS
            || window.targeting_text.trim().is_empty()
            || window.input_unit_ids.is_empty()
            || window.fragments.len() != window.input_unit_ids.len()
            || window.input_unit_ids.iter().collect::<BTreeSet<_>>().len()
                != window.input_unit_ids.len()
            || window
                .fragments
                .iter()
                .zip(&window.input_unit_ids)
                .any(|(fragment, id)| {
                    fragment.unit_id != *id || fragment.start_byte >= fragment.end_byte
                })
            || !window.norm.is_finite()
            || (checked.norm - window.norm).abs() > checked.norm * 1e-5
            || (window.section_id.is_none() && !window.section_path.is_empty())
        {
            return Err(failure(format!(
                "invalid section window {} in {parse_id}",
                window.window_id
            )));
        }
    }
    Ok(())
}

/// Read canonical evidence in the same sequence as passage chunking, retaining
/// even short nonempty leaves and excluding only explicitly labeled furniture.
fn read_leaves(conn: &Connection, source_id: &str, parse_id: &str) -> Result<Vec<Leaf>, ApiError> {
    let mut statement = conn
        .prepare(UNITS_SQL)
        .map_err(|source| failure(format!("prepare section leaves for {parse_id}: {source}")))?;
    let rows = statement
        .query_map(
            params![parse_id, MAX_CELL_BYTES, MAX_PARSE_UNITS + 1],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .map_err(|source| failure(format!("read section leaves for {parse_id}: {source}")))?;
    let mut leaves = Vec::new();
    for (index, row) in rows.enumerate() {
        if index == MAX_PARSE_UNITS {
            return Err(failure(format!(
                "section parse {parse_id} exceeds {MAX_PARSE_UNITS} canonical units"
            )));
        }
        let (id, source, kind, body) =
            row.map_err(|source| failure(format!("decode section leaf in {parse_id}: {source}")))?;
        if source != source_id {
            return Err(failure(format!(
                "section leaf {id} in {parse_id} belongs to unexpected source {source}"
            )));
        }
        let kind: ContentType = serde_json::from_value(Value::String(kind))
            .map_err(|source| failure(format!("section leaf type {id}: {source}")))?;
        let body = body.ok_or_else(|| {
            failure(format!(
                "section leaf {id} exceeds {MAX_CELL_BYTES} body bytes"
            ))
        })?;
        let body: Value = serde_json::from_str(&body)
            .map_err(|source| failure(format!("section leaf body {id}: {source}")))?;
        let Some(text) = eligible_text(kind, &body) else {
            continue;
        };
        let (section_id, section_path) = read_section(conn, parse_id, &id)?;
        leaves.push(Leaf {
            id,
            text,
            section_id,
            section_path,
        });
    }
    Ok(leaves)
}

/// Keep live and archived furniture exclusion and canonical evidence fields identical.
fn eligible_text(kind: ContentType, body: &Value) -> Option<String> {
    if kind == ContentType::TextBlock
        && matches!(
            body.get("blockRole").and_then(Value::as_str),
            Some("header" | "footer")
        )
    {
        return None;
    }
    evidence_text(kind, body).filter(|text| !text.trim().is_empty())
}

/// Preserve first-seen section order and canonical leaf order within each section.
fn grouped_leaves(leaves: &[Leaf]) -> Vec<Vec<&Leaf>> {
    let mut positions = BTreeMap::new();
    let mut groups: Vec<Vec<&Leaf>> = Vec::new();
    for leaf in leaves {
        let position = *positions
            .entry(leaf.section_id.as_deref())
            .or_insert_with(|| {
                groups.push(Vec::new());
                groups.len() - 1
            });
        groups[position].push(leaf);
    }
    groups
}

/// Greedy section windows retain all leaf bytes; only oversized leaves are split,
/// and heading plus separators participate in every exact token-cap measurement.
fn build_windows(
    leaves: &[Leaf],
    parse_id: &str,
    tokenizer: &Tokenizer,
) -> Result<Vec<SectionDenseWindow>, ApiError> {
    let mut windows = Vec::new();
    for group in grouped_leaves(leaves) {
        let mut pending: Option<SectionDenseWindow> = None;
        for leaf in group {
            let mut offset = 0;
            while offset < leaf.text.len() {
                let window = pending.get_or_insert_with(|| empty_window(leaf));
                let separator = if window.fragments.is_empty() {
                    ""
                } else {
                    "\n\n"
                };
                let full = format!(
                    "{}{separator}{}",
                    window.targeting_text,
                    &leaf.text[offset..]
                );
                if count_tokens(tokenizer, &full)? <= MAX_WINDOW_TOKENS {
                    window.targeting_text = full;
                    window.input_unit_ids.push(leaf.id.clone());
                    window.fragments.push(SectionDenseFragment {
                        unit_id: leaf.id.clone(),
                        start_byte: offset,
                        end_byte: leaf.text.len(),
                    });
                    offset = leaf.text.len();
                } else if !window.fragments.is_empty() {
                    flush_window(&mut pending, &mut windows, parse_id, tokenizer)?;
                } else {
                    let length =
                        fitting_prefix(tokenizer, &window.targeting_text, &leaf.text[offset..])?;
                    window
                        .targeting_text
                        .push_str(&leaf.text[offset..offset + length]);
                    window.input_unit_ids.push(leaf.id.clone());
                    window.fragments.push(SectionDenseFragment {
                        unit_id: leaf.id.clone(),
                        start_byte: offset,
                        end_byte: offset + length,
                    });
                    offset += length;
                    flush_window(&mut pending, &mut windows, parse_id, tokenizer)?;
                }
            }
        }
        flush_window(&mut pending, &mut windows, parse_id, tokenizer)?;
    }
    Ok(windows)
}

/// Prefix only actual heading metadata; document-scoped windows have no invented label.
fn heading_prefix(path: &[String]) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("{}\n\n", path.join(" / "))
    }
}

/// Start targeting context independently of the canonical text fragments that follow.
fn empty_window(leaf: &Leaf) -> SectionDenseWindow {
    SectionDenseWindow {
        window_id: String::new(),
        section_id: leaf.section_id.clone(),
        section_path: leaf.section_path.clone(),
        input_unit_ids: Vec::new(),
        targeting_text: heading_prefix(&leaf.section_path),
        token_count: 0,
        vector: Vec::new(),
        norm: 0.0,
        fragments: Vec::new(),
    }
}

/// Assign deterministic identities after grouping and recheck the exact completed input.
fn flush_window(
    pending: &mut Option<SectionDenseWindow>,
    windows: &mut Vec<SectionDenseWindow>,
    parse_id: &str,
    tokenizer: &Tokenizer,
) -> Result<(), ApiError> {
    if let Some(mut window) = pending.take() {
        if window.fragments.is_empty() {
            return Ok(());
        }
        window.window_id = window_id(parse_id, windows.len());
        window.token_count = count_tokens(tokenizer, &window.targeting_text)?;
        if window.token_count > MAX_WINDOW_TOKENS {
            return Err(failure(format!(
                "section window {} exceeds token cap",
                window.window_id
            )));
        }
        windows.push(window);
    }
    Ok(())
}

/// Search UTF-8 boundaries and keep only measured fitting prefixes. Token counts
/// need not be monotone: the search may underfill a window but can never overfill it.
fn fitting_prefix(tokenizer: &Tokenizer, prefix: &str, text: &str) -> Result<usize, ApiError> {
    let boundaries: Vec<usize> = text
        .char_indices()
        .map(|(index, _)| index)
        .skip(1)
        .chain(std::iter::once(text.len()))
        .collect();
    let mut low = 0;
    let mut high = boundaries.len();
    let mut fitting = 0;
    while low < high {
        let middle = low + (high - low) / 2;
        let length = boundaries[middle];
        if count_tokens(tokenizer, &format!("{prefix}{}", &text[..length]))? <= MAX_WINDOW_TOKENS {
            fitting = length;
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if fitting == 0 {
        return Err(failure(
            "section heading leaves no room for canonical text under the 2048-token cap".to_owned(),
        ));
    }
    Ok(fitting)
}

/// Use the untruncated tokenizer including its special tokens for every window bound.
fn count_tokens(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoded| encoded.len())
        .map_err(|source| failure(format!("count section input tokens: {source}")))
}

/// Validate exact input text, ancestry, order, and complete byte coverage against
/// immutable canonical leaves; zero windows are valid only for a text-empty parse.
fn validate_leaves(leaves: &[Leaf], plane: &SectionDensePlane) -> Result<(), ApiError> {
    let ordered: Vec<&Leaf> = grouped_leaves(leaves).into_iter().flatten().collect();
    let mut leaf_index = 0;
    let mut offset = 0;
    for window in &plane.windows {
        let mut text = heading_prefix(&window.section_path);
        for (index, fragment) in window.fragments.iter().enumerate() {
            let leaf = ordered.get(leaf_index).ok_or_else(|| {
                failure(format!(
                    "section plane {} has unexpected fragments",
                    plane.parse_id
                ))
            })?;
            if leaf.id != fragment.unit_id
                || offset != fragment.start_byte
                || leaf.section_id != window.section_id
                || leaf.section_path != window.section_path
            {
                return Err(failure(format!(
                    "section window {} canonical order/ancestry differs at {}",
                    window.window_id, fragment.unit_id
                )));
            }
            let part = leaf
                .text
                .get(fragment.start_byte..fragment.end_byte)
                .ok_or_else(|| {
                    failure(format!(
                        "section window {} has invalid UTF-8 range for {}",
                        window.window_id, leaf.id
                    ))
                })?;
            if index != 0 {
                text.push_str("\n\n");
            }
            text.push_str(part);
            offset = fragment.end_byte;
            if offset == leaf.text.len() {
                leaf_index += 1;
                offset = 0;
            }
        }
        if text != window.targeting_text {
            return Err(failure(format!(
                "section window {} input differs from canonical fragments",
                window.window_id
            )));
        }
    }
    if leaf_index != ordered.len() || offset != 0 {
        return Err(failure(format!(
            "section plane {} omits canonical text",
            plane.parse_id
        )));
    }
    Ok(())
}

/// Stable input membership is identical in the projection envelope and its payload.
pub(crate) fn plane_input_ids(plane: &SectionDensePlane) -> Vec<String> {
    plane
        .windows
        .iter()
        .flat_map(|window| window.input_unit_ids.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Record builder, model, window policy, and canonical input ownership in the envelope.
fn new_projection(plane: &SectionDensePlane) -> NewProjection {
    NewProjection {
        source_id: plane.source_id.clone(),
        parse_id: plane.parse_id.clone(),
        projection_type: ProjectionType::DenseVector,
        input_unit_ids: Some(plane_input_ids(plane)),
        input_annotation_ids: None,
        producer: Provenance {
            producer_type: ProducerType::Model,
            producer_name: "fabric-section-dense".to_owned(),
            producer_version: Some("1".to_owned()),
            config_hash: Some(plane.policy_hash.clone()),
            model_name: Some(plane.model_name.clone()),
            model_version: None,
            prompt_hash: None,
            temperature: None,
            confidence: None,
            memoized: None,
            memoized_from: None,
            memoization_key_hash: None,
            input_refs: None,
        },
        index_name: Some(SECTION_DENSE_INDEX_NAME.to_owned()),
        index_partition: None,
    }
}

/// Window identity is stable for the immutable parse and versioned build policy.
fn window_id(parse_id: &str, index: usize) -> String {
    format!("{SECTION_DENSE_INDEX_NAME}:{parse_id}:{index}")
}

/// One policy fingerprint prevents mixed window semantics across cache and restore.
fn policy_hash() -> String {
    sha256_hex(WINDOW_POLICY.as_bytes())
}

/// Keep section build and canonical corruption context in the storage error flow.
fn failure(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}
