//! Section context vectors are immutable targeting artifacts, never canonical evidence.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{BufReader, Read};
use std::time::Instant;

use crate::limits::{ResourceLimits, RuntimeLimits};
use crate::sqlite::{Connection, Transaction};
use rusqlite::params;
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
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
// This exact string is the archived v1 policy, never the current build setting.
const WINDOW_POLICY: &str = "section_dense_v1;nearest_logical_section;canonical_sequence;heading_path_slash;paragraph_separator_double_newline;utf8_prefix_split;max_tokens=2048;special_tokens=true;no_truncation;no_padding;exclude_header_footer;retain_short_text";
const UNITS_SQL: &str = "
SELECT id, source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?2 THEN body_json END
FROM content_units WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id LIMIT ?3";
const CANONICAL_LEAF_SQL: &str = "
SELECT content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?4 THEN body_json END
FROM content_units WHERE id = ?1 AND source_id = ?2 AND parse_id = ?3";
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) construction: Option<SectionConstruction>,
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

/// Archived construction semantics are independent of current admission budgets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionConstruction {
    version: u32,
    window_max_tokens: u32,
    tokenizer_hash: String,
    model_identity: String,
}

impl SectionDensePlane {
    /// The unversioned payload is explicitly the frozen 2048-token v1 format.
    fn window_max_tokens(&self) -> usize {
        self.construction
            .as_ref()
            .map_or(2048, |policy| policy.window_max_tokens as usize)
    }

    /// Fail closed for unknown versions and authenticate the complete recorded policy.
    fn recorded_policy_hash(&self) -> Result<String, ApiError> {
        match &self.construction {
            None => Ok(policy_hash()),
            Some(policy)
                if policy.version == 2
                    && policy.window_max_tokens > 0
                    && policy.tokenizer_hash == self.tokenizer_hash
                    && !policy.model_identity.is_empty() =>
            {
                crate::canonical::canonical_sha256_hex_of(&(WINDOW_POLICY, policy))
            }
            Some(_) => Err(failure(
                "unsupported or invalid section construction policy".to_owned(),
            )),
        }
    }
}

/// Captured immutable section publication; vector and canonical-text bytes stay
/// on storage so activation does not require a corpus-sized heap allocation.
#[derive(Debug)]
pub(crate) struct SectionDenseReference {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) dimension: usize,
    pub(crate) window_count: usize,
    payload_uri: String,
}

/// Envelope inputs are temporary validation metadata, never cached vector state.
struct SectionDenseEnvelope {
    source_id: String,
    payload_uri: String,
    input_unit_ids: Vec<String>,
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
    model_identity: &str,
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<usize, ApiError> {
    let started = Instant::now();
    info!(
        event = "section_dense.build.started",
        source_id,
        parse_id,
        max_tokens = tx.limits().indexing.section_max_tokens,
        "building section context vectors"
    );
    let result = (|| {
        super::annotation_io::admitted_value_count(
            1,
            config.dimension as usize,
            &tx.limits().resources,
        )?;
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
        let max_tokens = tx.limits().indexing.section_max_tokens;
        let windows = build_windows(&leaves, parse_id, &counter, max_tokens as usize)?;
        let tokenizer_hash = sha256_hex(&canonical_json_bytes_of(&tokenizer_value)?);
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
            construction: Some(SectionConstruction {
                version: 2,
                window_max_tokens: max_tokens,
                tokenizer_hash: tokenizer_hash.clone(),
                model_identity: model_identity.to_owned(),
            }),
            source_id: source_id.to_owned(),
            parse_id: parse_id.to_owned(),
            dimension: config.dimension as usize,
            policy_hash: String::new(),
            tokenizer_hash,
            model_backend: model_backend.to_owned(),
            model_name,
            model_pooling: config.pooling.clone(),
            windows,
        };
        plane.policy_hash = plane.recorded_policy_hash()?;
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
            if let Some(monitor) = monitor {
                monitor.stage(
                    "dense section embeddings",
                    Some(texts.len() as u64),
                    "section inputs embedded",
                );
            }
            let vectors =
                super::dense::embed_texts(backend, &texts, parse_id, monitor, "section embedding")?;
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

/// Resolve one fresh section envelope without reading its vector payload.
fn read_section_envelope(
    conn: &Connection,
    parse_id: &str,
) -> Result<Option<SectionDenseEnvelope>, ApiError> {
    let mut statement = conn
        .prepare(ENVELOPE_SQL)
        .map_err(|source| failure(format!("prepare section envelope for {parse_id}: {source}")))?;
    let envelopes = statement
        .query_map(
            params![
                parse_id,
                SECTION_DENSE_INDEX_NAME,
                conn.limits().resources.max_json_cell_bytes
            ],
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
            "section payload URI missing or exceeds configured JSON bytes for {parse_id}"
        ))
    })?;
    let inputs = inputs.ok_or_else(|| {
        failure(format!(
            "section envelope inputs missing or exceed configured JSON bytes for {parse_id}"
        ))
    })?;
    let inputs: Vec<String> = serde_json::from_str(&inputs)
        .map_err(|source| failure(format!("section envelope inputs for {parse_id}: {source}")))?;
    Ok(Some(SectionDenseEnvelope {
        source_id,
        payload_uri: uri,
        input_unit_ids: inputs,
    }))
}

/// Validate persisted sections incrementally before publishing a small active handle.
pub(crate) fn load_section_dense_reference(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    dimension: usize,
) -> Result<Option<SectionDenseReference>, ApiError> {
    let Some(envelope) = read_section_envelope(conn, parse_id)? else {
        return Ok(None);
    };
    let mut canonical = CanonicalSectionInputs::new(conn, &envelope.source_id, parse_id)?;
    let mut reference = SectionDenseReference {
        source_id: envelope.source_id,
        parse_id: parse_id.to_owned(),
        dimension,
        window_count: 0,
        payload_uri: envelope.payload_uri,
    };
    let (_, window_count) = stream_section_payload(store, &reference, |window| {
        canonical.validate_window(conn, &reference, window)
    })?;
    reference.window_count = window_count;
    canonical.finish(&reference.parse_id)?;
    let ids: BTreeSet<&str> = canonical.ordered.iter().map(String::as_str).collect();
    if envelope
        .input_unit_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        != ids.into_iter().collect::<Vec<_>>()
    {
        return Err(failure(format!(
            "section envelope input membership differs from payload for {parse_id}"
        )));
    }
    Ok(Some(reference))
}

/// Score one window at a time from the captured artifact. The caller's transaction
/// must still resolve the same envelope, and only a successful return authenticates
/// provisional callback results against the complete artifact hash.
pub(crate) fn visit_section_dense(
    conn: &Connection,
    store: &ArtifactStore,
    reference: &SectionDenseReference,
    mut visit: impl FnMut(&SectionDenseWindow) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let envelope = read_section_envelope(conn, &reference.parse_id)?.ok_or_else(|| {
        failure(format!(
            "captured section envelope disappeared for {}",
            reference.parse_id
        ))
    })?;
    if envelope.payload_uri != reference.payload_uri || envelope.source_id != reference.source_id {
        return Err(failure(format!(
            "section publication differs from captured handle for {}",
            reference.parse_id
        )));
    }
    let expected: BTreeSet<&str> = envelope.input_unit_ids.iter().map(String::as_str).collect();
    let mut seen = BTreeSet::new();
    let (_, count) = stream_section_payload(store, reference, |window| {
        for id in &window.input_unit_ids {
            if !expected.contains(id.as_str()) {
                return Err(failure(format!(
                    "section window {} has unexpected input {id}",
                    window.window_id
                )));
            }
            seen.insert(id.clone());
        }
        visit(window)
    })?;
    if count != reference.window_count || seen.len() != expected.len() {
        return Err(failure(format!(
            "section count/input membership differs for {}",
            reference.parse_id
        )));
    }
    Ok(())
}

/// Canonical validation keeps only capped input IDs and one current leaf body.
/// Group order matches build_windows, including sections interleaved in source order.
struct CanonicalSectionInputs {
    ordered: Vec<String>,
    next_index: usize,
    offset: usize,
    current: Option<Leaf>,
}

impl CanonicalSectionInputs {
    /// Recover canonical section grouping without retaining the document's text.
    fn new(conn: &Connection, source_id: &str, parse_id: &str) -> Result<Self, ApiError> {
        let mut positions = BTreeMap::new();
        let mut groups: Vec<Vec<String>> = Vec::new();
        visit_leaves(conn, source_id, parse_id, |leaf| {
            let index = *positions.entry(leaf.section_id).or_insert_with(|| {
                groups.push(Vec::new());
                groups.len() - 1
            });
            groups[index].push(leaf.id);
            Ok(())
        })?;
        Ok(Self {
            ordered: groups.into_iter().flatten().collect(),
            next_index: 0,
            offset: 0,
            current: None,
        })
    }

    /// Require continuous UTF-8 coverage in build order and reconstruct each
    /// heading-prefixed input exactly; a split leaf stays loaded until consumed.
    fn validate_window(
        &mut self,
        conn: &Connection,
        reference: &SectionDenseReference,
        window: &SectionDenseWindow,
    ) -> Result<(), ApiError> {
        let mut text = heading_prefix(&window.section_path);
        for (index, fragment) in window.fragments.iter().enumerate() {
            if self.current.is_none() {
                let id = self.ordered.get(self.next_index).ok_or_else(|| {
                    failure(format!(
                        "section plane {} has unexpected fragments",
                        reference.parse_id
                    ))
                })?;
                self.current = Some(read_canonical_leaf(conn, reference, id)?);
            }
            let leaf = self.current.as_ref().ok_or_else(|| {
                failure(format!(
                    "section canonical cursor missing for {}",
                    reference.parse_id
                ))
            })?;
            if leaf.id != fragment.unit_id
                || self.offset != fragment.start_byte
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
            // Malformed fragment metadata must not expand a bounded window into
            // an unbounded canonical-text buffer before the equality check.
            if text
                .len()
                .checked_add(part.len())
                .and_then(|length| length.checked_add(if index == 0 { 0 } else { 2 }))
                .is_none_or(|length| length > window.targeting_text.len())
            {
                return Err(failure(format!(
                    "section window {} canonical fragments exceed its input text",
                    window.window_id
                )));
            }
            if index != 0 {
                text.push_str("\n\n");
            }
            text.push_str(part);
            self.offset = fragment.end_byte;
            if self.offset == leaf.text.len() {
                self.next_index += 1;
                self.offset = 0;
                self.current = None;
            }
        }
        if text != window.targeting_text {
            return Err(failure(format!(
                "section window {} input differs from canonical fragments",
                window.window_id
            )));
        }
        Ok(())
    }

    /// A clean end of JSON is insufficient when eligible canonical text is omitted.
    fn finish(&self, parse_id: &str) -> Result<(), ApiError> {
        if self.next_index != self.ordered.len() || self.offset != 0 {
            return Err(failure(format!(
                "section plane {parse_id} omits canonical text"
            )));
        }
        Ok(())
    }
}

/// Read the single canonical leaf currently needed by streaming coverage checks.
fn read_canonical_leaf(
    conn: &Connection,
    reference: &SectionDenseReference,
    id: &str,
) -> Result<Leaf, ApiError> {
    let (kind, body) = conn
        .query_row(
            CANONICAL_LEAF_SQL,
            params![
                id,
                reference.source_id,
                reference.parse_id,
                conn.limits().resources.max_source_body_bytes
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .map_err(|source| failure(format!("read canonical section leaf {id}: {source}")))?;
    let body = body.ok_or_else(|| {
        failure(format!(
            "resource limit: section leaf {id} exceeds configured body bytes"
        ))
    })?;
    let kind: ContentType = serde_json::from_value(Value::String(kind))
        .map_err(|source| failure(format!("section leaf type {id}: {source}")))?;
    let body = serde_json::from_str(&body)
        .map_err(|source| failure(format!("section leaf body {id}: {source}")))?;
    let text = eligible_text(kind, &body)
        .ok_or_else(|| failure(format!("section leaf {id} is no longer eligible")))?;
    let (section_id, section_path) = read_section(conn, &reference.parse_id, id)?;
    Ok(Leaf {
        id: id.to_owned(),
        text,
        section_id,
        section_path,
    })
}

/// Decode each window with a bounded record buffer and a dimension-limited vector.
/// The artifact store authenticates the full stream before its count is accepted.
fn stream_section_payload(
    store: &ArtifactStore,
    reference: &SectionDenseReference,
    mut visit: impl FnMut(&SectionDenseWindow) -> Result<(), ApiError>,
) -> Result<(SectionDensePlane, usize), ApiError> {
    super::annotation_io::admitted_value_count(1, reference.dimension, &store.limits().resources)?;
    store.with_verified_reader(&reference.payload_uri, None, |reader| {
        let remaining = Cell::new(store.limits().resources.max_json_cell_bytes);
        let reader = SectionRecordReader {
            reader: BufReader::new(reader),
            remaining: &remaining,
        };
        let mut deserializer = serde_json::Deserializer::from_reader(reader);
        let mut callback_error = None;
        let result = SectionPayloadSeed {
            reference,
            limits: &store.limits().resources,
            remaining: &remaining,
            visit: &mut visit,
            callback_error: &mut callback_error,
        }
        .deserialize(&mut deserializer);
        let count = match result {
            Ok(count) => count,
            Err(source) => {
                return Err(callback_error.unwrap_or_else(|| {
                    failure(format!(
                        "decode section payload {} for {}: {source}",
                        reference.payload_uri, reference.parse_id
                    ))
                }));
            }
        };
        deserializer.end().map_err(|source| {
            failure(format!(
                "section payload {} trailing data: {source}",
                reference.payload_uri
            ))
        })?;
        Ok(count)
    })
}

/// The byte budget is renewed only at a window boundary. It limits allocations
/// for malformed strings/metadata before serde finishes decoding the value.
struct SectionRecordReader<'a, R> {
    reader: R,
    remaining: &'a Cell<usize>,
}

impl<R: Read> Read for SectionRecordReader<'_, R> {
    /// Enforce the current record limit without buffering the entire JSON artifact.
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let allowed = self.remaining.get().min(buffer.len());
        if allowed == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "resource limit: section JSON record exceeds its configured byte budget",
            ));
        }
        let read = self.reader.read(&mut buffer[..allowed])?;
        self.remaining.set(self.remaining.get() - read);
        Ok(read)
    }
}

/// Carry expected identity and the provisional callback across serde's map/array boundary.
struct SectionPayloadSeed<'a> {
    reference: &'a SectionDenseReference,
    limits: &'a ResourceLimits,
    remaining: &'a Cell<usize>,
    visit: &'a mut dyn FnMut(&SectionDenseWindow) -> Result<(), ApiError>,
    callback_error: &'a mut Option<ApiError>,
}

impl<'de> DeserializeSeed<'de> for SectionPayloadSeed<'_> {
    type Value = (SectionDensePlane, usize);

    /// Deserialize the envelope map while delegating its windows to an incremental reader.
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SectionPayloadSeed<'_> {
    type Value = (SectionDensePlane, usize);

    /// Preserve useful format context in malformed artifact errors.
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a section dense plane with a streamed windows array")
    }

    /// Validate the complete header even when JSON field order puts it after windows.
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut header = serde_json::Map::new();
        let mut window_count = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "windows" {
                if window_count.is_some() {
                    return Err(M::Error::duplicate_field("windows"));
                }
                window_count = Some(map.next_value_seed(SectionWindowsSeed {
                    reference: self.reference,
                    limits: self.limits,
                    remaining: self.remaining,
                    visit: self.visit,
                    callback_error: self.callback_error,
                })?);
            } else {
                if header.contains_key(&key) {
                    return Err(M::Error::custom(format!("duplicate section field {key}")));
                }
                if !matches!(
                    key.as_str(),
                    "sourceId"
                        | "parseId"
                        | "dimension"
                        | "policyHash"
                        | "tokenizerHash"
                        | "modelBackend"
                        | "modelName"
                        | "modelPooling"
                        | "construction"
                ) {
                    return Err(M::Error::custom(format!("unknown section field {key}")));
                }
                header.insert(key, map.next_value::<Value>()?);
            }
        }
        let (count, max_tokens) = window_count.ok_or_else(|| M::Error::missing_field("windows"))?;
        header.insert("windows".to_owned(), Value::Array(Vec::new()));
        let plane: SectionDensePlane =
            serde_json::from_value(Value::Object(header)).map_err(M::Error::custom)?;
        validate_payload(
            &plane,
            &self.reference.source_id,
            &self.reference.parse_id,
            self.reference.dimension,
        )
        .map_err(M::Error::custom)?;
        // Header order is unrestricted. Retain only the largest observed count
        // until the archived construction policy can authenticate every window.
        if max_tokens > plane.window_max_tokens() {
            return Err(M::Error::custom(
                "section window exceeds its recorded construction token limit",
            ));
        }
        Ok((plane, count))
    }
}

/// Sequence visits release each window before deserializing its successor.
struct SectionWindowsSeed<'a> {
    reference: &'a SectionDenseReference,
    limits: &'a ResourceLimits,
    remaining: &'a Cell<usize>,
    visit: &'a mut dyn FnMut(&SectionDenseWindow) -> Result<(), ApiError>,
    callback_error: &'a mut Option<ApiError>,
}

impl<'de> DeserializeSeed<'de> for SectionWindowsSeed<'_> {
    type Value = (usize, usize);

    /// Enter the array without serde allocating a vector for all windows.
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for SectionWindowsSeed<'_> {
    type Value = (usize, usize);

    /// Report the expected collection at the array boundary.
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of section dense windows")
    }

    /// Validate shape before the callback, but defer accepting scores until the
    /// header, trailing bytes, and whole-artifact digest also pass validation.
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
        let record_bytes = self
            .reference
            .dimension
            .checked_mul(32)
            .and_then(|bytes| bytes.checked_add(self.limits.max_json_cell_bytes))
            .ok_or_else(|| S::Error::custom("section record byte budget overflow"))?;
        let mut count = 0;
        let mut max_tokens = 0;
        loop {
            self.remaining.set(record_bytes);
            let Some(window) = sequence.next_element_seed(SectionWindowSeed {
                dimension: self.reference.dimension,
            })?
            else {
                break;
            };
            validate_window(
                &window,
                &self.reference.parse_id,
                self.reference.dimension,
                count,
                usize::MAX,
            )
            .map_err(S::Error::custom)?;
            max_tokens = max_tokens.max(window.token_count);
            if let Err(error) = (self.visit)(&window) {
                *self.callback_error = Some(error);
                return Err(S::Error::custom("section window callback failed"));
            }
            count += 1;
        }
        // Header data after windows receives the same cap as data before it.
        self.remaining.set(self.limits.max_json_cell_bytes);
        Ok((count, max_tokens))
    }
}

/// Bound the vector by the known model dimension before a malformed array grows.
struct SectionWindowSeed {
    dimension: usize,
}

impl<'de> DeserializeSeed<'de> for SectionWindowSeed {
    type Value = SectionDenseWindow;

    /// Decode metadata separately so vector elements bypass serde_json::Value allocations.
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SectionWindowSeed {
    type Value = SectionDenseWindow;

    /// Keep malformed per-window errors tied to the expected representation.
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a section window with a dimension-bounded vector")
    }

    /// Decode typed fields directly so duplicate fields in canonical fragments
    /// remain errors, rather than disappearing through an intermediate JSON map.
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut fields = BTreeSet::new();
        let mut window_id = None;
        let mut section_id = None;
        let mut section_path = None;
        let mut input_unit_ids = None;
        let mut targeting_text = None;
        let mut token_count = None;
        let mut norm = None;
        let mut fragments = None;
        let mut vector = None;
        while let Some(key) = map.next_key::<String>()? {
            // The set owns at most the nine field names while the current key
            // remains borrowed for dispatch and contextual duplicate errors.
            if !fields.insert(key.clone()) {
                return Err(M::Error::custom(format!(
                    "duplicate section window field {key}"
                )));
            }
            match key.as_str() {
                "windowId" => window_id = Some(map.next_value()?),
                "sectionId" => section_id = map.next_value()?,
                "sectionPath" => section_path = Some(map.next_value()?),
                "inputUnitIds" => input_unit_ids = Some(map.next_value()?),
                "targetingText" => targeting_text = Some(map.next_value()?),
                "tokenCount" => token_count = Some(map.next_value()?),
                "norm" => norm = Some(map.next_value()?),
                "fragments" => fragments = Some(map.next_value()?),
                "vector" => {
                    vector = Some(map.next_value_seed(SectionVectorSeed {
                        dimension: self.dimension,
                    })?)
                }
                _ => {
                    return Err(M::Error::custom(format!(
                        "unknown section window field {key}"
                    )));
                }
            }
        }
        Ok(SectionDenseWindow {
            window_id: window_id.ok_or_else(|| M::Error::missing_field("windowId"))?,
            section_id,
            section_path: section_path.ok_or_else(|| M::Error::missing_field("sectionPath"))?,
            input_unit_ids: input_unit_ids
                .ok_or_else(|| M::Error::missing_field("inputUnitIds"))?,
            targeting_text: targeting_text
                .ok_or_else(|| M::Error::missing_field("targetingText"))?,
            token_count: token_count.ok_or_else(|| M::Error::missing_field("tokenCount"))?,
            norm: norm.ok_or_else(|| M::Error::missing_field("norm"))?,
            fragments: fragments.ok_or_else(|| M::Error::missing_field("fragments"))?,
            vector: vector.ok_or_else(|| M::Error::missing_field("vector"))?,
        })
    }
}

/// Only the configured model dimension determines vector allocation, never artifact lengths.
struct SectionVectorSeed {
    dimension: usize,
}

impl<'de> DeserializeSeed<'de> for SectionVectorSeed {
    type Value = Vec<f32>;

    /// Route the vector through a sequence visitor that ignores untrusted size hints.
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for SectionVectorSeed {
    type Value = Vec<f32>;

    /// Identify the dimension contract when vector JSON is malformed.
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exactly {} dense vector values", self.dimension)
    }

    /// Reject the first excess scalar without allocating space for it.
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
        let mut vector = Vec::new();
        while let Some(value) = sequence.next_element::<f32>()? {
            if vector.len() == self.dimension {
                return Err(S::Error::custom(
                    "section vector exceeds expected dimension",
                ));
            }
            vector.push(value);
        }
        if vector.len() != self.dimension {
            return Err(S::Error::custom(
                "section vector is shorter than expected dimension",
            ));
        }
        Ok(vector)
    }
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
    let reference = SectionDenseReference {
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        dimension: expected_dimension,
        window_count: 0,
        payload_uri: uri.to_owned(),
    };
    let mut windows = Vec::new();
    // Full archive consumers use the same bounded record parser as query scans.
    // They still retain the full verified plane, unlike incremental query readers.
    let (mut plane, _) = stream_section_payload(store, &reference, |window| {
        windows.push(window.clone());
        Ok(())
    })?;
    plane.windows = windows;
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
    limits: &RuntimeLimits,
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
        if raw_body.len() > limits.resources.max_source_body_bytes {
            return Err(failure(format!(
                "resource limit: archived section unit {id} exceeds configured body bytes"
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
    if nodes.len() > limits.resources.max_parse_units {
        return Err(failure(format!(
            "resource limit: archived section parse {} exceeds configured unit count",
            plane.parse_id
        )));
    }
    order.sort_unstable();
    let mut parents: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for relation in relationships.iter().filter(|relation| {
        relation.get("parse_id").and_then(Value::as_str) == Some(plane.parse_id.as_str())
    }) {
        if archived_string(relation, "relationship_type")? != "contains" {
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
        let (section_id, section_path) = archived_section(
            id,
            &plane.parse_id,
            &nodes,
            &parents,
            limits.retrieval.max_section_ancestry,
        )?;
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
/// Parent sets mirror SQL DISTINCT over `contains` edges and, like the SQL,
/// never admit a `page` unit as an ancestor.
fn archived_section(
    unit_id: &str,
    parse_id: &str,
    nodes: &BTreeMap<&str, (ContentType, Value)>,
    parents: &BTreeMap<&str, BTreeSet<&str>>,
    max_ancestry: usize,
) -> Result<(Option<String>, Vec<String>), ApiError> {
    let mut current = unit_id;
    let mut visited = BTreeSet::from([current]);
    for _ in 0..max_ancestry {
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
        "resource limit: archived logical section ancestry of {unit_id} in {parse_id} exceeds configured depth limit"
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
        || plane.policy_hash != plane.recorded_policy_hash()?
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
    for (index, window) in plane.windows.iter().enumerate() {
        validate_window(
            window,
            parse_id,
            dimension,
            index,
            plane.window_max_tokens(),
        )?;
    }
    Ok(())
}

/// Validate one streamed window with the same invariants used by archive readers.
fn validate_window(
    window: &SectionDenseWindow,
    parse_id: &str,
    dimension: usize,
    index: usize,
    max_tokens: usize,
) -> Result<(), ApiError> {
    // The shared validator owns its input; retain the archived vector while
    // checking its norm with exactly the same arithmetic as fresh embeddings.
    let checked = validate_vector(window.window_id.clone(), window.vector.clone(), dimension)
        .map_err(failure)?;
    // Exact ordinal identity also rejects duplicate window IDs without a
    // set that grows with every streamed vector.
    if window.window_id != window_id(parse_id, index)
        || window.token_count == 0
        || window.token_count > max_tokens
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
        || window.norm <= 0.0
        || (checked.norm - window.norm).abs() > checked.norm * 1e-5
        || (window.section_id.is_none() && !window.section_path.is_empty())
    {
        return Err(failure(format!(
            "invalid section window {} in {parse_id}",
            window.window_id
        )));
    }
    Ok(())
}

/// Read canonical evidence in the same sequence as passage chunking, retaining
/// even short nonempty leaves.
fn read_leaves(conn: &Connection, source_id: &str, parse_id: &str) -> Result<Vec<Leaf>, ApiError> {
    let mut leaves = Vec::new();
    visit_leaves(conn, source_id, parse_id, |leaf| {
        leaves.push(leaf);
        Ok(())
    })?;
    Ok(leaves)
}

/// Offer canonical leaves one at a time so stream validation retains only IDs
/// needed to reproduce section grouping, not all of the document's evidence text.
fn visit_leaves(
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
    mut visit: impl FnMut(Leaf) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    let limits = &conn.limits().resources;
    let mut statement = conn
        .prepare(UNITS_SQL)
        .map_err(|source| failure(format!("prepare section leaves for {parse_id}: {source}")))?;
    let rows = statement
        .query_map(
            params![
                parse_id,
                limits.max_source_body_bytes,
                limits.max_parse_units + 1
            ],
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
    for (index, row) in rows.enumerate() {
        if index == limits.max_parse_units {
            return Err(failure(format!(
                "resource limit: section parse {parse_id} exceeds configured canonical unit count"
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
                "resource limit: section leaf {id} exceeds configured body bytes"
            ))
        })?;
        let body: Value = serde_json::from_str(&body)
            .map_err(|source| failure(format!("section leaf body {id}: {source}")))?;
        let Some(text) = eligible_text(kind, &body) else {
            continue;
        };
        let (section_id, section_path) = read_section(conn, parse_id, &id)?;
        visit(Leaf {
            id,
            text,
            section_id,
            section_path,
        })?;
    }
    Ok(())
}

/// Keep live and archived leaf selection identical: the shared evidence-text
/// contract, minus blank text. v0.4 has no furniture roles to exclude.
fn eligible_text(kind: ContentType, body: &Value) -> Option<String> {
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
    max_tokens: usize,
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
                if count_tokens(tokenizer, &full)? <= max_tokens {
                    window.targeting_text = full;
                    window.input_unit_ids.push(leaf.id.clone());
                    window.fragments.push(SectionDenseFragment {
                        unit_id: leaf.id.clone(),
                        start_byte: offset,
                        end_byte: leaf.text.len(),
                    });
                    offset = leaf.text.len();
                } else if !window.fragments.is_empty() {
                    flush_window(&mut pending, &mut windows, parse_id, tokenizer, max_tokens)?;
                } else {
                    let length = fitting_prefix(
                        tokenizer,
                        &window.targeting_text,
                        &leaf.text[offset..],
                        max_tokens,
                    )?;
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
                    flush_window(&mut pending, &mut windows, parse_id, tokenizer, max_tokens)?;
                }
            }
        }
        flush_window(&mut pending, &mut windows, parse_id, tokenizer, max_tokens)?;
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
    max_tokens: usize,
) -> Result<(), ApiError> {
    if let Some(mut window) = pending.take() {
        if window.fragments.is_empty() {
            return Ok(());
        }
        window.window_id = window_id(parse_id, windows.len());
        window.token_count = count_tokens(tokenizer, &window.targeting_text)?;
        if window.token_count > max_tokens {
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
fn fitting_prefix(
    tokenizer: &Tokenizer,
    prefix: &str,
    text: &str,
    max_tokens: usize,
) -> Result<usize, ApiError> {
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
        if count_tokens(tokenizer, &format!("{prefix}{}", &text[..length]))? <= max_tokens {
            fitting = length;
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if fitting == 0 {
        return Err(failure(format!(
            "section heading leaves no room for canonical text under the {max_tokens}-token cap"
        )));
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
            producer_version: Some(
                if plane.construction.is_some() {
                    "2"
                } else {
                    "1"
                }
                .to_owned(),
            ),
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
