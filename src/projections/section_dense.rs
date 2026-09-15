//! Context windows (PLAN-grains Section 2, context grain): section context
//! vectors over runs of consecutive fine chunks. They are immutable targeting
//! artifacts, never canonical evidence.
//!
//! Construction. Windows are packed from the parse's `chunk_projections` rows
//! in `chunk_index` order (the only reading-order authority for chunks), read
//! on the caller's transaction through the shared `lexical::read_parse_chunks`
//! reader. A run grows while the exact joined canonical text fits
//! `indexing.context_max_tokens`; runs cross section boundaries and never
//! split a chunk (every fine chunk fits the context cap because
//! `indexing.fine_max_tokens` is below it — a chunk that does not fails the
//! build). At document end a run below `indexing.min_fill_ratio` merges into
//! its predecessor when the combined text fits.
//!
//! Each window records its member `chunk_ids` in order, `fragments` as the
//! concatenation of its chunks' fragments (Unicode scalar offsets, never
//! bytes), `input_unit_ids` derived from those fragments, the `section_path`
//! of its first chunk (used only as the model-input prefix), its canonical
//! text (`targeting_text`: chunk texts joined by one blank line) and the
//! token count measured on exactly that text. The embedded model input is
//! `chunk::model_input(section_path, canonical)`; the prefix is never part of
//! any range, hash, or stored text.
//!
//! Validation re-keys every window to chunk rows and never runs a tokenizer:
//! chunk ids must be consecutive in chunk order, all windows together cover
//! every chunk exactly once, fragments and canonical text must equal the
//! members' concatenation, and the recorded token count must be within the
//! recorded cap.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::fmt;
use std::io::{BufReader, Read};
use std::time::Instant;

use crate::limits::{IndexingLimits, ResourceLimits};
use crate::sqlite::{Connection, Transaction};
use rusqlite::params;
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::artifact_store::ArtifactStore;
use crate::canonical::canonical_json_bytes_of;
use crate::config::{DenseBackendKind, DenseModelConfig};
use crate::error::ApiError;
use crate::inference::DenseEmbeddingBackend;
use crate::model::{ProducerType, Provenance};
use crate::primitives::sha256_hex;
use crate::primitives::validate::validate_vector;
use crate::sections::read_section;
use crate::state::ModelCallPermit;

use super::StoredChunk;
use super::chunk::{self, Fragment};
use super::envelope::{self, NewProjection, ProjectionType};

/// Distinguishes archived section vectors from the fine passage dense plane.
/// Consumers key on it as the envelope index name, not as a format version;
/// the construction descriptor and policy hash carry the version.
pub(crate) const SECTION_DENSE_INDEX_NAME: &str = "section_dense_v1";
// The version-3 construction: consecutive fine chunks in chunk_index order,
// packed under the context cap with the minimum-fill rule, never split,
// crossing sections; canonical text joined by one blank line and measured
// without the section-path prefix; fragments in scalar offsets. Hashed with
// the construction descriptor, so a change here is a visible policy change.
const WINDOW_POLICY: &str = "section_dense_v3;consecutive_fine_chunks;chunk_index_order;cross_sections;no_chunk_split;min_fill_merge_at_end;canonical_join_double_newline;model_input_section_path_slash_prefix;token_count_on_canonical;fragments_scalar_offsets;special_tokens=true;no_truncation;no_padding";
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
    pub(crate) construction: SectionConstruction,
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

/// Archived construction semantics are independent of current admission
/// budgets: the cap and minimum-fill ratio that shaped the runs travel with
/// the artifact so validation never consults today's limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionConstruction {
    version: u32,
    window_max_tokens: u32,
    min_fill_ratio: f64,
    tokenizer_hash: String,
    model_identity: String,
}

/// The construction version this module builds and accepts.
pub(crate) const CONSTRUCTION_VERSION: u32 = 3;

impl SectionDensePlane {
    /// The token cap every window of this artifact was measured against.
    fn window_max_tokens(&self) -> usize {
        self.construction.window_max_tokens as usize
    }

    /// Fail closed for unknown versions and authenticate the complete recorded
    /// policy: older payloads carry byte-offset fragments and no chunk ids, so
    /// only version 3 is readable.
    fn recorded_policy_hash(&self) -> Result<String, ApiError> {
        let policy = &self.construction;
        if policy.version == CONSTRUCTION_VERSION
            && policy.window_max_tokens > 0
            && policy.min_fill_ratio.is_finite()
            && policy.min_fill_ratio > 0.0
            && policy.min_fill_ratio < 1.0
            && policy.tokenizer_hash == self.tokenizer_hash
            && !policy.model_identity.is_empty()
        {
            crate::canonical::canonical_sha256_hex_of(&(WINDOW_POLICY, policy))
        } else {
            Err(failure(
                "unsupported or invalid section construction policy".to_owned(),
            ))
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

/// One context window: a run of consecutive fine chunks, its canonical text,
/// and its vector. `targeting_text` is the canonical text (no prefix);
/// `input_unit_ids` is always `chunk::ordered_unit_ids(fragments)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SectionDenseWindow {
    pub(crate) window_id: String,
    pub(crate) section_id: Option<String>,
    pub(crate) section_path: Vec<String>,
    /// Member chunk ids in chunk order; consecutive `chunk_index` positions.
    pub(crate) chunk_ids: Vec<String>,
    pub(crate) input_unit_ids: Vec<String>,
    pub(crate) targeting_text: String,
    pub(crate) token_count: usize,
    pub(crate) vector: Vec<f32>,
    pub(crate) norm: f32,
    /// The concatenation of the member chunks' fragments, scalar offsets.
    pub(crate) fragments: Vec<Fragment>,
}

/// The chunk fields the context grain consumes, in chunk order: hot rows come
/// from `StoredChunk`, archived rows from the snapshot's `chunk_projections`
/// JSONL. A member's position in the slice is its `chunk_index`.
struct ContextMember {
    id: String,
    targeting_text: String,
    fragments: Vec<Fragment>,
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
        max_tokens = tx.limits().indexing.context_max_tokens,
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
        // CPU-only copy counts the full window text and never silently clips it.
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
        // The chunk rows are read on the caller's transaction so the windows
        // describe exactly the chunk set this build's owner is publishing.
        let members = hot_members(
            super::lexical::read_parse_chunks(tx, parse_id)?,
            source_id,
            parse_id,
        )?;
        let indexing = &tx.limits().indexing;
        let windows = build_windows(&members, parse_id, &counter, indexing)?;
        let windows = resolve_section_ids(tx, parse_id, windows)?;
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
            construction: SectionConstruction {
                version: CONSTRUCTION_VERSION,
                window_max_tokens: indexing.context_max_tokens,
                min_fill_ratio: indexing.min_fill_ratio,
                tokenizer_hash: tokenizer_hash.clone(),
                model_identity: model_identity.to_owned(),
            },
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
        info!(event = "section_dense.windows.ready", source_id, parse_id, chunk_count = members.len(), window_count = plane.windows.len(), policy_hash = %plane.policy_hash, elapsed_ms = started.elapsed().as_millis() as u64, "section inputs constructed");
        let projection = envelope::insert_building(tx, &new_projection(&plane))?;
        let built = (|| {
            info!(
                event = "section_dense.embedding.started",
                source_id,
                parse_id,
                window_count = plane.windows.len(),
                "embedding section context inputs"
            );
            // The model sees the section-path prefix; the stored text does not.
            let inputs: Vec<String> = plane
                .windows
                .iter()
                .map(|window| chunk::model_input(&window.section_path, &window.targeting_text))
                .collect();
            let texts: Vec<&str> = inputs.iter().map(String::as_str).collect();
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
            // The archived plane must pass the same chunk-keyed check every
            // later reader applies, before any bytes are written.
            validate_members(&members, &plane)?;
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

/// Validate persisted sections incrementally against the parse's chunk rows
/// before publishing a small active handle. The chunk rows of one parse are
/// held for the duration of this check; the vector payload is still streamed.
pub(crate) fn load_section_dense_reference(
    conn: &Connection,
    store: &ArtifactStore,
    parse_id: &str,
    dimension: usize,
) -> Result<Option<SectionDenseReference>, ApiError> {
    let Some(envelope) = read_section_envelope(conn, parse_id)? else {
        return Ok(None);
    };
    let members = hot_members(
        super::lexical::read_parse_chunks(conn, parse_id)?,
        &envelope.source_id,
        parse_id,
    )?;
    let mut reference = SectionDenseReference {
        source_id: envelope.source_id,
        parse_id: parse_id.to_owned(),
        dimension,
        window_count: 0,
        payload_uri: envelope.payload_uri,
    };
    // The cap is only known once the header is decoded, and header order is
    // unrestricted; the seed checks every streamed count against the recorded
    // cap, so the cursor here checks structure and coverage only.
    let mut cursor = MemberCursor::new(&members, usize::MAX);
    let mut seen_units = BTreeSet::new();
    let (_, window_count) = stream_section_payload(store, &reference, |window| {
        cursor.validate_window(window)?;
        seen_units.extend(window.input_unit_ids.iter().cloned());
        Ok(())
    })?;
    reference.window_count = window_count;
    cursor.finish(&reference.parse_id)?;
    if envelope
        .input_unit_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        != seen_units.iter().map(String::as_str).collect::<Vec<_>>()
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

/// Walks the parse's chunk members in chunk order while windows arrive in
/// window order, so one pass checks consecutiveness and coverage:
/// `next` is the position of the first chunk the next window must start at.
struct MemberCursor<'a> {
    members: &'a [ContextMember],
    next: usize,
    max_tokens: usize,
}

impl<'a> MemberCursor<'a> {
    /// Start at the first chunk; `max_tokens` is the recorded cap, or
    /// `usize::MAX` when a streaming reader enforces the cap separately.
    fn new(members: &'a [ContextMember], max_tokens: usize) -> Self {
        Self {
            members,
            next: 0,
            max_tokens,
        }
    }

    /// Check one window against the members it must cover next. Consecutive:
    /// the window's chunk ids are exactly the members at positions
    /// `next..next + chunk_ids.len()`, in order. Content: fragments equal the
    /// members' fragments concatenated, canonical text equals the members'
    /// texts joined by one blank line, unit ids derive from the fragments, the
    /// section path is the first member's, and the token count is within the
    /// cap. No tokenizer runs here.
    fn validate_window(&mut self, window: &SectionDenseWindow) -> Result<(), ApiError> {
        let Some(first) = self.members.get(self.next) else {
            return Err(failure(format!(
                "section window {} lies beyond the parse's last chunk",
                window.window_id
            )));
        };
        if window.chunk_ids.is_empty() {
            return Err(failure(format!(
                "section window {} has no chunks",
                window.window_id
            )));
        }
        let mut fragments = Vec::new();
        let mut text = String::new();
        for (offset, chunk_id) in window.chunk_ids.iter().enumerate() {
            let member = self.members.get(self.next + offset).ok_or_else(|| {
                failure(format!(
                    "section window {} lists more chunks than the parse has",
                    window.window_id
                ))
            })?;
            if member.id != *chunk_id {
                return Err(failure(format!(
                    "section window {} chunk {chunk_id} is not the next chunk in chunk order (expected {})",
                    window.window_id, member.id
                )));
            }
            // Malformed metadata must not expand a bounded window into an
            // unbounded buffer before the equality check: the reconstruction
            // may never grow past the window's own text.
            let separator = if offset == 0 { 0 } else { 2 };
            if text
                .len()
                .saturating_add(member.targeting_text.len())
                .saturating_add(separator)
                > window.targeting_text.len()
            {
                return Err(failure(format!(
                    "section window {} canonical text is shorter than its chunks' text",
                    window.window_id
                )));
            }
            if offset == 0 {
                text.push_str(&member.targeting_text);
            } else {
                text = chunk::join_text(&text, &member.targeting_text);
            }
            fragments.extend(member.fragments.iter().cloned());
        }
        if fragments != window.fragments {
            return Err(failure(format!(
                "section window {} fragments differ from its chunks' fragments",
                window.window_id
            )));
        }
        if text != window.targeting_text {
            return Err(failure(format!(
                "section window {} canonical text differs from its chunks' text",
                window.window_id
            )));
        }
        if window.input_unit_ids != chunk::ordered_unit_ids(&window.fragments) {
            return Err(failure(format!(
                "section window {} unit ids do not derive from its fragments",
                window.window_id
            )));
        }
        if window.section_path != first.section_path {
            return Err(failure(format!(
                "section window {} section path differs from its first chunk's",
                window.window_id
            )));
        }
        if window.token_count > self.max_tokens {
            return Err(failure(format!(
                "section window {} token count {} exceeds the recorded cap {}",
                window.window_id, window.token_count, self.max_tokens
            )));
        }
        self.next += window.chunk_ids.len();
        Ok(())
    }

    /// Coverage: every chunk of the parse was consumed by exactly one window.
    /// Zero windows are valid only for a parse with zero chunks.
    fn finish(&self, parse_id: &str) -> Result<(), ApiError> {
        if self.next != self.members.len() {
            return Err(failure(format!(
                "section plane {parse_id} covers {} of {} chunks",
                self.next,
                self.members.len()
            )));
        }
        Ok(())
    }
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
        let mut chunk_ids = None;
        let mut input_unit_ids = None;
        let mut targeting_text = None;
        let mut token_count = None;
        let mut norm = None;
        let mut fragments = None;
        let mut vector = None;
        while let Some(key) = map.next_key::<String>()? {
            // The set owns at most the ten field names while the current key
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
                "chunkIds" => chunk_ids = Some(map.next_value()?),
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
            chunk_ids: chunk_ids.ok_or_else(|| M::Error::missing_field("chunkIds"))?,
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
/// Snapshot readers additionally call validate_plane against hot chunk rows.
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

/// Confirm the plane's windows are consecutive runs covering every hot chunk
/// row of the parse exactly once, with fragments and canonical text equal to
/// their chunks'. Reads the chunk rows through the shared reader on `conn`.
pub(crate) fn validate_plane(conn: &Connection, plane: &SectionDensePlane) -> Result<(), ApiError> {
    let members = hot_members(
        super::lexical::read_parse_chunks(conn, &plane.parse_id)?,
        &plane.source_id,
        &plane.parse_id,
    )?;
    validate_members(&members, plane)
}

/// Verify snapshot section windows against the archived `chunk_projections`
/// records (the corpus-wide JSONL the verifier already loads) before any
/// restore writes. The same checker runs after the rows are re-imported.
pub(crate) fn validate_archived_plane(
    chunks: &[Value],
    plane: &SectionDensePlane,
) -> Result<(), ApiError> {
    let members = archived_members(chunks, &plane.source_id, &plane.parse_id)?;
    validate_members(&members, plane)
}

/// Re-type hot chunk rows into context members, requiring that every row
/// belongs to the expected source and parse and that `chunk_index` is exactly
/// the row's position (0..n contiguous), so positions can stand in for it.
fn hot_members(
    chunks: Vec<StoredChunk>,
    source_id: &str,
    parse_id: &str,
) -> Result<Vec<ContextMember>, ApiError> {
    let mut members = Vec::with_capacity(chunks.len());
    for (position, chunk) in chunks.into_iter().enumerate() {
        if chunk.source_id != source_id || chunk.parse_id != parse_id {
            return Err(failure(format!(
                "chunk {} belongs to source {} parse {}, not source {source_id} parse {parse_id}",
                chunk.id, chunk.source_id, chunk.parse_id
            )));
        }
        if chunk.chunk_index != position {
            return Err(failure(format!(
                "chunk {} of parse {parse_id} has chunk_index {} at position {position}; chunk order is not contiguous",
                chunk.id, chunk.chunk_index
            )));
        }
        members.push(ContextMember {
            id: chunk.id,
            targeting_text: chunk.targeting_text,
            fragments: chunk.fragments,
            section_path: chunk.section_path,
        });
    }
    Ok(members)
}

/// Re-type archived `chunk_projections` JSONL records of one parse into
/// context members in `chunk_index` order, with the same ownership and
/// contiguity requirements as `hot_members`. SQL row snapshots keep snake_case
/// column names, raw JSON-string columns, and INTEGER columns as numbers.
fn archived_members(
    chunks: &[Value],
    source_id: &str,
    parse_id: &str,
) -> Result<Vec<ContextMember>, ApiError> {
    let mut indexed = Vec::new();
    for record in chunks
        .iter()
        .filter(|record| record.get("parse_id").and_then(Value::as_str) == Some(parse_id))
    {
        let id = archived_string(record, "id")?;
        if archived_string(record, "source_id")? != source_id {
            return Err(failure(format!(
                "archived chunk {id} of parse {parse_id} belongs to a different source than {source_id}"
            )));
        }
        let chunk_index = record
            .get("chunk_index")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                failure(format!(
                    "archived chunk {id} has a missing or invalid chunk_index"
                ))
            })?;
        let fragments: Vec<Fragment> =
            serde_json::from_str(archived_string(record, "fragments_json")?)
                .map_err(|source| failure(format!("archived chunk {id} fragments: {source}")))?;
        let section_path: Vec<String> =
            serde_json::from_str(archived_string(record, "section_path_json")?)
                .map_err(|source| failure(format!("archived chunk {id} section path: {source}")))?;
        indexed.push((
            chunk_index,
            ContextMember {
                id: id.to_owned(),
                targeting_text: archived_string(record, "targeting_text")?.to_owned(),
                fragments,
                section_path,
            },
        ));
    }
    indexed.sort_by_key(|(chunk_index, _)| *chunk_index);
    // Contiguity after sorting: a duplicate or missing index shows up as a
    // position mismatch.
    let mut members = Vec::with_capacity(indexed.len());
    for (position, (chunk_index, member)) in indexed.into_iter().enumerate() {
        if chunk_index != position {
            return Err(failure(format!(
                "archived chunk {} of parse {parse_id} has chunk_index {chunk_index} at position {position}; chunk order is not contiguous",
                member.id
            )));
        }
        members.push(member);
    }
    Ok(members)
}

/// SQL row snapshots retain snake_case column names and raw JSON-string columns.
fn archived_string<'a>(row: &'a Value, field: &str) -> Result<&'a str, ApiError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| failure(format!("archived chunk row has missing/invalid {field}")))
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

/// Validate one streamed window's own shape, with the same invariants used by
/// archive readers: identity, vector, non-empty unique chunk ids, well-formed
/// fragments, and unit ids derived from the fragments. Chunk-row agreement is
/// `MemberCursor`'s job.
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
        || window.chunk_ids.is_empty()
        || window.chunk_ids.iter().collect::<BTreeSet<_>>().len() != window.chunk_ids.len()
        || window.fragments.is_empty()
        || window
            .fragments
            .iter()
            .any(|fragment| fragment.start_char >= fragment.end_char)
        || window.input_unit_ids != chunk::ordered_unit_ids(&window.fragments)
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

/// The run being packed: the member positions it spans (`start..end`), its
/// canonical text, and the token count measured on exactly that text — every
/// path that changes `text` measures the new text whole before storing it.
struct OpenRun {
    start: usize,
    end: usize,
    text: String,
    tokens: usize,
}

/// Pack chunk members into windows under `indexing.context_max_tokens` with
/// the Section 2 minimum-fill rule, never splitting a chunk:
///   - a member joins the open run while the EXACT joined canonical text fits
///     the cap (tokenization is not additive across joins, so every candidate
///     is measured whole);
///   - when it would overflow, the run closes and the member opens the next
///     run — with no split available, a run below the minimum closes short
///     rather than borrowing part of the next chunk;
///   - a member that alone exceeds the cap fails the build naming both keys,
///     since `indexing.fine_max_tokens` below the cap is what guarantees every
///     chunk fits;
///   - at document end a final run below the minimum merges into the
///     preceding run when the combined text fits the cap; otherwise it stays
///     as the one permitted sub-minimum run.
///
/// Runs never consult section boundaries.
fn build_windows(
    members: &[ContextMember],
    parse_id: &str,
    tokenizer: &Tokenizer,
    indexing: &IndexingLimits,
) -> Result<Vec<SectionDenseWindow>, ApiError> {
    let max_tokens = indexing.context_max_tokens as usize;
    // The minimum is computed once, floored, so every run is held to one
    // integer bound.
    let min_tokens = (max_tokens as f64 * indexing.min_fill_ratio).floor() as usize;
    let mut runs: Vec<OpenRun> = Vec::new();
    let mut open: Option<OpenRun> = None;
    for (position, member) in members.iter().enumerate() {
        if let Some(mut run) = open.take() {
            let candidate = chunk::join_text(&run.text, &member.targeting_text);
            let tokens = count_tokens(tokenizer, &candidate)?;
            if tokens <= max_tokens {
                run.end = position + 1;
                run.text = candidate;
                run.tokens = tokens;
                open = Some(run);
                continue;
            }
            // The member would overflow: the run closes as is, even below the
            // minimum, because a chunk is never split to top it up.
            runs.push(run);
        }
        let tokens = count_tokens(tokenizer, &member.targeting_text)?;
        if tokens > max_tokens {
            return Err(failure(format!(
                "chunk {} of parse {parse_id} measures {tokens} tokens alone, above indexing.context_max_tokens={max_tokens}; indexing.fine_max_tokens={} must keep every fine chunk within the context cap",
                member.id, indexing.fine_max_tokens
            )));
        }
        open = Some(OpenRun {
            start: position,
            end: position + 1,
            // The run's text is extended in place as members join; the member
            // keeps its own text for the chunk-keyed validation of the result.
            text: member.targeting_text.clone(),
            tokens,
        });
    }
    if let Some(run) = open.take() {
        // Document end: a final sub-minimum run merges backward when the
        // combined text fits the cap; otherwise it is the one permitted
        // sub-minimum run.
        let merge_target = if run.tokens < min_tokens {
            runs.last_mut()
        } else {
            None
        };
        match merge_target {
            Some(previous) => {
                let candidate = chunk::join_text(&previous.text, &run.text);
                let tokens = count_tokens(tokenizer, &candidate)?;
                if tokens <= max_tokens {
                    previous.end = run.end;
                    previous.text = candidate;
                    previous.tokens = tokens;
                } else {
                    runs.push(run);
                }
            }
            None => runs.push(run),
        }
    }
    runs.into_iter()
        .enumerate()
        .map(|(index, run)| window_from_run(members, parse_id, index, run))
        .collect()
}

/// Materialize one run as a window: chunk ids in order, fragments
/// concatenated, unit ids derived from the fragments, and the section path of
/// the first chunk. `section_id` is resolved afterwards on the transaction.
fn window_from_run(
    members: &[ContextMember],
    parse_id: &str,
    index: usize,
    run: OpenRun,
) -> Result<SectionDenseWindow, ApiError> {
    let span = members.get(run.start..run.end).ok_or_else(|| {
        failure(format!(
            "section window {index} of {parse_id} spans chunks the parse does not have"
        ))
    })?;
    let first = span.first().ok_or_else(|| {
        failure(format!(
            "section window {index} of {parse_id} has no chunks"
        ))
    })?;
    // The window is the persisted artifact; the members remain borrowed for
    // the post-build validation, so their ids, fragments, and path are copied.
    let fragments: Vec<Fragment> = span
        .iter()
        .flat_map(|member| member.fragments.iter().cloned())
        .collect();
    Ok(SectionDenseWindow {
        window_id: window_id(parse_id, index),
        section_id: None,
        section_path: first.section_path.clone(),
        chunk_ids: span.iter().map(|member| member.id.clone()).collect(),
        input_unit_ids: chunk::ordered_unit_ids(&fragments),
        targeting_text: run.text,
        token_count: run.tokens,
        vector: Vec::new(),
        norm: 0.0,
        fragments,
    })
}

/// Resolve each window's `section_id` from its first fragment's unit on the
/// caller's transaction, and require the resolved path to equal the stored
/// `section_path` of the first chunk (the chunker resolved it from the same
/// unit); validation never re-derives it.
fn resolve_section_ids(
    tx: &Transaction<'_>,
    parse_id: &str,
    mut windows: Vec<SectionDenseWindow>,
) -> Result<Vec<SectionDenseWindow>, ApiError> {
    for window in &mut windows {
        let first_unit = window
            .fragments
            .first()
            .map(|fragment| fragment.unit_id.as_str())
            .ok_or_else(|| {
                failure(format!(
                    "section window {} has no fragments",
                    window.window_id
                ))
            })?;
        let (section_id, section_path) = read_section(tx, parse_id, first_unit)?;
        if section_path != window.section_path {
            return Err(failure(format!(
                "section window {} first chunk's stored section path differs from the resolved path of unit {first_unit}",
                window.window_id
            )));
        }
        window.section_id = section_id;
    }
    Ok(windows)
}

/// Use the untruncated tokenizer including its special tokens for every window bound.
fn count_tokens(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoded| encoded.len())
        .map_err(|source| failure(format!("count section input tokens: {source}")))
}

/// Run the chunk-keyed checks over a fully decoded plane: consecutiveness,
/// content equality, and coverage of every member exactly once.
fn validate_members(members: &[ContextMember], plane: &SectionDensePlane) -> Result<(), ApiError> {
    let mut cursor = MemberCursor::new(members, plane.window_max_tokens());
    for window in &plane.windows {
        cursor.validate_window(window)?;
    }
    cursor.finish(&plane.parse_id)
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
            producer_version: Some(CONSTRUCTION_VERSION.to_string()),
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

/// Keep section build and canonical corruption context in the storage error flow.
fn failure(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}
