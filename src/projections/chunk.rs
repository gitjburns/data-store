//! C6b fine chunker: packs a parse's evidence members into `chunk_projections`
//! rows — the fine grain of PLAN-grains Section 2 — carrying the chunker
//! identity and `chunkerConfigHash` from `super::ChunkerConfig`; chunks
//! reference canonical ContentUnit IDs and never mint document-slug IDs (spec
//! §22–§23).
//!
//! What a chunk is (spec §23). A chunk is a retrieval TARGETING artifact, not
//! canonical evidence. It carries `targeting_text` — the run's canonical text,
//! which the lexical channel indexes as-is and the dense channel embeds behind
//! the section-path prefix (`model_input`) — the canonical ContentUnit IDs it
//! targets (§23 rule 2), and `fragments`: the scalar ranges of each unit's
//! evidence text the run is sliced from. Chunk hits are always resolved back
//! to those ContentUnits before an EvidencePack is built (§23 rule 3), so a
//! chunk never appears as evidence and its text may be freely rebuilt,
//! re-split, or deleted.
//!
//! SEMANTIC BOUNDARY (PRINCIPLES "data changes meaning across a pipeline").
//! `evidence_unit` is the seam where a ContentUnit's body text — which
//! upstream is CANONICAL EVIDENCE (§8.3) — becomes a member's text, a
//! REBUILDABLE targeting artifact (§23). The bytes may be identical, but the
//! meaning changes: after this seam the text is something the system aims
//! with, never something it is allowed to cite. `input_unit_ids` and
//! `fragments` are the bridge back to the canonical meaning, so they must
//! always point at the real source units and their real ranges.
//!
//! Evidence derivation (PLAN-grains Section 3) is applied ONCE here and every
//! higher grain inherits it: a `text_block` with role `heading` is not
//! evidence, all cells of one `table_row` form one tab-joined member, and
//! everything else is one member per unit. Packing (Section 2) never consults
//! section boundaries; the section path is metadata carried from a run's
//! first member for the model-input prefix only.
//!
//! Harvest lineage. The greedy token-cap accumulation and the
//! sentence → word → scalar-prefix splitting are harvested from the retired
//! `src/units.rs` splitter. Splits now operate on scalar offsets so every
//! split range is recorded as a fragment, and text is never normalized: the
//! canonical text is the exact member text, members joined by one blank line.

// Implemented by the C6b chunk package; `build_chunks` is consumed by the C6
// integration wiring (Phase 1 orders chunk → lexical → dense → multivector) and
// by the C6a/C6c builders that read chunks back. Until that wiring lands the
// function is unreferenced, so the module keeps a dead-code allow naming that
// consumer; do not remove `super`'s module-level allow.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::time::Instant;

use crate::artifact_store::ArtifactStore;
use crate::sqlite::Transaction;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::error::ApiError;
use crate::ids::new_retrieval_projection_id;
use crate::model::{ContentType, ProducerType, Provenance, TextBlockRole};
use crate::primitives::utc_now;
use crate::sections::read_section;

use super::envelope::{self, NewProjection, ProjectionType};
use super::{CHUNKER_NAME, CHUNKER_VERSION, ChunkerConfig};

/// Ordered SELECT of a parse's content units in reading order, mirroring
/// `crate::annotations::producer::SELECT_PARSE_UNITS_SQL`. Chunking depends on
/// this exact ordering: members are packed in the order units come back, so
/// the `input_unit_ids` and `fragments` a chunk records are deterministic
/// across rebuilds, and the cells of one table row arrive consecutively.
/// `primary_parent_id` is the `table_row` a `table_cell` belongs to.
/// `sequence_index` is nullable; NULLs sort last under SQLite ordering and the
/// `id` tiebreak keeps the order total. No deleted-row filter: `content_units`
/// rows are hard-deleted by hot cleanup (§31.2), never soft-deleted.
const SELECT_PARSE_UNITS_SQL: &str = "
SELECT id, content_type, primary_parent_id, body_json
FROM content_units
WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id";

/// Delete every prior `chunk_projections` row for a parse before a rebuild.
/// REBUILD ORDERING INVARIANT: this must run before the new chunk rows are
/// inserted, and the WHOLE chunk build must complete before the lexical (C6a)
/// and dense (C6c) builders run — they read their input back out of
/// `chunk_projections`, so a partial or stale chunk set would corrupt their
/// index. The integration agent (Phase 1) is responsible for that cross-builder
/// ordering; this delete only guarantees the per-parse chunk set is replaced
/// wholesale rather than accumulating duplicates across rebuilds.
const DELETE_PARSE_CHUNKS_SQL: &str = "
DELETE FROM chunk_projections WHERE parse_id = ?1";

/// Insert one `chunk_projections` row. Column order matches the schema.sql
/// DDL; `token_count` is bound as the §22 optional `tokenCount`, present here
/// because a tokenizer was supplied. `input_unit_ids_json`, `fragments_json`,
/// and `section_path_json` are canonical JSON (§16.2). `chunk_index` is the
/// chunk's 0-based reading-order position within the parse — the only
/// reading-order authority for chunks (ids and `created_at` carry no order).
const INSERT_CHUNK_SQL: &str = "
INSERT INTO chunk_projections (
  id, projection_id, source_id, parse_id, input_unit_ids_json,
  targeting_text, token_count, chunker_name, chunker_version,
  chunker_config_hash, created_at, fragments_json, section_path_json,
  chunk_index
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)";

/// Build (or rebuild) the chunk projections for one parse, returning the number
/// of chunk rows written. This is the CONTRACT the lexical (C6a) and dense
/// (C6c) builders and the integration wiring consume: after this returns Ok,
/// `chunk_projections` holds exactly this parse's current chunk set and the
/// Chunk envelope is `fresh`.
///
/// `tokenizer` supplies the ColBERT vocabulary and special-token convention.
/// A per-build copy disables truncation and padding to measure complete
/// canonical text without changing the inference runtime's settings.
///
/// Atomicity and lifecycle: everything runs on the CALLER's transaction. We
/// open a `building` Chunk envelope, delete prior chunk rows, derive the
/// parse's evidence members, pack them into runs, INSERT the chunks, and
/// complete the envelope `fresh` with an immutable construction descriptor.
/// Chunk rows stay in the hot plane; the descriptor preserves historical
/// settings. Any failure marks the envelope `failed` on the SAME transaction
/// so the envelope's lifecycle and the audit event commit or roll back
/// together.
pub(crate) fn build_chunks(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    tokenizer: &Tokenizer,
    store: &ArtifactStore,
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<usize, ApiError> {
    let started = Instant::now();
    info!(
        event = "chunk_build.begin",
        source_id, parse_id, "building chunk projections for parse"
    );

    let config = ChunkerConfig::active(&tx.limits().indexing, tokenizer)?;
    let config_hash = config.config_hash()?;
    let descriptor_bytes = crate::canonical::canonical_json_bytes_of(&config)?;
    if descriptor_bytes.len() > store.limits().resources.max_json_cell_bytes {
        return Err(ApiError::StorageOperation {
            message: "resource limit: chunk construction descriptor exceeds configured JSON bytes"
                .to_owned(),
        });
    }
    let descriptor = store.put_bytes(&descriptor_bytes)?;
    let projection_id =
        envelope::insert_building(tx, &new_chunk_projection(source_id, parse_id, &config_hash))?;

    // Everything from here is fallible; on any error mark the envelope failed on
    // the same transaction, then propagate. `run_build` owns the actual work so
    // this one site handles the failure lifecycle for every failure path.
    match run_build(
        tx,
        source_id,
        parse_id,
        &projection_id,
        tokenizer,
        &config,
        monitor,
    ) {
        Ok(chunk_count) => {
            // Publish descriptor ownership with the rows in the same transaction.
            envelope::complete_fresh(tx, &projection_id, Some(&descriptor.uri))?;
            info!(
                event = "chunk_build.success",
                // The enclosing owner reports durability after its commit.
                persistence = "pending_commit",
                source_id,
                parse_id,
                projection_id = %projection_id,
                chunk_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "chunk projections built"
            );
            Ok(chunk_count)
        }
        Err(error) => {
            error!(
                event = "chunk_build.failure",
                error = %error,
                source_id,
                parse_id,
                projection_id = %projection_id,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "chunk projection build failed"
            );
            // Record the failure lifecycle on the caller's transaction. The
            // build error is the one returned; the mark_failed detail is a
            // bounded summary, and a mark_failed error (should the guard fail)
            // supersedes only because it means the envelope is itself corrupt.
            envelope::mark_failed(tx, &projection_id, &error.to_string())?;
            Err(error)
        }
    }
}

/// The fallible body of `build_chunks`, split out so its single caller can wrap
/// every failure path in the envelope `failed` transition. Derives the parse's
/// ordered evidence members, packs them under the token cap, and inserts the
/// chunk rows; returns the number of chunks written.
fn run_build(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    tokenizer: &Tokenizer,
    config: &ChunkerConfig,
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<usize, ApiError> {
    // Rebuild idempotence: replace this parse's chunk set wholesale. Must
    // precede the inserts below (see DELETE_PARSE_CHUNKS_SQL invariant).
    tx.execute(DELETE_PARSE_CHUNKS_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to clear prior chunks for parse {parse_id}: {source}"),
        })?;

    let members = read_parse_members(tx, parse_id)?;
    if let Some(monitor) = monitor {
        monitor.stage(
            "pack evidence members into chunks",
            Some(members.len() as u64),
            "members placed",
        );
    }
    let chunks = pack_members(members, tokenizer, config, monitor)?;

    // The same construction descriptor governs packing, the envelope, and
    // every chunk row; changing a boundary parameter changes all their hashes.
    let chunker_config_hash = config.config_hash()?;
    let now = utc_now()?;

    if let Some(monitor) = monitor {
        monitor.stage(
            "stage chunk rows",
            Some(chunks.len() as u64),
            "chunks staged",
        );
    }
    // `pack_members` returns chunks in reading order, so the enumeration index
    // is the persisted `chunk_index`.
    for (index, chunk) in chunks.iter().enumerate() {
        insert_chunk(
            tx,
            projection_id,
            source_id,
            parse_id,
            chunk,
            index,
            &chunker_config_hash,
            &now,
        )?;
        if let Some(monitor) = monitor {
            monitor.progress((index + 1) as u64, Some(chunks.len() as u64));
        }
    }

    Ok(chunks.len())
}

/// Assemble the `NewProjection` request that opens this build's Chunk envelope.
/// The producer is a Rule (the deterministic chunker, not a model): its name
/// and version are the recorded chunker identity, and its `config_hash` is the
/// active `ChunkerConfig` hash, so the envelope's provenance answers "which
/// chunker produced this" without reading a payload row. `input_unit_ids` is
/// left None on the envelope: the per-chunk `input_unit_ids` are the meaningful
/// §23 rule-2 links and live on each `chunk_projections` row, whereas a single
/// envelope-level union would flatten which chunk targets which unit.
fn new_chunk_projection(source_id: &str, parse_id: &str, config_hash: &str) -> NewProjection {
    NewProjection {
        source_id: source_id.to_string(),
        parse_id: parse_id.to_string(),
        projection_type: ProjectionType::Chunk,
        input_unit_ids: None,
        input_annotation_ids: None,
        producer: chunker_provenance(config_hash),
        index_name: None,
        index_partition: None,
    }
}

/// §20 provenance for the chunk producer: a deterministic Rule producer stamped
/// with the recorded chunker name/version and the active config hash. No model,
/// prompt, or input refs — the chunker is not a model and its per-chunk input
/// lineage lives on the chunk rows, not the envelope provenance.
fn chunker_provenance(config_hash: &str) -> Provenance {
    Provenance {
        producer_type: ProducerType::Rule,
        producer_name: CHUNKER_NAME.to_string(),
        producer_version: Some(CHUNKER_VERSION.to_string()),
        // Envelope and rows share the exact hash computed once for this build.
        config_hash: Some(config_hash.to_owned()),
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: None,
    }
}

/// One membership record (PLAN-grains Section 2): `start_char..end_char` are
/// Unicode scalar offsets over the evidence text of `unit_id`, end exclusive.
/// Byte offsets are never stored. Serialized camelCase as
/// `{ unitId, startChar, endChar }` into `chunk_projections.fragments_json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Fragment {
    pub(crate) unit_id: String,
    pub(crate) start_char: usize,
    pub(crate) end_char: usize,
}

/// Model input for a chunk (PLAN-grains Section 2): the section path joined by
/// " / ", one blank line, then the canonical text; the canonical text alone
/// when the path is empty. The prefix is never part of any range or hash —
/// callers embed this and persist `canonical`.
pub(crate) fn model_input(section_path: &[String], canonical: &str) -> String {
    if section_path.is_empty() {
        canonical.to_owned()
    } else {
        format!("{}\n\n{canonical}", section_path.join(" / "))
    }
}

/// One assembled chunk before persistence: the canonical text (member texts
/// joined by one blank line), the ordered, deduplicated source ContentUnit IDs
/// it targets (§23 rule 2), the fragments it is sliced from in order, the
/// section path of its first member, and its token count measured on exactly
/// the canonical text (§22 tokenCount).
#[derive(Debug)]
struct BuiltChunk {
    input_unit_ids: Vec<String>,
    targeting_text: String,
    token_count: usize,
    fragments: Vec<Fragment>,
    section_path: Vec<String>,
}

/// One packing member (PLAN-grains Section 3): its unit ids in first-occurrence
/// order, its text (one unit's evidence text, or a table row's cell texts
/// joined by one tab), the fragments that text is sliced from, and the section
/// path of its first unit. The text is never whitespace-only; both halves of a
/// split member are members again.
#[derive(Debug)]
struct ChunkMember {
    unit_ids: Vec<String>,
    text: String,
    fragments: Vec<MemberFragment>,
    section_path: Vec<String>,
}

/// A fragment together with the scalar offset at which its text starts inside
/// the owning member's `text`, so a split of the member text maps back onto
/// unit ranges without assuming how the member text was assembled.
#[derive(Debug)]
struct MemberFragment {
    fragment: Fragment,
    offset: usize,
}

impl ChunkMember {
    /// A member whose unit ids are derived from its fragments, so the id list
    /// and the fragment list can never disagree.
    fn new(text: String, fragments: Vec<MemberFragment>, section_path: Vec<String>) -> Self {
        Self {
            unit_ids: ordered_unit_ids(fragments.iter().map(|entry| &entry.fragment)),
            text,
            fragments,
            section_path,
        }
    }

    /// A member over one whole unit: one fragment `0..len` in scalar offsets.
    fn single(unit_id: String, text: String, section_path: Vec<String>) -> Self {
        let end_char = text.chars().count();
        let fragment = MemberFragment {
            fragment: Fragment {
                unit_id,
                start_char: 0,
                end_char,
            },
            offset: 0,
        };
        Self::new(text, vec![fragment], section_path)
    }

    /// The sub-member over `start..end` (scalar offsets of `text`): its text is
    /// the exact slice, and each fragment is narrowed to the part of its unit
    /// range the slice covers. Fragments the slice does not touch are dropped,
    /// so a split range never records a unit it holds no text of.
    fn slice(&self, start: usize, end: usize) -> Self {
        let mut fragments = Vec::new();
        for entry in &self.fragments {
            let length = entry.fragment.end_char - entry.fragment.start_char;
            let from = start.max(entry.offset);
            let to = end.min(entry.offset + length);
            if from >= to {
                continue;
            }
            fragments.push(MemberFragment {
                fragment: Fragment {
                    // Each half needs its own owned record of the shared unit.
                    unit_id: entry.fragment.unit_id.clone(),
                    start_char: entry.fragment.start_char + (from - entry.offset),
                    end_char: entry.fragment.start_char + (to - entry.offset),
                },
                offset: from - start,
            });
        }
        // Both halves of a split carry the same section metadata.
        Self::new(
            slice_chars(&self.text, start, end),
            fragments,
            self.section_path.clone(),
        )
    }
}

/// Unit ids in first-occurrence order without duplicates: a unit listed by
/// several fragments (a split unit whose halves landed in one run) appears once.
/// Shared with the higher grains so every run derives its unit ids from its
/// fragments by one rule.
pub(crate) fn ordered_unit_ids<'a>(
    fragments: impl IntoIterator<Item = &'a Fragment>,
) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for fragment in fragments {
        if !ids.iter().any(|id| id == &fragment.unit_id) {
            ids.push(fragment.unit_id.clone());
        }
    }
    ids
}

/// The scalar range `start..end` of `text` as an owned string.
fn slice_chars(text: &str, start: usize, end: usize) -> String {
    text.chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

/// Read one parse's content units in reading order and derive its evidence
/// members (Section 3): headings are dropped, the consecutive cells of one
/// `table_row` are gathered into one member, every other evidence unit is one
/// member. Each member's section path is resolved through the shared
/// `crate::sections::read_section` walk for its first unit. Bounded by the
/// parse's unit count.
fn read_parse_members(tx: &Transaction<'_>, parse_id: &str) -> Result<Vec<ChunkMember>, ApiError> {
    let mut statement =
        tx.prepare(SELECT_PARSE_UNITS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare parse-units query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok(UnitRow {
                id: row.get(0)?,
                content_type: row.get(1)?,
                primary_parent_id: row.get(2)?,
                body_json: row.get(3)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query units for parse {parse_id}: {source}"),
        })?;

    let mut members = Vec::new();
    let mut open_row: Option<OpenRow> = None;
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit row for parse {parse_id}: {source}"),
        })?;
        let Some(unit) = evidence_unit(row)? else {
            continue;
        };
        match unit.table_row {
            Some(row_id) => match open_row.take() {
                Some(mut open) if open.row_id == row_id => {
                    open.push_cell(unit.id, unit.text);
                    open_row = Some(open);
                }
                previous => {
                    if let Some(previous) = previous {
                        members.push(finish_row(tx, parse_id, previous)?);
                    }
                    open_row = Some(OpenRow::start(row_id, unit.id, unit.text));
                }
            },
            None => {
                // Any non-cell unit ends the row being gathered; cells of one
                // row are consecutive in reading order.
                if let Some(previous) = open_row.take() {
                    members.push(finish_row(tx, parse_id, previous)?);
                }
                let (_, section_path) = read_section(tx, parse_id, &unit.id)?;
                members.push(ChunkMember::single(unit.id, unit.text, section_path));
            }
        }
    }
    if let Some(previous) = open_row.take() {
        members.push(finish_row(tx, parse_id, previous)?);
    }
    Ok(members)
}

/// The `table_row` member being gathered: its row id, the cell texts joined so
/// far by one tab, and one fragment per cell covering that cell's whole text.
struct OpenRow {
    row_id: String,
    text: String,
    fragments: Vec<MemberFragment>,
}

impl OpenRow {
    /// Open a row with its first cell at offset 0.
    fn start(row_id: String, cell_id: String, text: String) -> Self {
        let end_char = text.chars().count();
        Self {
            row_id,
            text,
            fragments: vec![MemberFragment {
                fragment: Fragment {
                    unit_id: cell_id,
                    start_char: 0,
                    end_char,
                },
                offset: 0,
            }],
        }
    }

    /// Append one cell after a tab; its fragment covers the cell's whole text
    /// at the offset where it lands in the joined row text.
    fn push_cell(&mut self, cell_id: String, text: String) {
        self.text.push('\t');
        let offset = self.text.chars().count();
        let end_char = text.chars().count();
        self.text.push_str(&text);
        self.fragments.push(MemberFragment {
            fragment: Fragment {
                unit_id: cell_id,
                start_char: 0,
                end_char,
            },
            offset,
        });
    }
}

/// Close a gathered row into its member. The section path is the first cell's
/// (Section 2: a run carries the path of its first member).
fn finish_row(tx: &Transaction<'_>, parse_id: &str, row: OpenRow) -> Result<ChunkMember, ApiError> {
    let first_cell = row
        .fragments
        .first()
        .map(|entry| entry.fragment.unit_id.as_str())
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "table row {} of parse {parse_id} gathered no cells",
                row.row_id
            ),
        })?;
    let (_, section_path) = read_section(tx, parse_id, first_cell)?;
    Ok(ChunkMember::new(row.text, row.fragments, section_path))
}

/// The `content_units` columns member derivation reads: id for the
/// `input_unit_ids` link, content_type to select the body's text field,
/// primary_parent_id to gather a table row's cells, and body_json for the
/// typed body.
struct UnitRow {
    id: String,
    content_type: String,
    primary_parent_id: Option<String>,
    body_json: String,
}

/// One evidence unit after Section 3 derivation: its id, its evidence text, and
/// — for a `table_cell` — the `table_row` it belongs to.
struct EvidenceUnit {
    id: String,
    text: String,
    table_row: Option<String>,
}

/// SEMANTIC SEAM (see module doc): re-type one persisted unit row and pull the
/// evidence text out of its typed body, or None when the unit contributes no
/// member. Past this point the returned string is targeting text (§23), not
/// canonical evidence, even though the bytes came from a canonical unit body.
///
/// Which types contribute text (the SPEC-epub §2.1 evidence-bearing set):
///   - text_block  -> body.text, except role `heading`, which is not evidence
///     (Section 3) and reaches the chunk only through the section path;
///   - caption     -> body.text;
///   - table_cell  -> body.text, gathered per `table_row` by the caller — a
///     cell without a `table_row` parent is corruption (the EPUB emitter
///     always parents cells under their row);
///   - code_block  -> body.code (§2.2 text projection).
///
/// Skipped types (document, page, text_section, list, list_item, aside, table,
/// table_row, figure) are structural containers or markers with no direct
/// text; a text_section is a CONTAINER (§15.2) whose child text_blocks carry
/// the paragraph text. Whitespace-only text contributes no member. A
/// content_type outside the schema set, or an unparseable body, fails loudly
/// (the same corruption discipline as the annotation producer's `plan_unit`).
/// Field selection must stay aligned with the remaining per-unit evidence
/// readers, `assembly::evidence::evidence_text` and `projections::view`.
fn evidence_unit(row: UnitRow) -> Result<Option<EvidenceUnit>, ApiError> {
    let content_type: ContentType = serde_json::from_value(Value::String(row.content_type.clone()))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted content unit {} type {:?} is not a known variant: {source}",
                row.id, row.content_type
            ),
        })?;
    let body: Value =
        serde_json::from_str(&row.body_json).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted body of content unit {} is unparseable: {source}",
                row.id
            ),
        })?;

    let text = match content_type {
        ContentType::TextBlock => {
            if text_block_role(&row.id, &body)? == TextBlockRole::Heading {
                return Ok(None);
            }
            string_field(&body, "text")
        }
        ContentType::Caption | ContentType::TableCell => string_field(&body, "text"),
        ContentType::CodeBlock => string_field(&body, "code"),
        ContentType::Document
        | ContentType::Page
        | ContentType::TextSection
        | ContentType::List
        | ContentType::ListItem
        | ContentType::Aside
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure => None,
    };
    let Some(text) = text else {
        return Ok(None);
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    let table_row = match content_type {
        ContentType::TableCell => {
            Some(
                row.primary_parent_id
                    .ok_or_else(|| ApiError::StorageOperation {
                        message: format!("persisted table cell {} has no table_row parent", row.id),
                    })?,
            )
        }
        _ => None,
    };
    Ok(Some(EvidenceUnit {
        id: row.id,
        text,
        table_row,
    }))
}

/// The required `role` of a `text_block` body (SPEC-epub §2.2); a missing or
/// unknown role is corruption surfaced with the unit's identity.
fn text_block_role(unit_id: &str, body: &Value) -> Result<TextBlockRole, ApiError> {
    let role = body.get("role").ok_or_else(|| ApiError::StorageOperation {
        message: format!("persisted text block {unit_id} has no role"),
    })?;
    TextBlockRole::deserialize(role).map_err(|source| ApiError::StorageOperation {
        message: format!("persisted text block {unit_id} role is not a known variant: {source}"),
    })
}

/// Read one string field from a unit body, returning None when the field is
/// absent or not a JSON string.
fn string_field(body: &Value, field: &str) -> Option<String> {
    body.get(field).and_then(Value::as_str).map(str::to_string)
}

/// Pack members into runs under the token cap with the PLAN-grains Section 2
/// minimum-fill rule; returns the built chunks in reading order.
///
/// Packing invariants, in the order the loop applies them:
///   - a member that alone exceeds the cap is split (`split_to_fit`) and its
///     remainder continues as the next member;
///   - a member joins the open run while the EXACT joined canonical text fits
///     the cap (tokenization is not additive across joins, so every candidate
///     is measured whole and every stored count describes the final text);
///   - when it would overflow and the open run has reached the minimum, the
///     run closes and the member restarts the loop with no open run;
///   - when it would overflow and the open run is below the minimum, the
///     member is split so the run reaches at least the minimum, the run
///     closes with the head, and the remainder continues as the next member;
///   - at document end a final run below the minimum merges into the
///     preceding run when the combined text fits the cap; otherwise it stays
///     as the one permitted sub-minimum run.
///
/// Runs never consult section boundaries.
fn pack_members(
    members: Vec<ChunkMember>,
    tokenizer: &Tokenizer,
    config: &ChunkerConfig,
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<Vec<BuiltChunk>, ApiError> {
    // Length accounting must see the complete input, independently of inference
    // truncation/padding settings. Keep the shared model tokenizer unchanged.
    let mut counter = tokenizer.clone();
    counter
        .with_truncation(None)
        .map_err(|source| ApiError::UnitSplitting {
            message: format!("failed to disable chunk tokenizer truncation: {source}"),
        })?;
    counter.with_padding(None);
    let tokenizer = &counter;
    let max_tokens = config.max_unit_tokens as usize;
    let Some(ratio) = config.min_fill_ratio else {
        return Err(ApiError::UnitSplitting {
            message: "chunk construction policy carries no minimum fill ratio".to_owned(),
        });
    };
    // The minimum is computed once, floored, so every run in the build is held
    // to one integer bound.
    let min_tokens = (max_tokens as f64 * ratio).floor() as usize;

    let total_members = members.len() as u64;
    // A member counts as placed once all of its text is in a run; a remainder
    // pushed back to the front of the queue is placed later, under the same
    // count, so progress reaches the member total exactly once.
    let mut placed = 0u64;
    let mut pending: VecDeque<ChunkMember> = members.into();
    let mut runs: Vec<OpenRun> = Vec::new();
    let mut open: Option<OpenRun> = None;

    while let Some(member) = pending.pop_front() {
        match open.take() {
            None => {
                let tokens = count_tokens(tokenizer, &member.text)?;
                if tokens <= max_tokens {
                    open = Some(OpenRun::start(member, tokens));
                    placed += 1;
                } else {
                    // A member that alone exceeds the cap is split the same way
                    // as one that would overflow an open run.
                    let split = split_to_fit(None, &member, tokenizer, max_tokens, min_tokens)?;
                    open = Some(OpenRun::start(split.head, split.tokens));
                    match split.tail {
                        Some(tail) => pending.push_front(tail),
                        None => placed += 1,
                    }
                }
            }
            Some(mut run) => {
                let candidate = join_text(&run.text, &member.text);
                let tokens = count_tokens(tokenizer, &candidate)?;
                if tokens <= max_tokens {
                    run.append(member, candidate, tokens);
                    open = Some(run);
                    placed += 1;
                } else if run.tokens >= min_tokens {
                    // Full enough to close whole; the member restarts the loop.
                    runs.push(run);
                    pending.push_front(member);
                } else {
                    // Minimum fill: split the member so this run reaches at
                    // least the minimum, then close the run with the head.
                    let split =
                        split_to_fit(Some(&run.text), &member, tokenizer, max_tokens, min_tokens)?;
                    let text = join_text(&run.text, &split.head.text);
                    run.append(split.head, text, split.tokens);
                    runs.push(run);
                    match split.tail {
                        Some(tail) => pending.push_front(tail),
                        None => placed += 1,
                    }
                }
            }
        }
        if let Some(monitor) = monitor {
            monitor.progress(placed, Some(total_members));
        }
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
                let candidate = join_text(&previous.text, &run.text);
                let tokens = count_tokens(tokenizer, &candidate)?;
                if tokens <= max_tokens {
                    previous.absorb(run, candidate, tokens);
                } else {
                    runs.push(run);
                }
            }
            None => runs.push(run),
        }
    }

    // Every stored count was measured on exactly the run's final text, so this
    // is an invariant check on the packing above, not a re-measurement.
    let chunks: Vec<BuiltChunk> = runs.into_iter().map(OpenRun::into_chunk).collect();
    for chunk in &chunks {
        if chunk.token_count > max_tokens {
            return Err(ApiError::UnitSplitting {
                message: format!(
                    "chunk starting at unit {:?} has {} tokens, exceeding {max_tokens}",
                    chunk.input_unit_ids.first(),
                    chunk.token_count
                ),
            });
        }
    }
    Ok(chunks)
}

/// The run being packed: its placed members in order, its canonical text
/// (member texts joined by one blank line), and the token count measured on
/// exactly that text — every path that changes `text` measures the new text
/// whole before storing it.
struct OpenRun {
    members: Vec<PlacedMember>,
    text: String,
    tokens: usize,
}

/// A member after its text has been joined into a run: what the chunk still
/// needs from it (fragments and the section path); its unit ids are derived
/// from the run's fragments at chunk time.
struct PlacedMember {
    fragments: Vec<MemberFragment>,
    section_path: Vec<String>,
}

impl OpenRun {
    /// Open a run whose text is its first member's text, measured by the caller.
    fn start(member: ChunkMember, tokens: usize) -> Self {
        let ChunkMember {
            text,
            fragments,
            section_path,
            ..
        } = member;
        Self {
            members: vec![PlacedMember {
                fragments,
                section_path,
            }],
            text,
            tokens,
        }
    }

    /// Append a member whose joined text and token count the caller already
    /// measured (`text` is `join_text(self.text, member.text)`).
    fn append(&mut self, member: ChunkMember, text: String, tokens: usize) {
        let ChunkMember {
            fragments,
            section_path,
            ..
        } = member;
        self.members.push(PlacedMember {
            fragments,
            section_path,
        });
        self.text = text;
        self.tokens = tokens;
    }

    /// Merge a following run into this one (document-end rule); `text` and
    /// `tokens` are the measured combined text.
    fn absorb(&mut self, run: OpenRun, text: String, tokens: usize) {
        self.members.extend(run.members);
        self.text = text;
        self.tokens = tokens;
    }

    /// Finish the run: fragments are the members' fragments in order, unit ids
    /// are derived from them, and the section path is the first member's.
    fn into_chunk(mut self) -> BuiltChunk {
        let section_path = match self.members.first_mut() {
            Some(first) => std::mem::take(&mut first.section_path),
            None => Vec::new(),
        };
        let fragments: Vec<Fragment> = self
            .members
            .into_iter()
            .flat_map(|member| member.fragments.into_iter().map(|entry| entry.fragment))
            .collect();
        BuiltChunk {
            input_unit_ids: ordered_unit_ids(fragments.iter()),
            targeting_text: self.text,
            token_count: self.tokens,
            fragments,
            section_path,
        }
    }
}

/// One blank line between member texts is the canonical join (Section 2); it
/// is part of the measured and persisted text, never collapsed. Shared with
/// the higher grains, whose canonical text is member texts joined the same way.
pub(crate) fn join_text(left: &str, right: &str) -> String {
    format!("{left}\n\n{right}")
}

/// The result of splitting one member at a measured boundary: the head that
/// fits, the token count of the run text that includes it (the open text
/// joined with the head, or the head alone), and the remainder that continues
/// as the next member — None when the whole member fit after trimming.
struct Split {
    head: ChunkMember,
    tokens: usize,
    tail: Option<ChunkMember>,
}

/// A fitting prefix of a piece list: the index of the last piece taken and the
/// measured token count of the run text that includes it.
#[derive(Clone, Copy)]
struct Fit {
    index: usize,
    tokens: usize,
}

/// Split `member` at a measured boundary so the run text (`open_text` joined
/// with the head, or the head alone) fits the cap (Section 2): sentence
/// boundaries first, then words. A level is accepted when its longest fitting
/// prefix also brings the run to the minimum. When neither does, the longest
/// fitting word prefix is extended into the next word by measured scalar
/// prefixes and accepted at any nonempty fit; a cap that fits not even one
/// character is an error, as in the retired splitter.
fn split_to_fit(
    open_text: Option<&str>,
    member: &ChunkMember,
    tokenizer: &Tokenizer,
    max_tokens: usize,
    min_tokens: usize,
) -> Result<Split, ApiError> {
    let chars: Vec<char> = member.text.chars().collect();
    let sentences = sentence_pieces(&chars);
    if let Some(fit) = greedy_fit(open_text, member, &sentences, tokenizer, max_tokens)?
        && fit.tokens >= min_tokens
    {
        return Ok(split_at_piece(member, &sentences, fit));
    }
    let words = word_pieces(&chars);
    let word_fit = greedy_fit(open_text, member, &words, tokenizer, max_tokens)?;
    if let Some(fit) = word_fit
        && fit.tokens >= min_tokens
    {
        return Ok(split_at_piece(member, &words, fit));
    }
    let (Some(&(first_start, _)), Some(&(_, last_end))) = (words.first(), words.last()) else {
        return Err(ApiError::UnitSplitting {
            message: format!(
                "member starting at unit {:?} has no text to split",
                member.unit_ids.first()
            ),
        });
    };
    // Scalar level: the fitting words (if any) are kept and the cut lands
    // inside the next word.
    let (base_end, next_word) = match word_fit {
        Some(fit) => (words[fit.index].1, words.get(fit.index + 1).copied()),
        None => (first_start, words.first().copied()),
    };
    let Some((word_start, word_end)) = next_word else {
        // Every word fit yet the run stays below the minimum: the trimmed
        // member is consumed whole and nothing remains to split.
        let tokens = measure_head(open_text, member, first_start, base_end, tokenizer)?;
        return Ok(Split {
            head: member.slice(first_start, base_end),
            tokens,
            tail: None,
        });
    };
    // The whole next word is known not to fit; search only shorter, nonempty
    // prefixes of it. Token counts need not be monotone: remembering only
    // measured fits may underfill the run, but never permits an oversized one.
    let mut low = word_start + 1;
    let mut high = word_end;
    let mut fitting: Option<(usize, usize)> = None;
    while low < high {
        let middle = low + (high - low) / 2;
        let tokens = measure_head(open_text, member, first_start, middle, tokenizer)?;
        if tokens <= max_tokens {
            fitting = Some((middle, tokens));
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let Some((cut, tokens)) = fitting else {
        return Err(ApiError::UnitSplitting {
            message: format!(
                "could not find a nonempty scalar prefix within the {max_tokens}-token cap for unit {:?} at char {word_start}",
                member.unit_ids.first()
            ),
        });
    };
    Ok(Split {
        head: member.slice(first_start, cut),
        tokens,
        tail: Some(member.slice(cut, last_end)),
    })
}

/// The longest run of leading pieces whose head (from the first piece's start
/// to the taken piece's end) fits the cap after `open_text`. Stops at the first
/// piece that does not fit — tokenization is not additive across joins, so each
/// candidate is measured whole and no piece past a failing one is tried. None
/// when even the first piece does not fit.
fn greedy_fit(
    open_text: Option<&str>,
    member: &ChunkMember,
    pieces: &[(usize, usize)],
    tokenizer: &Tokenizer,
    max_tokens: usize,
) -> Result<Option<Fit>, ApiError> {
    let Some(&(start, _)) = pieces.first() else {
        return Ok(None);
    };
    let mut best = None;
    for (index, &(_, end)) in pieces.iter().enumerate() {
        let tokens = measure_head(open_text, member, start, end, tokenizer)?;
        if tokens > max_tokens {
            break;
        }
        best = Some(Fit { index, tokens });
    }
    Ok(best)
}

/// Token count of the run text that would result from taking `start..end` of
/// the member: the open text joined with that slice, or the slice alone.
fn measure_head(
    open_text: Option<&str>,
    member: &ChunkMember,
    start: usize,
    end: usize,
    tokenizer: &Tokenizer,
) -> Result<usize, ApiError> {
    let head = slice_chars(&member.text, start, end);
    let candidate = match open_text {
        Some(open) => join_text(open, &head),
        None => head,
    };
    count_tokens(tokenizer, &candidate)
}

/// Split the member after piece `fit.index`: the head spans the first piece's
/// start to that piece's end; the tail starts at the next piece and ends at the
/// last piece's end, or is None when every piece was taken.
fn split_at_piece(member: &ChunkMember, pieces: &[(usize, usize)], fit: Fit) -> Split {
    let start = pieces[0].0;
    let head = member.slice(start, pieces[fit.index].1);
    let tail = match (pieces.get(fit.index + 1), pieces.last()) {
        (Some(&(next_start, _)), Some(&(_, last_end))) => Some(member.slice(next_start, last_end)),
        _ => None,
    };
    Split {
        head,
        tokens: fit.tokens,
        tail,
    }
}

/// Sentence-like pieces as trimmed scalar ranges: a piece ends after '.', '?',
/// '!', or '\n' (the retired splitter's rule); whitespace-only pieces are
/// dropped; text without any terminator is one piece.
fn sentence_pieces(chars: &[char]) -> Vec<(usize, usize)> {
    let mut pieces = Vec::new();
    let mut start = 0;
    for (index, value) in chars.iter().enumerate() {
        if matches!(value, '.' | '?' | '!' | '\n') {
            if let Some(piece) = trimmed_range(chars, start, index + 1) {
                pieces.push(piece);
            }
            start = index + 1;
        }
    }
    if let Some(piece) = trimmed_range(chars, start, chars.len()) {
        pieces.push(piece);
    }
    pieces
}

/// Whitespace-separated words as scalar ranges.
fn word_pieces(chars: &[char]) -> Vec<(usize, usize)> {
    let mut pieces = Vec::new();
    let mut start = None;
    for (index, value) in chars.iter().enumerate() {
        match (value.is_whitespace(), start) {
            (true, Some(word_start)) => {
                pieces.push((word_start, index));
                start = None;
            }
            (false, None) => start = Some(index),
            _ => {}
        }
    }
    if let Some(word_start) = start {
        pieces.push((word_start, chars.len()));
    }
    pieces
}

/// `start..end` narrowed past leading and trailing whitespace, or None when
/// nothing remains.
fn trimmed_range(chars: &[char], mut start: usize, mut end: usize) -> Option<(usize, usize)> {
    while start < end && chars[start].is_whitespace() {
        start += 1;
    }
    while end > start && chars[end - 1].is_whitespace() {
        end -= 1;
    }
    (start < end).then_some((start, end))
}

/// Measure the exact text, including special tokens, with the build's
/// untruncated, unpadded counter. Candidate decisions and stored counts must
/// describe the same bytes; tokenization is not additive across joins.
fn count_tokens(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoding| encoding.len())
        .map_err(|source| ApiError::UnitSplitting {
            message: format!("tokenization failed during chunk splitting: {source}"),
        })
}

/// Insert one built chunk as a `chunk_projections` row under this build's
/// envelope. The row's `id` is a fresh `proj_` id (chunk rows and their
/// envelope share the id space but are distinct rows); `projection_id` links
/// back to the envelope so freshness stays single-sourced there.
/// `input_unit_ids`, `fragments`, and `section_path` are canonical JSON per
/// §16.2, matching how the envelope stores id arrays. `chunk_index` is the
/// chunk's 0-based reading-order position within the parse.
// Every argument is a distinct row column or the transaction; bundling them
// would only hide which column each value binds to.
#[allow(clippy::too_many_arguments)]
fn insert_chunk(
    tx: &Transaction<'_>,
    projection_id: &str,
    source_id: &str,
    parse_id: &str,
    chunk: &BuiltChunk,
    chunk_index: usize,
    chunker_config_hash: &str,
    created_at: &str,
) -> Result<(), ApiError> {
    let id = new_retrieval_projection_id()?;
    let input_unit_ids_json = canonical_json_string(
        &chunk.input_unit_ids,
        &format!("input unit ids for chunk {id}"),
    )?;
    let fragments_json =
        canonical_json_string(&chunk.fragments, &format!("fragments for chunk {id}"))?;
    let section_path_json =
        canonical_json_string(&chunk.section_path, &format!("section path for chunk {id}"))?;
    // token_count is stored as the §22 optional tokenCount; usize → i64 for the
    // INTEGER column (chunk token counts and positions are far below i64::MAX).
    let token_count = chunk.token_count as i64;
    let chunk_index = chunk_index as i64;

    tx.execute(
        INSERT_CHUNK_SQL,
        params![
            id,
            projection_id,
            source_id,
            parse_id,
            input_unit_ids_json,
            chunk.targeting_text,
            token_count,
            CHUNKER_NAME,
            CHUNKER_VERSION,
            chunker_config_hash,
            created_at,
            fragments_json,
            section_path_json,
            chunk_index,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to insert chunk {id} for parse {parse_id}: {source}"),
    })?;
    Ok(())
}

/// Render a model shape as a canonical JSON string (§16.2 deterministic bytes),
/// matching how `envelope::canonical_json_string_of` stores its id arrays so a
/// chunk's JSON columns and the envelope's id columns share one encoding.
fn canonical_json_string<T: serde::Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}
