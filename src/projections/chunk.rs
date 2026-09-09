//! C6b chunk builder: splits (text, unit-id context) into `chunk_projections`
//! rows carrying the chunker identity and `chunkerConfigHash` from
//! `super::ChunkerConfig`; chunks reference canonical ContentUnit IDs and
//! never mint document-slug IDs (spec §22–§23).
//!
//! What a chunk is (spec §23). A chunk is a retrieval TARGETING artifact, not
//! canonical evidence. It carries the exact `targeting_text` the lexical and
//! dense channels index, and the set of canonical ContentUnit IDs it targets
//! (§23 rule 2). Chunk hits are always resolved back to those ContentUnits
//! before an EvidencePack is built (§23 rule 3), so a chunk never appears as
//! evidence and its text may be freely rebuilt, re-split, or deleted.
//!
//! SEMANTIC BOUNDARY (PRINCIPLES "data changes meaning across a pipeline").
//! `extract_targeting_text` is the seam where a ContentUnit's body text — which
//! upstream is CANONICAL EVIDENCE (§8.3) — becomes a chunk's `targeting_text`,
//! a REBUILDABLE targeting artifact (§23). The bytes may be identical, but the
//! meaning changes: after this seam the text is something the system aims with,
//! never something it is allowed to cite. `input_unit_ids` is the bridge back
//! to the canonical meaning, so it must always point at the real source units.
//!
//! Harvest lineage. The greedy token-cap accumulation, the oversized-block
//! sentence/word splitting, the min-char drop, and the `tokenizers::Tokenizer`
//! token counting are harvested from the retired `src/units.rs` splitter. What
//! changed: the input is a parse's ordered canonical ContentUnits read from the
//! hot plane (canonical unit content, not a rendered markdown string), each produced
//! chunk records the SOURCE ContentUnit IDs it targets (not a minted
//! document-slug id), and no heading/page metadata is carried — a chunk's only
//! canonical link is `input_unit_ids`.

// Implemented by the C6b chunk package; `build_chunks` is consumed by the C6
// integration wiring (Phase 1 orders chunk → lexical → dense → multivector) and
// by the C6a/C6c builders that read chunks back. Until that wiring lands the
// function is unreferenced, so the module keeps a dead-code allow naming that
// consumer; do not remove `super`'s module-level allow.
#![allow(dead_code)]

use std::time::Instant;

use rusqlite::{Transaction, params};
use serde_json::Value;
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::error::ApiError;
use crate::ids::new_retrieval_projection_id;
use crate::model::{ContentType, ProducerType, Provenance};
use crate::primitives::utc_now;

use super::envelope::{self, NewProjection, ProjectionType};
use super::{CHUNKER_NAME, CHUNKER_VERSION, ChunkerConfig, MAX_UNIT_TOKENS, MIN_SEARCH_UNIT_CHARS};

/// Ordered SELECT of a parse's content units in reading order, mirroring
/// `crate::annotations::producer::SELECT_PARSE_UNITS_SQL`. Chunking depends on
/// this exact ordering: units are chunked in the order they come back, so the
/// `input_unit_ids` a chunk records are deterministic across rebuilds.
/// `sequence_index` is nullable; NULLs sort last under SQLite ordering and the
/// `id` tiebreak keeps the order total. No deleted-row filter: `content_units`
/// rows are hard-deleted by hot cleanup (§31.2), never soft-deleted.
const SELECT_PARSE_UNITS_SQL: &str = "
SELECT id, content_type, body_json
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
/// because a tokenizer was supplied. `input_unit_ids_json` is the canonical
/// JSON array of the targeted ContentUnit IDs (§23 rule 2).
const INSERT_CHUNK_SQL: &str = "
INSERT INTO chunk_projections (
  id, projection_id, source_id, parse_id, input_unit_ids_json,
  targeting_text, token_count, chunker_name, chunker_version,
  chunker_config_hash, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// Build (or rebuild) the chunk projections for one parse, returning the number
/// of chunk rows written. This is the CONTRACT the lexical (C6a) and dense
/// (C6c) builders and the integration wiring consume: after this returns Ok,
/// `chunk_projections` holds exactly this parse's current chunk set and the
/// Chunk envelope is `fresh`.
///
/// `tokenizer` supplies the ColBERT vocabulary and special-token convention.
/// A per-build copy disables truncation and padding to measure complete,
/// normalized targeting text without changing the inference runtime's settings.
///
/// Atomicity and lifecycle: everything runs on the CALLER's transaction. We
/// open a `building` Chunk envelope, delete prior chunk rows, extract and split
/// each unit's targeting text, INSERT the surviving chunks, and complete the
/// envelope `fresh` (payload_uri = None — the chunk payload lives entirely in
/// the hot-plane `chunk_projections` table, not an archived blob). Any failure
/// marks the envelope `failed` on the SAME transaction so the envelope's
/// lifecycle and the audit event commit or roll back together.
pub(crate) fn build_chunks(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    tokenizer: &Tokenizer,
) -> Result<usize, ApiError> {
    let started = Instant::now();
    info!(
        event = "chunk_build.begin",
        source_id, parse_id, "building chunk projections for parse"
    );

    let projection_id = envelope::insert_building(tx, &new_chunk_projection(source_id, parse_id))?;

    // Everything from here is fallible; on any error mark the envelope failed on
    // the same transaction, then propagate. `run_build` owns the actual work so
    // this one site handles the failure lifecycle for every failure path.
    match run_build(tx, source_id, parse_id, &projection_id, tokenizer) {
        Ok(chunk_count) => {
            // The chunk payload lives in `chunk_projections`, so there is no
            // archived payload URI to record (payload_uri = None).
            envelope::complete_fresh(tx, &projection_id, None)?;
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
/// every failure path in the envelope `failed` transition. Reads the parse's
/// ordered units, chunks their targeting text under the token cap, and inserts
/// the surviving chunk rows; returns the number of chunks written.
fn run_build(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    tokenizer: &Tokenizer,
) -> Result<usize, ApiError> {
    // Rebuild idempotence: replace this parse's chunk set wholesale. Must
    // precede the inserts below (see DELETE_PARSE_CHUNKS_SQL invariant).
    tx.execute(DELETE_PARSE_CHUNKS_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to clear prior chunks for parse {parse_id}: {source}"),
        })?;

    let units = read_parse_units(tx, parse_id)?;
    let chunks = split_units_into_chunks(&units, tokenizer)?;

    // Chunker identity/config hash are stamped from the ACTIVE ChunkerConfig
    // (super::ChunkerConfig) — the single source of the §22 chunker identity.
    // This builder never invents chunker constants; changing a boundary-
    // affecting parameter there changes this hash for every chunk.
    let config = ChunkerConfig::active();
    let chunker_config_hash = config.config_hash()?;
    let now = utc_now()?;

    for chunk in &chunks {
        insert_chunk(
            tx,
            projection_id,
            source_id,
            parse_id,
            chunk,
            &chunker_config_hash,
            &now,
        )?;
    }

    Ok(chunks.len())
}

/// Assemble the `NewProjection` request that opens this build's Chunk envelope.
/// The producer is a Rule (the deterministic chunker, not a model): its name
/// and version are the banked chunker identity, and its `config_hash` is the
/// active `ChunkerConfig` hash, so the envelope's provenance answers "which
/// chunker produced this" without reading a payload row. `input_unit_ids` is
/// left None on the envelope: the per-chunk `input_unit_ids` are the meaningful
/// §23 rule-2 links and live on each `chunk_projections` row, whereas a single
/// envelope-level union would flatten which chunk targets which unit.
fn new_chunk_projection(source_id: &str, parse_id: &str) -> NewProjection {
    NewProjection {
        source_id: source_id.to_string(),
        parse_id: parse_id.to_string(),
        projection_type: ProjectionType::Chunk,
        input_unit_ids: None,
        input_annotation_ids: None,
        producer: chunker_provenance(),
        index_name: None,
        index_partition: None,
    }
}

/// §20 provenance for the chunk producer: a deterministic Rule producer stamped
/// with the banked chunker name/version and the active config hash. No model,
/// prompt, or input refs — the chunker is not a model and its per-chunk input
/// lineage lives on the chunk rows, not the envelope provenance.
fn chunker_provenance() -> Provenance {
    Provenance {
        producer_type: ProducerType::Rule,
        producer_name: CHUNKER_NAME.to_string(),
        producer_version: Some(CHUNKER_VERSION.to_string()),
        // config_hash is intentionally left None here rather than recomputed:
        // the hash is fallible (it may fail canonical serialization) and the
        // authoritative per-chunk hash is stamped on each row from
        // ChunkerConfig::active().config_hash() in run_build. Duplicating a
        // fallible hash into the infallible envelope-provenance assembly would
        // force this function to return Result for no added lineage — the row
        // hash is the answer to "which chunker config produced this chunk".
        config_hash: None,
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

/// One assembled chunk before persistence: the exact targeting text, the
/// ordered set of source ContentUnit IDs it targets (§23 rule 2 — deterministic
/// in reading order), and its measured token count (§22 tokenCount).
#[derive(Debug, Clone)]
struct BuiltChunk {
    input_unit_ids: Vec<String>,
    targeting_text: String,
    token_count: usize,
}

/// One parse unit reduced to what chunking needs: its id (for `input_unit_ids`)
/// and the targeting text extracted from its typed body. Units that carry no
/// renderable targeting text are dropped before this stage, so `text` is always
/// non-empty here.
#[derive(Debug, Clone)]
struct ChunkableUnit {
    unit_id: String,
    text: String,
}

/// Read one parse's content units in reading order and reduce each to a
/// `ChunkableUnit`, dropping units whose content type carries no targeting text
/// or whose text is empty. Bounded by the parse's unit count. Mirrors the read
/// discipline of `crate::annotations::producer::read_parse_units`.
fn read_parse_units(tx: &Transaction<'_>, parse_id: &str) -> Result<Vec<ChunkableUnit>, ApiError> {
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
                body_json: row.get(2)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query units for parse {parse_id}: {source}"),
        })?;

    let mut units = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit row for parse {parse_id}: {source}"),
        })?;
        if let Some(unit) = chunkable_unit(row)? {
            units.push(unit);
        }
    }
    Ok(units)
}

/// The `content_units` columns chunking reads: id for the `input_unit_ids`
/// link, content_type to select the body's text field, and body_json for the
/// typed body. Nothing structural (parent/sequence) is needed — chunks target
/// units by id, not by structure.
struct UnitRow {
    id: String,
    content_type: String,
    body_json: String,
}

/// Re-type one persisted unit row and extract its targeting text. Returns None
/// when the unit's content type carries no targeting text, or when the extracted
/// text is empty/whitespace, so those units never contribute a chunk. A
/// content_type outside the schema set, or an unparseable body, fails loudly
/// (the same corruption discipline as the annotation producer's `plan_unit`).
fn chunkable_unit(row: UnitRow) -> Result<Option<ChunkableUnit>, ApiError> {
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

    let Some(text) = extract_targeting_text(content_type, &body) else {
        return Ok(None);
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(ChunkableUnit {
        unit_id: row.id,
        text,
    }))
}

/// SEMANTIC SEAM (see module doc): pull the renderable targeting text out of a
/// unit's typed body, or None for content types that carry no direct text.
/// Past this point the returned string is targeting text (§23), not canonical
/// evidence, even though the bytes came from a canonical unit body.
///
/// Which types contribute text and why the rest are skipped:
///   - text_block  -> body.text: the atomic textual evidence unit; its prose is
///     exactly what lexical/dense retrieval should aim at.
///   - text_section -> body.normalizedText: a section is a CONTAINER (§15.2),
///     not a paragraph; its only own text is the optional normalized rendering,
///     so we index that when present and skip the section otherwise (its child
///     text_blocks carry the paragraph text and are chunked in their own right).
///   - caption     -> body.text: caption prose is short but retrieval-relevant.
///   - table_cell  -> body.text, else body.normalizedText: a cell's displayed
///     text, falling back to its normalized form; the typed `value` is not text.
///
/// Skipped types (page, table, table_row, figure, image_region, code_block):
/// structural or non-prose containers with no direct renderable text. table and
/// table_row carry their text through their decomposed table_cell children;
/// figures/image_regions carry OCR/alt text out of scope for this text chunker;
/// code_block prose is skipped here (its `code` field is not natural-language
/// targeting text).
fn extract_targeting_text(content_type: ContentType, body: &Value) -> Option<String> {
    match content_type {
        ContentType::TextBlock => string_field(body, "text"),
        ContentType::TextSection => string_field(body, "normalizedText"),
        ContentType::Caption => string_field(body, "text"),
        ContentType::TableCell => {
            string_field(body, "text").or_else(|| string_field(body, "normalizedText"))
        }
        ContentType::Page
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure
        | ContentType::ImageRegion
        | ContentType::CodeBlock => None,
    }
}

/// Read one string field from a unit body, returning None when the field is
/// absent or not a JSON string.
fn string_field(body: &Value, field: &str) -> Option<String> {
    body.get(field).and_then(Value::as_str).map(str::to_string)
}

/// Greedily accumulate consecutive units' targeting text into chunks that honor
/// `MAX_UNIT_TOKENS`, dropping chunks under `MIN_SEARCH_UNIT_CHARS`. Harvested
/// from `units.rs::split_conversion_into_units`, repurposed so each "block" is a
/// canonical ContentUnit's text and each produced chunk records the source unit
/// ids that fed it.
///
/// Accumulation rule (harvested): a unit is appended to the current chunk while
/// the joined text stays within the token cap; when it would overflow, the
/// current chunk is flushed and the unit starts a fresh one. A single unit whose
/// own text exceeds the cap is flushed alone and split by
/// `split_oversized_text` (sentences, then words) — those sub-chunks all carry
/// that one unit's id, since sub-splitting cannot subdivide the canonical link.
/// Unlike `units.rs`, there is no heading-path gate on appending: units have no
/// heading metadata here, so the token cap is the only accumulation boundary.
fn split_units_into_chunks(
    units: &[ChunkableUnit],
    tokenizer: &Tokenizer,
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
    let max_tokens = MAX_UNIT_TOKENS as usize;
    let min_chars = MIN_SEARCH_UNIT_CHARS as usize;
    let mut chunks = Vec::new();
    let mut current: Option<ChunkAccumulator> = None;

    for unit in units {
        let unit_tokens = count_tokens(tokenizer, &unit.text)?;

        if unit_tokens > max_tokens {
            // Oversized unit: flush what we have, then split this unit's text
            // alone. Every sub-chunk carries this single unit's id.
            flush_chunk(&mut current, &mut chunks, min_chars);
            split_oversized_text(
                &unit.unit_id,
                &unit.text,
                tokenizer,
                max_tokens,
                min_chars,
                &mut chunks,
            )?;
            continue;
        }

        match current.take() {
            Some(mut accumulator)
                if accumulator.can_append(&unit.text, tokenizer, max_tokens)? =>
            {
                accumulator.append(&unit.unit_id, &unit.text, tokenizer)?;
                current = Some(accumulator);
            }
            Some(accumulator) => {
                push_chunk(&mut chunks, accumulator, min_chars);
                current = Some(ChunkAccumulator::from_unit(
                    &unit.unit_id,
                    &unit.text,
                    unit_tokens,
                ));
            }
            None => {
                current = Some(ChunkAccumulator::from_unit(
                    &unit.unit_id,
                    &unit.text,
                    unit_tokens,
                ));
            }
        }
    }

    flush_chunk(&mut current, &mut chunks, min_chars);
    // Both emission paths normalize text. Recount their exact output and enforce
    // the cap before any chunk rows are persisted by the caller's transaction.
    for chunk in &mut chunks {
        chunk.token_count = count_tokens(tokenizer, &chunk.targeting_text).map_err(|source| {
            ApiError::UnitSplitting {
                message: format!(
                    "failed to count normalized chunk starting at unit {:?}: {source}",
                    chunk.input_unit_ids.first()
                ),
            }
        })?;
        if chunk.token_count > max_tokens {
            return Err(ApiError::UnitSplitting {
                message: format!(
                    "chunk starting at unit {:?} has {} tokens after normalization, exceeding {max_tokens}",
                    chunk.input_unit_ids.first(),
                    chunk.token_count
                ),
            });
        }
    }
    Ok(chunks)
}

/// In-progress chunk: the joined targeting text, its running token count, and
/// the ordered, deduplicated source unit ids that contributed to it. The unit
/// ids are the §23 rule-2 canonical link and are kept in reading order.
#[derive(Debug, Clone)]
struct ChunkAccumulator {
    input_unit_ids: Vec<String>,
    text: String,
    token_count: usize,
}

impl ChunkAccumulator {
    /// Start a chunk from one unit's text and its measured token count.
    fn from_unit(unit_id: &str, text: &str, token_count: usize) -> Self {
        Self {
            input_unit_ids: vec![unit_id.to_string()],
            text: text.to_string(),
            token_count,
        }
    }

    /// Whether appending `text` keeps the EXACT joined candidate within the
    /// token cap (harvested from `units.rs::can_append_block`: the cap is
    /// checked against the real joined text, not the sum of separate counts,
    /// because tokenization is not additive across a join).
    fn can_append(
        &self,
        text: &str,
        tokenizer: &Tokenizer,
        max_tokens: usize,
    ) -> Result<bool, ApiError> {
        let candidate = join_text(&self.text, text);
        Ok(count_tokens(tokenizer, &candidate)? <= max_tokens)
    }

    /// Append one unit's text, recomputing the joined token count and recording
    /// the source unit id. The id is appended only if it is not already the last
    /// contributor, so a unit split across accumulate/flush cannot double-list.
    fn append(&mut self, unit_id: &str, text: &str, tokenizer: &Tokenizer) -> Result<(), ApiError> {
        let candidate = join_text(&self.text, text);
        self.token_count = count_tokens(tokenizer, &candidate)?;
        self.text = candidate;
        if self.input_unit_ids.last().map(String::as_str) != Some(unit_id) {
            self.input_unit_ids.push(unit_id.to_string());
        }
        Ok(())
    }
}

/// Retain block separation during accumulation. Counting and final emission both
/// collapse the separator through the same targeting-text normalization.
fn join_text(left: &str, right: &str) -> String {
    format!("{left}\n\n{right}")
}

/// Split one oversized unit's text into cap-respecting chunks, harvested from
/// `units.rs::split_oversized_block`: first by sentence-like boundaries, then —
/// for any single sentence still over the cap — by words
/// (`split_long_text_by_words`). Every produced chunk carries the one source
/// `unit_id`, because sub-splitting a unit's text cannot change which canonical
/// unit the text came from (§23 rule 2).
fn split_oversized_text(
    unit_id: &str,
    text: &str,
    tokenizer: &Tokenizer,
    max_tokens: usize,
    min_chars: usize,
    chunks: &mut Vec<BuiltChunk>,
) -> Result<(), ApiError> {
    let mut current: Option<ChunkAccumulator> = None;

    for sentence in split_sentences(text) {
        let sentence_tokens = count_tokens(tokenizer, sentence)?;
        if sentence_tokens > max_tokens {
            flush_chunk(&mut current, chunks, min_chars);
            split_long_text_by_words(unit_id, sentence, tokenizer, max_tokens, min_chars, chunks)?;
            continue;
        }

        match current.take() {
            Some(mut accumulator)
                if accumulator.can_append_sentence(sentence, tokenizer, max_tokens)? =>
            {
                accumulator.append_sentence(sentence, tokenizer)?;
                current = Some(accumulator);
            }
            Some(accumulator) => {
                push_chunk(chunks, accumulator, min_chars);
                current = Some(ChunkAccumulator::from_unit(
                    unit_id,
                    sentence,
                    sentence_tokens,
                ));
            }
            None => {
                current = Some(ChunkAccumulator::from_unit(
                    unit_id,
                    sentence,
                    sentence_tokens,
                ));
            }
        }
    }

    flush_chunk(&mut current, chunks, min_chars);
    Ok(())
}

impl ChunkAccumulator {
    /// Whether appending a sentence (space-joined) keeps the joined candidate
    /// within the cap. Sentences within one oversized unit join with a single
    /// space, matching `units.rs::can_append_sentence`.
    fn can_append_sentence(
        &self,
        sentence: &str,
        tokenizer: &Tokenizer,
        max_tokens: usize,
    ) -> Result<bool, ApiError> {
        let candidate = format!("{} {sentence}", self.text);
        Ok(count_tokens(tokenizer, &candidate)? <= max_tokens)
    }

    /// Append a sentence with a single-space join, recomputing the token count.
    /// The source unit id is unchanged: an oversized-unit accumulator only ever
    /// holds text from the one unit being split.
    fn append_sentence(&mut self, sentence: &str, tokenizer: &Tokenizer) -> Result<(), ApiError> {
        let candidate = format!("{} {sentence}", self.text);
        self.token_count = count_tokens(tokenizer, &candidate)?;
        self.text = candidate;
        Ok(())
    }
}

/// Pack sentence words under the token cap, splitting an individually oversized
/// word at UTF-8 boundaries. All fragments retain the unit's canonical identity
/// and remain subject to the existing minimum-character filter.
fn split_long_text_by_words(
    unit_id: &str,
    text: &str,
    tokenizer: &Tokenizer,
    max_tokens: usize,
    min_chars: usize,
    chunks: &mut Vec<BuiltChunk>,
) -> Result<(), ApiError> {
    let mut current_words: Vec<&str> = Vec::new();

    for word in text.split_whitespace() {
        let candidate = build_word_candidate(&current_words, word);
        let candidate_tokens = count_tokens(tokenizer, &candidate)?;
        if !current_words.is_empty() && candidate_tokens > max_tokens {
            let content = current_words.join(" ");
            let token_count = count_tokens(tokenizer, &content)?;
            push_built_chunk(chunks, unit_id, content, token_count, min_chars);
            current_words.clear();
        }
        if candidate_tokens > max_tokens {
            // Any preceding words have been flushed. Keep the final fitting
            // suffix available to join subsequent words, just like an ordinary
            // word; only full fragments are emitted inside the helper.
            let suffix =
                split_oversized_word(unit_id, word, tokenizer, max_tokens, min_chars, chunks)
                    .map_err(|source| ApiError::UnitSplitting {
                        message: format!(
                            "failed to split oversized word in unit {unit_id}: {source}"
                        ),
                    })?;
            current_words.push(suffix);
        } else {
            current_words.push(word);
        }
    }

    if !current_words.is_empty() {
        let content = current_words.join(" ");
        let token_count = count_tokens(tokenizer, &content)?;
        push_built_chunk(chunks, unit_id, content, token_count, min_chars);
    }

    Ok(())
}

/// Emit measured prefixes of an oversized word and return its fitting suffix for
/// normal word accumulation. Offsets address the original UTF-8 text, so splitting
/// never decodes token IDs or loses bytes before the existing min-char filter.
fn split_oversized_word<'a>(
    unit_id: &str,
    word: &'a str,
    tokenizer: &Tokenizer,
    max_tokens: usize,
    min_chars: usize,
    chunks: &mut Vec<BuiltChunk>,
) -> Result<&'a str, ApiError> {
    if count_tokens(tokenizer, word)? <= max_tokens {
        return Ok(word);
    }
    let boundaries: Vec<usize> = word
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(word.len()))
        .collect();
    let mut start = 0;
    loop {
        // The entire remainder is known to exceed the cap. Search only shorter,
        // nonempty prefixes. Token counts need not be monotone: remembering only
        // measured fits may underfill a chunk, but never permits an oversized one.
        let mut low = start + 1;
        let mut high = boundaries.len() - 1;
        let mut fitting = None;
        while low < high {
            let middle = low + (high - low) / 2;
            let fragment = &word[boundaries[start]..boundaries[middle]];
            let token_count = count_tokens(tokenizer, fragment)?;
            if token_count <= max_tokens {
                fitting = Some((middle, token_count));
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let (end, token_count) = fitting.ok_or_else(|| ApiError::UnitSplitting {
            message: format!(
                "could not find a nonempty UTF-8 prefix within the {max_tokens}-token cap for unit {unit_id} at byte {}",
                boundaries[start]
            ),
        })?;
        let fragment = &word[boundaries[start]..boundaries[end]];
        push_built_chunk(chunks, unit_id, fragment.to_owned(), token_count, min_chars);
        // Always advance, even if the unchanged minimum-length policy discarded
        // this fragment. The suffix remains contiguous with the emitted prefix.
        start = end;
        let suffix = &word[boundaries[start]..];
        if count_tokens(tokenizer, suffix)? <= max_tokens {
            return Ok(suffix);
        }
    }
}

/// Build the candidate text for adding one word to the current word run
/// (harvested from `units.rs::build_word_candidate`).
fn build_word_candidate(current_words: &[&str], word: &str) -> String {
    if current_words.is_empty() {
        return word.to_string();
    }
    format!("{} {word}", current_words.join(" "))
}

/// Flush the in-progress accumulator (if any) into the chunk list, applying the
/// min-char drop.
fn flush_chunk(
    current: &mut Option<ChunkAccumulator>,
    chunks: &mut Vec<BuiltChunk>,
    min_chars: usize,
) {
    if let Some(accumulator) = current.take() {
        push_chunk(chunks, accumulator, min_chars);
    }
}

/// Normalize and record one accumulated chunk, dropping it when it is under the
/// `MIN_SEARCH_UNIT_CHARS` search-target floor (harvested from
/// `units.rs::push_unit`). The min-char test is on the NORMALIZED text so
/// whitespace runs cannot inflate a chunk past the floor.
fn push_chunk(chunks: &mut Vec<BuiltChunk>, accumulator: ChunkAccumulator, min_chars: usize) {
    let normalized = normalize_targeting_text(&accumulator.text);
    if normalized.chars().count() < min_chars {
        return;
    }
    chunks.push(BuiltChunk {
        input_unit_ids: accumulator.input_unit_ids,
        targeting_text: normalized,
        token_count: accumulator.token_count,
    });
}

/// Record one single-unit chunk (from the oversized word/sentence path) under
/// the same min-char drop and normalization as `push_chunk`.
fn push_built_chunk(
    chunks: &mut Vec<BuiltChunk>,
    unit_id: &str,
    text: String,
    token_count: usize,
    min_chars: usize,
) {
    let normalized = normalize_targeting_text(&text);
    if normalized.chars().count() < min_chars {
        return;
    }
    chunks.push(BuiltChunk {
        input_unit_ids: vec![unit_id.to_string()],
        targeting_text: normalized,
        token_count,
    });
}

/// Collapse whitespace runs to single spaces and trim, harvested from
/// `units.rs::normalize_unit_content`. This is the exact text indexed by the
/// lexical and dense channels, so normalizing here keeps their input stable
/// across rebuilds regardless of incidental source whitespace.
fn normalize_targeting_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Split paragraph text into sentence-like fragments without a parser
/// dependency, harvested verbatim in behavior from `units.rs::split_sentences`.
fn split_sentences(text: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut start = 0;
    let mut previous_end = 0;

    for (index, value) in text.char_indices() {
        let end = index + value.len_utf8();
        previous_end = end;
        if !matches!(value, '.' | '?' | '!' | '\n') {
            continue;
        }
        let candidate = text[start..end].trim();
        if !candidate.is_empty() {
            results.push(candidate);
        }
        start = end;
    }

    if start < previous_end {
        let candidate = text[start..].trim();
        if !candidate.is_empty() {
            results.push(candidate);
        }
    }
    if results.is_empty() && !text.trim().is_empty() {
        results.push(text.trim());
    }

    results
}

/// Measure the normalized targeting text, including special tokens, with the
/// build's untruncated, unpadded counter. Candidate decisions and stored counts
/// must describe the same bytes; tokenization is not additive across joins.
fn count_tokens(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    let normalized = normalize_targeting_text(text);
    tokenizer
        .encode(normalized.as_str(), true)
        .map(|encoding| encoding.len())
        .map_err(|source| ApiError::UnitSplitting {
            message: format!("tokenization failed during chunk splitting: {source}"),
        })
}

/// Insert one built chunk as a `chunk_projections` row under this build's
/// envelope. The row's `id` is a fresh `proj_` id (chunk rows and their
/// envelope share the id space but are distinct rows); `projection_id` links
/// back to the envelope so freshness stays single-sourced there.
/// `input_unit_ids` is canonical JSON per §16.2, matching the schema's
/// `input_unit_ids_json` column and how the envelope stores id arrays.
fn insert_chunk(
    tx: &Transaction<'_>,
    projection_id: &str,
    source_id: &str,
    parse_id: &str,
    chunk: &BuiltChunk,
    chunker_config_hash: &str,
    created_at: &str,
) -> Result<(), ApiError> {
    let id = new_retrieval_projection_id()?;
    let input_unit_ids_json = canonical_json_string(
        &chunk.input_unit_ids,
        &format!("input unit ids for chunk {id}"),
    )?;
    // token_count is stored as the §22 optional tokenCount; usize → i64 for the
    // INTEGER column (chunk token counts are far below i64::MAX).
    let token_count = chunk.token_count as i64;

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
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to insert chunk {id} for parse {parse_id}: {source}"),
    })?;
    Ok(())
}

/// Render a model shape as a canonical JSON string (§16.2 deterministic bytes),
/// matching how `envelope::canonical_json_string_of` stores its id arrays so a
/// chunk's `input_unit_ids_json` and the envelope's id columns share one
/// encoding.
fn canonical_json_string<T: serde::Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}
