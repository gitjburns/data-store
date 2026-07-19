//! C6a lexical builder: FTS5 lexical projection over chunk targeting text
//! (`chunk_text_index`), parse-scoped and rebuildable from canonical state
//! (spec §22, §36). Reads chunks back through `super::StoredChunk`.
//!
//! The lexical channel targets chunk `targeting_text` at chunk granularity
//! (design fact 1): the builder reads a parse's `chunk_projections` rows and
//! populates `chunk_text_index` with `(chunk_id, targeting_text)` pairs. That
//! FTS5 table IS the lexical payload — it is a standalone table rebuilt from
//! `chunk_projections`, not an external-content mirror (schema.sql §22/§36) —
//! so the envelope archives with `payload_uri = None` (design fact 2; see
//! `envelope::complete_fresh` doc).
//!
//! Envelope discipline (design fact 2): every envelope write goes through
//! `super::envelope` on the CALLER's `&Transaction`. The build opens one
//! `LexicalDocument` envelope (`envelope::insert_building`), then transitions
//! it `building → fresh` on success (`envelope::complete_fresh`) or
//! `building → failed` on any failure (`envelope::mark_failed`). Row, FTS5
//! writes, and lifecycle events all commit or roll back together on that one
//! caller transaction.
//!
//! Purity (design fact 5): the functions here are pure over their arguments —
//! they take a `&Transaction`/`&Connection` plus parse/source identifiers and
//! never touch the scheduler, worker, or `main`. The integration agent wires
//! `build_lexical_index` into the per-parse build path (design fact 8); this
//! module never invokes it.

// Consumed by the integration wiring (`build_lexical_index`) and the C7b
// lexical channel (`match_chunks`); remove this allow when both are wired.
#![allow(dead_code)]

use std::time::Instant;

use rusqlite::{Connection, Transaction, params};
use tracing::{error, info};

use super::StoredChunk;
use super::envelope::{self, NewProjection, ProjectionType};
use crate::error::ApiError;
use crate::model::{ProducerType, Provenance};

/// Lexical projection producer name recorded in the envelope's Provenance
/// (spec §20). Stable across rebuilds so the lexical index's producer identity
/// is answerable from its envelope row.
const LEXICAL_PRODUCER_NAME: &str = "fabric-lexical-index";

/// Lexical projection producer version (spec §20). Bumped when the indexing
/// behavior changes in a way that alters what gets indexed, making a version
/// change a visible rebuild trigger.
const LEXICAL_PRODUCER_VERSION: &str = "1";

/// Ordered SELECT of one parse's chunk projections, read back into
/// `StoredChunk`. The order is deterministic (`created_at`, then `id` as a
/// total tiebreak) so a rebuild indexes chunks in the same order every run —
/// mirror of the producer's parse-unit ordering
/// (`annotations::producer::SELECT_PARSE_UNITS_SQL`), adapted to chunk rows
/// which carry `created_at` rather than a `sequence_index`. No deleted-row
/// filter: `chunk_projections` rows are hard-deleted by hot cleanup (§31.2),
/// never soft-deleted — the table has no `deleted_at` column.
const SELECT_PARSE_CHUNKS_SQL: &str = "
SELECT
  id, projection_id, source_id, parse_id, input_unit_ids_json,
  targeting_text, token_count, chunker_name, chunker_version,
  chunker_config_hash
FROM chunk_projections
WHERE parse_id = ?1
ORDER BY created_at, id";

/// Delete every `chunk_text_index` row belonging to this parse's chunks. The
/// FTS5 table stores `chunk_id` UNINDEXED, so parse scoping is a subselect on
/// `chunk_projections.id` for the parse (the FTS5 table has no `parse_id`
/// column). This runs before the re-insert so a rebuild is idempotent: the
/// index is fully rebuilt from `chunk_projections`, not an external-content
/// mirror (schema.sql §22/§36), so stale rows must be cleared first or a
/// rebuild would accumulate duplicates.
const DELETE_PARSE_INDEX_SQL: &str = "
DELETE FROM chunk_text_index
WHERE chunk_id IN (SELECT id FROM chunk_projections WHERE parse_id = ?1)";

/// Insert one `(chunk_id, targeting_text)` row into the FTS5 lexical index.
/// `chunk_id` is the TEXT `chunk_projections.id` (UNINDEXED, stored not
/// tokenized); `targeting_text` is the sole tokenized column (schema.sql
/// §22/§36).
const INSERT_INDEX_ROW_SQL: &str = "
INSERT INTO chunk_text_index (chunk_id, targeting_text) VALUES (?1, ?2)";

/// BM25 candidate query, parse-scoped. The MATCH runs against the FTS5 index,
/// joined to `chunk_projections` on the stored `chunk_id` so the filter on
/// `chunk_projections.parse_id` restricts hits to the requested parse (design:
/// the lexical channel is always parse-scoped). `bm25(chunk_text_index)` is the
/// FTS5 relevance rank (lower is better); results order by it and are bounded
/// by an explicit `LIMIT` (PRINCIPLES.md: every query bounded). The `?2` MATCH
/// argument is a caller-built FTS5 query string (see `match_chunks`).
const MATCH_CHUNKS_SQL: &str = "
SELECT cti.chunk_id, bm25(chunk_text_index) AS rank
FROM chunk_text_index AS cti
JOIN chunk_projections AS cp ON cp.id = cti.chunk_id
WHERE cp.parse_id = ?1 AND cti.targeting_text MATCH ?2
ORDER BY rank
LIMIT ?3";

/// Build (or rebuild) the parse's lexical FTS5 index and return the projection
/// id of the `LexicalDocument` envelope it opened.
///
/// Lifecycle: opens one `building` envelope, clears any prior index rows for
/// the parse (rebuild idempotency — see `DELETE_PARSE_INDEX_SQL`), inserts one
/// FTS5 row per chunk, then completes the envelope `fresh` with
/// `payload_uri = None` (design fact 2: the payload IS the FTS5 table, so there
/// is no archived URI). On ANY failure after the envelope is open, the envelope
/// is marked `failed` with bounded detail and the original error is returned;
/// all writes ride the caller's `tx`, so a failure that also fails the
/// `mark_failed` write rolls the whole transaction back.
pub(crate) fn build_lexical_index(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
) -> Result<String, ApiError> {
    let started_at = Instant::now();
    info!(
        event = "projection.build.started",
        projection_type = "lexical_document",
        source_id,
        parse_id,
        "lexical index build started"
    );

    let projection_id =
        envelope::insert_building(tx, &new_lexical_projection(source_id, parse_id))?;

    // Everything from the first index mutation onward is failure-wrapped so a
    // partial build always leaves the envelope `failed` rather than `building`.
    match index_parse_chunks(tx, parse_id, &projection_id) {
        Ok(chunk_count) => {
            // payload_uri = None: the lexical payload lives in chunk_text_index,
            // not an archived object, so there is no URI to record.
            envelope::complete_fresh(tx, &projection_id, None)?;
            info!(
                event = "projection.build.completed",
                projection_type = "lexical_document",
                projection_id = %projection_id,
                source_id,
                parse_id,
                chunk_count,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "lexical index build completed"
            );
            Ok(projection_id)
        }
        Err(build_error) => {
            error!(
                event = "projection.build.failed",
                projection_type = "lexical_document",
                projection_id = %projection_id,
                source_id,
                parse_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                error = %build_error,
                "lexical index build failed"
            );
            // Record the failure on the envelope; propagate the ORIGINAL build
            // error, not the mark_failed result, so the caller sees the root
            // cause. If mark_failed itself errors it returns via `?`, failing
            // loudly on the same transaction.
            envelope::mark_failed(tx, &projection_id, &build_error.to_string())?;
            Err(build_error)
        }
    }
}

/// The planned `LexicalDocument` projection request: parse-scoped, carrying the
/// lexical producer identity as its Provenance. `input_unit_ids` is left None —
/// the lexical index's inputs are chunk projections (whose own envelopes record
/// the ContentUnit lineage), not ContentUnits directly, and the envelope's
/// input-unit column is for annotation/unit-derived builders.
fn new_lexical_projection(source_id: &str, parse_id: &str) -> NewProjection {
    NewProjection {
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        projection_type: ProjectionType::LexicalDocument,
        input_unit_ids: None,
        input_annotation_ids: None,
        producer: lexical_producer(),
        index_name: Some("chunk_text_index".to_owned()),
        index_partition: None,
    }
}

/// The lexical index producer's Provenance (spec §20): a system producer named
/// by the stable lexical constants. No model/config hash — lexical indexing is
/// deterministic over chunk text with no learned parameters.
fn lexical_producer() -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: LEXICAL_PRODUCER_NAME.to_owned(),
        producer_version: Some(LEXICAL_PRODUCER_VERSION.to_owned()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: None,
    }
}

/// Clear the parse's prior index rows then insert one FTS5 row per chunk,
/// returning the chunk count. The delete-before-insert ordering is the rebuild
/// idempotency invariant (see `DELETE_PARSE_INDEX_SQL`): were the insert to run
/// first, a rebuild would duplicate every chunk's index row. Runs entirely on
/// the caller's transaction.
fn index_parse_chunks(
    tx: &Transaction<'_>,
    parse_id: &str,
    projection_id: &str,
) -> Result<usize, ApiError> {
    tx.execute(DELETE_PARSE_INDEX_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to clear lexical index for parse {parse_id} (projection {projection_id}): {source}"
            ),
        })?;

    // A `&Transaction` derefs to `&Connection`, so the chunk reader runs on the
    // same transaction — the read sees this build's uncommitted state and stays
    // atomic with the index writes.
    let chunks = read_parse_chunks(tx, parse_id)?;
    for chunk in &chunks {
        tx.execute(
            INSERT_INDEX_ROW_SQL,
            params![chunk.id, chunk.targeting_text],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to index chunk {} for parse {parse_id} (projection {projection_id}): {source}",
                chunk.id
            ),
        })?;
    }
    Ok(chunks.len())
}

/// Read one parse's chunk projections back into `StoredChunk`, in the
/// deterministic order of `SELECT_PARSE_CHUNKS_SQL`. Reader convention (mirror
/// of `annotations::store`): readers take `&Connection` and run one bounded
/// SELECT (bounded by the parse scope). `input_unit_ids_json` is decoded from
/// its canonical JSON array; a malformed value fails loudly with the chunk's
/// identity rather than being silently dropped.
fn read_parse_chunks(conn: &Connection, parse_id: &str) -> Result<Vec<StoredChunk>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_PARSE_CHUNKS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare parse-chunk query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok(ChunkRow {
                id: row.get(0)?,
                projection_id: row.get(1)?,
                source_id: row.get(2)?,
                parse_id: row.get(3)?,
                input_unit_ids_json: row.get(4)?,
                targeting_text: row.get(5)?,
                token_count: row.get(6)?,
                chunker_name: row.get(7)?,
                chunker_version: row.get(8)?,
                chunker_config_hash: row.get(9)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query chunks for parse {parse_id}: {source}"),
        })?;

    let mut chunks = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read chunk row for parse {parse_id}: {source}"),
        })?;
        chunks.push(chunk_from_row(row)?);
    }
    Ok(chunks)
}

/// One `chunk_projections` row as read from SQLite, before `input_unit_ids_json`
/// is decoded back into the `StoredChunk` string vector.
struct ChunkRow {
    id: String,
    projection_id: String,
    source_id: String,
    parse_id: String,
    input_unit_ids_json: String,
    targeting_text: String,
    token_count: Option<u64>,
    chunker_name: String,
    chunker_version: String,
    chunker_config_hash: String,
}

/// Decode one persisted chunk row into `StoredChunk`, parsing the canonical
/// `input_unit_ids_json` array. A stored value that no longer parses as a
/// string array is corruption surfaced with the chunk's identity.
fn chunk_from_row(row: ChunkRow) -> Result<StoredChunk, ApiError> {
    let input_unit_ids: Vec<String> =
        serde_json::from_str(&row.input_unit_ids_json).map_err(|source| {
            ApiError::StorageOperation {
                message: format!(
                    "input unit ids of chunk {} are unparseable: {source}",
                    row.id
                ),
            }
        })?;
    Ok(StoredChunk {
        id: row.id,
        projection_id: row.projection_id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        input_unit_ids,
        targeting_text: row.targeting_text,
        token_count: row.token_count,
        chunker_name: row.chunker_name,
        chunker_version: row.chunker_version,
        chunker_config_hash: row.chunker_config_hash,
    })
}

/// One lexical candidate: a chunk id and its BM25 rank (lower is more
/// relevant, per FTS5 `bm25()`). The C7b lexical channel fuses these ranks with
/// the other retrieval channels.
#[derive(Debug, Clone)]
pub(crate) struct LexicalMatch {
    pub(crate) chunk_id: String,
    pub(crate) bm25_rank: f64,
}

/// BM25 candidate generation for the C7b lexical channel: return up to `limit`
/// chunk ids of `parse_id` matching `fts_query`, ordered by ascending BM25 rank
/// (most relevant first).
///
/// `fts_query` is an FTS5 MATCH string the caller builds with
/// `crate::primitives::bm25::build_bm25_queries` (its strict/broad quoted-term
/// form) — this reader does NOT re-tokenize or re-escape it, so the escaping
/// conventions live in one place (`primitives::bm25`) rather than being
/// duplicated here. The MATCH is parse-scoped via the join to
/// `chunk_projections.parse_id`, and the result is bounded by the explicit
/// `limit` (PRINCIPLES.md: every query bounded).
// #[allow] is module-wide today; the named future consumer of this specific
// helper is C7b's lexical channel.
pub(crate) fn match_chunks(
    conn: &Connection,
    parse_id: &str,
    fts_query: &str,
    limit: usize,
) -> Result<Vec<LexicalMatch>, ApiError> {
    let mut statement =
        conn.prepare(MATCH_CHUNKS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare lexical match query for parse {parse_id}: {source}"
                ),
            })?;
    // rusqlite binds usize as i64; the LIMIT stays an explicit bound.
    let rows = statement
        .query_map(params![parse_id, fts_query, limit as i64], |row| {
            Ok(LexicalMatch {
                chunk_id: row.get(0)?,
                bm25_rank: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to run lexical match for parse {parse_id}: {source}"),
        })?;

    let mut matches = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read lexical match row for parse {parse_id}: {source}"),
        })?;
        matches.push(row);
    }
    Ok(matches)
}
