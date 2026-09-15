//! Retrieval channels (§24.3, §38): dense and lexical candidate generation
//! with chunk→unit resolution and RRF fusion (C7b-1), and the graph channel
//! with D9 tiering (C7b-2). Content lands with packages C7b-1 and C7b-2;
//! consumed at C8d.
//!
//! DP1 lifecycle contract (§31.1): every function here reads on ONE hot-plane
//! read-only connection that is ALREADY inside the single per-query DEFERRED
//! read transaction the C7d pipeline opens as its first act. Channels take a
//! borrowed `&Connection` (which a `&Transaction` derefs to) — they NEVER open
//! their own connection and NEVER begin their own transaction, so capture and
//! all channel reads share one WAL snapshot.
//!
//! Scope contract (§6, §38): scope is enforced SOLELY by the captured active
//! set. The C7d pipeline resolves scope (§24.2), captures the scope-filtered
//! `(source_id → parse_id)` active parses inside the read transaction, and
//! passes them here as `&[CapturedParse]`. Channels read ONLY those parses, so
//! out-of-scope sources are never scanned. "Do not post-filter ranked results
//! for scope; enforce scope at candidate generation" (§38) — there is NO
//! scope post-filter anywhere in this module.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::sqlite::Connection;
use rusqlite::{OptionalExtension, params};
use tracing::{debug, error, info};

use crate::artifact_store::ArtifactStore;
use crate::error::ApiError;
use crate::model::ContentType;
use crate::model::body::TextBlockBody;
use crate::policy::EntityMatchPolicy;
use crate::primitives::bm25::build_bm25_queries;
use crate::primitives::fusion::{Bm25Match, DenseMatch};
use crate::projections::dense_cache::DensePlane;
use crate::projections::graph::{
    EdgeDirection, entity_names_for_parse, mentions_for_name, normalize_entity_name, one_hop_edges,
};
use crate::projections::lexical::match_chunks;
use crate::projections::section_dense::SectionDenseWindow;
use crate::query::model::{RetrievalChannel, RetrievalHit, RetrievalHitType};
use crate::query::profile::RetrievalProfile;
use crate::query::provenance::{
    DenseRepresentation, DenseRetrievalMatch, GraphMatch, GraphReach, GraphRelationship, MatchClass,
};

/// One scope-captured active parse the C7d pipeline hands the channels. It
/// carries both identifiers a `RetrievalHit` needs (`source_id`, `parse_id`)
/// and, for the dense channel, the `Arc<DensePlane>` snapshot C7d captured for
/// this parse INSIDE the read transaction (via `DenseCache::snapshot_for_parse`)
/// so capture and scoring share one WAL snapshot. Capture rejects missing
/// passage or section planes before any retrieval stage can run.
///
/// This is the sole scope surface the channels see: reading only the parses in
/// the passed `&[CapturedParse]` IS the scope mechanism (§6, §38). C7b-2's graph
/// channel MUST reuse this same captured set — it is the shared scope contract.
pub(crate) struct CapturedParse {
    /// Source that owns this active parse (populates `RetrievalHit.source_id`).
    pub(crate) source_id: String,
    /// The active parse id every read in this capture is scoped to.
    pub(crate) parse_id: String,
    /// Dense plane snapshot captured for this parse inside the read
    /// transaction; `None` when no dense plane is loaded for the parse.
    pub(crate) dense_plane: Option<Arc<DensePlane>>,
}

/// One chunk-grained dense candidate before chunk→unit resolution: the chunk
/// id, its cosine similarity against the query vector, and the parse it came
/// from (so resolution and hit construction stay parse-scoped).
struct DenseChunkHit {
    chunk_id: String,
    similarity: f32,
    parse_id: String,
    source_id: String,
}

/// Keep the scored-row count without retaining a score record for every persisted vector.
struct DenseChannelOutcome {
    hits: Vec<DenseChunkHit>,
    scored_count: usize,
}

/// One chunk-grained lexical candidate before chunk→unit resolution: the chunk
/// id, its BM25 rank (lower is more relevant, per FTS5 `bm25()`), and the parse
/// it came from.
struct LexicalChunkHit {
    chunk_id: String,
    bm25_rank: f64,
    parse_id: String,
    source_id: String,
}

/// Scan captured passage rows with bounded storage buffers, retaining only the
/// direct shortlist. Equal cosines preserve captured parse and chunk scan order,
/// matching the prior stable global sort. Invalid query norms remain an empty
/// dense result; persisted-data corruption is an explicit query failure.
fn dense_channel(
    conn: &Connection,
    query_id: &str,
    parses: &[CapturedParse],
    query_vector: &[f32],
    limit: usize,
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> Result<DenseChannelOutcome, ApiError> {
    let started_at = Instant::now();
    let query_norm = l2_norm(query_vector);
    let mut hits: Vec<DenseChunkHit> = Vec::new();
    let mut scored_count = 0;

    // A zero/non-finite query norm makes every cosine undefined; skip scoring
    // and return empty rather than emitting NaNs into fusion.
    if query_norm > 0.0 && query_norm.is_finite() {
        for parse in parses {
            let plane = parse
                .dense_plane
                .as_ref()
                .ok_or_else(|| ApiError::ServiceUnavailable {
                    message: format!(
                        "passage dense projection missing for source {} parse {}",
                        parse.source_id, parse.parse_id
                    ),
                })?;
            if plane.dimension() != query_vector.len() {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "passage dense query dimension mismatch for source {} parse {}",
                        parse.source_id, parse.parse_id
                    ),
                });
            }
            plane.visit_vectors(conn, |row| {
                if row.parse_id != parse.parse_id || !chunk_unit_map.contains_key(&row.chunk_id) {
                    return Err(ApiError::StorageOperation {
                        message: format!(
                            "dense chunk {} has no input-unit mapping in captured parse {}",
                            row.chunk_id, parse.parse_id
                        ),
                    });
                }
                let dot: f32 = row
                    .vector
                    .iter()
                    .zip(query_vector.iter())
                    .map(|(left, right)| left * right)
                    .sum();
                // Row norms are finite and strictly positive (loader guarantee),
                // so the only divisor risk is the query norm, already excluded.
                let similarity = dot / (row.norm * query_norm);
                if !similarity.is_finite() {
                    return Err(ApiError::StorageOperation {
                        message: format!(
                            "non-finite passage cosine for chunk {} in parse {}",
                            row.chunk_id, parse.parse_id
                        ),
                    });
                }
                scored_count += 1;
                let position =
                    hits.partition_point(|hit| hit.similarity.total_cmp(&similarity).is_ge());
                if position < limit {
                    hits.insert(
                        position,
                        DenseChunkHit {
                            chunk_id: row.chunk_id.clone(),
                            similarity,
                            parse_id: parse.parse_id.clone(),
                            source_id: parse.source_id.clone(),
                        },
                    );
                    hits.truncate(limit);
                }
                Ok(())
            })?;
        }
    }

    debug!(
        event = "query.channel.dense.completed",
        query_id,
        parse_count = parses.len(),
        hit_count = hits.len(),
        scored_count,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "dense channel candidate generation completed"
    );
    Ok(DenseChannelOutcome { hits, scored_count })
}

/// Lexical retrieval channel (§24.3): FTS5/BM25 candidate generation over the
/// captured parses' `chunk_text_index`. Emits up to `limit` chunk-grained hits
/// per parse.
///
/// FTS5 shape (C7s finding): `chunk_text_index` is a STANDALONE FTS5 table
/// (`chunk_id UNINDEXED`, `targeting_text` tokenized), rebuilt from
/// `chunk_projections`; a MATCH returns the owning `chunk_projections.id`
/// (`chunk_id`) directly, so `match_chunks` hands back `chunk_id` with no join
/// for the caller to perform — the separate chunk→unit step is what resolves
/// grain before fusion.
///
/// Match-string discipline: the FTS5 MATCH string is built ONLY via
/// `build_bm25_queries` (never hand-built); `None` means no eligible terms and
/// is an empty lexical result, not an error. The strict (AND) query is
/// preferred when present — it is the narrower, higher-precision candidate set;
/// the broad (OR) query is the fallback so a query whose terms are all short or
/// stopworded still retrieves. Reads only the captured parses (scope §38).
fn lexical_channel(
    conn: &Connection,
    query_id: &str,
    parses: &[CapturedParse],
    query_text: &str,
    limit: usize,
) -> Result<Vec<LexicalChunkHit>, ApiError> {
    let started_at = Instant::now();
    let mut hits: Vec<LexicalChunkHit> = Vec::new();

    // Build the FTS5 MATCH string once (it is query-scoped, not parse-scoped).
    // No eligible terms → no lexical candidates for any parse.
    let Some(queries) = build_bm25_queries(query_text, conn.limits().retrieval.min_fts_term_chars)
    else {
        debug!(
            event = "query.channel.lexical.no_terms",
            query_id, "lexical channel produced no candidates: query has no eligible FTS terms"
        );
        return Ok(hits);
    };
    // Prefer the strict (AND) form; fall back to the broad (OR) form. When both
    // are None the query had eligible tokens that all dropped out of both forms
    // — treat as an empty lexical result.
    let Some(fts_query) = queries.strict_query.or(queries.broad_query) else {
        return Ok(hits);
    };

    for parse in parses {
        let matches = match_chunks(conn, &parse.parse_id, &fts_query, limit)?;
        for matched in matches {
            hits.push(LexicalChunkHit {
                chunk_id: matched.chunk_id,
                bm25_rank: matched.bm25_rank,
                parse_id: parse.parse_id.clone(),
                source_id: parse.source_id.clone(),
            });
        }
    }

    debug!(
        event = "query.channel.lexical.completed",
        query_id,
        parse_count = parses.len(),
        hit_count = hits.len(),
        limit,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "lexical channel candidate generation completed"
    );
    Ok(hits)
}

/// Load the chunk→unit mapping for the captured parses: `chunk_id → owning
/// ContentUnit ids`. This is the parse-scoped `chunk_projections.input_unit_ids`
/// relation — the canonical bridge from a chunk back to the ContentUnits it was
/// derived from. Read once for all in-scope parses on the shared connection so
/// resolution is a hash lookup, not a per-hit query.
///
/// A chunk may span MORE THAN ONE unit (a chunk that packs several short units),
/// so a value is a `Vec<String>`; every element is a fusion key. Runs only over
/// the captured parses (scope §38).
fn load_chunk_unit_map(
    conn: &Connection,
    parses: &[CapturedParse],
) -> Result<HashMap<String, Vec<String>>, ApiError> {
    // The database checks byte length before the JSON cell becomes an owned Rust String.
    const SELECT_CHUNK_UNITS_SQL: &str = "
SELECT id, CASE WHEN length(CAST(input_unit_ids_json AS BLOB)) <= ?2
               THEN input_unit_ids_json END
FROM chunk_projections WHERE parse_id = ?1";
    let max_cell_bytes = conn.limits().resources.max_json_cell_bytes;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for parse in parses {
        let mut statement =
            conn.prepare(SELECT_CHUNK_UNITS_SQL)
                .map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "failed to prepare chunk→unit query for parse {}: {source}",
                        parse.parse_id
                    ),
                })?;
        let rows = statement
            .query_map(params![parse.parse_id, max_cell_bytes], |row| {
                let chunk_id: String = row.get(0)?;
                let input_unit_ids_json: Option<String> = row.get(1)?;
                Ok((chunk_id, input_unit_ids_json))
            })
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to query chunk→unit mapping for parse {}: {source}",
                    parse.parse_id
                ),
            })?;
        for row in rows {
            let (chunk_id, input_unit_ids_json) =
                row.map_err(|source| ApiError::StorageOperation {
                    message: format!(
                        "failed to read chunk→unit row for parse {}: {source}",
                        parse.parse_id
                    ),
                })?;
            let input_unit_ids_json = input_unit_ids_json.ok_or_else(|| ApiError::StorageOperation {
                message: format!("resource limit: input unit IDs of chunk {chunk_id} in parse {} exceed resources.max_json_cell_bytes {max_cell_bytes}", parse.parse_id),
            })?;
            // `input_unit_ids_json` is canonical JSON per §16.2; a value that no
            // longer parses is corruption surfaced with the chunk identity, not
            // silently dropped (PRINCIPLES.md: never hide a specific error).
            let unit_ids: Vec<String> =
                serde_json::from_str(&input_unit_ids_json).map_err(|source| {
                    ApiError::StorageOperation {
                        message: format!(
                            "input unit ids of chunk {chunk_id} are unparseable: {source}"
                        ),
                    }
                })?;
            map.insert(chunk_id, unit_ids);
        }
    }
    Ok(map)
}

/// Resolve chunk-grained dense hits to unit-grained `DenseMatch`es. This is the
/// grain-change boundary: candidates enter chunk-grained and leave unit-grained,
/// because fusion keys on `unit_id` (PRINCIPLES.md: comment the boundary where
/// data changes meaning). A chunk fans out to each of its owning units; when
/// several chunks resolve to the same unit, the unit keeps its BEST (highest
/// similarity) chunk, so a unit is scored by its strongest chunk. Ranks are
/// assigned 1-based over the deduplicated units, ordered by similarity desc then
/// unit id asc for determinism.
fn resolve_dense_to_units(
    hits: &[DenseChunkHit],
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> Vec<DenseMatch> {
    // unit_id → best similarity seen for that unit across all its chunks.
    let mut best: HashMap<String, f32> = HashMap::new();
    for hit in hits {
        let Some(unit_ids) = chunk_unit_map.get(&hit.chunk_id) else {
            continue;
        };
        for unit_id in unit_ids {
            best.entry(unit_id.clone())
                .and_modify(|current| {
                    if hit.similarity > *current {
                        *current = hit.similarity;
                    }
                })
                .or_insert(hit.similarity);
        }
    }
    let mut ranked: Vec<(String, f32)> = best.into_iter().collect();
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .enumerate()
        .map(|(index, (unit_id, similarity))| DenseMatch {
            unit_id,
            similarity,
            rank: index + 1,
        })
        .collect()
}

/// Resolve chunk-grained lexical hits to unit-grained `Bm25Match`es. Same
/// grain-change boundary as `resolve_dense_to_units`: fusion keys on `unit_id`.
/// FTS5 `bm25()` rank is lower-is-better, so a unit keeps its LOWEST (best) rank
/// across its chunks. Output ranks are assigned 1-based over the deduplicated
/// units, ordered by bm25 rank asc (best first) then unit id asc.
fn resolve_lexical_to_units(
    hits: &[LexicalChunkHit],
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> Vec<Bm25Match> {
    // unit_id → best (lowest) bm25 rank seen for that unit across its chunks.
    let mut best: HashMap<String, f64> = HashMap::new();
    for hit in hits {
        let Some(unit_ids) = chunk_unit_map.get(&hit.chunk_id) else {
            continue;
        };
        for unit_id in unit_ids {
            best.entry(unit_id.clone())
                .and_modify(|current| {
                    if hit.bm25_rank < *current {
                        *current = hit.bm25_rank;
                    }
                })
                .or_insert(hit.bm25_rank);
        }
    }
    let mut ranked: Vec<(String, f64)> = best.into_iter().collect();
    ranked.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .enumerate()
        .map(|(index, (unit_id, bm25_score))| Bm25Match {
            unit_id,
            score: bm25_score,
            rank: index + 1,
        })
        .collect()
}

/// Section nominations retain their first (best section-rank, then unit-rank)
/// occurrence; duplicate windows must not give a unit extra fusion votes.
struct SectionNominations {
    units: Vec<String>,
    evidence: HashMap<String, DenseRetrievalMatch>,
    window_count: usize,
    section_units_without_fine_vectors: usize,
    fine_chunks_rescored: usize,
}

/// Only shortlisted section windows own payload bytes; their parse identity is
/// borrowed from the immutable query capture for later canonical nomination.
struct SectionWindowCandidate<'a> {
    similarity: f32,
    parse: &'a CapturedParse,
    window: SectionDenseWindow,
}

/// Best fine matches are retained only for member units of shortlisted windows.
struct SectionFineMatches {
    chunks: HashMap<String, DenseChunkHit>,
    scored_count: usize,
}

/// Rank section windows globally, then nominate only canonical units actually
/// inside each window using their strongest fine-chunk match. All reads retain
/// the captured transaction. A chunk belongs to exactly one window, but a unit
/// split across two chunks can straddle a window boundary; the ownership check
/// keeps a window from nominating units it does not contain.
fn section_nominations(
    conn: &Connection,
    store: &ArtifactStore,
    parses: &[CapturedParse],
    query_vector: &[f32],
    chunk_unit_map: &HashMap<String, Vec<String>>,
    owners: &mut HashMap<String, CandidateUnit>,
    profile: &RetrievalProfile,
) -> Result<SectionNominations, ApiError> {
    let query_norm = l2_norm(query_vector);
    let mut windows: Vec<SectionWindowCandidate<'_>> = Vec::new();
    let limit = profile.limits.section_candidate_limit as usize;
    for parse in parses {
        let plane = parse.dense_plane.as_ref().ok_or_else(|| ApiError::ServiceUnavailable {
            message: format!("source {} parse {} requires rebuilding passage and section dense projections; run data-store --config <config-path> --rebuild-all", parse.source_id, parse.parse_id),
        })?;
        let sections = plane.sections().ok_or_else(|| ApiError::ServiceUnavailable {
            message: format!("source {} parse {} requires rebuilding section dense projections; run data-store --config <config-path> --rebuild-all", parse.source_id, parse.parse_id),
        })?;
        if sections.source_id != parse.source_id
            || sections.parse_id != parse.parse_id
            || sections.dimension != query_vector.len()
            || plane.dimension() != query_vector.len()
        {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "dense projection ownership or query dimension mismatch for source {} parse {}",
                    parse.source_id, parse.parse_id
                ),
            });
        }
        if query_norm <= 0.0 || !query_norm.is_finite() {
            continue;
        }
        plane.visit_sections(conn, store, |window| {
            let dot: f32 = window
                .vector
                .iter()
                .zip(query_vector)
                .map(|(left, right)| left * right)
                .sum();
            let similarity = dot / (window.norm * query_norm);
            if !similarity.is_finite() {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "non-finite section cosine for window {} in parse {}",
                        window.window_id, parse.parse_id
                    ),
                });
            }
            // Bound retained payloads before scanning the next artifact window.
            // This comparator is the previous global section-sort order.
            let position = windows.partition_point(|entry| {
                entry
                    .similarity
                    .total_cmp(&similarity)
                    .reverse()
                    .then_with(|| entry.parse.parse_id.cmp(&parse.parse_id))
                    .then_with(|| entry.window.window_id.cmp(&window.window_id))
                    .is_le()
            });
            if position < limit {
                windows.insert(
                    position,
                    SectionWindowCandidate {
                        similarity,
                        parse,
                        // The visitor releases its window after returning; only a
                        // shortlisted window needs an owned copy for nomination.
                        window: window.clone(),
                    },
                );
                windows.truncate(limit);
            }
            Ok(())
        })?;
    }
    let fine_matches = best_section_chunks(conn, parses, &windows, query_vector, chunk_unit_map)?;
    let mut output = SectionNominations {
        units: Vec::new(),
        evidence: HashMap::new(),
        window_count: windows.len(),
        section_units_without_fine_vectors: 0,
        fine_chunks_rescored: fine_matches.scored_count,
    };
    for candidate in windows {
        let parse = candidate.parse;
        let window = candidate.window;
        let mut candidates = Vec::new();
        for unit_id in &window.input_unit_ids {
            validate_candidate_unit(conn, owners, unit_id, &parse.source_id, &parse.parse_id)?;
            if !owners[unit_id].eligible {
                continue;
            }
            let Some(chunk) = fine_matches.chunks.get(unit_id.as_str()) else {
                // Section context includes code and short leaves that the fine
                // chunker excludes. Without a fine cosine these units cannot
                // seed a section-guided passage, but their context is retained.
                output.section_units_without_fine_vectors += 1;
                continue;
            };
            if chunk.source_id != parse.source_id || chunk.parse_id != parse.parse_id {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "section window {} unit {unit_id} has a fine chunk from another source or parse",
                        window.window_id
                    ),
                });
            }
            candidates.push((unit_id, chunk));
        }
        candidates.sort_by(|left, right| {
            right
                .1
                .similarity
                .total_cmp(&left.1.similarity)
                .then_with(|| left.0.cmp(right.0))
        });
        candidates.dedup_by(|left, right| left.0 == right.0);
        candidates.truncate(profile.limits.section_passages_per_window as usize);
        for (unit_id, chunk) in candidates {
            if output.evidence.contains_key(unit_id) {
                continue;
            }
            output.units.push(unit_id.clone());
            output.evidence.insert(
                unit_id.clone(),
                DenseRetrievalMatch {
                    representation: DenseRepresentation::Section,
                    chunk_id: Some(chunk.chunk_id.clone()),
                    section_window_id: Some(window.window_id.clone()),
                    section_id: window.section_id.clone(),
                    section_path: window.section_path.clone(),
                },
            );
        }
    }
    Ok(output)
}

/// Rescore fine rows only for shortlisted section memberships, retaining one best
/// match per member. A unit split across chunks in two windows cannot be nominated
/// by a window that lacks it; section_nominations checks canonical ownership afterward.
fn best_section_chunks(
    conn: &Connection,
    parses: &[CapturedParse],
    windows: &[SectionWindowCandidate<'_>],
    query_vector: &[f32],
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> Result<SectionFineMatches, ApiError> {
    let selected_parses: HashSet<&str> = windows
        .iter()
        .map(|entry| entry.parse.parse_id.as_str())
        .collect();
    let selected_units: HashSet<&str> = windows
        .iter()
        .flat_map(|entry| entry.window.input_unit_ids.iter().map(String::as_str))
        .collect();
    let mut chunks: HashMap<String, DenseChunkHit> = HashMap::new();
    let mut scored_count = 0;
    let query_norm = l2_norm(query_vector);
    for parse in parses {
        if !selected_parses.contains(parse.parse_id.as_str()) {
            continue;
        }
        let plane = parse
            .dense_plane
            .as_ref()
            .ok_or_else(|| ApiError::ServiceUnavailable {
                message: format!(
                    "section fine-scoring plane missing for source {} parse {}",
                    parse.source_id, parse.parse_id
                ),
            })?;
        plane.visit_vectors(conn, |row| {
            let units =
                chunk_unit_map
                    .get(&row.chunk_id)
                    .ok_or_else(|| ApiError::StorageOperation {
                        message: format!(
                            "dense chunk {} has no input-unit mapping in parse {}",
                            row.chunk_id, parse.parse_id
                        ),
                    })?;
            if !units
                .iter()
                .any(|unit_id| selected_units.contains(unit_id.as_str()))
            {
                return Ok(());
            }
            let dot: f32 = row
                .vector
                .iter()
                .zip(query_vector)
                .map(|(left, right)| left * right)
                .sum();
            let similarity = dot / (row.norm * query_norm);
            if !similarity.is_finite() {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "non-finite section fine cosine for chunk {} in parse {}",
                        row.chunk_id, parse.parse_id
                    ),
                });
            }
            scored_count += 1;
            for unit_id in units {
                if selected_units.contains(unit_id.as_str())
                    && chunks
                        .get(unit_id)
                        .is_none_or(|current| similarity.total_cmp(&current.similarity).is_gt())
                {
                    // Keep the first equal-score row to preserve the original
                    // stable cosine order across captured parses and chunk IDs.
                    chunks.insert(
                        unit_id.clone(),
                        DenseChunkHit {
                            chunk_id: row.chunk_id.clone(),
                            similarity,
                            parse_id: parse.parse_id.clone(),
                            source_id: parse.source_id.clone(),
                        },
                    );
                }
            }
            Ok(())
        })?;
    }
    Ok(SectionFineMatches {
        chunks,
        scored_count,
    })
}

/// Combine two unit lists with equal-weight RRF, retaining a single dense vote
/// for outer fusion. Similarity now stores the inner RRF score, not a cosine.
fn merge_dense_representations(
    direct: &[DenseMatch],
    sections: &[String],
    profile: &RetrievalProfile,
) -> Vec<DenseMatch> {
    let mut scores: HashMap<&str, f64> = HashMap::new();
    for (rank, unit_id) in direct
        .iter()
        .map(|matched| matched.unit_id.as_str())
        .enumerate()
        .chain(sections.iter().map(String::as_str).enumerate())
    {
        *scores.entry(unit_id).or_default() +=
            1.0 / (f64::from(profile.limits.rrf_k) + (rank + 1) as f64);
    }
    let mut ranked: Vec<_> = scores.into_iter().collect();
    ranked.sort_by(|left, right| right.1.total_cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    ranked.truncate(profile.limits.max_candidates_per_channel as usize);
    ranked
        .into_iter()
        .enumerate()
        .map(|(index, (unit_id, score))| DenseMatch {
            unit_id: unit_id.to_owned(),
            similarity: score as f32,
            rank: index + 1,
        })
        .collect()
}

/// Attribute only admitted direct candidates and deduplicated section nominations;
/// scoring a fine chunk for section ordering alone is not direct discovery.
fn dense_evidence(
    direct: &[DenseMatch],
    direct_chunks: &[DenseChunkHit],
    chunk_unit_map: &HashMap<String, Vec<String>>,
    sections: SectionNominations,
) -> HashMap<String, Vec<DenseRetrievalMatch>> {
    let mut evidence: HashMap<String, Vec<DenseRetrievalMatch>> = direct
        .iter()
        .map(|matched| (matched.unit_id.clone(), Vec::new()))
        .collect();
    for chunk in direct_chunks {
        if let Some(units) = chunk_unit_map.get(&chunk.chunk_id) {
            for unit in units {
                if let Some(matches) = evidence.get_mut(unit) {
                    matches.push(DenseRetrievalMatch {
                        representation: DenseRepresentation::Passage,
                        chunk_id: Some(chunk.chunk_id.clone()),
                        section_window_id: None,
                        section_id: None,
                        section_path: Vec::new(),
                    });
                }
            }
        }
    }
    for (unit_id, matched) in sections.evidence {
        evidence.entry(unit_id).or_default().push(matched);
    }
    for matches in evidence.values_mut() {
        matches.sort();
        matches.dedup();
    }
    evidence
}

/// Build a unit-grained hit only after canonical ownership has been verified.
fn unit_hit(
    unit_id: &str,
    owner: &CandidateUnit,
    channel: RetrievalChannel,
    score: f64,
    rank: usize,
) -> RetrievalHit {
    RetrievalHit {
        hit_type: RetrievalHitType::ContentUnit,
        hit_id: unit_id.to_owned(),
        source_id: owner.source_id.clone(),
        parse_id: owner.parse_id.clone(),
        unit_ids: vec![unit_id.to_owned()],
        channel,
        score,
        rank: Some(rank as u32),
        matched_projection_id: None,
        matched_annotation_id: None,
        source_excerpt: None,
        annotation_matches: Vec::new(),
        explanation: None,
        graph_matches: Vec::new(),
        dense_matches: Vec::new(),
    }
}

/// Eligible source and graph candidates retain their channel scores and attribution
/// until annotation retrieval performs the single outer fusion.
pub(crate) struct ChannelCandidates {
    pub(crate) channel_hits: Vec<RetrievalHit>,
}

/// Collect eligible channels from one captured snapshot. Passage and section
/// representations merge within the dense channel; outer fusion belongs to the
/// annotation retrieval layer, where semantic annotations and graph matches combine.
// Separate borrows preserve caller ownership of the read snapshot, artifact store,
// captured scope, and query/model inputs at this storage boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_channels(
    conn: &Connection,
    store: &ArtifactStore,
    query_id: &str,
    parses: &[CapturedParse],
    query_vector: &[f32],
    query_text: &str,
    graph_hits: &[RetrievalHit],
    profile: &RetrievalProfile,
) -> Result<ChannelCandidates, ApiError> {
    let started_at = Instant::now();
    let candidate_limit = profile.limits.max_candidates_per_channel as usize;
    info!(
        event = "query.channels.started",
        query_id,
        parse_count = parses.len(),
        candidate_limit,
        rrf_k = profile.limits.rrf_k,
        graph_input_hits = graph_hits.len(),
        "dense, lexical, and graph candidate collection started"
    );

    // Preserve the failed read stage across every early return from collection.
    let mut stage = "lexical_retrieval";
    let result: Result<ChannelCandidates, ApiError> = (|| {
        let lexical_hits = lexical_channel(conn, query_id, parses, query_text, candidate_limit)?;

        // Grain-change boundary: chunk-grained hits → unit-grained matches. Fusion
        // keys on unit_id, so resolution MUST precede fusion (design rule, §6/§38).
        stage = "chunk_unit_mapping";
        let chunk_unit_map = load_chunk_unit_map(conn, parses)?;
        stage = "dense_retrieval";
        let dense = dense_channel(
            conn,
            query_id,
            parses,
            query_vector,
            candidate_limit,
            &chunk_unit_map,
        )?;
        let dense_hits = dense.hits.as_slice();
        stage = "unit_ownership";
        let mut owners = build_unit_owner_index(
            conn,
            parses,
            dense_hits,
            &lexical_hits,
            graph_hits,
            &chunk_unit_map,
        )?;
        let mut dense_matches = resolve_dense_to_units(dense_hits, &chunk_unit_map);
        let mut lexical_matches = resolve_lexical_to_units(&lexical_hits, &chunk_unit_map);
        let dense_units_before_filter = dense_matches.len();
        let lexical_units_before_filter = lexical_matches.len();
        // Every resolved unit was checked above; filtering changes eligibility, not scope.
        dense_matches.retain(|matched| owners[&matched.unit_id].eligible);
        lexical_matches.retain(|matched| owners[&matched.unit_id].eligible);
        let dense_excluded = dense_units_before_filter - dense_matches.len();
        let lexical_excluded = lexical_units_before_filter - lexical_matches.len();
        let dense_limit_excluded = dense_matches.len().saturating_sub(candidate_limit);
        let lexical_limit_excluded = lexical_matches.len().saturating_sub(candidate_limit);
        dense_matches.truncate(candidate_limit);
        lexical_matches.truncate(candidate_limit);
        for (index, matched) in dense_matches.iter_mut().enumerate() {
            matched.rank = index + 1;
        }
        for (index, matched) in lexical_matches.iter_mut().enumerate() {
            matched.rank = index + 1;
        }
        stage = "section_dense_retrieval";
        let section_started = Instant::now();
        info!(
            event = "query.channel.section_dense.started",
            query_id,
            section_limit = profile.limits.section_candidate_limit,
            passages_per_window = profile.limits.section_passages_per_window,
            "section-guided dense candidate generation started"
        );
        let sections = section_nominations(
            conn,
            store,
            parses,
            query_vector,
            &chunk_unit_map,
            &mut owners,
            profile,
        )?;
        let direct_units = dense_matches.len();
        let section_units = sections.units.len();
        let section_windows = sections.window_count;
        let section_units_without_fine_vectors = sections.section_units_without_fine_vectors;
        let section_fine_chunks_rescored = sections.fine_chunks_rescored;
        let combined_dense = merge_dense_representations(&dense_matches, &sections.units, profile);
        let dense_provenance =
            dense_evidence(&dense_matches, dense_hits, &chunk_unit_map, sections);
        dense_matches = combined_dense;
        info!(
            event = "query.channel.section_dense.completed",
            query_id,
            fine_chunks_scored = dense.scored_count,
            section_fine_chunks_rescored,
            direct_units,
            section_windows,
            section_units,
            combined_dense_units = dense_matches.len(),
            section_units_without_fine_vectors,
            elapsed_ms = section_started.elapsed().as_millis() as u64,
            "passage and section candidates merged into one dense channel"
        );
        let mut graph_eligible = Vec::new();
        let mut graph_seen = std::collections::HashSet::new();
        let mut graph_excluded = 0;
        for hit in graph_hits {
            if !owners[&hit.hit_id].eligible {
                graph_excluded += 1;
            } else if graph_seen.insert(hit.hit_id.as_str())
                && graph_eligible.len() < candidate_limit
            {
                graph_eligible.push(hit);
            }
        }
        // Preserve channel scores and complete attribution for the outer fusion;
        // graph evidence remains available when semantic annotation retrieval joins it.
        let mut channel_hits = Vec::new();
        for matched in &dense_matches {
            let mut hit = unit_hit(
                &matched.unit_id,
                &owners[&matched.unit_id],
                RetrievalChannel::Dense,
                f64::from(matched.similarity),
                matched.rank,
            );
            hit.dense_matches = dense_provenance[&matched.unit_id].clone();
            channel_hits.push(hit);
        }
        for matched in &lexical_matches {
            channel_hits.push(unit_hit(
                &matched.unit_id,
                &owners[&matched.unit_id],
                RetrievalChannel::Lexical,
                -matched.score,
                matched.rank,
            ));
        }
        channel_hits.extend(graph_eligible.iter().map(|hit| (*hit).clone()));
        info!(
            event = "query.channels.completed",
            query_id,
            dense_units = dense_matches.len(),
            lexical_units = lexical_matches.len(),
            graph_units = graph_eligible.len(),
            dense_units_before_filter,
            lexical_units_before_filter,
            dense_excluded,
            lexical_excluded,
            graph_excluded,
            // Eligibility exclusions above do not include deduplication or the cap.
            graph_duplicate_hits = graph_hits.len() - graph_excluded - graph_seen.len(),
            graph_limit_excluded = graph_seen.len() - graph_eligible.len(),
            dense_limit_excluded,
            lexical_limit_excluded,
            candidate_limit,
            retained_channel_hits = channel_hits.len(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "dense, lexical, and graph candidate collection completed"
        );
        Ok(ChannelCandidates { channel_hits })
    })();
    result.inspect_err(|source| {
        error!(event = "query.channels.failed", query_id, stage,
            parse_count = parses.len(), error = %source,
            error_chain = %crate::util::error_chain(source, &conn.limits().diagnostics),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "retrieval channel candidate collection failed");
    })
}

/// Canonical ownership and role eligibility retained once per candidate unit.
struct CandidateUnit {
    source_id: String,
    parse_id: String,
    eligible: bool,
}

/// Validate chunk and graph references against canonical units in the same snapshot.
/// Broken ownership is corruption, never an empty identifier or a silently lost hit.
fn build_unit_owner_index(
    conn: &Connection,
    parses: &[CapturedParse],
    dense_hits: &[DenseChunkHit],
    lexical_hits: &[LexicalChunkHit],
    graph_hits: &[RetrievalHit],
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, CandidateUnit>, ApiError> {
    let mut owners = HashMap::new();
    let chunks = dense_hits
        .iter()
        .map(|hit| (&hit.chunk_id, &hit.source_id, &hit.parse_id))
        .chain(
            lexical_hits
                .iter()
                .map(|hit| (&hit.chunk_id, &hit.source_id, &hit.parse_id)),
        );
    for (chunk_id, source_id, parse_id) in chunks {
        let unit_ids = chunk_unit_map
            .get(chunk_id)
            .ok_or_else(|| ApiError::StorageOperation {
                message: format!(
                    "candidate chunk {chunk_id} has no input-unit mapping in parse {parse_id}"
                ),
            })?;
        if unit_ids.is_empty() {
            return Err(ApiError::StorageOperation {
                message: format!("candidate chunk {chunk_id} has an empty input-unit mapping"),
            });
        }
        for unit_id in unit_ids {
            validate_candidate_unit(conn, &mut owners, unit_id, source_id, parse_id)?;
        }
    }
    for hit in graph_hits {
        if hit.hit_type != RetrievalHitType::ContentUnit
            || hit.unit_ids.as_slice() != [hit.hit_id.as_str()]
            || !parses
                .iter()
                .any(|parse| parse.parse_id == hit.parse_id && parse.source_id == hit.source_id)
        {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "graph candidate {} does not identify one unit in its captured source {} parse {}",
                    hit.hit_id, hit.source_id, hit.parse_id
                ),
            });
        }
        validate_candidate_unit(
            conn,
            &mut owners,
            &hit.hit_id,
            &hit.source_id,
            &hit.parse_id,
        )?;
    }
    Ok(owners)
}

/// Read each candidate once and reject cross-parse references before unit-only fusion.
fn validate_candidate_unit(
    conn: &Connection,
    owners: &mut HashMap<String, CandidateUnit>,
    unit_id: &str,
    source_id: &str,
    parse_id: &str,
) -> Result<(), ApiError> {
    if let Some(owner) = owners.get(unit_id) {
        if owner.source_id != source_id || owner.parse_id != parse_id {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "candidate unit {unit_id} has conflicting source/parse ownership: {source_id}/{parse_id} and {}/{}",
                    owner.source_id, owner.parse_id
                ),
            });
        }
        return Ok(());
    }
    // Even ineligible candidates must not allocate an oversized canonical body before validation.
    const SELECT_CANDIDATE_SQL: &str = "
SELECT source_id, content_type,
       CASE WHEN length(CAST(body_json AS BLOB)) <= ?3 THEN body_json END
FROM content_units WHERE id = ?1 AND parse_id = ?2";
    let max_body_bytes = conn.limits().resources.max_source_body_bytes;
    let row = conn
        .prepare_cached(SELECT_CANDIDATE_SQL)
        .and_then(|mut statement| {
            statement
                .query_row(params![unit_id, parse_id, max_body_bytes], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .optional()
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read candidate unit {unit_id} of parse {parse_id}: {source}"
            ),
        })?;
    let Some((stored_source, content_type, body_json)) = row else {
        return Err(ApiError::StorageOperation {
            message: format!("candidate unit {unit_id} is missing from captured parse {parse_id}"),
        });
    };
    if stored_source != source_id {
        return Err(ApiError::StorageOperation {
            message: format!(
                "candidate unit {unit_id} belongs to source {stored_source}, expected {source_id}"
            ),
        });
    }
    let body_json = body_json.ok_or_else(|| ApiError::StorageOperation {
        message: format!("resource limit: body of candidate unit {unit_id} in parse {parse_id} exceeds resources.max_source_body_bytes {max_body_bytes}"),
    })?;
    let content_type: ContentType = serde_json::from_value(serde_json::Value::String(content_type))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("invalid content type for candidate unit {unit_id}: {source}"),
        })?;
    // v0.4 has no furniture roles (SPEC-epub §2.2), so every decodable unit is
    // eligible; a `text_block` body is still decoded strictly so a malformed
    // one fails here rather than later in evidence assembly.
    if content_type == ContentType::TextBlock {
        let _body: TextBlockBody =
            serde_json::from_str(&body_json).map_err(|source| ApiError::StorageOperation {
                message: format!("invalid text-block body for candidate unit {unit_id}: {source}"),
            })?;
    }
    let eligible = true;
    owners.insert(
        unit_id.to_owned(),
        CandidateUnit {
            source_id: source_id.to_owned(),
            parse_id: parse_id.to_owned(),
            eligible,
        },
    );
    Ok(())
}

/// L2 norm of a vector. Used to normalize the query vector once for cosine
/// scoring; the plane's row norms are precomputed (`DensePlane::norms`).
fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

// ===========================================================================
// Graph channel (C7b-2) — D9 semantic-graph traversal with three-tier ordering.
//
// The pipeline generates graph hits before passing them to `collect_channels`.
// Graph ordering is retained as its ordinal RRF contribution. This channel
// reads mentions/edges ONLY within the passed `&[CapturedParse]`, so scope is
// enforced at the entity lookup (§6, §38) with no ranked post-filter — and it
// runs on the same `&Connection` already inside the per-query DEFERRED read
// transaction (DP1, §31.1). `CapturedParse.dense_plane` is dense-only and is
// ignored here.
// ===========================================================================

/// The maximum tier width in the D9 ordering. `Tier1` (units connected to more
/// than one matched entity) is the strongest, `Tier3` (one-hop related units)
/// the weakest. Order matters: `#[derive(Ord)]` makes `Tier1 < Tier2 < Tier3`,
/// which is "stronger first" so the derived ordering sorts strongest tier first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GraphTier {
    /// Units connected to MORE THAN ONE matched entity (D9 tier 1).
    Tier1,
    /// Direct-mention units of a single matched entity (D9 tier 2).
    Tier2,
    /// One-hop related units (D9 tier 3).
    Tier3,
}

/// A stable, bounded label for a match class — used in the debug explanation and
/// (as class keys) in the fuzzy diagnostics. Safe to log: a fixed enum label,
/// never operator or entity content.
fn match_class_label(class: MatchClass) -> &'static str {
    match class {
        MatchClass::Exact => "exact",
        MatchClass::Acronym => "acronym",
        MatchClass::TokenPrefix => "token_prefix",
        MatchClass::Semantic => "semantic",
    }
}

/// A stored NORMALIZED entity name a query matched, tagged with the class it
/// matched under. The name is the STORED node identity (not the query string):
/// it is what the graph lookups (`mentions_for_name` / `one_hop_edges`) are
/// probed with and whose length feeds within-tier strength.
///
/// Ordered as `(class, Reverse(char_count), name)` via the derived `Ord` so a
/// candidate's STRONGEST matched entry is its `min`: strongest class first, then
/// longest name, then name ascending — the ruled within-tier order below the
/// tier discriminator. Character count (not byte length) so multi-byte names
/// compare by the count a human would read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MatchedName {
    class: MatchClass,
    name: String,
}

impl MatchedName {
    /// The `(class, Reverse(char_count), &name)` sort key. Kept as a method so
    /// the borrowed `&str` in the key lives only as long as the borrow at the
    /// comparison site.
    fn strength_key(&self) -> (MatchClass, std::cmp::Reverse<usize>, &str) {
        (
            self.class,
            std::cmp::Reverse(self.name.chars().count()),
            self.name.as_str(),
        )
    }
}

impl PartialOrd for MatchedName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MatchedName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.strength_key().cmp(&other.strength_key())
    }
}

/// One graph candidate unit, carrying everything the D9 ordering needs before
/// it collapses into a `RetrievalHit`. `tier` is the unit's strongest tier
/// (a unit reached several ways keeps its strongest classification); `matched`
/// is the set of matched STORED entity names (each tagged with its match class)
/// by which this unit was reached (a set so a unit reached twice under the same
/// name+class is not double-counted). This full set determines within-tier
/// strength; direct reach records determine tier-1 membership separately.
/// `source_id`/`parse_id` are the owning parse the unit was reached in.
///
/// Within-tier strength (RULED 2026-07-15; D9-amended 2026-07-19 to lead with
/// match class): a candidate's strength is that of its STRONGEST matched entry —
/// strongest class first (exact > acronym > token-prefix), then the longest
/// name, then name ascending — so a unit is ranked by the strongest evidence
/// that reached it.
struct GraphUnitCandidate {
    unit_id: String,
    tier: GraphTier,
    matched: std::collections::BTreeSet<MatchedName>,
    // Reach kinds distinguish direct mentions from relational matches for tier promotion.
    graph_matches: std::collections::BTreeSet<GraphMatch>,
    source_id: String,
    parse_id: String,
}

/// The strongest matched-entry strength key across a candidate's matched set —
/// the key of its strongest class / longest name (see `MatchedName::strength_key`
/// for the ruled ordering). The matched set is never empty for a produced
/// candidate (every candidate was reached via at least one matched entity), so
/// `min` always yields a key.
fn candidate_strength_key(
    candidate: &GraphUnitCandidate,
) -> (MatchClass, std::cmp::Reverse<usize>, &str) {
    candidate
        .matched
        .iter()
        .map(MatchedName::strength_key)
        .min()
        .expect("a graph candidate is always reached via at least one matched entity")
}

/// Count distinct query-matched names mentioned directly by this unit. Related
/// entities cannot promote a direct match; different match classes for the same
/// normalized name still count as one entity.
fn distinct_direct_matched_name_count(candidate: &GraphUnitCandidate) -> usize {
    candidate
        .graph_matches
        .iter()
        .filter(|matched| matches!(&matched.reach, GraphReach::DirectMention))
        .map(|matched| matched.matched_entity.as_str())
        .collect::<std::collections::BTreeSet<&str>>()
        .len()
}

/// Derive candidate entity-name strings from the query text for the D9 entry
/// (lexical match of query text against stored NORMALIZED entity names; no LLM
/// in the query path). The lookup surface (`mentions_for_name` / `one_hop_edges`)
/// is exact-match on a normalized name, so the entry generates contiguous token
/// n-grams of the query, normalizes each via `normalize_entity_name` (the single
/// normalization authority, so the query key is byte-identical to the stored
/// key), and probes by exact normalized name. Multi-word entity names are found
/// because every contiguous run of query tokens up to `max_name_tokens` words is
/// probed. Deduplicated: an empty normalization (query fragment normalizes to
/// "") is dropped; a normalized candidate seen twice is probed once.
///
/// This is a pure lexical derivation — no model call, no fuzzy match — matching
/// D9's "lexical match … no LLM in query path". `max_name_tokens` bounds the
/// n-gram width so the probe count stays linear in query length.
fn candidate_entity_names(query_text: &str, max_name_tokens: usize) -> Vec<String> {
    let tokens: Vec<&str> = query_text.split_whitespace().collect();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for start in 0..tokens.len() {
        let end = (start + max_name_tokens).min(tokens.len());
        for finish in (start + 1)..=end {
            let candidate = tokens[start..finish].join(" ");
            let normalized = normalize_entity_name(&candidate);
            if normalized.is_empty() {
                continue;
            }
            if seen.insert(normalized.clone()) {
                names.push(normalized);
            }
        }
    }
    names
}

// ===========================================================================
// D9 fuzzy match classes (D9 amendment, CA2-P2 2026-07-19).
//
// Two DETERMINISTIC fuzzy classes augment the always-on EXACT class, each gated
// by the entity-match policy's enable flags. The effective EntityMatchPolicy
// carries numeric limits from config.toml, also sealed in the retrieval profile.
// Both classes operate on ALREADY-NORMALIZED strings — the
// query side is normalized by `candidate_entity_names`, the stored side by the
// builder's `normalize_entity_name` — so no side is re-normalized here.
//
// A stored name a query matches EXACTLY is never also fuzzy-classified (never
// double-classify): `fuzzy_matched_names` skips names already in the exact set.
// ===========================================================================

/// Derive the first-letter acronym of an already-normalized stored name from its
/// whitespace-split tokens (e.g. "international business machines" → "ibm").
/// Each token contributes its FIRST CHARACTER (by `chars`, so a multi-byte
/// leading code point is taken whole). Returns `None` when the name has fewer
/// than `min_name_tokens` tokens (no multi-token acronym to match) or when any
/// token is empty (cannot happen for a normalized name — whitespace is collapsed
/// — but guarded so the derivation is total). The stored name is already
/// normalized (lowercased), so the derived acronym is lowercase and compares
/// directly against a normalized query token.
fn derive_acronym(stored_name: &str, min_name_tokens: usize) -> Option<String> {
    let tokens: Vec<&str> = stored_name.split_whitespace().collect();
    if tokens.len() < min_name_tokens {
        return None;
    }
    let mut acronym = String::with_capacity(tokens.len());
    for token in tokens {
        let first = token.chars().next()?;
        acronym.push(first);
    }
    Some(acronym)
}

/// Whether the normalized query n-gram token-prefix-matches the normalized
/// stored name, per the RULED deterministic rule (D9 amendment):
///
///   - Split both into whitespace tokens. The query must have the SAME number of
///     tokens as the stored name OR FEWER, matching a LEADING subsequence: query
///     token `i` must be a prefix of stored token `i` for every `i` in
///     `0..query_tokens.len()` (the query's leading tokens align with the
///     stored name's leading tokens, in order).
///   - EACH query token must be at least `min_token_len` characters — a shorter
///     token is too weak to be an evidentiary prefix and disqualifies the match.
///   - A match that is EQUAL on ALL tokens with EQUAL token counts is EXACT, not
///     token-prefix (it would already be the exact class, and the fuzzy scan
///     excludes exact-matched names upstream); this predicate additionally
///     returns `false` for the all-equal-and-same-count case so it never
///     re-reports an exact identity as fuzzy even in isolation.
///
/// Example: query "acme corp" prefixes stored "acme corporation" ("acme"=="acme"
/// is a prefix, "corp" is a prefix of "corporation"), a token-prefix match.
fn is_token_prefix_match(query_ngram: &str, stored_name: &str, min_token_len: usize) -> bool {
    let query_tokens: Vec<&str> = query_ngram.split_whitespace().collect();
    let stored_tokens: Vec<&str> = stored_name.split_whitespace().collect();
    // Query must be no wider than the stored name (leading-subsequence rule) and
    // non-empty (an empty n-gram matches nothing).
    if query_tokens.is_empty() || query_tokens.len() > stored_tokens.len() {
        return false;
    }
    let mut all_equal = query_tokens.len() == stored_tokens.len();
    for (query_token, stored_token) in query_tokens.iter().zip(stored_tokens.iter()) {
        // Each query token must clear the minimum length (chars, not bytes).
        if query_token.chars().count() < min_token_len {
            return false;
        }
        if !stored_token.starts_with(query_token) {
            return false;
        }
        if query_token != stored_token {
            all_equal = false;
        }
    }
    // All-equal with equal counts is the EXACT identity — not a fuzzy match.
    !all_equal
}

/// Select the stored NORMALIZED entity names a query FUZZY-matches for one parse,
/// tagged with their class, deterministically ordered and capped (D9 amendment).
///
/// Inputs: `query_ngrams` are the query's normalized contiguous token n-grams
/// (from `candidate_entity_names`); `stored_names` is the parse's stored name set
/// in ascending order (from `entity_names_for_parse`); `exact_matched` is the set
/// of stored names already matched EXACTLY this parse (skipped here so no name is
/// double-classified); `policy` supplies the class enables and thresholds.
///
/// Per stored name, the STRONGEST class that matches is chosen (exact was already
/// excluded, so acronym outranks token-prefix). A single query TOKEN drives the
/// acronym class (a multi-token n-gram is not an acronym); any query n-gram may
/// drive token-prefix.
///
/// Ordering (deterministic, RULED selection order — strongest first): by class
/// (acronym before token-prefix), then longest stored name, then name ascending.
/// The first `max_fuzzy_candidates` names in that order are kept; the rest are
/// dropped. Selection is over the whole per-query fuzzy set for THIS parse.
fn fuzzy_matched_names(
    query_ngrams: &[String],
    stored_names: &[String],
    exact_matched: &std::collections::BTreeSet<String>,
    policy: &EntityMatchPolicy,
) -> Vec<MatchedName> {
    let min_name_tokens = policy.acronym.min_name_tokens as usize;
    let min_token_len = policy.token_prefix.min_token_len as usize;

    let mut matched: Vec<MatchedName> = Vec::new();
    for stored_name in stored_names {
        // Never double-classify: a stored name matched exactly is not fuzzy.
        if exact_matched.contains(stored_name) {
            continue;
        }

        // Strongest class first: acronym outranks token-prefix, so test acronym
        // before token-prefix and record only the strongest that matches.
        let mut class: Option<MatchClass> = None;

        if policy.acronym.enabled
            && let Some(acronym) = derive_acronym(stored_name, min_name_tokens)
        {
            // A single query TOKEN (a one-token n-gram, no internal space) equal
            // to the derived acronym is an acronym match.
            let acronym_hit = query_ngrams
                .iter()
                .any(|ngram| !ngram.contains(' ') && *ngram == acronym);
            if acronym_hit {
                class = Some(MatchClass::Acronym);
            }
        }

        if class.is_none()
            && policy.token_prefix.enabled
            && query_ngrams
                .iter()
                .any(|ngram| is_token_prefix_match(ngram, stored_name, min_token_len))
        {
            class = Some(MatchClass::TokenPrefix);
        }

        if let Some(class) = class {
            matched.push(MatchedName {
                class,
                name: stored_name.clone(),
            });
        }
    }

    // Deterministic selection order (strongest first): class, then longest name,
    // then name ascending — the same key the within-tier ordering uses, so the
    // cap keeps the names that would rank highest. `MatchedName`'s `Ord` IS this
    // key, so a plain sort orders strongest-first.
    matched.sort();
    matched.truncate(policy.max_fuzzy_candidates as usize);
    matched
}

/// Build a graph `RetrievalHit` from a ranked graph candidate. Parallel to
/// `fused_hit` (C7b-1) — same population convention — but written separately per
/// the handoff contract (do NOT generalize `fused_hit`). The graph channel's
/// EMITTED ORDER is the entire meaning of a graph result (fusion is rank-only),
/// so `score` is assigned strictly consistent with that order by the caller and
/// passed in here; this helper only stamps the fields.
///
/// Population convention (mirrors `fused_hit`):
/// - `hit_type` = `ContentUnit`; `hit_id` = the unit id; `unit_ids` =
///   `[unit_id]`.
/// - `channel` = `RetrievalChannel::Graph`.
/// - `score` = a rank-monotonic score (higher = stronger) the caller computes
///   from the D9 order; `rank` = the 1-based D9 rank.
/// - `matched_projection_id` / `matched_annotation_id` = `None`.
/// - `explanation` = the matched-entity explanation (the matched normalized
///   names, each tagged with its match class, and the tier), the one optional
///   field the graph channel populates so a debug/trace surface can see WHY the
///   unit was reached. The matched set iterates strongest-first (`MatchedName`'s
///   `Ord`: class, then longest name), so the explanation lists the strongest
///   evidence first.
fn graph_hit(candidate: &GraphUnitCandidate, score: f64, rank: usize) -> RetrievalHit {
    let matched_names: Vec<String> = candidate
        .matched
        .iter()
        .map(|matched| format!("{}[{}]", matched.name, match_class_label(matched.class)))
        .collect();
    let tier_label = match candidate.tier {
        GraphTier::Tier1 => "tier1_multi_entity",
        GraphTier::Tier2 => "tier2_direct_mention",
        GraphTier::Tier3 => "tier3_one_hop",
    };
    let explanation = format!("{tier_label}: matched {}", matched_names.join(", "));
    RetrievalHit {
        hit_type: RetrievalHitType::ContentUnit,
        hit_id: candidate.unit_id.clone(),
        source_id: candidate.source_id.clone(),
        parse_id: candidate.parse_id.clone(),
        unit_ids: vec![candidate.unit_id.clone()],
        channel: RetrievalChannel::Graph,
        score,
        rank: Some(rank as u32),
        matched_projection_id: None,
        matched_annotation_id: None,
        explanation: Some(explanation),
        source_excerpt: None,
        annotation_matches: Vec::new(),
        graph_matches: candidate.graph_matches.iter().cloned().collect(),
        dense_matches: Vec::new(),
    }
}

/// Accumulate one reached unit into the global candidate map, recording its
/// tier and the matched entity (stored name + its match class) it was reached
/// under. A unit reached several ways KEEPS ITS STRONGEST tier (`min` over
/// `GraphTier`, where `Tier1 < Tier2 < Tier3`) and UNIONS its matched set for
/// within-tier strength (strongest class, then longest name). Reach records
/// retain directness for `distinct_direct_matched_name_count`; relational names
/// never count toward the later multi-direct-entity promotion. Keyed by
/// `(parse_id, unit_id)` across all parses.
///
/// The SAME stored name may be recorded under two different classes only across
/// separate reach paths; the exact class is never among them for a name reached
/// fuzzily, because a name matched exactly is excluded from the fuzzy set
/// upstream (never double-classify). Deduplication is by `(class, name)`, so a
/// name recorded twice under one class is stored once.
fn record_graph_unit(
    candidates: &mut HashMap<(String, String), GraphUnitCandidate>,
    unit_id: &str,
    tier: GraphTier,
    matched_name: &MatchedName,
    source_id: &str,
    parse_id: &str,
    reach: GraphReach,
) {
    let graph_match = GraphMatch {
        matched_entity: matched_name.name.clone(),
        match_class: matched_name.class,
        reach,
    };
    candidates
        .entry((parse_id.to_owned(), unit_id.to_owned()))
        .and_modify(|existing| {
            if tier < existing.tier {
                existing.tier = tier;
            }
            existing.matched.insert(matched_name.clone());
            existing.graph_matches.insert(graph_match.clone());
        })
        .or_insert_with(|| {
            let mut matched = std::collections::BTreeSet::new();
            matched.insert(matched_name.clone());
            GraphUnitCandidate {
                unit_id: unit_id.to_owned(),
                tier,
                matched,
                graph_matches: std::collections::BTreeSet::from([graph_match.clone()]),
                source_id: source_id.to_owned(),
                parse_id: parse_id.to_owned(),
            }
        });
}

/// Graph retrieval channel (§24.3, D9): semantic-graph traversal from
/// entity-name matches, tiered by the D9 ordering. The pipeline passes these
/// ranked hits to `collect_channels` alongside dense and lexical candidates.
///
/// Contract (mirrors C7b-1):
/// - `conn` is already inside the per-query DEFERRED read transaction (DP1,
///   §31.1); no connection is opened and no transaction is begun here.
/// - `parses` is the scope-captured active set. Reading mentions/edges ONLY
///   within these parse ids IS the scope mechanism (§6, §38): scope is applied
///   at the entity lookup, never as a post-filter. "Do not post-filter ranked
///   results for scope; enforce scope at candidate generation" (§38).
/// - `query_text` is the raw query; the entry derives candidate entity names
///   from it and matches them against stored NORMALIZED entity names. NO LLM is
///   called anywhere in this path (D9).
/// - `hop_budget` is the D9 relational hop budget (C7a profile `graph_hop_budget`
///   = 1); C7d passes it. Only `hop_budget >= 1` walks the one-hop tier; the
///   structural `UnitRelationships` are NEVER walked at query time (D9).
/// - `policy` is the operator-editable entity-match policy (D9 amendment,
///   CA2-P2 2026-07-19) loaded at startup — deliberately threaded in, NOT the
///   sealed RetrievalProfile (D3 amendment). It governs the two fuzzy classes
///   (acronym, token-prefix) and the `max_fuzzy_candidates` cap. When both fuzzy
///   classes are DISABLED (the shipped default) this path is BYTE-IDENTICAL to
///   the pre-amendment behavior: `entity_names_for_parse` is never called (zero
///   fuzzy scans), no fuzzy names are produced, and every matched name is
///   `MatchClass::Exact`, so the ordering key's class component is constant and
///   the emitted order is exactly the pre-amendment (tier, name-length, unitId)
///   order.
///
/// Match classes per captured parse (D9 amendment):
///   - EXACT (always on): a normalized query n-gram equal to a stored name.
///     Every query n-gram is treated as an exact candidate name and probed —
///     `mentions_for_name` returns empty for a name with no mention row, so a
///     non-matching n-gram contributes nothing (this preserves the exact path
///     verbatim).
///   - ACRONYM / TOKEN_PREFIX (each gated by `policy`): computed by
///     `fuzzy_matched_names` over the parse's enumerated stored names, excluding
///     any name already matched exactly (never double-classify), capped at
///     `policy.max_fuzzy_candidates` per query for this parse.
///
/// Traversal (D9 semantic-only; per in-scope parse, for each matched entity —
/// candidates then merge into one global order across parses):
///   1. `mentions_for_name` → the entity's direct-mention units (tier 2 source).
///   2. If `hop_budget >= 1`, `one_hop_edges` (both directions unioned) → each
///      edge's own supporting `target_unit_ids` are one-hop related (tier 3),
///      and each `far_normalized_name` is followed back through
///      `mentions_for_name` so the FAR entity's mention units are also one-hop
///      related (tier 3). A far entity that is itself a matched entity does not
///      upgrade the tier here; tier 1 is detected by multi-entity DIRECT-mention
///      membership below. The far entity's mention units inherit the NEAR matched
///      entity's class (the class that reached them), keeping within-tier
///      strength keyed on the query-matched evidence.
///   3. Tier 1: a unit is upgraded to tier 1 iff it is a DIRECT mention (tier 2)
///      of MORE THAN ONE distinct matched entity. `distinct_direct_matched_name_count`
///      filters reach records to direct mentions and deduplicates normalized
///      names, independently of match class and relational traversal order.
///
/// Ordering IS the score (D9 amendment): fusion is rank-only, so the emitted
/// ORDER is the entire meaning of a graph result. Candidates are sorted by
/// (tier strongest first; then within-tier by match class exact > acronym >
/// token-prefix; then longest matched normalized name; then name ascending; then
/// unit id ascending) and the emitted `score` is assigned STRICTLY DECREASING
/// with rank so any downstream f64 comparison agrees with the emitted order. A
/// future maintainer MUST NOT "improve" this ordering — it is the ruled D9
/// order and the score is derived from it, not the reverse.
pub(crate) fn graph_channel(
    conn: &Connection,
    query_id: &str,
    parses: &[CapturedParse],
    query_text: &str,
    hop_budget: usize,
    policy: &EntityMatchPolicy,
    semantic_names: &[(String, String)],
) -> Result<Vec<RetrievalHit>, ApiError> {
    let started_at = Instant::now();

    // D9 entry: derive candidate entity n-grams from the query text. Each is
    // already normalized by `candidate_entity_names`, so it is both the exact
    // probe key AND (for fuzzy) the query side compared against stored names.
    let query_ngrams =
        candidate_entity_names(query_text, conn.limits().retrieval.max_entity_name_tokens);

    // Whether ANY fuzzy class runs at all. When false the fuzzy scan surface
    // (`entity_names_for_parse`) is NEVER read — the disabled default does zero
    // extra reads and is byte-identical to the pre-amendment path.
    let fuzzy_enabled = policy.acronym.enabled || policy.token_prefix.enabled;

    // Bounded fuzzy diagnostics (D9 amendment): counts per class and the applied
    // cap, never any name text (DIAGNOSTICS-ONBOARDING forbidden data).
    let mut acronym_matched: usize = 0;
    let mut token_prefix_matched: usize = 0;

    // Global candidate accumulation across ALL in-scope parses, keyed by
    // (parse_id, unit_id) — a unit lives in exactly one parse (mentions/edges are
    // parse-scoped), and the same unit_id string in two different parses is two
    // distinct units. Scope is enforced here: the loop iterates ONLY the captured
    // in-scope parses, so no out-of-scope parse is ever probed. The D9 order is a
    // SINGLE global order over all matched units, so accumulation and ordering
    // span parses — emitting per-parse would grain the emitted order by parse and
    // break the ruled global (tier, class, strength, unitId) order that IS the
    // score.
    let mut candidates: HashMap<(String, String), GraphUnitCandidate> = HashMap::new();
    for parse in parses {
        // Build this parse's matched entities: EXACT for every query n-gram
        // (always on), then the fuzzy classes when enabled. Exact names carry the
        // query n-gram verbatim (byte-identical to the stored name on a hit).
        let mut parse_matches: Vec<MatchedName> = query_ngrams
            .iter()
            .map(|ngram| MatchedName {
                class: MatchClass::Exact,
                name: ngram.clone(),
            })
            .collect();

        if fuzzy_enabled {
            // Enumerate the parse's stored names (the fuzzy scan surface) ONLY
            // when a fuzzy class is enabled. The exact-matched set is the query
            // n-grams that ARE stored names in this parse; fuzzy excludes them so
            // no name is double-classified.
            let stored_names = entity_names_for_parse(conn, &parse.parse_id)?;
            let stored_set: std::collections::BTreeSet<&str> =
                stored_names.iter().map(String::as_str).collect();
            let exact_matched: std::collections::BTreeSet<String> = query_ngrams
                .iter()
                .filter(|ngram| stored_set.contains(ngram.as_str()))
                .cloned()
                .collect();

            let fuzzy = fuzzy_matched_names(&query_ngrams, &stored_names, &exact_matched, policy);
            for matched in &fuzzy {
                match matched.class {
                    MatchClass::Acronym => acronym_matched += 1,
                    MatchClass::TokenPrefix => token_prefix_matched += 1,
                    // Exact is never produced by the fuzzy selector.
                    MatchClass::Exact | MatchClass::Semantic => {}
                }
            }
            parse_matches.extend(fuzzy);
        }

        // Semantic entry is scoped to this captured parse and never displaces
        // a stronger spelling-based classification of the same stored entity.
        for (_, name) in semantic_names
            .iter()
            .filter(|(parse_id, _)| parse_id == &parse.parse_id)
        {
            if !parse_matches.iter().any(|matched| matched.name == *name) {
                parse_matches.push(MatchedName {
                    class: MatchClass::Semantic,
                    name: name.clone(),
                });
            }
        }

        // Pass 1 — direct mentions (tier 2). Every matched entity's mention units
        // are recorded under that entity's stored name and match class. A unit
        // that is a direct mention of two distinct matched entities accumulates
        // two distinct matched names, which pass 3 uses to upgrade it to tier 1.
        for matched in &parse_matches {
            let (_, unit_ids) = mentions_for_name(conn, &parse.parse_id, &matched.name)?;
            for unit_id in unit_ids {
                record_graph_unit(
                    &mut candidates,
                    &unit_id,
                    GraphTier::Tier2,
                    matched,
                    &parse.source_id,
                    &parse.parse_id,
                    GraphReach::DirectMention,
                );
            }
        }

        // Pass 2 — one-hop related units (tier 3), only when the hop budget
        // allows a relational hop. Both the edge's own supporting units and the
        // far entity's mention units are one-hop related. `record_graph_unit`
        // keeps a unit's strongest tier, so a unit that is ALSO a direct mention
        // stays tier 2 (its stronger classification).
        if hop_budget >= 1 {
            for matched in &parse_matches {
                let edges = one_hop_edges(conn, &parse.parse_id, &matched.name)?;
                for edge in edges {
                    // Render the stored subject→object assertion, not traversal
                    // direction: incoming lookups must not reverse the predicate.
                    let (subject, object) = match edge.direction {
                        EdgeDirection::Out => (&matched.name, &edge.far_normalized_name),
                        EdgeDirection::In => (&edge.far_normalized_name, &matched.name),
                    };
                    let relationship = GraphRelationship {
                        subject: subject.clone(),
                        predicate: edge.relation_type.clone(),
                        object: object.clone(),
                        supporting_unit_ids: edge.target_unit_ids.clone(),
                    };
                    // The edge's own supporting units are one-hop related; the
                    // matched entity that reached them is the near (query-matched)
                    // entity, so within-tier strength keys on the near matched
                    // name AND its class, not the far name.
                    for unit_id in &edge.target_unit_ids {
                        record_graph_unit(
                            &mut candidates,
                            unit_id,
                            GraphTier::Tier3,
                            matched,
                            &parse.source_id,
                            &parse.parse_id,
                            GraphReach::RelationSupport {
                                relationship: relationship.clone(),
                            },
                        );
                    }
                    // Follow the far entity back to ITS mention units (D9:
                    // "far-end entities' target units"). These too are one-hop
                    // related and reached via the near matched entity, so they
                    // inherit the near entity's matched name and class.
                    let (_, far_units) =
                        mentions_for_name(conn, &parse.parse_id, &edge.far_normalized_name)?;
                    for unit_id in far_units {
                        record_graph_unit(
                            &mut candidates,
                            &unit_id,
                            GraphTier::Tier3,
                            matched,
                            &parse.source_id,
                            &parse.parse_id,
                            GraphReach::RelatedEntityMention {
                                relationship: relationship.clone(),
                            },
                        );
                    }
                }
            }
        }
    }

    // Only distinct direct mentions qualify for tier 1. The full matched set
    // also contains relational names, which must not inflate this count.
    for candidate in candidates.values_mut() {
        if candidate.tier == GraphTier::Tier2 && distinct_direct_matched_name_count(candidate) > 1 {
            candidate.tier = GraphTier::Tier1;
        }
    }

    // D9 ordering (single global order, fusion is rank-only so this IS the
    // score): tier strongest first; then within-tier by match class (exact >
    // acronym > token-prefix), then longest matched normalized name, then name
    // ascending — all three carried by `candidate_strength_key`; then unit id
    // ascending. `parse_id` is the final discriminator only to keep the sort
    // total when the SAME unit_id string appears in two different parses (two
    // distinct units) — it never reorders within one parse and is not part of the
    // ruled tiebreak.
    let mut ordered: Vec<GraphUnitCandidate> = candidates.into_values().collect();
    ordered.sort_by(|left, right| {
        left.tier
            .cmp(&right.tier)
            .then_with(|| candidate_strength_key(left).cmp(&candidate_strength_key(right)))
            .then_with(|| left.unit_id.cmp(&right.unit_id))
            .then_with(|| left.parse_id.cmp(&right.parse_id))
    });

    // Emit hits with a rank-monotonic score. Score is assigned strictly
    // DECREASING with rank across the GLOBAL ordered candidate list so any f64
    // comparison downstream agrees with the emitted order; the ordinal is what
    // carries meaning (fusion is rank-only). Ranks are 1-based over the whole
    // channel result.
    let mut hits: Vec<RetrievalHit> = Vec::with_capacity(ordered.len());
    for (index, candidate) in ordered.iter().enumerate() {
        let rank = index + 1;
        let score = -(rank as f64);
        hits.push(graph_hit(candidate, score, rank));
    }

    debug!(
        event = "query.channel.graph.completed",
        query_id,
        parse_count = parses.len(),
        query_ngram_count = query_ngrams.len(),
        hop_budget,
        // Bounded fuzzy facts (D9 amendment): per-class matched-name counts and
        // the applied cap; never any name text. When fuzzy is disabled these are
        // zero and no fuzzy scan ran.
        fuzzy_enabled,
        acronym_enabled = policy.acronym.enabled,
        token_prefix_enabled = policy.token_prefix.enabled,
        acronym_matched,
        token_prefix_matched,
        max_fuzzy_candidates = policy.max_fuzzy_candidates,
        hit_count = hits.len(),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "graph channel candidate generation completed"
    );
    Ok(hits)
}
