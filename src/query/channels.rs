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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rusqlite::{Connection, params};
use tracing::{debug, info};

use crate::error::ApiError;
use crate::primitives::bm25::build_bm25_queries;
use crate::primitives::fusion::{Bm25Match, DenseMatch, FusedMatch, fuse_matches};
use crate::projections::dense_cache::DensePlane;
use crate::projections::graph::{mentions_for_name, normalize_entity_name, one_hop_edges};
use crate::projections::lexical::match_chunks;
use crate::query::model::{RetrievalChannel, RetrievalHit, RetrievalHitType};

/// One scope-captured active parse the C7d pipeline hands the channels. It
/// carries both identifiers a `RetrievalHit` needs (`source_id`, `parse_id`)
/// and, for the dense channel, the `Arc<DensePlane>` snapshot C7d captured for
/// this parse INSIDE the read transaction (via `DenseCache::snapshot_for_parse`)
/// so capture and scoring share one WAL snapshot. `dense_plane` is `None` when
/// the parse has no loaded dense plane (never activated a dense projection, or
/// evicted); the dense channel skips such parses without error.
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

/// One chunk-grained lexical candidate before chunk→unit resolution: the chunk
/// id, its BM25 rank (lower is more relevant, per FTS5 `bm25()`), and the parse
/// it came from.
struct LexicalChunkHit {
    chunk_id: String,
    bm25_rank: f64,
    parse_id: String,
    source_id: String,
}

/// Dense retrieval channel (§24.3): exact cosine of `query_vector` against the
/// captured `Arc<DensePlane>` snapshots, one per in-scope parse. Emits up to
/// `limit` chunk-grained hits per parse, highest similarity first.
///
/// DP1/scope: reads only the captured planes on the passed parses — no
/// connection is opened here, no out-of-scope parse is scanned. Cosine reuses
/// each plane's precomputed row norms (`DensePlane::norms`); a query vector with
/// a non-finite or zero norm yields no dense candidates (all cosines are
/// undefined), which is a benign empty result, not an error. Vector VALUES are
/// never logged (forbidden).
fn dense_channel(
    query_id: &str,
    parses: &[CapturedParse],
    query_vector: &[f32],
    limit: usize,
) -> Vec<DenseChunkHit> {
    let started_at = Instant::now();
    let query_norm = l2_norm(query_vector);
    let mut hits: Vec<DenseChunkHit> = Vec::new();

    // A zero/non-finite query norm makes every cosine undefined; skip scoring
    // and return empty rather than emitting NaNs into fusion.
    if query_norm > 0.0 && query_norm.is_finite() {
        for parse in parses {
            let Some(plane) = parse.dense_plane.as_ref() else {
                continue;
            };
            // Dimension mismatch means the query embedding does not belong to
            // this plane's model; scoring it would be meaningless, so skip the
            // plane rather than score across incompatible dimensions.
            if plane.dimension() != query_vector.len() {
                continue;
            }
            let dimension = plane.dimension();
            let vectors = plane.vectors();
            let chunk_ids = plane.chunk_ids();
            let norms = plane.norms();
            let mut parse_hits: Vec<DenseChunkHit> = Vec::with_capacity(plane.row_count());
            for row in 0..plane.row_count() {
                let row_vector = &vectors[row * dimension..(row + 1) * dimension];
                let dot: f32 = row_vector
                    .iter()
                    .zip(query_vector.iter())
                    .map(|(left, right)| left * right)
                    .sum();
                // Row norms are finite and strictly positive (loader guarantee),
                // so the only divisor risk is the query norm, already excluded.
                let similarity = dot / (norms[row] * query_norm);
                parse_hits.push(DenseChunkHit {
                    chunk_id: chunk_ids[row].clone(),
                    similarity,
                    parse_id: parse.parse_id.clone(),
                    source_id: parse.source_id.clone(),
                });
            }
            // Bound per parse to `limit`, most similar first, before merging.
            parse_hits.sort_by(|left, right| right.similarity.total_cmp(&left.similarity));
            parse_hits.truncate(limit);
            hits.extend(parse_hits);
        }
        // Merge across parses and keep the global top `limit` by similarity.
        hits.sort_by(|left, right| right.similarity.total_cmp(&left.similarity));
        hits.truncate(limit);
    }

    debug!(
        event = "query.channel.dense.completed",
        query_id,
        parse_count = parses.len(),
        hit_count = hits.len(),
        limit,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "dense channel candidate generation completed"
    );
    hits
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
    let Some(queries) = build_bm25_queries(query_text) else {
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
    const SELECT_CHUNK_UNITS_SQL: &str =
        "SELECT id, input_unit_ids_json FROM chunk_projections WHERE parse_id = ?1";
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
            .query_map(params![parse.parse_id], |row| {
                let chunk_id: String = row.get(0)?;
                let input_unit_ids_json: String = row.get(1)?;
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

/// Build a unit-grained fused `RetrievalHit` from a `FusedMatch`, tagging it
/// with the fused `channel` and its owning `(source_id, parse_id)`. This is the
/// single `RetrievalHit`-construction convention for fused hits; C7b-2's graph
/// channel MUST mirror this shape (see `graph_hit` when it lands) so every hit
/// the pipeline emits is populated identically.
///
/// Population convention for a fused hit:
/// - `hit_type` = `ContentUnit` (fusion is unit-grained; chunk grain ended at
///   resolution).
/// - `hit_id` = the unit id (identity of the targeted artifact for a unit hit).
/// - `unit_ids` = `[unit_id]` (a fused hit resolves to exactly its one unit).
/// - `channel` = `RetrievalChannel::Dense` (the fused dense+lexical pool is the
///   dense/lexical arm; graph hits carry `Graph`).
/// - `score` = the RRF fused score; `rank` = the fused 1-based rank.
/// - `matched_projection_id` / `matched_annotation_id` = `None` (fused hits
///   originate from chunk projections, not a projection/annotation surface).
fn fused_hit(matched: &FusedMatch, source_id: &str, parse_id: &str) -> RetrievalHit {
    RetrievalHit {
        hit_type: RetrievalHitType::ContentUnit,
        hit_id: matched.unit_id.clone(),
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        unit_ids: vec![matched.unit_id.clone()],
        channel: RetrievalChannel::Dense,
        score: matched.score,
        rank: Some(matched.rank as u32),
        matched_projection_id: None,
        matched_annotation_id: None,
        explanation: None,
    }
}

/// Run the dense and lexical channels over the captured parses, resolve both to
/// unit grain, and RRF-fuse them into ranked `RetrievalHit`s. This is the
/// C7b-1 entry point the C7d pipeline calls after it opens the read transaction
/// and captures the scope-filtered active set.
///
/// Contract:
/// - `conn` is already inside the per-query DEFERRED read transaction (DP1); no
///   connection is opened here.
/// - `query_id` is the operation correlation id stamped on every stage log event
///   this channel and its sub-channels emit.
/// - `parses` is the scope-captured active set; reading only these parses IS
///   the scope mechanism (§6, §38) — nothing is post-filtered for scope.
/// - `query_vector` is the dense query embedding (C7d obtains it via
///   `embed_query_vector`; this channel takes it, it does not embed).
/// - `query_text` is the raw query for FTS5 match-string construction.
/// - `top_k` bounds the fused result; `candidate_limit` bounds each channel's
///   pre-fusion candidate pool per parse; `rrf_k` is the RRF constant (C7a
///   profile: 60).
///
/// Grain: dense/lexical hits are chunk-grained and resolved to units BEFORE
/// fusion (fusion keys on `unit_id`). Each fused hit maps `(source_id, parse_id)`
/// from the chunk→unit mapping's owning parse — a fused unit belongs to exactly
/// one parse because the captured active set has one active parse per source and
/// chunk ids are parse-unique.
// The DP2 explicit-handle discipline plus operation-id correlation force a wide
// flat signature (mirrors execute.rs's allows); a one-call-site params bundle
// would be indirection without reuse.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dense_lexical_fusion_channel(
    conn: &Connection,
    query_id: &str,
    parses: &[CapturedParse],
    query_vector: &[f32],
    query_text: &str,
    top_k: usize,
    candidate_limit: usize,
    rrf_k: u32,
) -> Result<Vec<RetrievalHit>, ApiError> {
    let started_at = Instant::now();
    info!(
        event = "query.fusion.started",
        query_id,
        parse_count = parses.len(),
        top_k,
        candidate_limit,
        rrf_k,
        "dense+lexical fusion started"
    );

    let dense_hits = dense_channel(query_id, parses, query_vector, candidate_limit);
    let lexical_hits = lexical_channel(conn, query_id, parses, query_text, candidate_limit)?;

    // Grain-change boundary: chunk-grained hits → unit-grained matches. Fusion
    // keys on unit_id, so resolution MUST precede fusion (design rule, §6/§38).
    let chunk_unit_map = load_chunk_unit_map(conn, parses)?;
    let dense_matches = resolve_dense_to_units(&dense_hits, &chunk_unit_map);
    let lexical_matches = resolve_lexical_to_units(&lexical_hits, &chunk_unit_map);

    let fused = fuse_matches(&dense_matches, &lexical_matches, top_k, rrf_k);

    // Map each fused unit back to its owning (source_id, parse_id). The unit's
    // owning parse is the parse of any chunk that resolved to it; because each
    // captured source contributes exactly one active parse and chunk ids are
    // parse-unique, this ownership is unambiguous.
    let unit_owner = build_unit_owner_index(&dense_hits, &lexical_hits, &chunk_unit_map);
    let hits: Vec<RetrievalHit> = fused
        .iter()
        .map(|matched| {
            let (source_id, parse_id) = unit_owner
                .get(&matched.unit_id)
                .map(|(source_id, parse_id)| (source_id.as_str(), parse_id.as_str()))
                .unwrap_or(("", ""));
            fused_hit(matched, source_id, parse_id)
        })
        .collect();

    info!(
        event = "query.fusion.completed",
        query_id,
        dense_units = dense_matches.len(),
        lexical_units = lexical_matches.len(),
        fused_hits = hits.len(),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "dense+lexical fusion completed"
    );
    Ok(hits)
}

/// Index each resolved unit to its owning `(source_id, parse_id)` so fused hits
/// can be tagged with provenance. Built from the chunk hits (which carry their
/// parse) joined through the chunk→unit map. First writer wins per unit; a unit
/// resolves to one owning parse (see `dense_lexical_fusion_channel`'s ownership
/// note), so contention is not expected.
fn build_unit_owner_index(
    dense_hits: &[DenseChunkHit],
    lexical_hits: &[LexicalChunkHit],
    chunk_unit_map: &HashMap<String, Vec<String>>,
) -> HashMap<String, (String, String)> {
    let mut owner: HashMap<String, (String, String)> = HashMap::new();
    for hit in dense_hits {
        if let Some(unit_ids) = chunk_unit_map.get(&hit.chunk_id) {
            for unit_id in unit_ids {
                owner
                    .entry(unit_id.clone())
                    .or_insert_with(|| (hit.source_id.clone(), hit.parse_id.clone()));
            }
        }
    }
    for hit in lexical_hits {
        if let Some(unit_ids) = chunk_unit_map.get(&hit.chunk_id) {
            for unit_id in unit_ids {
                owner
                    .entry(unit_id.clone())
                    .or_insert_with(|| (hit.source_id.clone(), hit.parse_id.clone()));
            }
        }
    }
    owner
}

/// L2 norm of a vector. Used to normalize the query vector once for cosine
/// scoring; the plane's row norms are precomputed (`DensePlane::norms`).
fn l2_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

// ===========================================================================
// Graph channel (C7b-2) — D9 semantic-graph traversal with three-tier ordering.
//
// This arm is a SIBLING of `dense_lexical_fusion_channel`, not part of it: C7d
// calls both and concatenates. It reuses C7b-1's scope surface verbatim — it
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

/// One graph candidate unit, carrying everything the D9 ordering needs before
/// it collapses into a `RetrievalHit`. `tier` is the unit's strongest tier
/// (a unit reached several ways keeps its strongest classification); `matched`
/// is the set of NORMALIZED matched-entity names by which this unit was reached
/// (a set so a unit reached twice under the same name is not double-counted and
/// so tier-1 membership — reached via MORE THAN ONE matched entity — is exact);
/// `source_id`/`parse_id` are the owning parse the unit was reached in.
///
/// Within-tier strength (RULED 2026-07-15) keys on the character length of the
/// matched normalized entity name (longer = stronger), then name ascending. A
/// unit may be reached under several matched names (notably a tier-1 unit); the
/// unit's within-tier strength uses its STRONGEST matched name — the longest,
/// breaking ties by name ascending — so a unit is ranked by the strongest
/// evidence that reached it.
struct GraphUnitCandidate {
    unit_id: String,
    tier: GraphTier,
    matched: std::collections::BTreeSet<String>,
    source_id: String,
    parse_id: String,
}

/// The within-tier sort key of a matched normalized entity name (RULED
/// 2026-07-15): longer name is stronger, then name ascending. Encoded as
/// `(Reverse(char_count), name)` so a plain ascending sort yields
/// longest-first, then name-ascending. Character count (not byte length) is
/// used so multi-byte names are compared by the count a human would read.
fn name_strength_key(name: &str) -> (std::cmp::Reverse<usize>, &str) {
    (std::cmp::Reverse(name.chars().count()), name)
}

/// The strongest matched-name strength key across a candidate's matched set —
/// the key of its longest matched name (ties broken by name ascending). The
/// matched set is never empty for a produced candidate (every candidate was
/// reached via at least one matched entity), so `min` always yields a key.
fn candidate_strength_key(candidate: &GraphUnitCandidate) -> (std::cmp::Reverse<usize>, &str) {
    candidate
        .matched
        .iter()
        .map(|name| name_strength_key(name))
        .min()
        .expect("a graph candidate is always reached via at least one matched entity")
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

/// The widest entity name (in tokens) the D9 entry probes for. Entity-annotation
/// names are short; probing every contiguous run up to this width finds
/// multi-word names without an unbounded probe count. Not a config knob — a
/// query-path derivation constant local to the graph entry.
const MAX_ENTITY_NAME_TOKENS: usize = 6;

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
/// - `explanation` = the matched-entity explanation (the normalized matched
///   names and the tier), the one optional field the graph channel populates so
///   a debug/trace surface can see WHY the unit was reached.
fn graph_hit(candidate: &GraphUnitCandidate, score: f64, rank: usize) -> RetrievalHit {
    let matched_names: Vec<&str> = candidate.matched.iter().map(String::as_str).collect();
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
    }
}

/// Accumulate one reached unit into the global candidate map, recording its
/// tier and the matched normalized name it was reached under. A unit reached
/// several ways KEEPS ITS STRONGEST tier (`min` over `GraphTier`, where
/// `Tier1 < Tier2 < Tier3`) and UNIONS its matched-name set — this is exactly
/// how a unit that is a direct mention of two matched entities lands in tier 1
/// (two distinct matched names in its set), while its within-tier strength uses
/// its longest matched name. Keyed by `(parse_id, unit_id)` across all parses.
fn record_graph_unit(
    candidates: &mut HashMap<(String, String), GraphUnitCandidate>,
    unit_id: &str,
    tier: GraphTier,
    matched_name: &str,
    source_id: &str,
    parse_id: &str,
) {
    candidates
        .entry((parse_id.to_owned(), unit_id.to_owned()))
        .and_modify(|existing| {
            if tier < existing.tier {
                existing.tier = tier;
            }
            existing.matched.insert(matched_name.to_owned());
        })
        .or_insert_with(|| {
            let mut matched = std::collections::BTreeSet::new();
            matched.insert(matched_name.to_owned());
            GraphUnitCandidate {
                unit_id: unit_id.to_owned(),
                tier,
                matched,
                source_id: source_id.to_owned(),
                parse_id: parse_id.to_owned(),
            }
        });
}

/// Graph retrieval channel (§24.3, D9): semantic-graph traversal from
/// entity-name matches, tiered by the D9 ordering. This is the C7b-2 entry point
/// the C7d pipeline calls alongside `dense_lexical_fusion_channel`; C7d
/// concatenates the two arms' hits.
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
///
/// Traversal (D9 semantic-only; per in-scope parse, for each matched normalized
/// name — candidates then merge into one global order across parses):
///   1. `mentions_for_name` → the entity's direct-mention units (tier 2 source).
///   2. If `hop_budget >= 1`, `one_hop_edges` (both directions unioned) → each
///      edge's own supporting `target_unit_ids` are one-hop related (tier 3),
///      and each `far_normalized_name` is followed back through
///      `mentions_for_name` so the FAR entity's mention units are also one-hop
///      related (tier 3). A far entity that is itself a matched entity does not
///      upgrade the tier here; tier 1 is detected by multi-entity DIRECT-mention
///      membership below.
///   3. Tier 1: a unit is upgraded to tier 1 iff it is a DIRECT mention (tier 2)
///      of MORE THAN ONE distinct matched entity — detected because such a unit
///      accumulates two distinct matched names in its set. This is computed by
///      recording every direct mention first, then upgrading units whose matched
///      set has size > 1.
///
/// Ordering IS the score (D9): fusion is rank-only, so the emitted ORDER is the
/// entire meaning of a graph result. Candidates are sorted by (tier strongest
/// first; then within-tier by longest matched normalized name, then name
/// ascending; then unit id ascending) and the emitted `score` is assigned
/// STRICTLY DECREASING with rank so any downstream f64 comparison agrees with
/// the emitted order. A future maintainer MUST NOT "improve" this ordering — it
/// is the ruled D9 order and the score is derived from it, not the reverse.
pub(crate) fn graph_channel(
    conn: &Connection,
    query_id: &str,
    parses: &[CapturedParse],
    query_text: &str,
    hop_budget: usize,
) -> Result<Vec<RetrievalHit>, ApiError> {
    let started_at = Instant::now();

    // D9 entry: derive candidate entity names from the query text and match them
    // against stored NORMALIZED entity names (no LLM, no fuzzy match). Names are
    // already normalized by `candidate_entity_names`, so they are passed to the
    // graph lookups as-is (which require a normalized key).
    let matched_names = candidate_entity_names(query_text, MAX_ENTITY_NAME_TOKENS);

    // Global candidate accumulation across ALL in-scope parses, keyed by
    // (parse_id, unit_id) — a unit lives in exactly one parse (mentions/edges are
    // parse-scoped), and the same unit_id string in two different parses is two
    // distinct units. Scope is enforced here: the loop iterates ONLY the captured
    // in-scope parses, so no out-of-scope parse is ever probed. The D9 order is a
    // SINGLE global order over all matched units, so accumulation and ordering
    // span parses — emitting per-parse would grain the emitted order by parse and
    // break the ruled global (tier, strength, unitId) order that IS the score.
    let mut candidates: HashMap<(String, String), GraphUnitCandidate> = HashMap::new();
    for parse in parses {
        // Pass 1 — direct mentions (tier 2). Every matched entity's mention units
        // are recorded under that entity's normalized name. A unit that is a
        // direct mention of two matched entities accumulates two matched names,
        // which pass 3 uses to upgrade it to tier 1.
        for name in &matched_names {
            let (normalized_name, unit_ids) = mentions_for_name(conn, &parse.parse_id, name)?;
            for unit_id in unit_ids {
                record_graph_unit(
                    &mut candidates,
                    &unit_id,
                    GraphTier::Tier2,
                    &normalized_name,
                    &parse.source_id,
                    &parse.parse_id,
                );
            }
        }

        // Pass 2 — one-hop related units (tier 3), only when the hop budget
        // allows a relational hop. Both the edge's own supporting units and the
        // far entity's mention units are one-hop related. `record_graph_unit`
        // keeps a unit's strongest tier, so a unit that is ALSO a direct mention
        // stays tier 2 (its stronger classification).
        if hop_budget >= 1 {
            for name in &matched_names {
                let edges = one_hop_edges(conn, &parse.parse_id, name)?;
                for edge in edges {
                    // The edge's own supporting units are one-hop related; the
                    // matched name that reached them is the near (query-matched)
                    // entity, so within-tier strength keys on the matched name,
                    // not the far name.
                    for unit_id in &edge.target_unit_ids {
                        record_graph_unit(
                            &mut candidates,
                            unit_id,
                            GraphTier::Tier3,
                            name,
                            &parse.source_id,
                            &parse.parse_id,
                        );
                    }
                    // Follow the far entity back to ITS mention units (D9:
                    // "far-end entities' target units"). These too are one-hop
                    // related and reached via the near matched name.
                    let (_, far_units) =
                        mentions_for_name(conn, &parse.parse_id, &edge.far_normalized_name)?;
                    for unit_id in far_units {
                        record_graph_unit(
                            &mut candidates,
                            &unit_id,
                            GraphTier::Tier3,
                            name,
                            &parse.source_id,
                            &parse.parse_id,
                        );
                    }
                }
            }
        }
    }

    // Pass 3 — tier-1 upgrade: a unit reached as a DIRECT mention of MORE THAN
    // ONE distinct matched entity is tier 1. Such a unit is currently tier 2 with
    // two-or-more matched names; upgrade it. A tier-3-only unit is never upgraded
    // here (it is not a direct mention), even if it was reached via several
    // matched names, because tier 1 is a multi-entity DIRECT-mention property per
    // D9. Runs once over the global candidate set.
    for candidate in candidates.values_mut() {
        if candidate.tier == GraphTier::Tier2 && candidate.matched.len() > 1 {
            candidate.tier = GraphTier::Tier1;
        }
    }

    // D9 ordering (single global order, fusion is rank-only so this IS the
    // score): tier strongest first; then within-tier by longest matched
    // normalized name, then name ascending; then unit id ascending. `parse_id` is
    // the final discriminator only to keep the sort total when the SAME unit_id
    // string appears in two different parses (two distinct units) — it never
    // reorders within one parse and is not part of the ruled tiebreak.
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
        matched_name_count = matched_names.len(),
        hop_budget,
        hit_count = hits.len(),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "graph channel candidate generation completed"
    );
    Ok(hits)
}
