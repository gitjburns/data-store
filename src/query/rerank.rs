//! Persisted ColBERT-window MaxSim and final passage reranking on the query snapshot.
//! Exact source-window matrices are scored separately by query::annotation.
//!
//! Two scoring stages live here, in pipeline order:
//!
//! 1. **ColBERT MaxSim** scores the ColBERT windows (PLAN-grains Section 2:
//!    runs of fine chunks) reached from the fused pool's source hits, using
//!    the window matrices PERSISTED at build time in `colbert_windows`. A hit
//!    resolves to windows through chunk membership: its units to the fine
//!    chunks whose fragments cite them, those chunks to the windows holding
//!    them. §38 forbids recomputing document vectors at search time, so the
//!    loader decodes stored blobs and MaxSim never re-embeds a document. The
//!    caller embeds the query once for source and annotation scoring. Local
//!    scoring acquires the model-call gate after matrix loading; HTTP CPU
//!    MaxSim runs without that accelerator permit.
//!
//! 2. **Final reranker** scores passages built from ranked windows and exact windows via the config-selected
//!    reranker backend. The gate discipline is CALLER-SIDE and backend-aware:
//!    the Local backend is a live accelerator call and MUST run under the
//!    model-call gate; the Http backend is network I/O and MUST NOT hold the
//!    gate across the request. `uses_local_model_gate()` is the predicate that
//!    decides which path a given backend takes (it acquires nothing itself).
//!
//! NOTE on the deferred `multi_vector` CHANNEL: ColBERT MaxSim here is a
//! rerank/scoring stage over an ALREADY-fused subset, not a candidate-generation
//! channel. The `multi_vector` retrieval channel is deferred post-MVP
//! (2026-07-15 rescope); this stage is retained.
//!
//! DP1: persisted-matrix reads run on the caller-supplied connection that is
//! already inside the per-query read transaction. Stage functions never open a
//! connection or begin a transaction of their own.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use crate::sqlite::Connection;
use rusqlite::params;
use serde::Serialize;
use tracing::{error, info};

use crate::error::ApiError;
use crate::inference::{
    ColbertBackend, PreparedColbertQuery, RerankerBackend, RerankerCandidateInput,
};
use crate::projections::chunk::Fragment;
use crate::projections::multivector;
use crate::query::model::RetrievalHit;
use crate::query::passages::PassageCandidate;
use crate::query::provenance::SourceFragment;
use crate::state::{ExclusiveGate, acquire_model_call_gate_on};

/// Fragment-only read of one parse's fine chunks in reading order
/// (`chunk_index`, the sole order authority), for unit-to-chunk resolution:
/// the chunk id and its fragments, never the text. SQL bounds the JSON cell
/// before it becomes a Rust string; a NULL cell is a resource refusal.
const SELECT_PARSE_CHUNK_FRAGMENTS_SQL: &str = "
SELECT id, CASE WHEN length(CAST(fragments_json AS BLOB)) <= ?2 THEN fragments_json END
FROM chunk_projections WHERE parse_id = ?1 ORDER BY chunk_index";

/// Model-call-gate identities for the two local model calls this stage may make.
/// Kept identical to the scheduler's colbert acquire (`scheduler.rs`) so the
/// `model_gate.*` acquisition logging is byte-identical across the query and
/// sync paths that share the one process-global gate.
const COLBERT_MODEL_ROLE: &str = "colbert";
const COLBERT_CALL_PURPOSE: &str = "persisted_candidate_scoring";
const RERANKER_MODEL_ROLE: &str = "reranker";
const RERANKER_CALL_PURPOSE: &str = "candidate_batch_scoring";

/// The per-query handles both rerank stages share, grouped so each stage's
/// signature carries its stage-specific inputs without re-threading the four
/// common handles. This is a parameter bundle, NOT a state-management seam: the
/// C7d caller constructs it from the explicit handles it already holds (DP2),
/// and it borrows for the stage call only.
///
/// - `conn` is the caller-owned read connection already inside the per-query
///   read transaction (DP1); every hot-plane read a stage performs rides it.
/// - `gate` is the process-global model-call gate handle threaded from C7d
///   (amendment 3, DP2 explicit-handles seam); stages acquire it caller-side
///   for local model calls.
/// - `query` is the query text embedded live by ColBERT / scored by the
///   reranker; `query_id` is the operation id used as the gate `operation_id`.
pub(crate) struct RerankStageContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) gate: &'a Arc<ExclusiveGate>,
    pub(crate) query: &'a str,
    pub(crate) query_id: &'a str,
}

/// A final passage score uses the complete candidate identity, independent of its canonical anchor.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PassageScore {
    pub(crate) candidate_id: String,
    pub(crate) score: f32,
    pub(crate) rank: usize,
    /// Backend diagnostics remain absent when the provider did not report them.
    pub(crate) logit: Option<f32>,
    pub(crate) token_count: Option<usize>,
}

/// One ColBERT window scored by MaxSim: the window id the score keys on, the
/// parse it belongs to, and the membership downstream consumes. `fragments`
/// are the window's unit ranges in reading order in the wire fragment shape,
/// so the passage builder slices them without conversion; `chunk_ids` are
/// reported in diagnostics. Unit identity reaches provenance and citations
/// only through the fragments.
#[derive(Debug)]
pub(crate) struct ColbertWindowScore {
    pub(crate) window_id: String,
    pub(crate) parse_id: String,
    pub(crate) chunk_ids: Vec<String>,
    pub(crate) fragments: Vec<SourceFragment>,
    pub(crate) score: f32,
    pub(crate) rank: usize,
    /// Matrix row count of the scored window, for the stage's safe aggregate log.
    pub(crate) document_tokens: usize,
}

/// ColBERT MaxSim over the windows the fused pool's source hits reach.
///
/// Resolves the pool to distinct ColBERT window ids (`capped_pool_windows`),
/// caps them at `colbert_candidate_pool_size`, loads only the capped windows'
/// persisted matrices via the query-time loader — NO document recomputation
/// (§38) — then scores them against the caller's prepared query. Matrix
/// loading precedes the local scoring permit; the caller must release its
/// query-embedding permit before entering this stage. HTTP CPU MaxSim needs no gate.
///
/// Exact `source_excerpt` hits are handled by annotation::score_excerpts and skipped here.
/// Remaining candidates are grouped by
/// their originating `parse_id` because both the matrix load and the store are
/// parse-scoped; a query in scope over several sources fuses hits from several
/// active parses. `expected_dimension` is the runtime ColBERT projection
/// dimension the decoder validates against.
///
/// Returns MaxSim scores ranked best first, with window ids breaking ties.
/// Windows with no stored matrix are omitted by the loader and carry no score.
pub(crate) fn run_maxsim_stage(
    ctx: &RerankStageContext<'_>,
    colbert: &ColbertBackend,
    prepared: &PreparedColbertQuery,
    pool: &[RetrievalHit],
    expected_dimension: usize,
    colbert_candidate_pool_size: usize,
) -> Result<Vec<ColbertWindowScore>, ApiError> {
    let started_at = Instant::now();

    // Resolve hits to windows and cap at the ColBERT candidate-pool size
    // BEFORE loading any matrices: matrix movement is the cost the pool cap
    // bounds, so the loader never reads beyond the capped set. Resolution
    // reads membership columns only.
    let capped = capped_pool_windows(ctx.conn, pool, colbert_candidate_pool_size).inspect_err(
        |source| {
            error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                stage = "window_resolution", pool_size = pool.len(),
                error = %source, error_chain = %crate::util::error_chain(source, &ctx.conn.limits().diagnostics),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "fused pool could not be resolved to ColBERT windows");
        },
    )?;
    let candidate_count: usize = capped.values().map(|windows| windows.len()).sum();

    info!(
        event = "rerank.maxsim.started",
        backend = ?colbert.backend_kind(),
        uses_local_model_gate = colbert.uses_local_model_gate(),
        query_id = ctx.query_id,
        parse_count = capped.len(),
        candidate_count,
        colbert_candidate_pool_size,
        query_tokens = prepared.token_count(),
        expected_dimension,
        "ColBERT MaxSim stage started"
    );

    // Load persisted matrices per parse (parse-scoped, §14). Concatenate across
    // parses into one candidate batch; MaxSim scoring is order-independent and
    // re-ranks globally, so a flat batch is correct.
    // Each loaded window is paired with its parse id (borrowed from the capped
    // map) because the score must name the parse the passage builder reads.
    let mut candidates = Vec::with_capacity(candidate_count);
    for (parse_id, window_ids) in &capped {
        let loaded = multivector::load_colbert_windows(
            ctx.conn,
            parse_id,
            window_ids,
            expected_dimension,
        )
        .inspect_err(|source| {
            error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                parse_id, stage = "matrix_loading", candidate_count = window_ids.len(),
                error = %source, error_chain = %crate::util::error_chain(source, &ctx.conn.limits().diagnostics),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "ColBERT window matrices could not be loaded");
        })?;
        candidates.extend(loaded.into_iter().map(|window| (parse_id.as_str(), window)));
    }
    let loaded_count = candidates.len();

    // The prepared query is reused; only local scoring needs admission, after persisted I/O finishes.
    let gate_started = Instant::now();
    let (scores, gate_wait_ms, scoring_ms) = {
        let permit = if colbert.uses_local_model_gate() {
            Some(
                acquire_model_call_gate_on(
                    ctx.gate,
                    ctx.query_id,
                    COLBERT_MODEL_ROLE,
                    COLBERT_CALL_PURPOSE,
                )
                .inspect_err(|source| {
                    error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                    stage = "model_gate", error = %source,
                    error_chain = %crate::util::error_chain(source, &ctx.conn.limits().diagnostics),
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "ColBERT scoring could not acquire the model gate");
                })?,
            )
        } else {
            None
        };
        let gate_wait_ms = if permit.is_some() {
            gate_started.elapsed().as_millis() as u64
        } else {
            0
        };
        let scoring_started = Instant::now();
        let mut scores = Vec::with_capacity(loaded_count);
        // Windows are consumed so their membership moves into the scores
        // instead of being copied; the matrix is dropped once scored.
        for (parse_id, window) in candidates {
            let score = colbert
                .score_matrix(prepared, &window.vector, window.token_count, window.dimension)
                .inspect_err(|source| {
                    error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                        backend = ?colbert.backend_kind(),
                        stage = "scoring", candidate_count = loaded_count,
                        window_id = %window.window_id, parse_id,
                        error = %source, error_chain = %crate::util::error_chain(source, &ctx.conn.limits().diagnostics),
                        gate_wait_ms, scoring_ms = scoring_started.elapsed().as_millis() as u64,
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        "ColBERT MaxSim scoring failed");
                })?;
            scores.push(ColbertWindowScore {
                window_id: window.window_id,
                parse_id: parse_id.to_owned(),
                chunk_ids: window.chunk_ids,
                fragments: window.fragments.into_iter().map(wire_fragment).collect(),
                score,
                rank: 0,
                document_tokens: window.token_count,
            });
        }
        let scoring_ms = scoring_started.elapsed().as_millis() as u64;
        // Release local admission immediately after its accelerator work.
        drop(permit);
        // Matrix load order spans parses; stable global score ranks must not
        // depend on that order. Ties break on window id.
        scores.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.window_id.cmp(&right.window_id))
        });
        for (index, score) in scores.iter_mut().enumerate() {
            score.rank = index + 1;
        }
        (scores, gate_wait_ms, scoring_ms)
    };

    info!(
        event = "rerank.maxsim.completed",
        backend = ?colbert.backend_kind(),
        query_id = ctx.query_id,
        parse_count = capped.len(),
        candidate_count,
        loaded_candidates = loaded_count,
        missing_matrices = candidate_count.saturating_sub(loaded_count),
        gate_wait_ms,
        scoring_ms,
        scored = scores.len(),
        colbert_candidate_pool_size,
        query_tokens = prepared.token_count(),
        document_tokens = scores.iter().map(|score| score.document_tokens).sum::<usize>(),
        expected_dimension,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "ColBERT MaxSim stage completed"
    );

    Ok(scores)
}

/// Score the caller-bounded passages, including their section headings, using
/// the configured backend. Candidate IDs keep disjoint passages from the same
/// canonical anchor distinct across the backend's opaque scoring-key contract.
///
/// Gate discipline (CALLER-SIDE, §1.5 pinned; mirrors the scheduler's local
/// acquire). `uses_local_model_gate()` is a PREDICATE that returns `true` for
/// the Local backend (a live accelerator call) and `false` for Http (network
/// I/O); it acquires nothing itself. When it is `true`, this caller acquires the
/// model-call gate around the scoring call and drops it immediately after. When
/// it is `false`, the caller must NEVER hold the gate across the HTTP request:
/// the Http branch below does not acquire the gate at all, so no code path holds
/// the gate across network I/O.
pub(crate) fn run_reranker_stage(
    ctx: &RerankStageContext<'_>,
    reranker: &RerankerBackend,
    passages: &[PassageCandidate],
) -> Result<Vec<PassageScore>, ApiError> {
    let started_at = Instant::now();

    // Passage construction already resolved canonical content on the query snapshot.
    // Do not reload the representative unit and accidentally score a fragment again.
    let inputs: Vec<RerankerCandidateInput> = passages
        .iter()
        .map(|passage| RerankerCandidateInput {
            // The inference API calls this field unit_id, but returns the caller's opaque key.
            unit_id: passage.candidate_id.clone(),
            content: passage.ranking_text(),
        })
        .collect();
    let input_char_count: usize = inputs
        .iter()
        .map(|input| input.content.chars().count())
        .sum();

    let uses_gate = reranker.uses_local_model_gate();
    info!(
        event = "rerank.final.started",
        query_id = ctx.query_id,
        backend = reranker.kind(),
        uses_local_model_gate = uses_gate,
        candidate_count = inputs.len(),
        input_char_count,
        "final reranker stage started"
    );

    // Gate boundary: acquire ONLY for the local accelerator path. The Http
    // branch performs no acquire, so the gate is never held across network I/O.
    // Keep waiting separate from backend scoring; HTTP scoring includes the
    // network round trip and is not a claim about provider inference time.
    let mut gate_wait_ms = 0_u64;
    let mut scoring_ms = None;
    let mut failed_stage = "model_gate";
    let scoring_result = (|| {
        if uses_gate {
            let gate_started = Instant::now();
            let admission = acquire_model_call_gate_on(
                ctx.gate,
                ctx.query_id,
                RERANKER_MODEL_ROLE,
                RERANKER_CALL_PURPOSE,
            );
            gate_wait_ms = gate_started.elapsed().as_millis() as u64;
            let permit = admission?;
            failed_stage = "scoring";
            let scoring_started = Instant::now();
            let scores = reranker.score_candidates(ctx.query, &inputs);
            scoring_ms = Some(scoring_started.elapsed().as_millis() as u64);
            // `permit` drops here: the local reranker gate release follows scoring.
            drop(permit);
            scores
        } else {
            // Http backend: no gate. Holding the model-call gate across the HTTP
            // request would serialize a network round-trip behind the exclusive
            // local-accelerator gate — forbidden (§1.5 gate discipline).
            failed_stage = "scoring";
            let scoring_started = Instant::now();
            let scores = reranker.score_candidates(ctx.query, &inputs);
            scoring_ms = Some(scoring_started.elapsed().as_millis() as u64);
            scores
        }
    })();
    let scores = match scoring_result {
        Ok(scores) => scores,
        Err(source) => {
            error!(
                event = "rerank.final.failed",
                stage = failed_stage,
                gate_wait_ms,
                scoring_ms,
                query_id = ctx.query_id,
                backend = reranker.kind(),
                uses_local_model_gate = uses_gate,
                candidate_count = inputs.len(),
                input_char_count,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                error = %source,
                error_chain = %crate::util::error_chain(&source, &ctx.conn.limits().diagnostics),
                "passage reranking failed"
            );
            return Err(source);
        }
    };

    info!(
        event = "rerank.final.completed",
        gate_wait_ms,
        scoring_ms,
        query_id = ctx.query_id,
        backend = reranker.kind(),
        uses_local_model_gate = uses_gate,
        candidate_count = inputs.len(),
        scored = scores.len(),
        input_char_count,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "final reranker stage completed"
    );

    Ok(scores
        .into_iter()
        .map(|score| PassageScore {
            candidate_id: score.unit_id,
            score: score.score,
            rank: score.rank,
            logit: score.logit,
            token_count: score.token_count,
        })
        .collect())
}

/// Resolve the fused pool to the ColBERT windows to score, grouped by
/// originating `parse_id`, capping the TOTAL distinct window count at
/// `colbert_candidate_pool_size` in fused (first-occurrence) order.
///
/// Every source hit carries unit ids (dense and lexical chunk hits were
/// resolved to units at candidate generation; graph hits name one unit), so
/// one rule serves all: a hit's units resolve to the fine chunks whose
/// fragments cite them (`unit_chunks`, one membership read per parse, bounded
/// by the parse's chunk count), and those chunks to their windows
/// (`windows_for_chunks`, one membership read per parse, bounded by the
/// parse's window count). Neither read moves a matrix. Exact-window hits
/// belong to the annotation scorer and are skipped. A unit split across a
/// window boundary reaches both windows. A window already collected from an
/// earlier (higher-ranked) hit is not re-added, so the cap counts distinct
/// windows; resolution completes before the cap so the capped set is the
/// best-ranked windows, and the cap is applied before any matrix load.
fn capped_pool_windows(
    conn: &Connection,
    pool: &[RetrievalHit],
    colbert_candidate_pool_size: usize,
) -> Result<BTreeMap<String, Vec<String>>, ApiError> {
    // Exact windows have separate persisted matrices and are scored by
    // annotation::score_excerpts; only source hits resolve to ColBERT windows.
    let source_hits = || pool.iter().filter(|hit| hit.source_excerpt.is_none());

    // Step 1: one unit-to-chunk membership read per parse in the pool.
    let mut chunk_index_of_parse: BTreeMap<&str, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    for hit in source_hits() {
        if !chunk_index_of_parse.contains_key(hit.parse_id.as_str()) {
            chunk_index_of_parse.insert(hit.parse_id.as_str(), unit_chunks(conn, &hit.parse_id)?);
        }
    }

    // Step 2: units to chunks, distinct, in fused hit order.
    let mut ordered_chunks: Vec<(&str, &str)> = Vec::new();
    let mut seen_chunks: BTreeSet<(&str, &str)> = BTreeSet::new();
    for hit in source_hits() {
        let Some(index) = chunk_index_of_parse.get(hit.parse_id.as_str()) else {
            continue;
        };
        for unit_id in &hit.unit_ids {
            for chunk_id in index.get(unit_id).into_iter().flatten() {
                let entry = (hit.parse_id.as_str(), chunk_id.as_str());
                if seen_chunks.insert(entry) {
                    ordered_chunks.push(entry);
                }
            }
        }
    }

    // Step 3: chunks to windows, one membership read per parse.
    let mut window_of_chunk: BTreeMap<&str, BTreeMap<String, String>> = BTreeMap::new();
    for parse_id in chunk_index_of_parse.keys().copied() {
        let chunk_ids: Vec<String> = ordered_chunks
            .iter()
            .filter(|(parse, _)| *parse == parse_id)
            .map(|(_, chunk_id)| (*chunk_id).to_owned())
            .collect();
        window_of_chunk.insert(
            parse_id,
            multivector::windows_for_chunks(conn, parse_id, &chunk_ids)?,
        );
    }

    // Step 4: distinct windows in fused order, capped BEFORE any matrix load.
    let mut by_parse: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut total = 0usize;
    for (parse_id, chunk_id) in ordered_chunks {
        if total >= colbert_candidate_pool_size {
            break;
        }
        let Some(window_id) = window_of_chunk
            .get(parse_id)
            .and_then(|windows| windows.get(chunk_id))
        else {
            // A chunk with no window has no persisted matrix to score.
            continue;
        };
        let windows = by_parse.entry(parse_id.to_owned()).or_default();
        if windows.iter().any(|known| known == window_id) {
            continue;
        }
        windows.push(window_id.clone());
        total += 1;
    }
    Ok(by_parse)
}

/// Read one parse's unit-to-chunk membership from `chunk_projections`
/// fragments: for each cited unit, the ids of the chunks holding a fragment
/// of it, in `chunk_index` order. A chunk citing a unit through several
/// fragments (a split unit re-joined in one chunk) is listed once. Bounded by
/// the parse's chunk count and reads no chunk text.
fn unit_chunks(
    conn: &Connection,
    parse_id: &str,
) -> Result<BTreeMap<String, Vec<String>>, ApiError> {
    let max_cell_bytes = conn.limits().resources.max_json_cell_bytes;
    let mut statement = conn
        .prepare(SELECT_PARSE_CHUNK_FRAGMENTS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare chunk fragment query for parse {parse_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![parse_id, max_cell_bytes], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query chunk fragments for parse {parse_id}: {source}"),
        })?;
    let mut index: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let (chunk_id, fragments_json) = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read chunk fragment row for parse {parse_id}: {source}"),
        })?;
        let fragments_json = fragments_json.ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "resource limit: fragments of chunk {chunk_id} in parse {parse_id} exceed resources.max_json_cell_bytes {max_cell_bytes}"
            ),
        })?;
        let fragments: Vec<Fragment> =
            serde_json::from_str(&fragments_json).map_err(|source| ApiError::StorageOperation {
                message: format!("fragments of chunk {chunk_id} are unparseable: {source}"),
            })?;
        for fragment in fragments {
            let chunks = index.entry(fragment.unit_id).or_default();
            // Rows arrive in chunk_index order, so a repeat can only be this
            // same chunk's earlier fragment of the unit.
            if chunks.last().is_none_or(|last| last != &chunk_id) {
                chunks.push(chunk_id.clone());
            }
        }
    }
    Ok(index)
}

/// The wire form of a persisted fragment; the two types are declared apart
/// only because the CLI binary path-includes the wire module.
fn wire_fragment(fragment: Fragment) -> SourceFragment {
    SourceFragment {
        unit_id: fragment.unit_id,
        start_char: fragment.start_char,
        end_char: fragment.end_char,
    }
}
