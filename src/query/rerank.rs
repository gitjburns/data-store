//! Rerank stage (§24, §38): ColBERT MaxSim over the fused candidate pool via
//! persisted matrices, then the final reranker. Content lands with package
//! C7c; consumed at C7d/C8d.
//!
//! Two scoring stages live here, in pipeline order:
//!
//! 1. **ColBERT MaxSim** re-scores the fused candidate pool (dense+lexical+graph,
//!    already fused by RRF) using the ColBERT document matrices PERSISTED at
//!    build time. §38 forbids recomputing those document vectors at search
//!    time, so the loader decodes stored blobs and MaxSim never re-embeds a
//!    document. The query is embedded live by the selected ColBERT backend.
//!    Local inference/scoring holds the model-call gate; remote query embedding
//!    and CPU MaxSim run without that accelerator permit.
//!
//! 2. **Final reranker** scores passages built from MaxSim-ranked units via the config-selected
//!    reranker backend. The gate discipline is CALLER-SIDE and backend-aware:
//!    the Local backend is a live accelerator call and MUST run under the
//!    model-call gate; the Http backend is network I/O and MUST NOT hold the
//!    gate across the request. `uses_local_model_gate()` is the predicate that
//!    decides which path a given backend takes (it acquires nothing itself).
//!
//! NOTE on the deferred `multi_vector` CHANNEL: ColBERT MaxSim here is a
//! rerank/scoring stage over the ALREADY-fused pool, not a candidate-generation
//! channel. The `multi_vector` retrieval channel is deferred post-MVP
//! (2026-07-15 rescope); this stage is retained.
//!
//! DP1: persisted-matrix reads run on the caller-supplied connection that is
//! already inside the per-query read transaction. Stage functions never open a
//! connection or begin a transaction of their own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use rusqlite::Connection;
use tracing::{error, info};

use crate::error::ApiError;
use crate::inference::{
    ColbertBackend, ColbertCandidateScore, RerankerBackend, RerankerCandidateInput,
    RerankerCandidateScore,
};
use crate::projections::multivector;
use crate::query::model::RetrievalHit;
use crate::query::passages::PassageCandidate;
use crate::state::{ExclusiveGate, acquire_model_call_gate_on};

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

/// ColBERT MaxSim over the fused candidate pool.
///
/// Loads the persisted ColBERT matrices for the fused pool (capped at
/// `colbert_candidate_pool_size`) via the query-time loader — NO document
/// recomputation (§38) — then scores them against a LIVE query embedding.
/// The local backend holds the model-call gate while embedding and scoring;
/// the HTTP backend embeds remotely and performs CPU MaxSim without that gate.
///
/// `pool` is the RRF-fused, unit-grained hit list. Candidates are grouped by
/// their originating `parse_id` because both the matrix load and the store are
/// parse-scoped; a query in scope over several sources fuses hits from several
/// active parses. `expected_dimension` is the runtime ColBERT projection
/// dimension the decoder validates against.
///
/// Returns the MaxSim scores (already ranked, best first, by
/// `score_persisted_candidates`). Units in the pool with no stored matrix are
/// omitted by the loader and therefore carry no MaxSim score.
pub(crate) fn run_maxsim_stage(
    ctx: &RerankStageContext<'_>,
    colbert: &ColbertBackend,
    pool: &[RetrievalHit],
    expected_dimension: usize,
    colbert_candidate_pool_size: usize,
) -> Result<Vec<ColbertCandidateScore>, ApiError> {
    let started_at = Instant::now();

    // Cap the fused pool at the ColBERT candidate-pool size BEFORE loading any
    // matrices: matrix movement is the cost the post-MVP rescope bounds, so the
    // loader never reads beyond the capped set.
    let capped = capped_pool_units(pool, colbert_candidate_pool_size);
    let candidate_count: usize = capped.values().map(|units| units.len()).sum();

    info!(
        event = "rerank.maxsim.started",
        backend = ?colbert.backend_kind(),
        uses_local_model_gate = colbert.uses_local_model_gate(),
        query_id = ctx.query_id,
        parse_count = capped.len(),
        candidate_count,
        colbert_candidate_pool_size,
        expected_dimension,
        "ColBERT MaxSim stage started"
    );

    // Load persisted matrices per parse (parse-scoped, §14). Concatenate across
    // parses into one candidate batch; MaxSim scoring is order-independent and
    // re-ranks globally, so a flat batch is correct.
    let mut candidates = Vec::with_capacity(candidate_count);
    for (parse_id, unit_ids) in &capped {
        let mut loaded = multivector::load_multivectors_for_units(
            ctx.conn,
            parse_id,
            unit_ids,
            expected_dimension,
        )
        .inspect_err(|source| {
            error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                parse_id, stage = "matrix_loading", candidate_count = unit_ids.len(),
                error = %source, error_chain = %crate::util::error_chain(source),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "ColBERT candidate matrices could not be loaded");
        })?;
        candidates.append(&mut loaded);
    }

    // Only the local backend embeds and scores on the accelerator. HTTP query
    // embedding and its CPU MaxSim comparison must not hold that exclusive gate.
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
                    error_chain = %crate::util::error_chain(source),
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
        let scores = colbert
            .score_persisted_candidates(ctx.query, &candidates)
            .inspect_err(|source| {
                error!(event = "rerank.maxsim.failed", query_id = ctx.query_id,
                    backend = ?colbert.backend_kind(),
                    stage = "scoring", candidate_count = candidates.len(),
                    error = %source, error_chain = %crate::util::error_chain(source),
                    gate_wait_ms, scoring_ms = scoring_started.elapsed().as_millis() as u64,
                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                    "ColBERT MaxSim scoring failed");
            })?;
        let scoring_ms = scoring_started.elapsed().as_millis() as u64;
        // Release local admission immediately after its accelerator work.
        drop(permit);
        (scores, gate_wait_ms, scoring_ms)
    };

    info!(
        event = "rerank.maxsim.completed",
        backend = ?colbert.backend_kind(),
        query_id = ctx.query_id,
        parse_count = capped.len(),
        candidate_count,
        loaded_candidates = candidates.len(),
        missing_matrices = candidate_count.saturating_sub(candidates.len()),
        gate_wait_ms,
        scoring_ms,
        scored = scores.len(),
        colbert_candidate_pool_size,
        expected_dimension,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "ColBERT MaxSim stage completed"
    );

    Ok(scores)
}

/// Score the caller-bounded passages, including their section headings, using
/// the configured backend. Scores belong to complete passages; the representative
/// anchor id is only the stable join key used to attach each returned score.
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
) -> Result<Vec<RerankerCandidateScore>, ApiError> {
    let started_at = Instant::now();

    // Passage construction already resolved canonical content on the query snapshot.
    // Do not reload the representative unit and accidentally score a fragment again.
    let inputs: Vec<RerankerCandidateInput> = passages
        .iter()
        .map(|passage| RerankerCandidateInput {
            unit_id: passage.anchor_unit_id.clone(),
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
                error_chain = %crate::util::error_chain(&source),
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

    Ok(scores)
}

/// Group the fused pool's unit ids by originating `parse_id`, capping the TOTAL
/// candidate count at `colbert_candidate_pool_size` while preserving fused
/// order. The pool is unit-grained (chunk→unit resolution happened before
/// fusion), so each hit's `unit_ids` are the candidate units it contributes; a
/// unit already collected (a later hit resolving to the same unit) is not
/// re-added, so the cap counts distinct candidates.
fn capped_pool_units(
    pool: &[RetrievalHit],
    colbert_candidate_pool_size: usize,
) -> BTreeMap<String, Vec<String>> {
    let mut by_parse: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut seen: BTreeMap<String, ()> = BTreeMap::new();
    let mut total = 0usize;
    'pool: for hit in pool {
        for unit_id in &hit.unit_ids {
            if total >= colbert_candidate_pool_size {
                break 'pool;
            }
            // Skip a unit already collected from an earlier (higher-ranked) hit
            // so the cap counts distinct candidate units.
            if seen.insert(unit_id.clone(), ()).is_some() {
                continue;
            }
            by_parse
                .entry(hit.parse_id.clone())
                .or_default()
                .push(unit_id.clone());
            total += 1;
        }
    }
    by_parse
}
