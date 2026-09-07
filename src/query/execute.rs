//! Query pipeline integration (§24, §31, §38): `execute_query`, the single
//! synchronous per-query pipeline that assembles the C7 retrieval fabric.
//!
//! `execute_query` opens ONE read-only WAL snapshot as its first act (DP1),
//! captures the scope-filtered active `(source_id → parse_id)` set INSIDE that
//! transaction, then drives the fabric stages in order — dense+lexical fusion
//! (C7b-1), graph traversal (C7b-2), ColBERT MaxSim over the fused pool and the
//! final reranker (C7c) — and returns the ranked candidates plus per-stage
//! latencies. Every hot-plane read for one query rides the single connection
//! this function owns; nothing here opens a second connection or begins a second
//! transaction.
//!
//! DP1 (RULED 2026-07-15): "every query executes ALL its hot-plane reads inside
//! ONE per-query read-only transaction on one connection, opened as the
//! pipeline's first act; the scope-filtered active (source_id → parse_id)
//! capture is read INSIDE that transaction so capture and reads share one WAL
//! snapshot (§31.1)." The SQLite reads (active-set capture, lexical, chunk→unit,
//! multivector, unit content) share that ONE WAL snapshot via the single read
//! transaction; the dense planes are IN-MEMORY `Arc` clones whose isolation
//! comes from the `DenseCache` Arc-clone immutability discipline
//! (`dense_cache.rs::snapshot_for_parse`), not from WAL. Recorded tradeoff: a
//! pinned WAL read snapshot blocks checkpointing past it for the query's
//! duration (bounded by C8d's single-search admission window); the
//! snapshot-held duration is logged at completion.
//!
//! DP2 (RULED 2026-07-15; barrier probe amended 2026-07-15 by C8d-2): this is
//! the SOLE synchronous pipeline entry point and it takes EXPLICIT handles (no
//! globals, no `AppState` reach-through). The in-flight admission gate stays the
//! C8d-2 CALLER's job (the `/query` handler acquires the permit before
//! `spawn_blocking`). The cutover-barrier check (`reject_if_active`), however,
//! now runs INSIDE this pipeline: it must probe the ACTUAL captured active set,
//! which is only known after capture, so the probe is placed in
//! `run_pipeline_body` immediately post-capture and pre-retrieval (R2 — "never
//! partially executed", §31.1). `execute_query` therefore consumes the barrier
//! registry (no longer a dormant handle). Synchronous by contract: the SQLite
//! and local-accelerator work is blocking, and the caller owns the blocking
//! boundary (a `spawn_blocking` seam), so this function does no async and holds
//! no runtime.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use rusqlite::{Connection, params, params_from_iter};
use tracing::{error, info, warn};

use crate::assembly::evidence::{EvidenceOptions, PassageEvidence, build_evidence_pack};
use crate::assembly::model::EvidencePack;
use crate::assembly::policy::{CapturedParseRef, active_policy};
use crate::error::ApiError;
use crate::inference::{ColbertCandidateScore, InferenceRuntime, RerankerCandidateScore};
use crate::policy::EntityMatchPolicy;
use crate::projections::dense_cache::DenseCache;
use crate::query::channels::{CapturedParse, fused_channels, graph_channel};
use crate::query::model::{ResolvedScope, ResolvedScopeKind, RetrievalHit};
use crate::query::passages::{PassageCandidate, SearchResult, build_passages};
use crate::query::profile::RetrievalProfile;
use crate::query::request::EvidenceOptions as RequestEvidenceOptions;
use crate::query::rerank::{RerankStageContext, run_maxsim_stage, run_reranker_stage};
use crate::state::{CutoverRegistry, ExclusiveGate, acquire_model_call_gate_on};

/// Log-event namespace passed to the shared hot-plane transaction helpers so the
/// per-query begin boundary log is attributable to the query pipeline (mirrors
/// `annotations/worker.rs::TX_LOG_NAMESPACE`).
const TX_LOG_NAMESPACE: &str = "query";

/// The gate model-role/purpose for the LOCAL dense query embedding. Kept
/// identical to the scheduler's dense acquire (`scheduler.rs`) so the
/// `model_gate.*` acquisition logging is byte-identical across the query and
/// sync paths that share the one process-global gate.
const DENSE_MODEL_ROLE: &str = "dense";
const DENSE_CALL_PURPOSE: &str = "query_embedding";

/// Per-stage wall-clock latencies for the fabric query pipeline (§24). One field
/// per meaningful pipeline stage `execute_query` runs, so a single value carries
/// the whole operation's timing to the C8d caller / trace surface.
///
/// This is the fabric-shaped replacement for the legacy `primitives/latency.rs`
/// struct (whose six legacy stage fields do not fit this pipeline; it is being
/// deleted by the main loop). Times are milliseconds; a stage that did not run
/// (e.g. no fused candidates, so MaxSim scored nothing) records its measured
/// elapsed regardless, which for a no-op stage is ~0.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryStageLatencies {
    /// Opening the read connection and beginning the DEFERRED read transaction.
    pub(crate) open_transaction_ms: u64,
    /// Capturing the scope-filtered active set and cloning the dense planes,
    /// inside the transaction (DP1).
    pub(crate) capture_ms: u64,
    /// The live dense query embedding (`embed_query_vector`, gated dense role).
    pub(crate) query_embed_ms: u64,
    /// Dense+lexical candidate generation, chunk→unit resolution, and RRF fusion.
    pub(crate) dense_lexical_fusion_ms: u64,
    /// Graph channel D9 traversal + tiering.
    pub(crate) graph_ms: u64,
    /// ColBERT MaxSim over the fused pool (persisted matrices; gated colbert role).
    pub(crate) maxsim_ms: u64,
    /// Canonical passage construction and overlap merging, before final scoring.
    pub(crate) passage_build_ms: u64,
    /// Final reranker scoring (gated only on the local backend).
    pub(crate) rerank_ms: u64,
    /// Context-assembly stage: EvidencePack construction from the reranked
    /// anchors (§25–§27), run inside the read transaction after rerank.
    pub(crate) assembly_ms: u64,
    /// Wall-clock duration the WAL read snapshot was held (transaction open →
    /// drop). This is the DP1 checkpoint-blocking window; logged and returned so
    /// the tradeoff is observable.
    pub(crate) snapshot_held_ms: u64,
}

/// The result of one query pipeline run: the assembled EvidencePack plus the raw
/// stage candidates (for `debug` diagnostics) and per-stage latencies. Returned
/// in-memory to the C8d-2 caller, which serializes the pack into the `/query`
/// response and, when `debug` is set, the raw stage pools too. The
/// per-stage/per-channel RankingTrace tier and the QueryExecutionRecord are
/// deferred, so traces stay in the diagnostics log rather than a persisted QER
/// (recorded deviation).
pub(crate) struct QueryPipelineOutcome {
    /// Ranked passages and citations, ready for presentation without client logic.
    pub(crate) results: Vec<SearchResult>,
    /// Canonical constituent records and their selection trace for raw inspection.
    pub(crate) evidence_pack: EvidencePack,
    /// The deduplicated three-channel RRF pool that ColBERT scored.
    pub(crate) fused_pool: Vec<RetrievalHit>,
    /// Eligible channel records before fusion, preserving their individual scores.
    pub(crate) channel_hits: Vec<RetrievalHit>,
    /// Complete passage candidates offered to the final reranker, including those
    /// outside the requested final result count.
    pub(crate) passage_candidates: Vec<PassageCandidate>,
    /// ColBERT MaxSim scores over the fused pool, best-first (empty when the pool
    /// had no unit with a persisted ColBERT matrix). `debug`-only.
    pub(crate) maxsim: Vec<ColbertCandidateScore>,
    /// Final passage scores, best-first, including candidates beyond the result cap.
    pub(crate) reranked: Vec<RerankerCandidateScore>,
    /// Per-stage timings, including the DP1 snapshot-held duration.
    pub(crate) latencies: QueryStageLatencies,
}

/// One scope-captured active source before dense-plane cloning: the source and
/// its single active parse. Read INSIDE the transaction so the capture shares the
/// query's one WAL snapshot (DP1).
struct ActiveParse {
    source_id: String,
    parse_id: String,
}

/// The request-scoped inputs the pipeline threads into candidate sizing and the
/// assembly stage, bundled so the pipeline signature does not grow two more loose
/// request parameters. Both values answer "what did THIS request ask for" (the
/// resolved output cap and the evidence-shaping options), so threading them as
/// one context keeps the `execute_query`/`run_pipeline_body` call sites legible
/// rather than absorbing more args under the existing `too_many_arguments`
/// allow. The `debug` flag is NOT bundled here: it gates only the HTTP response
/// shape (raw-diagnostics attachment), which the handler owns, so the pipeline
/// never consults it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryRequestContext {
    /// Maximum final passages; never used to shrink early candidate generation.
    pub(crate) max_final_evidence_units: u32,
    /// Resolved evidence-output options (locators/relationships/annotations)
    /// mapped into the assembly-layer `EvidenceOptions` at the assembly stage.
    pub(crate) evidence_options: RequestEvidenceOptions,
}

/// Execute one retrieval-fabric query end to end (§24), synchronously, against a
/// single per-query WAL read snapshot.
///
/// Handles are EXPLICIT (DP2): every dependency arrives as a parameter, never
/// through `AppState` or a global.
/// - `registry` is the process cutover-barrier registry. The pipeline probes it
///   (`reject_if_active`) post-capture, pre-retrieval (R2): the probe must see
///   the ACTUAL captured active sources, so it runs inside `run_pipeline_body`
///   after capture. The in-flight admission gate remains the C8d-2 caller's job
///   (the handler holds a permit around this whole call); this pipeline gates
///   only the barrier, not concurrency (§31.1).
/// - `dense_cache` supplies the per-parse `Arc<DensePlane>` snapshots the dense
///   channel scores against; captured for the in-scope parses.
/// - `inference` supplies the dense / colbert / reranker runtimes.
/// - `gate` is the process-global model-call gate (`AppState`'s handle, threaded
///   in — never reached through `InferenceRuntime`). The three local model calls
///   (dense query embed here; ColBERT query embed inside MaxSim; local reranker)
///   are gated CALLER-SIDE via `acquire_model_call_gate_on`, each permit acquired
///   immediately before its call and dropped immediately after — never held
///   across SQL reads or HTTP I/O (mirrors `scheduler.rs`).
/// - `index_root` is the fabric index root the read connection opens against.
/// - `profile` is the sealed §24.2 `RetrievalProfile`; every knob (top_k, pool
///   sizes, rrf_k, hop budget) is threaded from it, never from config.
/// - `entity_match_policy` is the operator-editable entity-match policy (D9
///   amendment, CA2-P2 2026-07-19) loaded at startup and threaded to the graph
///   channel for its fuzzy match classes. It is DELIBERATELY separate from
///   `profile`: the sealed RetrievalProfile stays sealed (D3 amendment); this
///   document is operator-tunable. Passed by reference (no clone).
/// - `colbert_expected_dimension` is the runtime ColBERT projection width the
///   multi-vector decoder validates against. It arrives as an explicit value
///   because `ColbertRuntime` exposes no dimension accessor; the build path
///   receives the same value threaded from config (`config.models.colbert.dimension`,
///   `scheduler.rs::ProjectionRuntime::colbert_dimension`), so the C8d caller
///   passes `config.models.colbert.dimension as usize` here (mirroring the build
///   path).
/// - `query_id` is the operation id used as the model-gate `operation_id` and the
///   diagnostics correlation id; `query_text` is the raw user query.
/// - `scope` is the already-resolved §24.2 scope (from `profile::resolve_scope`);
///   it bounds the captured active set — scope is enforced at capture / candidate
///   generation and NEVER as a post-filter over ranked hits (§6, §38).
/// - `request_ctx` bundles the request-scoped assembly inputs (the resolved
///   final-evidence-unit cap, the evidence options, and the debug flag).
///
/// DP1: the read connection + DEFERRED read transaction are opened as the FIRST
/// act, the active-set capture and dense-plane clones happen INSIDE it, and every
/// hot-plane read (fusion, graph, multivector load, unit content) rides that one
/// connection. The snapshot-held duration is logged at completion.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query(
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    inference: &InferenceRuntime,
    gate: &Arc<ExclusiveGate>,
    index_root: &Path,
    profile: &RetrievalProfile,
    entity_match_policy: &EntityMatchPolicy,
    colbert_expected_dimension: usize,
    query_id: &str,
    query_text: &str,
    scope: &ResolvedScope,
    request_ctx: QueryRequestContext,
) -> Result<QueryPipelineOutcome, ApiError> {
    let operation_started_at = Instant::now();
    let mut latencies = QueryStageLatencies::default();

    info!(
        event = "query.execute.started",
        query_id,
        scope_kind = ?scope.kind,
        // Keep the established log field; it now caps final passages only.
        top_k = request_ctx.max_final_evidence_units as usize,
        colbert_candidate_pool_size = profile.colbert_candidate_pool_size,
        reranker_candidate_pool_size = profile.reranker_candidate_pool_size,
        "query pipeline started"
    );

    // === DP1 FIRST ACT: open the read connection and begin the ONE per-query
    // DEFERRED read transaction. Every hot-plane read below runs on `conn`
    // inside `tx`, so capture and all channel reads share one WAL snapshot
    // (§31.1). `tx` derefs to `Connection`, so it is passed to the channel and
    // rerank stages as `&*tx`. ===
    let open_started_at = Instant::now();
    let mut connection = crate::hot_plane::open_read(index_root).inspect_err(|error| {
        error!(
            event = "query.execute.failed",
            query_id,
            stage = "open_read",
            error = %error,
            "query pipeline failed opening the read connection"
        );
    })?;
    let tx = crate::hot_plane::begin_read_transaction(
        &mut connection,
        TX_LOG_NAMESPACE,
        "execute_query",
    )
    .inspect_err(|error| {
        error!(
            event = "query.execute.failed",
            query_id,
            stage = "begin_read_transaction",
            error = %error,
            "query pipeline failed beginning the read transaction"
        );
    })?;
    // The WAL read snapshot is now pinned; measure how long it is held so the
    // checkpoint-blocking tradeoff is observable (DP1-mandated log at drop).
    let snapshot_started_at = Instant::now();
    latencies.open_transaction_ms = open_started_at.elapsed().as_millis() as u64;

    // Run the body against `&*tx`. Split out so the snapshot-held log below fires
    // on BOTH success and failure — the read transaction drops when `tx` leaves
    // scope regardless of outcome (a read tx needs no commit).
    let result = run_pipeline_body(
        &tx,
        registry,
        dense_cache,
        inference,
        gate,
        profile,
        entity_match_policy,
        colbert_expected_dimension,
        query_id,
        query_text,
        scope,
        request_ctx,
        &mut latencies,
    );

    // DP1 snapshot-held-duration log: emitted at the boundary where the read
    // transaction completes for BOTH success and failure, because the pinned WAL
    // snapshot blocked checkpointing for exactly this window either way.
    latencies.snapshot_held_ms = snapshot_started_at.elapsed().as_millis() as u64;
    info!(
        event = "query.execute.snapshot_released",
        query_id,
        snapshot_held_ms = latencies.snapshot_held_ms,
        "query WAL read snapshot released; checkpointing unblocked past this query"
    );
    // `tx` drops here (read transaction: no commit needed), releasing the
    // snapshot and the connection.
    drop(tx);

    match result {
        Ok(mut outcome) => {
            outcome.latencies = latencies.clone();
            info!(
                event = "query.execute.completed",
                query_id,
                fused_pool = outcome.fused_pool.len(),
                maxsim_scored = outcome.maxsim.len(),
                reranked = outcome.reranked.len(),
                evidence_units = outcome.evidence_pack.evidence_units.len(),
                assembly_ms = latencies.assembly_ms,
                snapshot_held_ms = latencies.snapshot_held_ms,
                elapsed_ms = operation_started_at.elapsed().as_millis() as u64,
                "query pipeline completed"
            );
            Ok(outcome)
        }
        Err(error) => {
            error!(
                event = "query.execute.failed",
                query_id,
                stage = "pipeline_body",
                error = %error,
                snapshot_held_ms = latencies.snapshot_held_ms,
                elapsed_ms = operation_started_at.elapsed().as_millis() as u64,
                "query pipeline failed"
            );
            Err(error)
        }
    }
}

/// The pipeline body, run on the caller-owned read transaction. Split out so the
/// snapshot-held log in `execute_query` fires on every exit path (the `?`
/// operators here propagate to the caller, which logs the snapshot release before
/// returning the error). `latencies` is filled in-place as stages run.
#[allow(clippy::too_many_arguments)]
fn run_pipeline_body(
    conn: &Connection,
    registry: &CutoverRegistry,
    dense_cache: &DenseCache,
    inference: &InferenceRuntime,
    gate: &Arc<ExclusiveGate>,
    profile: &RetrievalProfile,
    entity_match_policy: &EntityMatchPolicy,
    colbert_expected_dimension: usize,
    query_id: &str,
    query_text: &str,
    scope: &ResolvedScope,
    request_ctx: QueryRequestContext,
    latencies: &mut QueryStageLatencies,
) -> Result<QueryPipelineOutcome, ApiError> {
    // === Capture the scope-filtered active set INSIDE the transaction (DP1),
    // then clone the dense planes for those parses. The SQLite active-set read
    // shares the query's one WAL snapshot; the dense planes are in-memory `Arc`
    // clones whose isolation comes from the DenseCache Arc-clone immutability
    // discipline (dense_cache.rs::snapshot_for_parse), not from WAL. ===
    let capture_started_at = Instant::now();
    let active = capture_active_set(conn, scope)?;

    // === R2 CUTOVER-BARRIER PROBE (§31.1). Placement: post-capture (the probe
    // must see the ACTUAL captured active sources) and PRE-retrieval (before any
    // dense-plane clone / embed / channel read), so a query targeting a source
    // mid-cutover aborts the WHOLE query BEFORE any retrieval stage runs — "never
    // partially executed". Probing each captured source's per-source barrier; ANY
    // active barrier propagates `CutoverBarrierActive` (mapped to the retryable
    // 503 `ApiError::CutoverBarrierActive` via `From<state::CutoverBarrierActive>`)
    // out through `?`, so this exit rides the same `snapshot_released` log
    // `execute_query` emits on every return. A barrier engaging AFTER this probe
    // passes is licensed §31.1 in-flight behavior: this query already holds a
    // consistent WAL read snapshot (DP1), so the cutover cannot tear its reads. ===
    for parse in &active {
        registry.reject_if_active(&parse.source_id)?;
    }

    let captured = clone_dense_planes(dense_cache, active, query_id);
    latencies.capture_ms = capture_started_at.elapsed().as_millis() as u64;
    info!(
        event = "query.execute.captured",
        query_id,
        parse_count = captured.len(),
        with_dense_plane = captured.iter().filter(|p| p.dense_plane.is_some()).count(),
        capture_ms = latencies.capture_ms,
        "scope-filtered active set captured inside the read snapshot"
    );

    // === Dense query embedding, backend-aware gate discipline (§1.5, mirrors the
    // reranker stage in `query::rerank` and the scheduler's dense build). For the
    // LOCAL backend, `uses_local_model_gate()` is true: acquire the dense-role
    // permit immediately before the embed and drop it immediately after — never
    // held across the SQL reads that follow. For the HTTP backend it is false: no
    // permit is acquired, so the exclusive gate is never held across the network
    // round-trip. `embed_query_vector` never acquires the gate itself. ===
    let embed_started_at = Instant::now();
    let query_vector = if inference.dense.uses_local_model_gate() {
        let permit =
            acquire_model_call_gate_on(gate, query_id, DENSE_MODEL_ROLE, DENSE_CALL_PURPOSE)?;
        let vector = inference.dense.embed_query_vector(query_text)?;
        // `permit` drops here, releasing the gate before any further SQL read.
        drop(permit);
        vector
    } else {
        // HTTP backend: no gate across the network round-trip (§1.5).
        inference.dense.embed_query_vector(query_text)?
    };
    latencies.query_embed_ms = embed_started_at.elapsed().as_millis() as u64;

    // Generate graph hits before fusion so they compete in the same bounded
    // three-channel pool instead of being appended after its cutoff.
    let graph_started_at = Instant::now();
    let graph_hits = graph_channel(
        conn,
        query_id,
        &captured,
        query_text,
        profile.graph_hop_budget as usize,
        entity_match_policy,
    )?;
    latencies.graph_ms = graph_started_at.elapsed().as_millis() as u64;
    // Candidate depth is independent of the requested final passage count.
    let fusion_started_at = Instant::now();
    let top_k = request_ctx.max_final_evidence_units as usize;
    let fusion = fused_channels(
        conn,
        query_id,
        &captured,
        &query_vector,
        query_text,
        &graph_hits,
        profile,
    )?;
    latencies.dense_lexical_fusion_ms = fusion_started_at.elapsed().as_millis() as u64;

    let pool = fusion.pool;

    // === Build the parse-of-unit index passage construction needs to resolve unit
    // content parse-scoped. A pool can span several active parses (a query in
    // scope over several sources), so each hit's units map to that hit's parse.
    // First writer wins per unit (a unit belongs to one active parse). ===
    let parse_of_unit = build_parse_of_unit(&pool);

    // === Rerank context: constructed from the handles this function holds (DP2).
    // `conn` is the shared read transaction; `gate` is the model-call gate; the
    // two rerank stages acquire the gate caller-side for THEIR local model calls
    // (ColBERT query embed inside MaxSim; local reranker) — already handled inside
    // the C7c stages, so C7d does NOT double-gate them. ===
    let ctx = RerankStageContext {
        conn,
        gate,
        query: query_text,
        query_id,
    };

    // === ColBERT MaxSim over the fused pool (C7c): re-scores the pool via
    // persisted matrices (loaded on `conn`); the ColBERT query embed inside is
    // gated by the stage itself (not double-gated here). ===
    let maxsim_started_at = Instant::now();
    let maxsim = run_maxsim_stage(
        &ctx,
        &inference.colbert,
        &pool,
        colbert_expected_dimension,
        profile.colbert_candidate_pool_size as usize,
    )?;
    latencies.maxsim_ms = maxsim_started_at.elapsed().as_millis() as u64;

    // Form distinct passages before final scoring. Larger requested result sets
    // raise passage candidate depth without exceeding the already-bounded seeds.
    let passage_started_at = Instant::now();
    let passage_limit = (profile.reranker_candidate_pool_size as usize)
        .max(top_k)
        .min(profile.colbert_candidate_pool_size as usize);
    let passage_candidates = build_passages(
        conn,
        &maxsim,
        &parse_of_unit,
        inference.colbert.tokenizer(),
        passage_limit,
        query_id,
    )?;
    latencies.passage_build_ms = passage_started_at.elapsed().as_millis() as u64;

    // The final model evaluates the same passage and section context that will
    // be presented. Gate ownership remains inside the reranking boundary.
    let rerank_started_at = Instant::now();
    let reranked = run_reranker_stage(&ctx, &inference.reranker, &passage_candidates)?;
    latencies.rerank_ms = rerank_started_at.elapsed().as_millis() as u64;

    // Final count applies here, after passage ranking. A score with an unknown
    // candidate identity is a protocol fault, never silently dropped evidence.
    let selected = reranked
        .iter()
        .take(top_k)
        .map(|score| {
            let candidate = passage_candidates
                .iter()
                .find(|candidate| candidate.anchor_unit_id == score.unit_id)
                .ok_or_else(|| ApiError::StorageOperation {
                    message: format!("reranker returned unknown passage {}", score.unit_id),
                })?;
            Ok((candidate, f64::from(score.score)))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    // Canonical evidence and source citations are resolved before releasing this
    // snapshot, so the two response surfaces cannot disagree across a cutover.
    let assembly_started_at = Instant::now();
    let evidence_pack = assemble_evidence_pack(
        conn,
        inference,
        &captured,
        &selected,
        request_ctx.evidence_options,
        query_id,
        query_text,
    )?;
    // Candidates are retained for debug inspection; clone only the final
    // passages to produce independently owned presentation records.
    let results = selected
        .into_iter()
        .map(|(candidate, score)| candidate.clone().into_result(conn, score))
        .collect::<Result<Vec<_>, _>>()?;
    latencies.assembly_ms = assembly_started_at.elapsed().as_millis() as u64;

    Ok(QueryPipelineOutcome {
        results,
        evidence_pack,
        fused_pool: pool,
        channel_hits: fusion.channel_hits,
        passage_candidates,
        maxsim,
        reranked,
        // Filled by `execute_query` once the snapshot-held time is known.
        latencies: QueryStageLatencies::default(),
    })
}

/// Bridge ranked passage selections to canonical evidence assembly. Membership
/// is explicit and ordered; assembly adds no further neighbors after ranking.
fn assemble_evidence_pack(
    conn: &Connection,
    inference: &InferenceRuntime,
    captured: &[CapturedParse],
    selected: &[(&PassageCandidate, f64)],
    request_options: RequestEvidenceOptions,
    query_id: &str,
    query_text: &str,
) -> Result<EvidencePack, ApiError> {
    let policy = active_policy()?;

    // Captured active parses → C8a's borrowed-primitive capture contract.
    let captured_refs: Vec<CapturedParseRef<'_>> = captured
        .iter()
        .map(|parse| CapturedParseRef {
            source_id: &parse.source_id,
            parse_id: &parse.parse_id,
        })
        .collect();

    let passages: Vec<PassageEvidence<'_>> = selected
        .iter()
        .map(|(candidate, score)| PassageEvidence {
            anchor_unit_id: &candidate.anchor_unit_id,
            parse_id: &candidate.parse_id,
            unit_ids: &candidate.unit_ids,
            score: *score,
        })
        .collect();

    // Map the request-layer options into the assembly-layer contract field by
    // field (the EvidenceOptions seam — see fn doc).
    let options = EvidenceOptions {
        include_source_locators: request_options.include_source_locators,
        include_relationships: request_options.include_relationships,
        include_annotations: request_options.include_annotations,
    };

    // R10 TOKEN CLOSURE: wrap the ColBERT tokenizer — the SAME tokenizer the
    // chunker measures its token cap against (`chunk.rs::count_tokens`,
    // `runtime.colbert.tokenizer()`), so the assembler's token budget is measured
    // in the same units the corpus was banked under. `add_special_tokens = true`
    // matches the chunker. This is CPU-only tokenizer work and MUST NOT acquire
    // the model-call gate: the gate protects accelerator (dense/colbert/reranker)
    // calls only, and assembly never touches it.
    let tokenizer = inference.colbert.tokenizer();
    let count_tokens = |text: &str| -> Result<usize, ApiError> {
        tokenizer
            .encode(text, true)
            .map(|encoding| encoding.len())
            .map_err(|source| ApiError::StorageOperation {
                message: format!("assembly token count failed: {source}"),
            })
    };

    build_evidence_pack(
        conn,
        &captured_refs,
        policy,
        &passages,
        options,
        count_tokens,
        query_id,
        query_text,
    )
}

/// Capture the scope-filtered active `(source_id → parse_id)` set inside the
/// read transaction (DP1). The resolved scope selects WHICH sources' active
/// parses are in scope; reading only these parses IS the scope mechanism (§6,
/// §38), applied at capture / candidate generation and never as a post-filter.
///
/// An active source is one with a non-null `active_parse_id` and no
/// `deactivated_at` (mirrors `annotations/worker.rs::SELECT_ACTIVE_SOURCES_SQL`).
/// - `All`: every active source (§6 reservation 1: default scope is all sources).
/// - `SourceSet`: active sources whose id is in the requested set. When the scope
///   ALSO carries `governance_domains` (the R3 both-present conjunction), the
///   capture additionally requires a `current` location in one of those domains —
///   the constraints CONJOIN (§24.3), so the captured set is the intersection.
///   An empty intersection captures nothing (an empty `Vec`), never an error.
/// - `DomainSet`: active sources whose `source_locations.governance_domain` is in
///   the requested set (distinct sources, since a source may have several
///   locations).
fn capture_active_set(
    conn: &Connection,
    scope: &ResolvedScope,
) -> Result<Vec<ActiveParse>, ApiError> {
    match scope.kind {
        ResolvedScopeKind::All => capture_all_active(conn),
        ResolvedScopeKind::SourceSet => {
            // An empty/absent source set never reaches here as SourceSet
            // (`resolve_scope` maps an empty set to `All`); a defensively-empty
            // list captures no sources rather than scanning everything.
            let ids = scope.source_ids.as_deref().unwrap_or(&[]);
            match scope.governance_domains.as_deref() {
                // R3 conjunction: both source_ids and governance_domains present.
                // Capture the INTERSECTION — sources in the id set that also have
                // a current location in one of the domains.
                Some(domains) if !domains.is_empty() => {
                    capture_active_by_source_ids_and_domains(conn, ids, domains)
                }
                // Source set alone (no domain narrowing).
                _ => capture_active_by_source_ids(conn, ids),
            }
        }
        ResolvedScopeKind::DomainSet => {
            let domains = scope.governance_domains.as_deref().unwrap_or(&[]);
            capture_active_by_domains(conn, domains)
        }
    }
}

/// SQL selecting every active source and its parse (the `All` scope base).
const SELECT_ALL_ACTIVE_SQL: &str = "
SELECT id, active_parse_id FROM source_objects
WHERE active_parse_id IS NOT NULL AND deactivated_at IS NULL";

/// Capture all active sources' parses (`All` scope).
fn capture_all_active(conn: &Connection) -> Result<Vec<ActiveParse>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_ALL_ACTIVE_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to prepare active-source capture: {source}"),
            })?;
    let rows = statement
        .query_map(params![], |row| {
            Ok(ActiveParse {
                source_id: row.get(0)?,
                parse_id: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query active sources: {source}"),
        })?;
    collect_active_parses(rows)
}

/// Capture active parses for an explicit `source_ids` set (`SourceSet` scope).
/// The placeholder list is built from the id count so scope is enforced in SQL
/// (the read never scans out-of-scope sources).
fn capture_active_by_source_ids(
    conn: &Connection,
    source_ids: &[String],
) -> Result<Vec<ActiveParse>, ApiError> {
    if source_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = sql_placeholder_list(source_ids.len());
    let sql = format!(
        "SELECT id, active_parse_id FROM source_objects \
         WHERE active_parse_id IS NOT NULL AND deactivated_at IS NULL \
         AND id IN ({placeholders})"
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare source-set active capture: {source}"),
        })?;
    let rows = statement
        .query_map(params_from_iter(source_ids.iter()), |row| {
            Ok(ActiveParse {
                source_id: row.get(0)?,
                parse_id: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query source-set active sources: {source}"),
        })?;
    collect_active_parses(rows)
}

/// Capture active parses for a `governance_domains` set (`DomainSet` scope), by
/// joining `source_locations`. `DISTINCT` because a source may have several
/// locations in the same domain. Scope is enforced in SQL by admitting a source
/// only via a `current` location in a requested domain (§10 rule 4): both the
/// domain filter AND `locations.status = 'current'` are required — a source is
/// visible in a governance domain only through a `current` location in it.
fn capture_active_by_domains(
    conn: &Connection,
    domains: &[String],
) -> Result<Vec<ActiveParse>, ApiError> {
    if domains.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = sql_placeholder_list(domains.len());
    let sql = format!(
        "SELECT DISTINCT objects.id, objects.active_parse_id \
         FROM source_objects AS objects \
         JOIN source_locations AS locations ON locations.source_id = objects.id \
         WHERE objects.active_parse_id IS NOT NULL AND objects.deactivated_at IS NULL \
         AND locations.status = 'current' \
         AND locations.governance_domain IN ({placeholders})"
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare domain-set active capture: {source}"),
        })?;
    let rows = statement
        .query_map(params_from_iter(domains.iter()), |row| {
            Ok(ActiveParse {
                source_id: row.get(0)?,
                parse_id: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query domain-set active sources: {source}"),
        })?;
    collect_active_parses(rows)
}

/// Capture active parses for the R3 CONJUNCTION scope: sources whose id is in
/// `source_ids` AND that have a `current` location in one of `governance_domains`
/// (§24.3 constraints conjoin; §6 "reduce to source-set scoping"). Both
/// predicates are enforced in SQL, so scope is applied at capture and never as a
/// post-filter. `DISTINCT` because a source may have several current locations in
/// the same domain. An empty intersection returns an empty vec — a legitimate
/// empty result (the query runs and finds nothing), never an error.
///
/// Both id lists are non-empty at the sole call site (`capture_active_set`
/// guards both), so both placeholder lists are non-empty; the domain predicate
/// mirrors `capture_active_by_domains` (the `current`-location join per the
/// SPEC-1 ruling: a source is visible in a domain only through a `current`
/// location in it).
fn capture_active_by_source_ids_and_domains(
    conn: &Connection,
    source_ids: &[String],
    domains: &[String],
) -> Result<Vec<ActiveParse>, ApiError> {
    if source_ids.is_empty() || domains.is_empty() {
        return Ok(Vec::new());
    }
    let source_placeholders = sql_placeholder_list(source_ids.len());
    let domain_placeholders = sql_placeholder_list(domains.len());
    let sql = format!(
        "SELECT DISTINCT objects.id, objects.active_parse_id \
         FROM source_objects AS objects \
         JOIN source_locations AS locations ON locations.source_id = objects.id \
         WHERE objects.active_parse_id IS NOT NULL AND objects.deactivated_at IS NULL \
         AND objects.id IN ({source_placeholders}) \
         AND locations.status = 'current' \
         AND locations.governance_domain IN ({domain_placeholders})"
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare source-and-domain active capture: {source}"),
        })?;
    // Bind the source-id placeholders first (they appear first in the SQL), then
    // the domain placeholders, matching positional order.
    let bindings = source_ids.iter().chain(domains.iter());
    let rows = statement
        .query_map(params_from_iter(bindings), |row| {
            Ok(ActiveParse {
                source_id: row.get(0)?,
                parse_id: row.get(1)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query source-and-domain active sources: {source}"),
        })?;
    collect_active_parses(rows)
}

/// Build a `?,?,…` placeholder list of `count` positional parameters for an
/// `IN (…)` clause. `count` is always > 0 at every call site (the callers return
/// early on an empty set), so the list is never empty.
fn sql_placeholder_list(count: usize) -> String {
    let mut list = String::with_capacity(count * 2);
    for index in 0..count {
        if index > 0 {
            list.push(',');
        }
        list.push('?');
    }
    list
}

/// Drain a mapped active-parse row iterator into a vector, surfacing a row read
/// error with local context (never a generic message; PRINCIPLES.md).
fn collect_active_parses(
    rows: impl Iterator<Item = rusqlite::Result<ActiveParse>>,
) -> Result<Vec<ActiveParse>, ApiError> {
    let mut parses = Vec::new();
    for row in rows {
        let parse = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read active-source row: {source}"),
        })?;
        parses.push(parse);
    }
    Ok(parses)
}

/// Clone the `Arc<DensePlane>` snapshot for each captured active parse. The clone
/// is the in-memory isolation boundary (DenseCache Arc-clone immutability
/// discipline, `dense_cache.rs::snapshot_for_parse`); once captured, a concurrent
/// cutover that evicts the plane does not affect this query.
///
/// A captured active parse whose dense plane is ABSENT is possible only in the
/// tiny window where a concurrent cutover evicted a predecessor plane after the
/// active-set read; it is a VISIBLE degradation, never silent (§3: silent
/// degradation is prohibited): logged as an explicit `warn` boundary event, and
/// the parse proceeds with its remaining channels (lexical + graph) via a `None`
/// `dense_plane`.
fn clone_dense_planes(
    dense_cache: &DenseCache,
    active: Vec<ActiveParse>,
    query_id: &str,
) -> Vec<CapturedParse> {
    active
        .into_iter()
        .map(|parse| {
            let dense_plane = dense_cache.snapshot_for_parse(&parse.parse_id);
            if dense_plane.is_none() {
                warn!(
                    event = "query.execute.dense_plane_missing",
                    query_id,
                    source_id = %parse.source_id,
                    parse_id = %parse.parse_id,
                    "captured active parse has no dense plane snapshot; proceeding \
                     with its lexical and graph channels only (degraded, not silent)"
                );
            }
            CapturedParse {
                source_id: parse.source_id,
                parse_id: parse.parse_id,
                dense_plane,
            }
        })
        .collect()
}

/// Map each candidate unit id in the pool to its originating `parse_id`, so the
/// passage builder can resolve unit content parse-scoped even when the pool spans
/// several active parses. First writer wins per unit: a unit belongs to exactly
/// one active parse (the captured active set has one active parse per source and
/// unit ids are parse-unique), so contention is not expected.
fn build_parse_of_unit(pool: &[RetrievalHit]) -> std::collections::BTreeMap<String, String> {
    let mut parse_of_unit: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for hit in pool {
        for unit_id in &hit.unit_ids {
            parse_of_unit
                .entry(unit_id.clone())
                .or_insert_with(|| hit.parse_id.clone());
        }
    }
    parse_of_unit
}
