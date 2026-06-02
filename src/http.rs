use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use tracing::info;

use crate::{
    docling::convert_source_to_markdown,
    error::ApiError,
    inference::{ColbertCandidateScore, ColbertDocumentCandidate},
    source::resolve_source_reference,
    state::AppState,
    storage::{SearchCandidate, UnitDenseVector},
    types::{
        HealthResponse, IngestRequest, IngestResponse, SearchRequest, SearchResponse, SearchResult,
        ShutdownResponse,
    },
    units::{build_document_id, split_conversion_into_units},
};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

/// Build the Axum router for the versioned HTTP API and protected admin controls.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/ingest", post(post_ingest))
        .route("/v1/search", post(post_search))
        .route("/admin/shutdown", post(post_admin_shutdown))
        .with_state(state)
}

/// Return service readiness and startup diagnostics.
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(state.health())
}

/// Run one synchronous ingest request while keeping file bytes inside the service-owned corpus.
async fn post_ingest(
    State(state): State<Arc<AppState>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    request.validate()?;
    state.inference()?;
    state.storage()?;

    let source = resolve_source_reference(&state.config.storage, &request.source)?;
    let conversion = convert_source_to_markdown(
        &state.config.docling,
        &state.config.storage.index_root,
        &request,
        source,
    )
    .await?;
    let units = split_conversion_into_units(
        &conversion,
        &state.config.retrieval,
        &state.config.models.colbert.path.join("tokenizer.json"),
    )?;
    let inference = state.inference()?;
    let vectors = units
        .iter()
        .map(|unit| {
            inference
                .dense
                .embed_passage_vector(&unit.content)
                .map(|vector| UnitDenseVector {
                    unit_id: unit.unit_id.clone(),
                    vector,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let storage = state.storage()?;
    storage.ingest_document(&conversion, &units, vectors, &state.config.models.dense)?;
    let first_unit = units.first();
    let last_unit = units.last();

    info!(
        requested_source = %conversion.source.requested,
        relative_source = %conversion.source.relative_path.display(),
        absolute_source = %conversion.source.absolute_path.display(),
        output_dir = %conversion.output_dir.display(),
        markdown_path = %conversion.markdown_path.display(),
        markdown_chars = conversion.markdown.chars().count(),
        pdf_backend = %conversion.options.pdf_backend,
        ocr_mode = %conversion.options.ocr_mode,
        page_batch_size = ?conversion.options.page_batch_size,
        docling_args = ?conversion.args,
        stdout = %conversion.stdout,
        stderr = %conversion.stderr,
        units = units.len(),
        first_unit_id = first_unit.map(|unit| unit.unit_id.as_str()).unwrap_or("none"),
        first_unit_sequence = first_unit.map(|unit| unit.sequence),
        first_unit_document_id = first_unit.map(|unit| unit.document_id.as_str()).unwrap_or("none"),
        first_unit_source_path = first_unit.map(|unit| unit.source_path.as_str()).unwrap_or("none"),
        first_unit_chars = first_unit.map(|unit| unit.content.chars().count()),
        first_unit_tokens = first_unit.map(|unit| unit.token_count),
        first_unit_heading_path = ?first_unit.map(|unit| &unit.heading_path),
        first_unit_page_numbers = ?first_unit.map(|unit| &unit.page_numbers),
        last_unit_id = last_unit.map(|unit| unit.unit_id.as_str()).unwrap_or("none"),
        last_unit_sequence = last_unit.map(|unit| unit.sequence),
        last_unit_chars = last_unit.map(|unit| unit.content.chars().count()),
        last_unit_tokens = last_unit.map(|unit| unit.token_count),
        last_unit_heading_path = ?last_unit.map(|unit| &unit.heading_path),
        last_unit_page_numbers = ?last_unit.map(|unit| &unit.page_numbers),
        "Docling conversion, unit splitting, dense embedding, and SQLite ingest completed"
    );

    Ok(Json(IngestResponse {
        document_id: first_unit
            .map(|unit| unit.document_id.clone())
            .unwrap_or_else(|| build_document_id(&conversion.source.relative_path)),
        units_ingested: units.len() as u32,
        status: "ingested".to_string(),
    }))
}

/// Run dense, BM25, RRF, and bounded ColBERT reranking for one search request.
async fn post_search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let started = Instant::now();
    request.validate(state.config.retrieval.max_top_k)?;
    let top_k = request
        .top_k
        .unwrap_or(state.config.retrieval.default_top_k);
    let inference = state.inference()?;
    let storage = state.storage()?;
    let embedding_started = Instant::now();
    let query_vector = inference.dense.embed_query_vector(&request.query)?;
    let embedding_latency_ms = embedding_started.elapsed().as_millis() as u64;
    let storage_output = storage.build_search_candidate_pool(
        &request.query,
        query_vector,
        top_k,
        &state.config.retrieval,
    )?;
    let colbert_candidates = storage_output
        .candidates
        .iter()
        .map(|candidate| ColbertDocumentCandidate {
            unit_id: candidate.unit_id.clone(),
            content: candidate.content.clone(),
        })
        .collect::<Vec<_>>();
    let colbert_started = Instant::now();
    let colbert_scores = inference
        .colbert
        .score_candidates(&request.query, &colbert_candidates)?;
    let colbert_latency_ms = colbert_started.elapsed().as_millis() as u64;
    let (results, final_result_raw) =
        build_colbert_results(&storage_output.candidates, &colbert_scores, top_k)?;
    let latency_ms = started.elapsed().as_millis() as u64;
    let raw = serde_json::json!({
        "search": {
            "mode": "dense_bm25_rrf_colbert",
            "topK": top_k,
            "embeddingLatencyMs": embedding_latency_ms,
            "colbertLatencyMs": colbert_latency_ms,
            "latencyMs": latency_ms
        },
        "storage": storage_output.raw,
        "colbert": {
            "mode": "on_demand_candidate_pool_maxsim",
            "candidateCount": storage_output.candidates.len(),
            "scores": colbert_scores.iter().map(|score| {
                serde_json::json!({
                    "unitId": score.unit_id,
                    "score": score.score,
                    "rank": score.rank,
                    "queryTokens": score.query_tokens,
                    "documentTokens": score.document_tokens
                })
            }).collect::<Vec<_>>(),
            "finalResults": final_result_raw
        }
    });

    Ok(Json(SearchResponse {
        results,
        latency_ms,
        raw,
    }))
}

/// Materialize public results from ColBERT-ranked candidates while preserving first-stage score context.
fn build_colbert_results(
    candidates: &[SearchCandidate],
    colbert_scores: &[ColbertCandidateScore],
    top_k: u32,
) -> Result<(Vec<SearchResult>, Vec<serde_json::Value>), ApiError> {
    let candidate_by_id = candidates
        .iter()
        .map(|candidate| (candidate.unit_id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let mut results = Vec::new();
    let mut raw = Vec::new();
    for score in colbert_scores.iter().take(top_k as usize) {
        let Some(candidate) = candidate_by_id.get(score.unit_id.as_str()) else {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "ColBERT score referenced missing candidate {}",
                    score.unit_id
                ),
            });
        };
        results.push(SearchResult {
            unit_id: candidate.unit_id.clone(),
            // Phase 11F makes ColBERT MaxSim the public score after first-stage RRF candidate generation.
            score: score.score,
            content: candidate.content.clone(),
            heading_path: candidate.heading_path.clone(),
            source_path: candidate.source_path.clone(),
            page_numbers: candidate.page_numbers.clone(),
        });
        raw.push(serde_json::json!({
            "unitId": candidate.unit_id,
            "colbertScore": score.score,
            "colbertRank": score.rank,
            "rrfScore": candidate.rrf_score,
            "rrfRank": candidate.rrf_rank,
            "denseRank": candidate.dense_rank,
            "denseSimilarity": candidate.dense_similarity,
            "bm25Rank": candidate.bm25_rank,
            "bm25Score": candidate.bm25_score
        }));
    }

    Ok((results, raw))
}

/// Authorize and request graceful service shutdown.
async fn post_admin_shutdown(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ShutdownResponse>, ApiError> {
    let token = bearer_token_from_headers(&headers)?;
    state.authorize_admin_token(token)?;
    state.request_shutdown()?;

    Ok(Json(ShutdownResponse {
        status: "shutting_down".to_string(),
    }))
}

/// Extract the bearer token from the Authorization header.
fn bearer_token_from_headers(headers: &HeaderMap) -> Result<&str, ApiError> {
    let Some(value) = headers.get(AUTHORIZATION_HEADER) else {
        return Err(ApiError::Unauthorized {
            message: "missing Authorization bearer token".to_string(),
        });
    };
    let value = value.to_str().map_err(|_| ApiError::Unauthorized {
        message: "Authorization header must be valid UTF-8".to_string(),
    })?;
    let Some(token) = value.strip_prefix(BEARER_PREFIX) else {
        return Err(ApiError::Unauthorized {
            message: "Authorization header must use Bearer token".to_string(),
        });
    };
    if token.trim().is_empty() || token.trim() != token {
        return Err(ApiError::Unauthorized {
            message: "Authorization bearer token is malformed".to_string(),
        });
    }

    Ok(token)
}
