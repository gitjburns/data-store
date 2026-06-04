use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::{
    docling::convert_source_to_markdown,
    error::ApiError,
    inference::{
        ColbertCandidateScore, ColbertDocumentEmbedding, RerankerCandidateInput,
        RerankerCandidateScore,
    },
    source::resolve_source_reference,
    state::AppState,
    storage::{
        SearchCandidate, UnitColbertDocumentVector, UnitDenseVector, allocate_version_label,
        build_versioned_document_id,
    },
    types::{
        DocumentVersionRollbackRequest, DocumentVersionRollbackResponse, HealthResponse,
        IngestRequest, IngestResponse, LimitsResponse, OperationControlRequest, OperationEvent,
        OperationRequest, RequestLimitsResponse, RetrievalLimitsResponse, SearchRequest,
        SearchResponse, SearchResult, ShutdownResponse,
    },
    units::{assign_units_to_document_version, split_conversion_into_units},
};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";
const INGEST_STATUS_INGESTED: &str = "ingested";
const SHUTDOWN_STATUS_SHUTTING_DOWN: &str = "shutting_down";
const ROLLBACK_STATUS_ROLLED_BACK: &str = "rolled_back";
const SEARCH_MODE_FULL_RETRIEVAL: &str = "dense_bm25_rrf_colbert_reranker";
const COLBERT_MODE_PERSISTED_MAXSIM: &str = "persisted_candidate_pool_maxsim";
const COLBERT_DOCUMENT_VECTOR_SOURCE_SQLITE: &str = "sqlite";
const RERANKER_MODE_QWEN3_YES_NO: &str = "qwen3_yes_no_candidate_rerank";
const RERANKER_CANDIDATE_SOURCE_COLBERT_POOL: &str = "colbert_ranked_candidate_pool";
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const OPERATION_STREAM_CHANNEL_CAPACITY: usize = 16;
static NEXT_SERVER_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// Build the Axum router for the versioned HTTP API and protected admin controls.
pub fn build_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.server.max_request_body_bytes;

    Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/limits", get(get_limits))
        .route("/v1/ingest", post(post_ingest))
        .route("/v1/search", post(post_search))
        .route("/v1/operations", post(post_operation))
        .route(
            "/v1/operations/{operation_id}/control",
            post(post_operation_control),
        )
        .route("/admin/shutdown", post(post_admin_shutdown))
        .route("/admin/document-versions", get(get_admin_document_versions))
        .route(
            "/admin/document-versions/rollback",
            post(post_admin_document_version_rollback),
        )
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

/// Return service readiness and startup diagnostics.
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(state.health())
}

/// Return public request and retrieval limits for caller-side request construction.
async fn get_limits(State(state): State<Arc<AppState>>) -> Json<LimitsResponse> {
    Json(build_limits_response(&state))
}

/// Build the public limits response from validated runtime config.
fn build_limits_response(state: &AppState) -> LimitsResponse {
    LimitsResponse {
        request: RequestLimitsResponse {
            max_request_body_bytes: state.config.server.max_request_body_bytes,
            max_ingest_source_chars: state.config.server.max_ingest_source_chars,
            max_search_query_chars: state.config.server.max_search_query_chars,
        },
        retrieval: RetrievalLimitsResponse {
            default_top_k: state.config.retrieval.default_top_k,
            max_top_k: state.config.retrieval.max_top_k,
        },
    }
}

/// Run one synchronous ingest request while keeping file bytes inside the service-owned corpus.
async fn post_ingest(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<IngestRequest>, JsonRejection>,
) -> Result<Json<IngestResponse>, ApiError> {
    let Json(request) = payload.map_err(json_rejection_to_api_error)?;

    Ok(Json(execute_ingest(&state, request, None).await?))
}

/// Execute the ingest pipeline shared by the route-specific and operation-stream APIs.
async fn execute_ingest(
    state: &AppState,
    request: IngestRequest,
    mut emitter: Option<&mut OperationEmitter>,
) -> Result<IngestResponse, ApiError> {
    let started = Instant::now();
    request.validate(state.config.server.max_ingest_source_chars)?;
    let _admission_permit = match state.try_acquire_ingest_admission() {
        Ok(permit) => permit,
        Err(error) => {
            let admission = state.ingest_admission_snapshot();
            info!(
                event = "ingest.admission_rejected",
                in_flight = admission.in_flight,
                max_in_flight = admission.max_in_flight,
                error = %error,
                "ingest request rejected by admission gate"
            );
            return Err(error);
        }
    };
    let admission = state.ingest_admission_snapshot();
    info!(
        event = "ingest.admitted",
        in_flight = admission.in_flight,
        max_in_flight = admission.max_in_flight,
        "ingest request admitted"
    );
    state.inference()?;
    state.storage()?;

    // Ingest stage timings isolate the slow external and model-backed work so a
    // background service log can explain long synchronous requests.
    let source_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "source_resolving",
        "resolving source reference",
    )
    .await?;
    let source = resolve_source_reference(&state.config.storage, &request.source)?;
    let source_resolution_latency_ms = source_started.elapsed().as_millis() as u64;
    let conversion_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "docling_converting",
        "converting source document",
    )
    .await?;
    let conversion = convert_source_to_markdown(
        &state.config.docling,
        &state.config.storage.index_root,
        source,
    )
    .await?;
    let conversion_latency_ms = conversion_started.elapsed().as_millis() as u64;
    let splitting_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "unit_splitting",
        "splitting document into retrieval units",
    )
    .await?;
    let base_units = split_conversion_into_units(
        &conversion,
        &state.config.retrieval,
        &state.config.models.colbert.path.join("tokenizer.json"),
    )?;
    let splitting_latency_ms = splitting_started.elapsed().as_millis() as u64;
    let version_label = allocate_version_label()?;
    let versioned_document_id =
        build_versioned_document_id(&conversion.source.relative_path, &version_label);
    let units = assign_units_to_document_version(&base_units, &versioned_document_id);
    let inference = state.inference()?;
    let dense_embedding_started = Instant::now();
    emit_operation_status(&mut emitter, "dense_embedding", "embedding document units").await?;
    let total_units = units.len() as u64;
    let mut vectors = Vec::with_capacity(units.len());
    for (index, unit) in units.iter().enumerate() {
        let vector = inference.dense.embed_passage_vector(&unit.content)?;
        vectors.push(UnitDenseVector {
            unit_id: unit.unit_id.clone(),
            vector,
        });
        emit_operation_progress(
            &mut emitter,
            "dense_embedding",
            "embedding document units",
            (index + 1) as u64,
            total_units,
        )
        .await?;
    }
    let dense_embedding_latency_ms = dense_embedding_started.elapsed().as_millis() as u64;
    let colbert_embedding_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "colbert_embedding",
        "embedding ColBERT document vectors",
    )
    .await?;
    let mut colbert_vectors = Vec::with_capacity(units.len());
    for (index, unit) in units.iter().enumerate() {
        let embedding = inference
            .colbert
            .embed_document(&unit.unit_id, &unit.content)?;
        colbert_vectors.push(UnitColbertDocumentVector {
            unit_id: embedding.unit_id,
            token_count: embedding.token_count,
            dimension: embedding.dimension,
            vector: embedding.vector,
        });
        emit_operation_progress(
            &mut emitter,
            "colbert_embedding",
            "embedding ColBERT document vectors",
            (index + 1) as u64,
            total_units,
        )
        .await?;
    }
    let colbert_vector_count = colbert_vectors.len();
    let colbert_vector_values = colbert_vectors
        .iter()
        .map(|value| value.vector.len())
        .sum::<usize>();
    let storage = state.storage()?;
    let colbert_embedding_latency_ms = colbert_embedding_started.elapsed().as_millis() as u64;
    let storage_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "storage_publishing",
        "publishing document version",
    )
    .await?;
    storage.ingest_document(
        &conversion,
        &version_label,
        &units,
        vectors,
        colbert_vectors,
        &state.config.models.dense,
        &state.config.models.colbert,
    )?;
    let storage_latency_ms = storage_started.elapsed().as_millis() as u64;
    let latency_ms = started.elapsed().as_millis() as u64;
    let first_unit = units.first();

    // The log records an operational summary only. Full conversion diagnostics
    // stay in durable ingest metadata rather than expanding the service log.
    info!(
        event = "ingest.completed",
        status = 200,
        requested_source = %conversion.source.requested,
        version_label = %version_label,
        relative_source = %conversion.source.relative_path.display(),
        markdown_chars = conversion.markdown.chars().count(),
        pdf_backend = %conversion.options.pdf_backend,
        ocr_mode = %conversion.options.ocr_mode,
        page_batch_size = ?conversion.options.page_batch_size,
        units = units.len(),
        document_id = first_unit.map(|unit| unit.document_id.as_str()).unwrap_or("none"),
        colbert_document_vectors = colbert_vector_count,
        colbert_document_vector_values = colbert_vector_values,
        source_resolution_latency_ms,
        conversion_latency_ms,
        splitting_latency_ms,
        dense_embedding_latency_ms,
        colbert_embedding_latency_ms,
        storage_latency_ms,
        latency_ms,
        "ingest completed"
    );

    Ok(IngestResponse {
        document_id: first_unit
            .map(|unit| unit.document_id.clone())
            .unwrap_or(versioned_document_id),
        version_label,
        units_ingested: units.len() as u32,
        status: INGEST_STATUS_INGESTED.to_string(),
    })
}

/// Run dense, BM25, RRF, bounded ColBERT reranking, and final Qwen3 reranking for one search request.
async fn post_search(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<SearchRequest>, JsonRejection>,
) -> Result<Json<SearchResponse>, ApiError> {
    let Json(request) = payload.map_err(json_rejection_to_api_error)?;

    Ok(Json(execute_search(&state, request, None).await?))
}

/// Execute the retrieval pipeline shared by the route-specific and operation-stream APIs.
async fn execute_search(
    state: &AppState,
    request: SearchRequest,
    mut emitter: Option<&mut OperationEmitter>,
) -> Result<SearchResponse, ApiError> {
    let started = Instant::now();
    request.validate(
        state.config.server.max_search_query_chars,
        state.config.retrieval.max_top_k,
    )?;
    let _admission_permit = match state.try_acquire_search_admission() {
        Ok(permit) => permit,
        Err(error) => {
            let admission = state.search_admission_snapshot();
            info!(
                event = "search.admission_rejected",
                in_flight = admission.in_flight,
                max_in_flight = admission.max_in_flight,
                error = %error,
                "search request rejected by admission gate"
            );
            return Err(error);
        }
    };
    let admission = state.search_admission_snapshot();
    info!(
        event = "search.admitted",
        in_flight = admission.in_flight,
        max_in_flight = admission.max_in_flight,
        "search request admitted"
    );
    let top_k = request
        .top_k
        .unwrap_or(state.config.retrieval.default_top_k);
    let inference = state.inference()?;
    let storage = state.storage()?;
    let embedding_started = Instant::now();
    emit_operation_status(&mut emitter, "embedding_query", "embedding search query").await?;
    let query_vector = inference.dense.embed_query_vector(&request.query)?;
    let embedding_latency_ms = embedding_started.elapsed().as_millis() as u64;
    // Storage builds the dense/BM25/RRF pool and returns raw retrieval
    // diagnostics for the API response; the log keeps only summary counts.
    emit_operation_status(
        &mut emitter,
        "retrieving_candidates",
        "retrieving candidate units",
    )
    .await?;
    let storage_output = storage.build_search_candidate_pool(
        &request.query,
        query_vector,
        top_k,
        &state.config.retrieval,
    )?;
    let colbert_candidates = storage_output
        .candidates
        .iter()
        .map(|candidate| ColbertDocumentEmbedding {
            unit_id: candidate.unit_id.clone(),
            token_count: candidate.colbert_token_count,
            dimension: candidate.colbert_dimension,
            vector: candidate.colbert_vector.clone(),
        })
        .collect::<Vec<_>>();
    let colbert_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "colbert_scoring",
        "scoring ColBERT candidates",
    )
    .await?;
    let colbert_scores = if let Some(operation_emitter) = emitter.as_deref_mut() {
        tokio::task::block_in_place(|| {
            inference.colbert.score_persisted_candidates_with_progress(
                &request.query,
                &colbert_candidates,
                |current, total| {
                    operation_emitter.progress_blocking(
                        "colbert_scoring",
                        "scoring ColBERT candidates",
                        current,
                        total,
                    )
                },
            )
        })?
    } else {
        inference
            .colbert
            .score_persisted_candidates(&request.query, &colbert_candidates)?
    };
    let colbert_latency_ms = colbert_started.elapsed().as_millis() as u64;
    let reranker_candidates =
        build_reranker_candidates(&storage_output.candidates, &colbert_scores)?;
    let reranker_started = Instant::now();
    emit_operation_status(&mut emitter, "reranking", "reranking candidates").await?;
    let reranker_scores = if let Some(operation_emitter) = emitter.as_deref_mut() {
        tokio::task::block_in_place(|| {
            inference.reranker.score_candidates_with_progress(
                &request.query,
                &reranker_candidates,
                |current, total| {
                    operation_emitter.progress_blocking(
                        "reranking",
                        "reranking candidates",
                        current,
                        total,
                    )
                },
            )
        })?
    } else {
        inference
            .reranker
            .score_candidates(&request.query, &reranker_candidates)?
    };
    let reranker_latency_ms = reranker_started.elapsed().as_millis() as u64;
    emit_operation_status(
        &mut emitter,
        "result_assembling",
        "assembling search results",
    )
    .await?;
    let (results, final_result_raw) = build_reranker_results(
        &storage_output.candidates,
        &colbert_scores,
        &reranker_scores,
        top_k,
    )?;
    let latency_ms = started.elapsed().as_millis() as u64;
    let raw = serde_json::json!({
        "search": {
            "mode": SEARCH_MODE_FULL_RETRIEVAL,
            "topK": top_k,
            "admission": {
                "inFlight": admission.in_flight,
                "maxInFlight": admission.max_in_flight
            },
            "embeddingLatencyMs": embedding_latency_ms,
            "colbertLatencyMs": colbert_latency_ms,
            "rerankerLatencyMs": reranker_latency_ms,
            "latencyMs": latency_ms
        },
        "storage": storage_output.raw,
        "colbert": {
            "mode": COLBERT_MODE_PERSISTED_MAXSIM,
            "documentVectorSource": COLBERT_DOCUMENT_VECTOR_SOURCE_SQLITE,
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
            "rankedCandidateCount": colbert_scores.len()
        },
        "reranker": {
            "mode": RERANKER_MODE_QWEN3_YES_NO,
            "candidateSource": RERANKER_CANDIDATE_SOURCE_COLBERT_POOL,
            "candidateCount": reranker_scores.len(),
            "scores": reranker_scores.iter().map(|score| {
                serde_json::json!({
                    "unitId": score.unit_id,
                    "score": score.score,
                    "rank": score.rank,
                    "trueLogit": score.true_logit,
                    "falseLogit": score.false_logit,
                    "tokenCount": score.token_count
                })
            }).collect::<Vec<_>>(),
            "finalResults": final_result_raw
        }
    });
    // Search logs avoid content and full candidate lists; the authoritative
    // lossless retrieval diagnostics remain in SearchResponse.raw.
    info!(
        event = "search.completed",
        status = 200,
        top_k,
        query_chars = request.query.chars().count(),
        first_stage_candidates = storage_output.candidates.len(),
        colbert_candidates = colbert_scores.len(),
        reranker_candidates = reranker_scores.len(),
        results = results.len(),
        embedding_latency_ms,
        colbert_latency_ms,
        reranker_latency_ms,
        latency_ms,
        "search completed"
    );

    Ok(SearchResponse {
        results,
        latency_ms,
        raw,
    })
}

#[derive(Debug, Clone, Copy)]
enum OperationName {
    Health,
    Limits,
    Ingest,
    Search,
    Versions,
    Rollback,
    Shutdown,
}

impl OperationName {
    /// Parse the public operation contract name without accepting aliases.
    fn parse(value: &str) -> Result<Self, ApiError> {
        match value {
            "health" => Ok(Self::Health),
            "limits" => Ok(Self::Limits),
            "ingest" => Ok(Self::Ingest),
            "search" => Ok(Self::Search),
            "versions" => Ok(Self::Versions),
            "rollback" => Ok(Self::Rollback),
            "shutdown" => Ok(Self::Shutdown),
            _ => Err(ApiError::BadRequest {
                message: format!("unknown operation {value}"),
            }),
        }
    }

    /// Return the stable protocol name for logging and errors.
    fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Limits => "limits",
            Self::Ingest => "ingest",
            Self::Search => "search",
            Self::Versions => "versions",
            Self::Rollback => "rollback",
            Self::Shutdown => "shutdown",
        }
    }

    /// Return whether the operation must pass startup-token authorization before streaming.
    fn is_protected(self) -> bool {
        matches!(self, Self::Versions | Self::Rollback | Self::Shutdown)
    }
}

struct OperationFailure {
    stage: &'static str,
    error: ApiError,
}

impl OperationFailure {
    /// Attach a protocol stage to an operation failure for terminal error events.
    fn new(stage: &'static str, error: ApiError) -> Self {
        Self { stage, error }
    }
}

struct OperationEmitter {
    operation_id: String,
    sequence: u64,
    sender: mpsc::Sender<Result<Bytes, Infallible>>,
}

impl OperationEmitter {
    /// Create a sequence-owning emitter for one accepted operation stream.
    fn new(operation_id: String, sender: mpsc::Sender<Result<Bytes, Infallible>>) -> Self {
        Self {
            operation_id,
            sequence: 0,
            sender,
        }
    }

    /// Emit one newline-worthy status event for a real operation boundary.
    async fn status(&mut self, stage: &'static str, message: &'static str) -> Result<(), ApiError> {
        let event = OperationEvent::Status {
            operation_id: self.operation_id.clone(),
            sequence: self.next_sequence(),
            stage: Some(stage.to_string()),
            message: Some(message.to_string()),
        };
        self.send(event).await
    }

    /// Emit one counted progress event through the async operation stream.
    async fn progress(
        &mut self,
        stage: &'static str,
        message: &'static str,
        current: u64,
        total: u64,
    ) -> Result<(), ApiError> {
        let event = OperationEvent::Progress {
            operation_id: self.operation_id.clone(),
            sequence: self.next_sequence(),
            stage: Some(stage.to_string()),
            message: Some(message.to_string()),
            current: Some(current),
            total: Some(total),
        };
        self.send(event).await
    }

    /// Emit one counted progress event from synchronous model-scoring loops.
    fn progress_blocking(
        &mut self,
        stage: &'static str,
        message: &'static str,
        current: u64,
        total: u64,
    ) -> Result<(), ApiError> {
        let event = OperationEvent::Progress {
            operation_id: self.operation_id.clone(),
            sequence: self.next_sequence(),
            stage: Some(stage.to_string()),
            message: Some(message.to_string()),
            current: Some(current),
            total: Some(total),
        };
        let mut line = serde_json::to_vec(&event).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize operation event: {source}"),
        })?;
        line.push(b'\n');
        self.sender
            .blocking_send(Ok(Bytes::from(line)))
            .map_err(|_| ApiError::InternalIo {
                message: "operation response stream closed before event delivery".to_string(),
            })
    }

    /// Emit one terminal success event and let the response stream finish.
    async fn result<T: Serialize>(&mut self, payload: T) -> Result<(), ApiError> {
        let payload = serde_json::to_value(payload).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize operation result payload: {source}"),
        })?;
        let event = OperationEvent::Result {
            operation_id: self.operation_id.clone(),
            sequence: self.next_sequence(),
            payload,
        };
        self.send(event).await
    }

    /// Emit one terminal error event and let the response stream finish.
    async fn error(&mut self, stage: &'static str, error: ApiError) -> Result<(), ApiError> {
        let event = OperationEvent::Error {
            operation_id: self.operation_id.clone(),
            sequence: self.next_sequence(),
            stage: Some(stage.to_string()),
            error: error.operation_error_detail(),
        };
        self.send(event).await
    }

    /// Serialize and send one complete NDJSON line.
    async fn send(&mut self, event: OperationEvent) -> Result<(), ApiError> {
        let mut line = serde_json::to_vec(&event).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize operation event: {source}"),
        })?;
        line.push(b'\n');
        self.sender
            .send(Ok(Bytes::from(line)))
            .await
            .map_err(|_| ApiError::InternalIo {
                message: "operation response stream closed before event delivery".to_string(),
            })
    }

    /// Allocate the next monotonic sequence number for this operation.
    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }
}

/// Emit a status event only when the shared pipeline is serving an operation stream.
async fn emit_operation_status(
    emitter: &mut Option<&mut OperationEmitter>,
    stage: &'static str,
    message: &'static str,
) -> Result<(), ApiError> {
    if let Some(emitter) = emitter.as_deref_mut() {
        emitter.status(stage, message).await?;
    }

    Ok(())
}

/// Emit a progress event only when the shared pipeline is serving an operation stream.
async fn emit_operation_progress(
    emitter: &mut Option<&mut OperationEmitter>,
    stage: &'static str,
    message: &'static str,
    current: u64,
    total: u64,
) -> Result<(), ApiError> {
    if let Some(emitter) = emitter.as_deref_mut() {
        emitter.progress(stage, message, current, total).await?;
    }

    Ok(())
}

/// Accept one operation request and return an operation-scoped NDJSON stream.
async fn post_operation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Result<Json<OperationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = payload.map_err(json_rejection_to_api_error)?;
    let operation = OperationName::parse(&request.operation)?;
    if operation.is_protected() {
        let token = bearer_token_from_headers(&headers)?;
        state.authorize_admin_token(token)?;
    }
    let operation_id = operation_id_for_request(request.operation_id)?;
    let (sender, receiver) = mpsc::channel(OPERATION_STREAM_CHANNEL_CAPACITY);
    let stream_state = state.clone();
    tokio::spawn(async move {
        run_operation_stream(
            stream_state,
            operation,
            operation_id,
            request.payload,
            sender,
        )
        .await;
    });
    let body = Body::from_stream(ReceiverStream::new(receiver));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, NDJSON_CONTENT_TYPE)
        .body(body)
        .map(IntoResponse::into_response)
        .map_err(|source| ApiError::InternalIo {
            message: format!("failed to build operation stream response: {source}"),
        })
}

/// Reject reserved control messages explicitly until operation cancellation is implemented.
async fn post_operation_control(
    Path(operation_id): Path<String>,
    payload: Result<Json<OperationControlRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let Json(request) = payload.map_err(json_rejection_to_api_error)?;
    if request.control_type.trim().is_empty() {
        return Err(ApiError::BadRequest {
            message: "operation control type must be non-empty".to_string(),
        });
    }

    Err(ApiError::BadRequest {
        message: format!(
            "operation control '{}' is reserved but not implemented for operationId {}",
            request.control_type, operation_id
        ),
    })
}

/// Run accepted operation work and convert post-acceptance failures into terminal stream errors.
async fn run_operation_stream(
    state: Arc<AppState>,
    operation: OperationName,
    operation_id: String,
    payload: serde_json::Value,
    sender: mpsc::Sender<Result<Bytes, Infallible>>,
) {
    let mut emitter = OperationEmitter::new(operation_id, sender);
    info!(
        event = "operation.accepted",
        operation = operation.as_str(),
        "operation stream accepted"
    );
    if let Err(failure) = execute_operation(state, operation, payload, &mut emitter).await {
        let error_message = failure.error.to_string();
        let error_kind = failure.error.error_kind();
        let status = failure.error.status_code().as_u16();
        info!(
            event = "operation.failed",
            operation = operation.as_str(),
            stage = failure.stage,
            status,
            error_kind,
            error = %error_message,
            "operation stream failed"
        );
        let _ = emitter.error(failure.stage, failure.error).await;
    }
}

/// Dispatch one accepted operation to the route-compatible service implementation.
async fn execute_operation(
    state: Arc<AppState>,
    operation: OperationName,
    payload: serde_json::Value,
    emitter: &mut OperationEmitter,
) -> Result<(), OperationFailure> {
    match operation {
        OperationName::Health => {
            emitter
                .status("health_checking", "reading service health")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            emitter
                .result(state.health())
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Limits => {
            emitter
                .status("limits_reading", "reading request and retrieval limits")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            emitter
                .result(build_limits_response(&state))
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Ingest => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status("ingest_running", "running ingest pipeline")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_ingest(&state, request, Some(emitter))
                .await
                .map_err(|error| OperationFailure::new("ingest_running", error))?;
            emitter
                .result(response)
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Search => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status("search_running", "running search pipeline")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_search(&state, request, Some(emitter))
                .await
                .map_err(|error| OperationFailure::new("search_running", error))?;
            emitter
                .result(response)
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Versions => {
            emitter
                .status("versions_listing", "listing retained document versions")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_document_versions(&state)
                .map_err(|error| OperationFailure::new("versions_listing", error))?;
            emitter
                .result(response)
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Rollback => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status(
                    "rollback_publishing",
                    "publishing retained document version",
                )
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_document_version_rollback(&state, request)
                .map_err(|error| OperationFailure::new("rollback_publishing", error))?;
            emitter
                .result(response)
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
        OperationName::Shutdown => {
            emitter
                .status("shutdown_requesting", "requesting graceful shutdown")
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_shutdown(&state)
                .map_err(|error| OperationFailure::new("shutdown_requesting", error))?;
            emitter
                .result(response)
                .await
                .map_err(|error| OperationFailure::new("operation_streaming", error))
        }
    }
}

/// Decode an operation payload into the existing request DTO while preserving validation errors.
fn decode_operation_payload<T: serde::de::DeserializeOwned>(
    payload: serde_json::Value,
    operation: &str,
) -> Result<T, ApiError> {
    serde_json::from_value(payload).map_err(|source| ApiError::BadRequest {
        message: format!("invalid {operation} payload: {source}"),
    })
}

/// Use a client-provided operation ID or allocate a server-local opaque correlation ID.
fn operation_id_for_request(operation_id: Option<String>) -> Result<String, ApiError> {
    match operation_id {
        Some(value) => {
            if value.trim().is_empty() {
                return Err(ApiError::BadRequest {
                    message: "operationId must be non-empty when provided".to_string(),
                });
            }
            Ok(value)
        }
        None => Ok(generate_operation_id()),
    }
}

/// Generate a local operation correlation ID without adding a persistence dependency.
fn generate_operation_id() -> String {
    let counter = NEXT_SERVER_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);

    format!("server-{millis}-{counter}")
}

/// Build final reranker inputs from the ColBERT-ranked candidate pool while preserving candidate identity.
fn build_reranker_candidates(
    candidates: &[SearchCandidate],
    colbert_scores: &[ColbertCandidateScore],
) -> Result<Vec<RerankerCandidateInput>, ApiError> {
    let candidate_by_id = candidates
        .iter()
        .map(|candidate| (candidate.unit_id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let mut reranker_candidates = Vec::with_capacity(colbert_scores.len());
    for score in colbert_scores {
        let Some(candidate) = candidate_by_id.get(score.unit_id.as_str()) else {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "ColBERT score referenced missing candidate {}",
                    score.unit_id
                ),
            });
        };
        reranker_candidates.push(RerankerCandidateInput {
            unit_id: candidate.unit_id.clone(),
            content: candidate.content.clone(),
        });
    }

    Ok(reranker_candidates)
}

/// Materialize public results from reranker-ranked candidates while preserving previous-stage score context.
fn build_reranker_results(
    candidates: &[SearchCandidate],
    colbert_scores: &[ColbertCandidateScore],
    reranker_scores: &[RerankerCandidateScore],
    top_k: u32,
) -> Result<(Vec<SearchResult>, Vec<serde_json::Value>), ApiError> {
    let candidate_by_id = candidates
        .iter()
        .map(|candidate| (candidate.unit_id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let colbert_by_id = colbert_scores
        .iter()
        .map(|score| (score.unit_id.as_str(), score))
        .collect::<HashMap<_, _>>();
    let mut results = Vec::new();
    let mut raw = Vec::new();
    for score in reranker_scores.iter().take(top_k as usize) {
        let Some(candidate) = candidate_by_id.get(score.unit_id.as_str()) else {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "reranker score referenced missing candidate {}",
                    score.unit_id
                ),
            });
        };
        let Some(colbert_score) = colbert_by_id.get(score.unit_id.as_str()) else {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "reranker score referenced candidate without ColBERT score {}",
                    score.unit_id
                ),
            });
        };
        results.push(SearchResult {
            unit_id: candidate.unit_id.clone(),
            // The public score is the final Qwen3 yes/no probability after ColBERT candidate reranking.
            score: score.score,
            content: candidate.content.clone(),
            heading_path: candidate.heading_path.clone(),
            source_path: candidate.source_path.clone(),
            page_numbers: candidate.page_numbers.clone(),
        });
        raw.push(serde_json::json!({
            "unitId": candidate.unit_id,
            "rerankerScore": score.score,
            "rerankerRank": score.rank,
            "rerankerTrueLogit": score.true_logit,
            "rerankerFalseLogit": score.false_logit,
            "rerankerTokenCount": score.token_count,
            "colbertScore": colbert_score.score,
            "colbertRank": colbert_score.rank,
            "colbertQueryTokens": colbert_score.query_tokens,
            "colbertDocumentTokens": colbert_score.document_tokens,
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

    Ok(Json(execute_shutdown(&state)?))
}

/// Request graceful shutdown after the caller has passed admin authorization.
fn execute_shutdown(state: &AppState) -> Result<ShutdownResponse, ApiError> {
    state.request_shutdown()?;
    info!(event = "admin.shutdown.accepted", "admin shutdown accepted");

    Ok(ShutdownResponse {
        status: SHUTDOWN_STATUS_SHUTTING_DOWN.to_string(),
    })
}

/// Authorize and return retained source-document version diagnostics.
async fn get_admin_document_versions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<crate::storage::DocumentVersionListing>, ApiError> {
    let token = bearer_token_from_headers(&headers)?;
    state.authorize_admin_token(token)?;

    Ok(Json(execute_document_versions(&state)?))
}

/// Return retained source-document versions after the caller has passed admin authorization.
fn execute_document_versions(
    state: &AppState,
) -> Result<crate::storage::DocumentVersionListing, ApiError> {
    let storage = state.storage()?;
    info!(
        event = "admin.document_versions.listed",
        "admin document versions listed"
    );

    storage.list_document_versions()
}

/// Authorize and repoint one source document to an already-retained version.
async fn post_admin_document_version_rollback(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Result<Json<DocumentVersionRollbackRequest>, JsonRejection>,
) -> Result<Json<DocumentVersionRollbackResponse>, ApiError> {
    let token = bearer_token_from_headers(&headers)?;
    state.authorize_admin_token(token)?;
    let Json(request) = payload.map_err(json_rejection_to_api_error)?;

    Ok(Json(execute_document_version_rollback(&state, request)?))
}

/// Publish an already-retained document version after the caller has passed admin authorization.
fn execute_document_version_rollback(
    state: &AppState,
    request: DocumentVersionRollbackRequest,
) -> Result<DocumentVersionRollbackResponse, ApiError> {
    request.validate(state.config.server.max_ingest_source_chars)?;
    let storage = state.storage()?;
    let rollback = storage.rollback_document_version(&request.source, &request.version_label)?;
    info!(
        event = "admin.document_version_rollback.completed",
        source_path = %rollback.source_path,
        active_version_label = %rollback.active_version_label,
        published_at_ms = rollback.published_at_ms,
        vector_count = rollback.vector_count,
        "admin document version rollback completed"
    );

    Ok(DocumentVersionRollbackResponse {
        source_path: rollback.source_path,
        active_version_label: rollback.active_version_label,
        published_at_ms: rollback.published_at_ms,
        vector_count: rollback.vector_count,
        status: ROLLBACK_STATUS_ROLLED_BACK.to_string(),
    })
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

/// Normalize JSON extractor failures into this service's explicit client-error response shape.
fn json_rejection_to_api_error(rejection: JsonRejection) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::PayloadTooLarge {
            message: rejection.body_text(),
        };
    }

    ApiError::BadRequest {
        message: rejection.body_text(),
    }
}
