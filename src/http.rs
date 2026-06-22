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
use tracing::{error, info, warn};

// Axum, Tokio channels, and HTTP body bytes are confined to this transport
// module. Operation pipelines run on standard threads and cross this boundary
// only through OperationStreamSender, so synchronous domain work never depends
// on async runtime types directly.
use crate::{
    docling::{DoclingProgressUpdate, convert_source_to_markdown, panic_payload_message},
    error::{ApiError, OperationErrorDetail},
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
        BenchmarkStage, DocumentVersionRollbackRequest, DocumentVersionRollbackResponse,
        HealthResponse, IngestRequest, IngestResponse, LimitsResponse, OperationBenchmarks,
        OperationControlRequest, OperationEvent, OperationRequest, RequestLimitsResponse,
        RetrievalLimitsResponse, SearchRequest, SearchResponse, SearchResult, ShutdownResponse,
    },
    units::{assign_units_to_document_version, split_conversion_into_units},
};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";
const INGEST_STATUS_INGESTED: &str = "ingested";
const SHUTDOWN_STATUS_COMPLETE: &str = "shutdown_complete";
const SHUTDOWN_COMPLETE_MESSAGE: &str = "shutdown complete; service process is terminating";
const ROLLBACK_STATUS_ROLLED_BACK: &str = "rolled_back";
const SEARCH_MODE_FULL_RETRIEVAL: &str = "dense_bm25_rrf_colbert_reranker";
const COLBERT_MODE_PERSISTED_MAXSIM: &str = "persisted_candidate_pool_maxsim";
const COLBERT_DOCUMENT_VECTOR_SOURCE_SQLITE: &str = "sqlite";
const RERANKER_CANDIDATE_SOURCE_COLBERT_POOL: &str = "colbert_ranked_candidate_pool";
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const OPERATION_STREAM_CHANNEL_CAPACITY: usize = 16;
const OPERATION_STREAM_CLOSED_MESSAGE: &str =
    "operation response stream closed before event delivery";
static NEXT_SERVER_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// Build the Axum router for supported health, operation, and operation-control routes.
pub fn build_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.server.max_request_body_bytes;

    Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/operations", post(post_operation))
        .route(
            "/v1/operations/{operation_id}/control",
            post(post_operation_control),
        )
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

/// Return service readiness and startup diagnostics.
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let started = log_route_started("/v1/health", "health_reading");
    let response = state.health();
    info!(
        event = "http.route.result_ready",
        route = "/v1/health",
        stage = "health_reading",
        status = 200_u16,
        ready = response.ready,
        components = response.components.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );

    Json(response)
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

/// Log the accepted boundary for one route-specific HTTP request.
fn log_route_started(route: &'static str, stage: &'static str) -> Instant {
    let started = Instant::now();
    info!(
        event = "http.route.started",
        route,
        stage,
        elapsed_ms = 0_u64,
        "HTTP route started"
    );

    started
}

/// JSON envelope for route-level error responses rendered by the Axum transport.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: OperationErrorDetail,
}

impl IntoResponse for ApiError {
    /// Render service errors as explicit JSON API responses.
    fn into_response(self) -> Response {
        let error_kind = self.error_kind();
        let message = self.to_string();
        let mut detail = self.operation_error_detail();
        // status_u16 only emits valid HTTP status codes; a conversion failure is
        // a programming error surfaced as a logged 500 instead of a panic.
        let status = match StatusCode::from_u16(detail.status) {
            Ok(status) => status,
            Err(source) => {
                error!(
                    event = "api.error_status_invalid",
                    status = detail.status,
                    error = %source,
                    "API error status conversion failed"
                );
                // Keep the JSON body consistent with the HTTP status actually sent.
                detail.status = 500;
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        // Central response logging guarantees every failed HTTP request is
        // visible even when the failing stage returned before its completion log.
        if status.is_server_error() {
            error!(
                event = "api.error_response",
                status = status.as_u16(),
                error_kind,
                error = %message,
                "API error response"
            );
        } else {
            warn!(
                event = "api.error_response",
                status = status.as_u16(),
                error_kind,
                error = %message,
                "API error response"
            );
        }
        let body = ErrorBody { error: detail };

        (status, Json(body)).into_response()
    }
}

/// Log a route-local failure before the shared API error renderer loses route context.
fn log_route_failed(route: &'static str, stage: &'static str, error: &ApiError, started: &Instant) {
    let status = error.status_u16();
    let error_kind = error.error_kind();
    let error_message = error.to_string();
    if status >= 500 {
        error!(
            event = "http.route.failed",
            route,
            stage,
            status,
            error_kind,
            error = %error_message,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "HTTP route failed"
        );
    } else {
        warn!(
            event = "http.route.failed",
            route,
            stage,
            status,
            error_kind,
            error = %error_message,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "HTTP route failed"
        );
    }
}

/// Execute the ingest pipeline behind the canonical operation-stream API.
fn execute_ingest(
    state: &AppState,
    request: IngestRequest,
    mut emitter: Option<&mut OperationEmitter>,
) -> Result<IngestResponse, ApiError> {
    let started = Instant::now();
    let requested_source = request.source.clone();
    let force_requested = request.force_enabled();
    let operation_id = operation_id_for_log(&emitter);
    if let Err(source) = request.validate(state.config.server.max_ingest_source_chars) {
        info!(
            event = "ingest.validation_failed",
            operation_id = %operation_id,
            requested_source = %requested_source,
            force = force_requested,
            rejected_field = "source",
            max_ingest_source_chars = state.config.server.max_ingest_source_chars,
            source_chars = requested_source.chars().count(),
            status = source.status_u16(),
            error_kind = source.error_kind(),
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "ingest request validation failed"
        );
        return Err(source);
    }
    let _admission_permit = match state.try_acquire_ingest_admission() {
        Ok(permit) => permit,
        Err(error) => {
            let admission = state.ingest_admission_snapshot();
            info!(
                event = "ingest.admission_rejected",
                operation_id = %operation_id,
                requested_source = %requested_source,
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
        operation_id = %operation_id,
        requested_source = %requested_source,
        force = force_requested,
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
    )?;
    info!(
        event = "ingest.source_resolution.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        "ingest source resolution started"
    );
    let source = match resolve_source_reference(&state.config.storage, &request.source) {
        Ok(source) => source,
        Err(source) => {
            error!(
                event = "ingest.source_resolution.failed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                error = %source,
                elapsed_ms = source_started.elapsed().as_millis() as u64,
                "ingest source resolution failed"
            );
            return Err(source);
        }
    };
    let source_resolution_latency_ms = source_started.elapsed().as_millis() as u64;
    info!(
        event = "ingest.source_resolution.completed",
        operation_id = %operation_id,
        requested_source = %requested_source,
        relative_source = %source.relative_path.display(),
        absolute_source = %source.absolute_path.display(),
        elapsed_ms = source_resolution_latency_ms,
        "ingest source resolution completed"
    );
    let source_path = source.relative_path.display().to_string();
    let duplicate_check_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "existing_source_checking",
        "checking existing source version",
    )?;
    info!(
        event = "ingest.existing_source_check.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        source_path,
        force = force_requested,
        "ingest existing-source check started"
    );
    let active_version = match state.storage()?.active_version_for_source(&source_path) {
        Ok(version) => version,
        Err(source) => {
            error!(
                event = "ingest.existing_source_check.failed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                source_path,
                force = force_requested,
                error = %source,
                elapsed_ms = duplicate_check_started.elapsed().as_millis() as u64,
                "ingest existing-source check failed"
            );
            return Err(source);
        }
    };
    match active_version {
        Some(active_version_label) if !force_requested => {
            let error = ApiError::SourceAlreadyIngested {
                message: format!(
                    "Source {source_path} is already ingested. Use --force to override."
                ),
            };
            warn!(
                event = "ingest.existing_source_check.rejected",
                operation_id = %operation_id,
                requested_source = %requested_source,
                source_path,
                active_version_label,
                force = force_requested,
                status = error.status_u16(),
                error_kind = error.error_kind(),
                error = %error,
                elapsed_ms = duplicate_check_started.elapsed().as_millis() as u64,
                "ingest existing source rejected"
            );
            return Err(error);
        }
        Some(active_version_label) => {
            info!(
                event = "ingest.existing_source_check.override_allowed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                source_path,
                active_version_label,
                force = force_requested,
                elapsed_ms = duplicate_check_started.elapsed().as_millis() as u64,
                "ingest existing source force override allowed"
            );
        }
        None => {
            info!(
                event = "ingest.existing_source_check.completed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                source_path,
                force = force_requested,
                elapsed_ms = duplicate_check_started.elapsed().as_millis() as u64,
                "ingest existing-source check completed"
            );
        }
    }
    let conversion_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "docling_converting",
        "converting source document",
    )?;
    info!(
        event = "ingest.docling_conversion.started",
        operation = "ingest",
        operation_id = %operation_id,
        requested_source = %requested_source,
        "ingest Docling conversion started"
    );
    let (docling_progress_sender, docling_progress_receiver) =
        std::sync::mpsc::sync_channel(OPERATION_STREAM_CHANNEL_CAPACITY);
    // Move the sender unconditionally so the parent never retains one; a retained
    // sender would keep the channel open and stall the forwarding loop below.
    let progress_sender = emitter.is_some().then_some(docling_progress_sender);
    // Run conversion on an independent thread so progress delivery to the client
    // never controls backend conversion; channel disconnect signals conversion end.
    let docling_config = state.config.docling.clone();
    let index_root = state.config.storage.index_root.clone();
    let conversion_thread = std::thread::spawn(move || {
        convert_source_to_markdown(&docling_config, &index_root, source, progress_sender)
    });
    let mut docling_progress_delivery_open = emitter.is_some();
    // Receiving until disconnect drains all conversion progress even after a
    // delivery failure, so bounded progress sends never block the conversion.
    while let Ok(progress) = docling_progress_receiver.recv() {
        forward_docling_progress(
            &mut emitter,
            progress,
            &mut docling_progress_delivery_open,
            &operation_id,
            &requested_source,
            &conversion_started,
        );
    }
    let conversion = match conversion_thread.join() {
        Ok(Ok(conversion)) => conversion,
        Ok(Err(source)) => {
            error!(
                event = "ingest.docling_conversion.failed",
                operation = "ingest",
                operation_id = %operation_id,
                requested_source = %requested_source,
                error = %source,
                elapsed_ms = conversion_started.elapsed().as_millis() as u64,
                "ingest Docling conversion failed"
            );
            return Err(source);
        }
        // std thread joins fail only on panic; there is no cancellation state.
        Err(panic_payload) => {
            let panic_message = panic_payload_message(panic_payload.as_ref());
            error!(
                event = "ingest.docling_conversion.task_join_failed",
                operation = "ingest",
                operation_id = %operation_id,
                requested_source = %requested_source,
                is_panic = true,
                panic_message = %panic_message,
                elapsed_ms = conversion_started.elapsed().as_millis() as u64,
                "ingest Docling conversion thread join failed"
            );
            return Err(ApiError::InternalIo {
                message: format!("Docling conversion thread panicked: {panic_message}"),
            });
        }
    };
    let conversion_latency_ms = conversion_started.elapsed().as_millis() as u64;
    info!(
        event = "ingest.docling_conversion.completed",
        operation = "ingest",
        operation_id = %operation_id,
        requested_source = %requested_source,
        markdown_path = %conversion.markdown_path.display(),
        markdown_chars = conversion.markdown.chars().count(),
        elapsed_ms = conversion_latency_ms,
        "ingest Docling conversion completed"
    );
    let splitting_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "unit_splitting",
        "splitting document into retrieval units",
    )?;
    info!(
        event = "ingest.unit_splitting.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        "ingest unit splitting started"
    );
    let base_units = match split_conversion_into_units(
        &conversion,
        &state.config.retrieval,
        &state.config.models.colbert.path.join("tokenizer.json"),
    ) {
        Ok(units) => units,
        Err(source) => {
            error!(
                event = "ingest.unit_splitting.failed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                error = %source,
                elapsed_ms = splitting_started.elapsed().as_millis() as u64,
                "ingest unit splitting failed"
            );
            return Err(source);
        }
    };
    emit_operation_progress(
        &mut emitter,
        "unit_splitting",
        "retrieval units ready",
        base_units.len() as u64,
        base_units.len() as u64,
    )?;
    let splitting_latency_ms = splitting_started.elapsed().as_millis() as u64;
    let version_label = allocate_version_label()?;
    let versioned_document_id =
        build_versioned_document_id(&conversion.source.relative_path, &version_label);
    let units = assign_units_to_document_version(&base_units, &versioned_document_id);
    info!(
        event = "ingest.unit_splitting.completed",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        document_id = %versioned_document_id,
        units = units.len(),
        elapsed_ms = splitting_latency_ms,
        "ingest unit splitting completed"
    );
    let inference = state.inference()?;
    let dense_embedding_started = Instant::now();
    emit_operation_status(&mut emitter, "dense_embedding", "embedding document units")?;
    info!(
        event = "ingest.dense_embedding.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        units = units.len(),
        "ingest dense embedding started"
    );
    let total_units = units.len() as u64;
    let mut vectors = Vec::with_capacity(units.len());
    for (index, unit) in units.iter().enumerate() {
        let vector = {
            let _model_permit =
                match state.acquire_model_call_gate(&operation_id, "dense", "passage_embedding") {
                    Ok(permit) => permit,
                    Err(source) => {
                        error!(
                            event = "ingest.dense_embedding.failed",
                            operation_id = %operation_id,
                            requested_source = %requested_source,
                            version_label = %version_label,
                            unit_id = %unit.unit_id,
                            completed_units = index,
                            total_units = units.len(),
                            phase = "model_gate_acquire",
                            error = %source,
                            elapsed_ms = dense_embedding_started.elapsed().as_millis() as u64,
                            "ingest dense embedding failed"
                        );
                        return Err(source);
                    }
                };
            match inference.dense.embed_passage_vector(&unit.content) {
                Ok(vector) => vector,
                Err(source) => {
                    error!(
                        event = "ingest.dense_embedding.failed",
                        operation_id = %operation_id,
                        requested_source = %requested_source,
                        version_label = %version_label,
                        unit_id = %unit.unit_id,
                        completed_units = index,
                        total_units = units.len(),
                        phase = "model_call",
                        error = %source,
                        elapsed_ms = dense_embedding_started.elapsed().as_millis() as u64,
                        "ingest dense embedding failed"
                    );
                    return Err(source);
                }
            }
        };
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
        )?;
    }
    let dense_embedding_latency_ms = dense_embedding_started.elapsed().as_millis() as u64;
    info!(
        event = "ingest.dense_embedding.completed",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        dense_vectors = vectors.len(),
        elapsed_ms = dense_embedding_latency_ms,
        "ingest dense embedding completed"
    );
    let colbert_embedding_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "colbert_embedding",
        "embedding ColBERT document vectors",
    )?;
    info!(
        event = "ingest.colbert_embedding.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        units = units.len(),
        "ingest ColBERT embedding started"
    );
    let mut colbert_vectors = Vec::with_capacity(units.len());
    for (index, unit) in units.iter().enumerate() {
        let embedding = {
            let _model_permit =
                match state.acquire_model_call_gate(&operation_id, "colbert", "document_embedding")
                {
                    Ok(permit) => permit,
                    Err(source) => {
                        error!(
                            event = "ingest.colbert_embedding.failed",
                            operation_id = %operation_id,
                            requested_source = %requested_source,
                            version_label = %version_label,
                            unit_id = %unit.unit_id,
                            completed_units = index,
                            total_units = units.len(),
                            phase = "model_gate_acquire",
                            error = %source,
                            elapsed_ms = colbert_embedding_started.elapsed().as_millis() as u64,
                            "ingest ColBERT embedding failed"
                        );
                        return Err(source);
                    }
                };
            match inference
                .colbert
                .embed_document(&unit.unit_id, &unit.content)
            {
                Ok(embedding) => embedding,
                Err(source) => {
                    error!(
                        event = "ingest.colbert_embedding.failed",
                        operation_id = %operation_id,
                        requested_source = %requested_source,
                        version_label = %version_label,
                        unit_id = %unit.unit_id,
                        completed_units = index,
                        total_units = units.len(),
                        phase = "model_call",
                        error = %source,
                        elapsed_ms = colbert_embedding_started.elapsed().as_millis() as u64,
                        "ingest ColBERT embedding failed"
                    );
                    return Err(source);
                }
            }
        };
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
        )?;
    }
    let colbert_vector_count = colbert_vectors.len();
    let colbert_vector_values = colbert_vectors
        .iter()
        .map(|value| value.vector.len())
        .sum::<usize>();
    let storage = state.storage()?;
    let colbert_embedding_latency_ms = colbert_embedding_started.elapsed().as_millis() as u64;
    info!(
        event = "ingest.colbert_embedding.completed",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        colbert_document_vectors = colbert_vector_count,
        colbert_document_vector_values = colbert_vector_values,
        elapsed_ms = colbert_embedding_latency_ms,
        "ingest ColBERT embedding completed"
    );
    let storage_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "storage_publishing",
        "publishing document version",
    )?;
    info!(
        event = "ingest.storage_publishing.started",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        document_id = %versioned_document_id,
        units = units.len(),
        dense_vectors = vectors.len(),
        colbert_document_vectors = colbert_vector_count,
        "ingest storage publishing started"
    );
    let storage_phase_latencies = match storage.ingest_document(
        &conversion,
        &version_label,
        &units,
        vectors,
        colbert_vectors,
        &state.config.models.dense,
        &state.config.models.colbert,
        emitter.as_deref_mut().map(|emitter| {
            move |message: &'static str, current: u64, total: u64| {
                emitter.progress("storage_publishing", message, current, total)
            }
        }),
    ) {
        Ok(latencies) => latencies,
        Err(source) => {
            error!(
                event = "ingest.storage_publishing.failed",
                operation_id = %operation_id,
                requested_source = %requested_source,
                version_label = %version_label,
                document_id = %versioned_document_id,
                error = %source,
                elapsed_ms = storage_started.elapsed().as_millis() as u64,
                "ingest storage publishing failed"
            );
            return Err(source);
        }
    };
    let storage_latency_ms = storage_started.elapsed().as_millis() as u64;
    info!(
        event = "ingest.storage_publishing.completed",
        operation_id = %operation_id,
        requested_source = %requested_source,
        version_label = %version_label,
        document_id = %versioned_document_id,
        elapsed_ms = storage_latency_ms,
        "ingest storage publishing completed"
    );
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

    let benchmarks = OperationBenchmarks {
        stages: vec![
            leaf_benchmark_stage("docling_converting", conversion_latency_ms),
            leaf_benchmark_stage("unit_splitting", splitting_latency_ms),
            leaf_benchmark_stage("dense_embedding", dense_embedding_latency_ms),
            leaf_benchmark_stage("colbert_embedding", colbert_embedding_latency_ms),
            BenchmarkStage {
                stage: "storage_publishing".to_string(),
                elapsed_ms: storage_latency_ms,
                children: vec![
                    leaf_benchmark_stage(
                        "vector_validation",
                        storage_phase_latencies.vector_validation_ms,
                    ),
                    leaf_benchmark_stage(
                        "document_persistence",
                        storage_phase_latencies.document_persistence_ms,
                    ),
                    leaf_benchmark_stage(
                        "cache_preparation",
                        storage_phase_latencies.cache_preparation_ms,
                    ),
                    leaf_benchmark_stage("commit", storage_phase_latencies.commit_ms),
                ],
            },
        ],
        total_ms: latency_ms,
    };

    Ok(IngestResponse {
        document_id: first_unit
            .map(|unit| unit.document_id.clone())
            .unwrap_or(versioned_document_id),
        version_label,
        units_ingested: units.len() as u32,
        status: INGEST_STATUS_INGESTED.to_string(),
        benchmarks,
    })
}

/// Execute the retrieval pipeline behind the canonical operation-stream API.
fn execute_search(
    state: &AppState,
    request: SearchRequest,
    mut emitter: Option<&mut OperationEmitter>,
) -> Result<SearchResponse, ApiError> {
    let started = Instant::now();
    let operation_id = operation_id_for_log(&emitter);
    let max_query_chars = state.config.server.max_search_query_chars;
    let max_top_k = state.config.retrieval.max_top_k;
    if let Err(source) = request.validate(max_query_chars, max_top_k) {
        let rejected_field = if request
            .top_k
            .is_some_and(|top_k| top_k == 0 || top_k > max_top_k)
        {
            "topK"
        } else {
            "query"
        };
        info!(
            event = "search.validation_failed",
            operation_id = %operation_id,
            rejected_field,
            max_query_chars,
            max_top_k,
            top_k = ?request.top_k,
            status = source.status_u16(),
            error_kind = source.error_kind(),
            error = %source,
            "search request validation failed"
        );
        return Err(source);
    }
    let query_chars = request.query.chars().count();
    let top_k = request
        .top_k
        .unwrap_or(state.config.retrieval.default_top_k);
    let _admission_permit = match state.try_acquire_search_admission() {
        Ok(permit) => permit,
        Err(error) => {
            let admission = state.search_admission_snapshot();
            info!(
                event = "search.admission_rejected",
                operation_id = %operation_id,
                query_chars,
                top_k,
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
        operation_id = %operation_id,
        query_chars,
        top_k,
        in_flight = admission.in_flight,
        max_in_flight = admission.max_in_flight,
        "search request admitted"
    );
    let storage = state.storage()?;
    let search_snapshot = match storage.capture_search_snapshot(&operation_id, query_chars, top_k) {
        Ok(snapshot) => snapshot,
        Err(source) => {
            error!(
                event = "search.snapshot_capture.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "search snapshot capture failed"
            );
            return Err(source);
        }
    };
    let inference = state.inference()?;
    let embedding_started = Instant::now();
    emit_operation_status(&mut emitter, "embedding_query", "embedding search query")?;
    info!(
        event = "search.query_embedding.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        max_tokens = state.config.models.dense.max_tokens,
        model_dimension = state.config.models.dense.dimension,
        "search query embedding started"
    );
    let query_vector = {
        let _model_permit =
            match state.acquire_model_call_gate(&operation_id, "dense", "query_embedding") {
                Ok(permit) => permit,
                Err(source) => {
                    error!(
                        event = "search.query_embedding.failed",
                        operation_id = %operation_id,
                        query_chars,
                        top_k,
                        max_tokens = state.config.models.dense.max_tokens,
                        model_dimension = state.config.models.dense.dimension,
                        phase = "model_gate_acquire",
                        error = %source,
                        elapsed_ms = embedding_started.elapsed().as_millis() as u64,
                        "search query embedding failed"
                    );
                    return Err(source);
                }
            };
        match inference.dense.embed_query_vector(&request.query) {
            Ok(vector) => vector,
            Err(source) => {
                error!(
                    event = "search.query_embedding.failed",
                    operation_id = %operation_id,
                    query_chars,
                    top_k,
                    max_tokens = state.config.models.dense.max_tokens,
                    model_dimension = state.config.models.dense.dimension,
                    phase = "model_call",
                    error = %source,
                    elapsed_ms = embedding_started.elapsed().as_millis() as u64,
                    "search query embedding failed"
                );
                return Err(source);
            }
        }
    };
    let embedding_latency_ms = embedding_started.elapsed().as_millis() as u64;
    let query_vector_dimension = query_vector.len();
    info!(
        event = "search.query_embedding.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        vector_dimension = query_vector_dimension,
        elapsed_ms = embedding_latency_ms,
        "search query embedding completed"
    );
    // Storage builds the dense/BM25/RRF pool and returns raw retrieval
    // diagnostics for the API response; the log keeps only summary counts.
    let retrieval_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "retrieving_candidates",
        "retrieving candidate units",
    )?;
    info!(
        event = "search.retrieval.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        query_vector_dimension,
        candidate_overfetch_multiplier = state.config.retrieval.candidate_overfetch_multiplier,
        colbert_candidate_pool_size = state.config.retrieval.colbert_candidate_pool_size,
        "search retrieval candidate pool started"
    );
    let storage_output = match storage.build_search_candidate_pool(
        &operation_id,
        &request.query,
        query_vector,
        search_snapshot,
        top_k,
        &state.config.retrieval,
    ) {
        Ok(output) => output,
        Err(source) => {
            error!(
                event = "search.retrieval.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                query_vector_dimension,
                error = %source,
                elapsed_ms = retrieval_started.elapsed().as_millis() as u64,
                "search retrieval candidate pool failed"
            );
            return Err(source);
        }
    };
    let retrieval_latency_ms = retrieval_started.elapsed().as_millis() as u64;
    info!(
        event = "search.retrieval.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = storage_output.candidates.len(),
        elapsed_ms = retrieval_latency_ms,
        "search retrieval candidate pool completed"
    );
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
    let colbert_candidate_count = colbert_candidates.len();
    let colbert_document_tokens = colbert_candidates
        .iter()
        .map(|candidate| candidate.token_count)
        .sum::<usize>();
    emit_operation_status(
        &mut emitter,
        "colbert_scoring",
        "scoring ColBERT candidates",
    )?;
    info!(
        event = "search.colbert_scoring.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = colbert_candidate_count,
        document_tokens = colbert_document_tokens,
        query_max_tokens = state.config.models.colbert.query_max_tokens,
        document_max_tokens = state.config.models.colbert.document_max_tokens,
        model_dimension = state.config.models.colbert.dimension,
        "search ColBERT scoring started"
    );
    let colbert_score_result = {
        let _model_permit = match state.acquire_model_call_gate(
            &operation_id,
            "colbert",
            "persisted_candidate_scoring",
        ) {
            Ok(permit) => permit,
            Err(source) => {
                error!(
                    event = "search.colbert_scoring.failed",
                    operation_id = %operation_id,
                    query_chars,
                    top_k,
                    candidates = colbert_candidate_count,
                    document_tokens = colbert_document_tokens,
                    phase = "model_gate_acquire",
                    error = %source,
                    elapsed_ms = colbert_started.elapsed().as_millis() as u64,
                    "search ColBERT scoring failed"
                );
                return Err(source);
            }
        };
        if let Some(operation_emitter) = emitter.as_deref_mut() {
            inference.colbert.score_persisted_candidates_with_progress(
                &request.query,
                &colbert_candidates,
                |current, total| {
                    operation_emitter.progress(
                        "colbert_scoring",
                        "scoring ColBERT candidates",
                        current,
                        total,
                    )
                },
            )
        } else {
            inference
                .colbert
                .score_persisted_candidates(&request.query, &colbert_candidates)
        }
    };
    let colbert_scores = match colbert_score_result {
        Ok(scores) => scores,
        Err(source) => {
            error!(
                event = "search.colbert_scoring.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                candidates = colbert_candidate_count,
                document_tokens = colbert_document_tokens,
                phase = "model_call",
                error = %source,
                elapsed_ms = colbert_started.elapsed().as_millis() as u64,
                "search ColBERT scoring failed"
            );
            return Err(source);
        }
    };
    let colbert_latency_ms = colbert_started.elapsed().as_millis() as u64;
    info!(
        event = "search.colbert_scoring.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = colbert_candidate_count,
        scores = colbert_scores.len(),
        elapsed_ms = colbert_latency_ms,
        "search ColBERT scoring completed"
    );
    let reranker_candidate_started = Instant::now();
    // Effective reranker pool is the configured size or the requested topK,
    // whichever is larger; assembly below clamps it to the available
    // ColBERT-ranked candidates.
    let reranker_pool_size = state
        .config
        .retrieval
        .reranker_candidate_pool_size
        .max(top_k);
    info!(
        event = "search.reranker_candidates.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        storage_candidates = storage_output.candidates.len(),
        colbert_scores = colbert_scores.len(),
        reranker_candidate_limit = reranker_pool_size,
        "search reranker candidate assembly started"
    );
    let reranker_candidates = match build_reranker_candidates(
        &storage_output.candidates,
        &colbert_scores,
        reranker_pool_size,
    ) {
        Ok(candidates) => candidates,
        Err(source) => {
            error!(
                event = "search.reranker_candidates.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                storage_candidates = storage_output.candidates.len(),
                colbert_scores = colbert_scores.len(),
                reranker_candidate_limit = reranker_pool_size,
                error = %source,
                elapsed_ms = reranker_candidate_started.elapsed().as_millis() as u64,
                "search reranker candidate assembly failed"
            );
            return Err(source);
        }
    };
    info!(
        event = "search.reranker_candidates.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = reranker_candidates.len(),
        colbert_scores = colbert_scores.len(),
        reranker_candidate_limit = top_k,
        elapsed_ms = reranker_candidate_started.elapsed().as_millis() as u64,
        "search reranker candidate assembly completed"
    );
    let reranker_started = Instant::now();
    emit_operation_status(&mut emitter, "reranking", "reranking candidates")?;
    info!(
        event = "search.reranking.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = reranker_candidates.len(),
        backend = inference.reranker.kind(),
        mode = inference.reranker.mode(),
        uses_model_gate = inference.reranker.uses_local_model_gate(),
        max_tokens = state.config.models.reranker.max_tokens,
        "search reranking started"
    );
    let reranker_score_result = if inference.reranker.uses_local_model_gate() {
        let _model_permit = match state.acquire_model_call_gate(
            &operation_id,
            "reranker",
            "candidate_batch_scoring",
        ) {
            Ok(permit) => permit,
            Err(source) => {
                error!(
                    event = "search.reranking.failed",
                    operation_id = %operation_id,
                    query_chars,
                    top_k,
                    candidates = reranker_candidates.len(),
                    phase = "model_gate_acquire",
                    error = %source,
                    elapsed_ms = reranker_started.elapsed().as_millis() as u64,
                    "search reranking failed"
                );
                return Err(source);
            }
        };
        if let Some(operation_emitter) = emitter.as_deref_mut() {
            inference.reranker.score_candidates_with_progress(
                &request.query,
                &reranker_candidates,
                |current, total| {
                    operation_emitter.progress("reranking", "reranking candidates", current, total)
                },
            )
        } else {
            inference
                .reranker
                .score_candidates(&request.query, &reranker_candidates)
        }
    } else if let Some(operation_emitter) = emitter.as_deref_mut() {
        inference.reranker.score_candidates_with_progress(
            &request.query,
            &reranker_candidates,
            |current, total| {
                operation_emitter.progress("reranking", "reranking candidates", current, total)
            },
        )
    } else {
        inference
            .reranker
            .score_candidates(&request.query, &reranker_candidates)
    };
    let reranker_scores = match reranker_score_result {
        Ok(scores) => scores,
        Err(source) => {
            error!(
                event = "search.reranking.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                candidates = reranker_candidates.len(),
                phase = "model_call",
                error = %source,
                elapsed_ms = reranker_started.elapsed().as_millis() as u64,
                "search reranking failed"
            );
            return Err(source);
        }
    };
    let reranker_latency_ms = reranker_started.elapsed().as_millis() as u64;
    info!(
        event = "search.reranking.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        candidates = reranker_candidates.len(),
        scores = reranker_scores.len(),
        elapsed_ms = reranker_latency_ms,
        "search reranking completed"
    );
    let result_assembly_started = Instant::now();
    emit_operation_status(
        &mut emitter,
        "result_assembling",
        "assembling search results",
    )?;
    info!(
        event = "search.result_assembly.started",
        operation_id = %operation_id,
        query_chars,
        top_k,
        storage_candidates = storage_output.candidates.len(),
        colbert_scores = colbert_scores.len(),
        reranker_scores = reranker_scores.len(),
        "search result assembly started"
    );
    let (results, final_result_raw) = match build_reranker_results(
        &storage_output.candidates,
        &colbert_scores,
        &reranker_scores,
        top_k,
    ) {
        Ok(results) => results,
        Err(source) => {
            error!(
                event = "search.result_assembly.failed",
                operation_id = %operation_id,
                query_chars,
                top_k,
                storage_candidates = storage_output.candidates.len(),
                colbert_scores = colbert_scores.len(),
                reranker_scores = reranker_scores.len(),
                error = %source,
                elapsed_ms = result_assembly_started.elapsed().as_millis() as u64,
                "search result assembly failed"
            );
            return Err(source);
        }
    };
    let result_assembling_latency_ms = result_assembly_started.elapsed().as_millis() as u64;
    info!(
        event = "search.result_assembly.completed",
        operation_id = %operation_id,
        query_chars,
        top_k,
        results = results.len(),
        elapsed_ms = result_assembling_latency_ms,
        "search result assembly completed"
    );
    let latency_ms = started.elapsed().as_millis() as u64;
    // Assemble server-authoritative benchmarks before `raw` consumes
    // `storage_output.raw`; retrieval substages are read from that lossless
    // payload (the only place storage emits them).
    let benchmarks = OperationBenchmarks {
        stages: vec![
            leaf_benchmark_stage(
                "search_preparation",
                embedding_started.duration_since(started).as_millis() as u64,
            ),
            leaf_benchmark_stage("embedding_query", embedding_latency_ms),
            BenchmarkStage {
                stage: "retrieving_candidates".to_string(),
                elapsed_ms: retrieval_latency_ms,
                children: vec![
                    leaf_benchmark_stage(
                        "query_vector_validation",
                        retrieval_substage_ms(
                            &storage_output.raw,
                            "queryVectorValidationLatencyMs",
                        )?,
                    ),
                    leaf_benchmark_stage(
                        "dense",
                        retrieval_substage_ms(&storage_output.raw, "denseLatencyMs")?,
                    ),
                    leaf_benchmark_stage(
                        "bm25",
                        retrieval_substage_ms(&storage_output.raw, "bm25LatencyMs")?,
                    ),
                    leaf_benchmark_stage(
                        "rrf_fusion",
                        retrieval_substage_ms(&storage_output.raw, "rrfFusionLatencyMs")?,
                    ),
                    leaf_benchmark_stage(
                        "candidate_materialization",
                        retrieval_substage_ms(
                            &storage_output.raw,
                            "candidateMaterializationLatencyMs",
                        )?,
                    ),
                    leaf_benchmark_stage(
                        "raw_diagnostics",
                        retrieval_substage_ms(&storage_output.raw, "rawDiagnosticsLatencyMs")?,
                    ),
                ],
            },
            leaf_benchmark_stage("colbert_scoring", colbert_latency_ms),
            leaf_benchmark_stage("reranking", reranker_latency_ms),
            leaf_benchmark_stage("result_assembling", result_assembling_latency_ms),
        ],
        total_ms: latency_ms,
    };
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
            "mode": inference.reranker.mode(),
            "candidateSource": RERANKER_CANDIDATE_SOURCE_COLBERT_POOL,
            "colbertCandidateCount": colbert_scores.len(),
            "candidateLimit": top_k,
            "candidateCount": reranker_scores.len(),
            "scores": reranker_scores.iter().map(|score| {
                let mut entry = serde_json::json!({
                    "unitId": score.unit_id,
                    "score": score.score,
                    "rank": score.rank
                });
                // Optional diagnostics are omitted when the reranker backend
                // cannot provide them; values are never synthesized.
                if let Some(logit) = score.logit {
                    entry["logit"] = serde_json::json!(logit);
                }
                if let Some(token_count) = score.token_count {
                    entry["tokenCount"] = serde_json::json!(token_count);
                }
                entry
            }).collect::<Vec<_>>(),
            "finalResults": final_result_raw
        }
    });
    // Search logs avoid content and full candidate lists; the authoritative
    // lossless retrieval diagnostics remain in SearchResponse.raw.
    info!(
        event = "search.completed",
        operation_id = %operation_id,
        status = 200,
        top_k,
        query_chars,
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
        benchmarks,
        raw,
    })
}

/// Build a leaf benchmark stage (no children) for the operation benchmark contract.
fn leaf_benchmark_stage(stage: &str, elapsed_ms: u64) -> BenchmarkStage {
    BenchmarkStage {
        stage: stage.to_string(),
        elapsed_ms,
        children: Vec::new(),
    }
}

/// Read a measured retrieval substage latency from the lossless search raw
/// payload. Fails explicitly when the contract field is absent or non-numeric
/// rather than silently substituting a value, since these substages are always
/// produced on the full-retrieval search path.
fn retrieval_substage_ms(raw: &serde_json::Value, key: &str) -> Result<u64, ApiError> {
    raw.get("retrieval")
        .and_then(|retrieval| retrieval.get(key))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!("search retrieval diagnostics missing substage latency '{key}'"),
        })
}

#[derive(Debug, Clone, Copy)]
enum OperationName {
    Health,
    Limits,
    Sources,
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
            "sources" => Ok(Self::Sources),
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
            Self::Sources => "sources",
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

enum OperationStreamSendStatus {
    Sent,
    Full,
    Closed,
}

struct OperationStreamSender {
    sender: mpsc::Sender<Result<Bytes, Infallible>>,
}

impl OperationStreamSender {
    /// Own the Tokio response-body channel at the transport boundary.
    fn new(sender: mpsc::Sender<Result<Bytes, Infallible>>) -> Self {
        Self { sender }
    }

    /// Try to deliver a reporting line without blocking synchronous operation work.
    fn try_send_line(&self, line: Vec<u8>) -> OperationStreamSendStatus {
        match self.sender.try_send(Ok(Bytes::from(line))) {
            Ok(()) => OperationStreamSendStatus::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => OperationStreamSendStatus::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => OperationStreamSendStatus::Closed,
        }
    }

    /// Deliver a terminal line through the response stream and report client loss explicitly.
    fn blocking_send_line(&self, line: Vec<u8>) -> Result<(), ApiError> {
        self.sender
            .blocking_send(Ok(Bytes::from(line)))
            .map_err(|_| operation_stream_closed_api_error())
    }
}

struct OperationEmitter {
    operation_id: String,
    operation: &'static str,
    sequence: u64,
    started: Instant,
    sender: OperationStreamSender,
    reporting_delivery_open: bool,
}

impl OperationEmitter {
    /// Create a sequence-owning emitter for one accepted operation stream.
    fn new(operation_id: String, operation: &'static str, sender: OperationStreamSender) -> Self {
        Self {
            operation_id,
            operation,
            sequence: 0,
            started: Instant::now(),
            sender,
            reporting_delivery_open: true,
        }
    }

    /// Emit one newline-worthy status event for a real operation boundary.
    fn status(&mut self, stage: &'static str, message: &'static str) -> Result<(), ApiError> {
        let sequence = self.next_sequence();
        let event = OperationEvent::Status {
            operation_id: self.operation_id.clone(),
            sequence,
            stage: Some(stage.to_string()),
            message: Some(message.to_string()),
        };
        info!(
            event = "operation.event_ready",
            operation = self.operation,
            operation_id = %self.operation_id,
            sequence,
            event_type = "status",
            stage,
            message,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "operation status event ready"
        );
        self.send_reporting("status", sequence, Some(stage), Some(message), event)
    }

    /// Emit one counted progress event through the operation stream.
    fn progress(
        &mut self,
        stage: &'static str,
        message: &'static str,
        current: u64,
        total: u64,
    ) -> Result<(), ApiError> {
        let sequence = self.next_sequence();
        let event = OperationEvent::Progress {
            operation_id: self.operation_id.clone(),
            sequence,
            stage: Some(stage.to_string()),
            message: Some(message.to_string()),
            current: Some(current),
            total: Some(total),
        };
        if should_log_progress_checkpoint(current, total) {
            info!(
                event = "operation.progress_checkpoint",
                operation = self.operation,
                operation_id = %self.operation_id,
                sequence,
                stage,
                message,
                current,
                total,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "operation progress checkpoint"
            );
        }
        self.send_reporting("progress", sequence, Some(stage), Some(message), event)
    }

    /// Emit one uncounted progress event through the operation stream.
    fn progress_message(&mut self, stage: &'static str, message: String) -> Result<(), ApiError> {
        let sequence = self.next_sequence();
        let event = OperationEvent::Progress {
            operation_id: self.operation_id.clone(),
            sequence,
            stage: Some(stage.to_string()),
            message: Some(message),
            current: None,
            total: None,
        };
        self.send_reporting("progress", sequence, Some(stage), None, event)
    }

    /// Emit one terminal success event and let the response stream finish.
    fn result<T: Serialize>(&mut self, payload: T) -> Result<(), ApiError> {
        let next_sequence = self.sequence + 1;
        let payload = match serde_json::to_value(payload) {
            Ok(payload) => payload,
            Err(source) => {
                error!(
                    event = "operation.result_prepare_failed",
                    operation = self.operation,
                    operation_id = %self.operation_id,
                    sequence = next_sequence,
                    stage = "result_preparing",
                    error = %source,
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "operation terminal result preparation failed"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed to serialize operation result payload: {source}"),
                });
            }
        };
        let terminal_status = payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("result")
            .to_string();
        let sequence = self.next_sequence();
        let event = OperationEvent::Result {
            operation_id: self.operation_id.clone(),
            sequence,
            payload,
        };
        info!(
            event = "operation.result_ready",
            operation = self.operation,
            operation_id = %self.operation_id,
            sequence,
            stage = "terminal_result_ready",
            terminal_status,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "operation terminal result ready"
        );
        self.send(
            "result",
            sequence,
            Some("terminal_result"),
            Some(&terminal_status),
            event,
        )
    }

    /// Emit one terminal error event and let the response stream finish.
    fn error(&mut self, stage: &'static str, error: ApiError) -> Result<(), ApiError> {
        let error_message = error.to_string();
        let detail = error.operation_error_detail();
        let error_status = detail.status;
        let error_kind = detail.kind.clone();
        let sequence = self.next_sequence();
        let event = OperationEvent::Error {
            operation_id: self.operation_id.clone(),
            sequence,
            stage: Some(stage.to_string()),
            error: detail,
        };
        info!(
            event = "operation.error_ready",
            operation = self.operation,
            operation_id = %self.operation_id,
            sequence,
            stage,
            status = error_status,
            error_kind = %error_kind,
            error = %error_message,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "operation terminal error ready"
        );
        self.send("error", sequence, Some(stage), Some(&error_message), event)
    }

    /// Send one nonterminal reporting event without letting client disconnects abort backend work.
    fn send_reporting(
        &mut self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        event: OperationEvent,
    ) -> Result<(), ApiError> {
        if !self.reporting_delivery_open {
            return Ok(());
        }

        let delivery = self.try_send(event_type, sequence, stage, message, event);
        self.finish_reporting_delivery(event_type, sequence, stage, message, delivery)
    }

    /// Serialize and try to send one reporting NDJSON line without blocking domain work.
    fn try_send(
        &mut self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        event: OperationEvent,
    ) -> Result<(), ApiError> {
        let line = self.serialize_event_line(event_type, sequence, stage, message, event)?;
        match self.sender.try_send_line(line) {
            OperationStreamSendStatus::Sent => {
                let delivery = Ok(());
                self.log_delivery_result(event_type, sequence, stage, message, &delivery);
                Ok(())
            }
            OperationStreamSendStatus::Full => {
                info!(
                    event = "operation.reporting_event_not_delivered",
                    operation = self.operation,
                    operation_id = %self.operation_id,
                    sequence,
                    event_type,
                    stage = stage.unwrap_or("none"),
                    message = message.unwrap_or("none"),
                    reason = "stream_channel_full",
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "operation reporting event not delivered"
                );
                Ok(())
            }
            OperationStreamSendStatus::Closed => {
                let delivery = Err(operation_stream_closed_api_error());
                self.log_delivery_result(event_type, sequence, stage, message, &delivery);
                delivery
            }
        }
    }

    /// Serialize and send one complete NDJSON line.
    fn send(
        &mut self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        event: OperationEvent,
    ) -> Result<(), ApiError> {
        let line = self.serialize_event_line(event_type, sequence, stage, message, event)?;
        let delivery = self.sender.blocking_send_line(line);
        self.log_delivery_result(event_type, sequence, stage, message, &delivery);
        delivery
    }

    /// Serialize one operation event into an owned NDJSON body chunk.
    fn serialize_event_line(
        &self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        event: OperationEvent,
    ) -> Result<Vec<u8>, ApiError> {
        let mut line = match serde_json::to_vec(&event) {
            Ok(line) => line,
            Err(source) => {
                error!(
                    event = "operation.event_prepare_failed",
                    operation = self.operation,
                    operation_id = %self.operation_id,
                    sequence,
                    event_type,
                    stage = stage.unwrap_or("none"),
                    message = message.unwrap_or("none"),
                    error = %source,
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "operation event preparation failed"
                );
                return Err(ApiError::InternalIo {
                    message: format!("failed to serialize operation event: {source}"),
                });
            }
        };
        line.push(b'\n');
        Ok(line)
    }

    /// Convert nonterminal stream-close failures into durable reporting-only diagnostics.
    fn finish_reporting_delivery(
        &mut self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        delivery: Result<(), ApiError>,
    ) -> Result<(), ApiError> {
        match delivery {
            Ok(()) => Ok(()),
            Err(error) if is_operation_stream_closed_error(&error) => {
                warn!(
                    event = "operation.reporting_delivery_closed",
                    operation = self.operation,
                    operation_id = %self.operation_id,
                    sequence,
                    event_type,
                    stage = stage.unwrap_or("none"),
                    message = message.unwrap_or("none"),
                    status = error.status_u16(),
                    error_kind = error.error_kind(),
                    error = %error,
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "operation reporting delivery closed; backend execution continues"
                );
                self.reporting_delivery_open = false;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Allocate the next monotonic sequence number for this operation.
    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Log whether a prepared operation event reached the response stream.
    fn log_delivery_result(
        &self,
        event_type: &'static str,
        sequence: u64,
        stage: Option<&str>,
        message: Option<&str>,
        delivery: &Result<(), ApiError>,
    ) {
        match delivery {
            Ok(()) => {
                if event_type == "result" || event_type == "error" || event_type == "status" {
                    info!(
                        event = "operation.event_delivered",
                        operation = self.operation,
                        operation_id = %self.operation_id,
                        sequence,
                        event_type,
                        stage = stage.unwrap_or("none"),
                        message = message.unwrap_or("none"),
                        elapsed_ms = self.started.elapsed().as_millis() as u64,
                        "operation event delivered"
                    );
                }
            }
            Err(source) => {
                error!(
                    event = "operation.event_delivery_failed",
                    operation = self.operation,
                    operation_id = %self.operation_id,
                    sequence,
                    event_type,
                    stage = stage.unwrap_or("none"),
                    message = message.unwrap_or("none"),
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    error = %source,
                    "operation event delivery failed"
                );
            }
        }
    }
}

/// Emit a status event only when the shared pipeline is serving an operation stream.
fn emit_operation_status(
    emitter: &mut Option<&mut OperationEmitter>,
    stage: &'static str,
    message: &'static str,
) -> Result<(), ApiError> {
    if let Some(emitter) = emitter.as_deref_mut() {
        emitter.status(stage, message)?;
    }

    Ok(())
}

/// Emit a progress event only when the shared pipeline is serving an operation stream.
fn emit_operation_progress(
    emitter: &mut Option<&mut OperationEmitter>,
    stage: &'static str,
    message: &'static str,
    current: u64,
    total: u64,
) -> Result<(), ApiError> {
    if let Some(emitter) = emitter.as_deref_mut() {
        emitter.progress(stage, message, current, total)?;
    }

    Ok(())
}

/// Return whether a counted progress update is useful enough to write to the durable service log.
fn should_log_progress_checkpoint(current: u64, total: u64) -> bool {
    current == 1 || current == total || current % 10 == 0
}

/// Return a stable log correlation value for route-specific and operation-stream calls.
fn operation_id_for_log(emitter: &Option<&mut OperationEmitter>) -> String {
    emitter
        .as_ref()
        .map(|emitter| emitter.operation_id.clone())
        .unwrap_or_else(|| "route".to_string())
}

/// Return whether operation-stream reporting events should still attempt client delivery.
fn operation_reporting_delivery_open(emitter: &Option<&mut OperationEmitter>) -> bool {
    emitter
        .as_ref()
        .map(|emitter| emitter.reporting_delivery_open)
        .unwrap_or(false)
}

/// Build the canonical internal error value for response-stream delivery loss.
fn operation_stream_closed_api_error() -> ApiError {
    ApiError::InternalIo {
        message: OPERATION_STREAM_CLOSED_MESSAGE.to_string(),
    }
}

/// Return whether an operation-stream error represents client delivery loss only.
fn is_operation_stream_closed_error(error: &ApiError) -> bool {
    matches!(
        error,
        ApiError::InternalIo { message } if message == OPERATION_STREAM_CLOSED_MESSAGE
    )
}

/// Emit one uncounted progress event only when the pipeline serves an operation stream.
fn emit_operation_progress_message(
    emitter: &mut Option<&mut OperationEmitter>,
    stage: &'static str,
    message: String,
) -> Result<(), ApiError> {
    if let Some(emitter) = emitter.as_deref_mut() {
        emitter.progress_message(stage, message)?;
    }

    Ok(())
}

/// Forward one parsed Docling progress update to the operation stream.
fn emit_docling_progress(
    emitter: &mut Option<&mut OperationEmitter>,
    progress: DoclingProgressUpdate,
) -> Result<(), ApiError> {
    match progress.percentage {
        Some(percentage) => emit_operation_progress(
            emitter,
            "docling_converting",
            "loading model weights",
            percentage,
            100,
        ),
        None => emit_operation_progress_message(emitter, "docling_converting", progress.message),
    }
}

/// Forward one Docling progress update without letting client delivery control backend conversion.
fn forward_docling_progress(
    emitter: &mut Option<&mut OperationEmitter>,
    progress: DoclingProgressUpdate,
    delivery_open: &mut bool,
    operation_id: &str,
    requested_source: &str,
    conversion_started: &Instant,
) {
    if !*delivery_open {
        return;
    }

    let was_delivery_open = operation_reporting_delivery_open(emitter);
    if let Err(error) = emit_docling_progress(emitter, progress) {
        error!(
            event = "ingest.docling_progress_delivery.failed",
            operation = "ingest",
            operation_id = %operation_id,
            requested_source = %requested_source,
            stage = "docling_converting",
            status = error.status_u16(),
            error_kind = error.error_kind(),
            error = %error,
            elapsed_ms = conversion_started.elapsed().as_millis() as u64,
            "ingest Docling progress delivery failed; conversion continues"
        );
        *delivery_open = false;
        return;
    }

    if was_delivery_open && !operation_reporting_delivery_open(emitter) {
        let error = operation_stream_closed_api_error();
        error!(
            event = "ingest.docling_progress_delivery.failed",
            operation = "ingest",
            operation_id = %operation_id,
            requested_source = %requested_source,
            stage = "docling_converting",
            status = error.status_u16(),
            error_kind = error.error_kind(),
            error = %error,
            elapsed_ms = conversion_started.elapsed().as_millis() as u64,
            "ingest Docling progress delivery failed; conversion continues"
        );
        *delivery_open = false;
    }
}

/// Accept one operation request and return an operation-scoped NDJSON stream.
async fn post_operation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Result<Json<OperationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let setup_started = Instant::now();
    info!(
        event = "operation.stream_setup_started",
        route = "/v1/operations",
        stage = "request_decoding",
        elapsed_ms = 0_u64,
        "operation stream setup started"
    );
    let Json(request) = match payload.map_err(json_rejection_to_api_error) {
        Ok(request) => request,
        Err(error) => {
            log_operation_stream_setup_failed(
                "request_decoding",
                None,
                None,
                &error,
                &setup_started,
            );
            return Err(error);
        }
    };
    let requested_operation = request.operation.clone();
    let requested_operation_id = request.operation_id.clone();
    info!(
        event = "operation.stream_setup_checkpoint",
        route = "/v1/operations",
        operation = %requested_operation,
        operation_id = requested_operation_id.as_deref().unwrap_or("none"),
        stage = "request_decoded",
        has_client_operation_id = requested_operation_id.is_some(),
        elapsed_ms = setup_started.elapsed().as_millis() as u64,
        "operation stream request decoded"
    );
    let operation = match OperationName::parse(&request.operation) {
        Ok(operation) => operation,
        Err(error) => {
            log_operation_stream_setup_failed(
                "operation_parsing",
                Some(&requested_operation),
                requested_operation_id.as_deref(),
                &error,
                &setup_started,
            );
            return Err(error);
        }
    };
    info!(
        event = "operation.stream_setup_checkpoint",
        route = "/v1/operations",
        operation = operation.as_str(),
        operation_id = requested_operation_id.as_deref().unwrap_or("none"),
        stage = "operation_parsed",
        elapsed_ms = setup_started.elapsed().as_millis() as u64,
        "operation stream operation parsed"
    );
    if operation.is_protected() {
        info!(
            event = "operation.stream_setup_checkpoint",
            route = "/v1/operations",
            operation = operation.as_str(),
            operation_id = requested_operation_id.as_deref().unwrap_or("none"),
            stage = "authorization_checking",
            elapsed_ms = setup_started.elapsed().as_millis() as u64,
            "operation stream authorization checking"
        );
        let token = match bearer_token_from_headers(&headers) {
            Ok(token) => token,
            Err(error) => {
                log_operation_stream_setup_failed(
                    "authorization_checking",
                    Some(operation.as_str()),
                    requested_operation_id.as_deref(),
                    &error,
                    &setup_started,
                );
                return Err(error);
            }
        };
        if let Err(error) = state.authorize_admin_token(token) {
            log_operation_stream_setup_failed(
                "authorization_checking",
                Some(operation.as_str()),
                requested_operation_id.as_deref(),
                &error,
                &setup_started,
            );
            return Err(error);
        }
        info!(
            event = "operation.stream_setup_checkpoint",
            route = "/v1/operations",
            operation = operation.as_str(),
            operation_id = requested_operation_id.as_deref().unwrap_or("none"),
            stage = "authorization_completed",
            elapsed_ms = setup_started.elapsed().as_millis() as u64,
            "operation stream authorization completed"
        );
    }
    let operation_id = match operation_id_for_request(request.operation_id) {
        Ok(operation_id) => operation_id,
        Err(error) => {
            log_operation_stream_setup_failed(
                "operation_id_validating",
                Some(operation.as_str()),
                requested_operation_id.as_deref(),
                &error,
                &setup_started,
            );
            return Err(error);
        }
    };
    info!(
        event = "operation.stream_setup_checkpoint",
        route = "/v1/operations",
        operation = operation.as_str(),
        operation_id = %operation_id,
        stage = "operation_id_ready",
        elapsed_ms = setup_started.elapsed().as_millis() as u64,
        "operation stream operation ID ready"
    );
    let (sender, receiver) = mpsc::channel(OPERATION_STREAM_CHANNEL_CAPACITY);
    let stream_state = state.clone();
    let task_operation = operation;
    let task_operation_id = operation_id.clone();
    let response_operation_id = operation_id.clone();
    let handle = std::thread::spawn(move || {
        run_operation_stream(
            stream_state,
            operation,
            operation_id,
            request.payload,
            OperationStreamSender::new(sender),
        );
    });
    info!(
        event = "operation.task_spawned",
        operation = task_operation.as_str(),
        operation_id = %task_operation_id,
        stage = "task_spawned",
        task = "operation_stream",
        elapsed_ms = setup_started.elapsed().as_millis() as u64,
        "operation stream task spawned"
    );
    std::thread::spawn(move || {
        let join_started = Instant::now();
        let mut observed_child_outcome = "completed";
        let mut observed_child_panicked = false;
        let observed_child_cancelled = false;
        info!(
            event = "operation.task_join_watcher_started",
            operation = task_operation.as_str(),
            operation_id = %task_operation_id,
            stage = "task_joining",
            task = "operation_stream_join_watcher",
            "operation task join watcher started"
        );
        match handle.join() {
            Ok(()) => {
                info!(
                    event = "operation.task_join_completed",
                    operation = task_operation.as_str(),
                    operation_id = %task_operation_id,
                    stage = "task_joined",
                    task = "operation_stream",
                    elapsed_ms = join_started.elapsed().as_millis() as u64,
                    "operation task join completed"
                );
            }
            Err(panic_payload) => {
                observed_child_outcome = "join_failed";
                observed_child_panicked = true;
                let panic_message = panic_payload_message(panic_payload.as_ref());
                error!(
                    event = "operation.task_join_failed",
                    operation = task_operation.as_str(),
                    operation_id = %task_operation_id,
                    stage = "task_join_failed",
                    task = "operation_stream",
                    is_panic = observed_child_panicked,
                    is_cancelled = observed_child_cancelled,
                    panic_message = %panic_message,
                    elapsed_ms = join_started.elapsed().as_millis() as u64,
                    error = %panic_message,
                    "operation task join failed"
                );
            }
        }
        info!(
            event = "operation.task_join_watcher_completed",
            operation = task_operation.as_str(),
            operation_id = %task_operation_id,
            stage = "task_join_watcher_completed",
            task = "operation_stream_join_watcher",
            observed_task = "operation_stream",
            observed_child_outcome,
            observed_child_panicked,
            observed_child_cancelled,
            elapsed_ms = join_started.elapsed().as_millis() as u64,
            "operation task join watcher completed"
        );
    });
    let body = Body::from_stream(ReceiverStream::new(receiver));

    let response = match Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, NDJSON_CONTENT_TYPE)
        .body(body)
        .map(IntoResponse::into_response)
    {
        Ok(response) => response,
        Err(source) => {
            let error = ApiError::InternalIo {
                message: format!("failed to build operation stream response: {source}"),
            };
            log_operation_stream_setup_failed(
                "response_building",
                Some(operation.as_str()),
                Some(&response_operation_id),
                &error,
                &setup_started,
            );
            return Err(error);
        }
    };
    info!(
        event = "operation.stream_setup_completed",
        route = "/v1/operations",
        operation = operation.as_str(),
        operation_id = %response_operation_id,
        stage = "response_built",
        elapsed_ms = setup_started.elapsed().as_millis() as u64,
        "operation stream setup completed"
    );

    Ok(response)
}

/// Log a local setup failure before the operation stream has been returned to the client.
fn log_operation_stream_setup_failed(
    stage: &'static str,
    operation: Option<&str>,
    operation_id: Option<&str>,
    error: &ApiError,
    started: &Instant,
) {
    let status = error.status_u16();
    let error_kind = error.error_kind();
    let error_message = error.to_string();
    if status >= 500 {
        error!(
            event = "operation.stream_setup_failed",
            route = "/v1/operations",
            operation = operation.unwrap_or("unknown"),
            operation_id = operation_id.unwrap_or("unknown"),
            stage,
            status,
            error_kind,
            error = %error_message,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "operation stream setup failed"
        );
    } else {
        warn!(
            event = "operation.stream_setup_failed",
            route = "/v1/operations",
            operation = operation.unwrap_or("unknown"),
            operation_id = operation_id.unwrap_or("unknown"),
            stage,
            status,
            error_kind,
            error = %error_message,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "operation stream setup failed"
        );
    }
}

/// Reject reserved control messages explicitly until operation cancellation is implemented.
async fn post_operation_control(
    Path(operation_id): Path<String>,
    payload: Result<Json<OperationControlRequest>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let started = log_route_started("/v1/operations/{operation_id}/control", "request_decoding");
    let Json(request) = match payload.map_err(json_rejection_to_api_error) {
        Ok(request) => request,
        Err(error) => {
            log_route_failed(
                "/v1/operations/{operation_id}/control",
                "request_decoding",
                &error,
                &started,
            );
            return Err(error);
        }
    };
    info!(
        event = "operation.control.request_decoded",
        route = "/v1/operations/{operation_id}/control",
        operation_id = %operation_id,
        stage = "request_decoded",
        control_type = %request.control_type,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "operation control request decoded"
    );
    if request.control_type.trim().is_empty() {
        let error = ApiError::BadRequest {
            message: "operation control type must be non-empty".to_string(),
        };
        log_route_failed(
            "/v1/operations/{operation_id}/control",
            "control_validating",
            &error,
            &started,
        );
        return Err(error);
    }

    let error = ApiError::BadRequest {
        message: format!(
            "operation control '{}' is reserved but not implemented for operationId {}",
            request.control_type, operation_id
        ),
    };
    warn!(
        event = "operation.control.rejected",
        route = "/v1/operations/{operation_id}/control",
        operation_id = %operation_id,
        stage = "control_reserved",
        control_type = %request.control_type,
        status = error.status_u16(),
        error_kind = error.error_kind(),
        error = %error,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "operation control request rejected"
    );

    Err(error)
}

/// Run accepted operation work and convert post-acceptance failures into terminal stream errors.
fn run_operation_stream(
    state: Arc<AppState>,
    operation: OperationName,
    operation_id: String,
    payload: serde_json::Value,
    sender: OperationStreamSender,
) {
    let mut emitter = OperationEmitter::new(operation_id, operation.as_str(), sender);
    info!(
        event = "operation.accepted",
        operation = operation.as_str(),
        operation_id = %emitter.operation_id,
        stage = "accepted",
        elapsed_ms = emitter.started.elapsed().as_millis() as u64,
        "operation stream accepted"
    );
    if let Err(failure) = execute_operation(state, operation, payload, &mut emitter) {
        let error_message = failure.error.to_string();
        let error_kind = failure.error.error_kind();
        let status = failure.error.status_u16();
        info!(
            event = "operation.failed",
            operation = operation.as_str(),
            operation_id = %emitter.operation_id,
            stage = failure.stage,
            status,
            error_kind,
            error = %error_message,
            elapsed_ms = emitter.started.elapsed().as_millis() as u64,
            "operation stream failed"
        );
        let _ = emitter.error(failure.stage, failure.error);
        info!(
            event = "operation.task_finished",
            operation = operation.as_str(),
            operation_id = %emitter.operation_id,
            stage = "task_finished",
            terminal = "error",
            elapsed_ms = emitter.started.elapsed().as_millis() as u64,
            "operation task finished"
        );
        return;
    }
    info!(
        event = "operation.task_finished",
        operation = operation.as_str(),
        operation_id = %emitter.operation_id,
        stage = "task_finished",
        terminal = "result",
        elapsed_ms = emitter.started.elapsed().as_millis() as u64,
        "operation task finished"
    );
}

/// Dispatch one accepted operation to the route-compatible service implementation.
fn execute_operation(
    state: Arc<AppState>,
    operation: OperationName,
    payload: serde_json::Value,
    emitter: &mut OperationEmitter,
) -> Result<(), OperationFailure> {
    match operation {
        OperationName::Health => {
            emitter
                .status("health_checking", "reading service health")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            emit_terminal_result(emitter, operation.as_str(), state.health())
        }
        OperationName::Limits => {
            emitter
                .status("limits_reading", "reading request and retrieval limits")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            emit_terminal_result(emitter, operation.as_str(), build_limits_response(&state))
        }
        OperationName::Sources => {
            emitter
                .status("sources_listing", "listing ingested source documents")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_ingested_sources(&state)
                .map_err(|error| OperationFailure::new("sources_listing", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
        OperationName::Ingest => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status("ingest_running", "running ingest pipeline")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_ingest(&state, request, Some(emitter))
                .map_err(|error| OperationFailure::new("ingest_running", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
        OperationName::Search => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status("search_running", "running search pipeline")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_search(&state, request, Some(emitter))
                .map_err(|error| OperationFailure::new("search_running", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
        OperationName::Versions => {
            emitter
                .status("versions_listing", "listing retained document versions")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_document_versions(&state)
                .map_err(|error| OperationFailure::new("versions_listing", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
        OperationName::Rollback => {
            let request = decode_operation_payload(payload, operation.as_str())
                .map_err(|error| OperationFailure::new("request_validating", error))?;
            emitter
                .status(
                    "rollback_publishing",
                    "publishing retained document version",
                )
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_document_version_rollback(&state, request)
                .map_err(|error| OperationFailure::new("rollback_publishing", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
        OperationName::Shutdown => {
            emitter
                .status("shutdown_requesting", "requesting graceful shutdown")
                .map_err(|error| OperationFailure::new("operation_streaming", error))?;
            let response = execute_shutdown(&state)
                .map_err(|error| OperationFailure::new("shutdown_requesting", error))?;
            emit_terminal_result(emitter, operation.as_str(), response)
        }
    }
}

/// Emit a terminal result without treating client delivery failure as execution failure.
fn emit_terminal_result<T: Serialize>(
    emitter: &mut OperationEmitter,
    operation: &'static str,
    response: T,
) -> Result<(), OperationFailure> {
    if let Err(error) = emitter.result(response) {
        error!(
            event = "operation.terminal_result_emit_failed",
            operation,
            operation_id = %emitter.operation_id,
            stage = "terminal_result_emitting",
            status = error.status_u16(),
            error_kind = error.error_kind(),
            error = %error,
            elapsed_ms = emitter.started.elapsed().as_millis() as u64,
            "operation terminal result emission failed after execution completed"
        );
    }

    Ok(())
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

/// Build reranker inputs from the top pool-size ColBERT-ranked candidates while preserving candidate identity.
fn build_reranker_candidates(
    candidates: &[SearchCandidate],
    colbert_scores: &[ColbertCandidateScore],
    pool_size: u32,
) -> Result<Vec<RerankerCandidateInput>, ApiError> {
    let candidate_by_id = candidates
        .iter()
        .map(|candidate| (candidate.unit_id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let reranker_limit = pool_size as usize;
    let mut reranker_candidates = Vec::with_capacity(colbert_scores.len().min(reranker_limit));
    for score in colbert_scores.iter().take(reranker_limit) {
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
            // The public score is the final ModernBERT sigmoid score after ColBERT candidate reranking.
            score: score.score,
            content: candidate.content.clone(),
            heading_path: candidate.heading_path.clone(),
            source_path: candidate.source_path.clone(),
            page_numbers: candidate.page_numbers.clone(),
        });
        let mut raw_entry = serde_json::json!({
            "unitId": candidate.unit_id,
            "rerankerScore": score.score,
            "rerankerRank": score.rank,
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
        });
        // Optional reranker diagnostics are omitted when the backend cannot
        // provide them; values are never synthesized.
        if let Some(logit) = score.logit {
            raw_entry["rerankerLogit"] = serde_json::json!(logit);
        }
        if let Some(token_count) = score.token_count {
            raw_entry["rerankerTokenCount"] = serde_json::json!(token_count);
        }
        raw.push(raw_entry);
    }

    Ok((results, raw))
}

/// Request graceful shutdown after the caller has passed admin authorization.
fn execute_shutdown(state: &AppState) -> Result<ShutdownResponse, ApiError> {
    state.request_shutdown()?;
    info!(
        event = "admin.shutdown.confirmed",
        "admin shutdown confirmation emitted"
    );

    Ok(ShutdownResponse {
        status: SHUTDOWN_STATUS_COMPLETE.to_string(),
        message: SHUTDOWN_COMPLETE_MESSAGE.to_string(),
    })
}

/// Return active ingested sources after the public caller has opened the route or stream.
fn execute_ingested_sources(
    state: &AppState,
) -> Result<crate::storage::IngestedSourceListing, ApiError> {
    let started = Instant::now();
    let storage = match state.storage() {
        Ok(storage) => storage,
        Err(source) => {
            error!(
                event = "sources.listing_failed",
                stage = "storage_runtime",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ingested sources listing failed"
            );
            return Err(source);
        }
    };
    let listing = match storage.list_ingested_sources() {
        Ok(listing) => listing,
        Err(source) => {
            error!(
                event = "sources.listing_failed",
                stage = "storage_listing",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ingested sources listing failed"
            );
            return Err(source);
        }
    };
    info!(
        event = "sources.listed",
        sources = listing.sources.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "ingested sources listed"
    );

    Ok(listing)
}

/// Return retained source-document versions after the caller has passed admin authorization.
fn execute_document_versions(
    state: &AppState,
) -> Result<crate::storage::DocumentVersionListing, ApiError> {
    let started = Instant::now();
    let storage = match state.storage() {
        Ok(storage) => storage,
        Err(source) => {
            error!(
                event = "admin.document_versions.failed",
                stage = "storage_runtime",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "admin document versions listing failed"
            );
            return Err(source);
        }
    };
    let listing = match storage.list_document_versions() {
        Ok(listing) => listing,
        Err(source) => {
            error!(
                event = "admin.document_versions.failed",
                stage = "storage_listing",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "admin document versions listing failed"
            );
            return Err(source);
        }
    };
    let version_count = listing
        .sources
        .iter()
        .map(|source| source.versions.len())
        .sum::<usize>();
    info!(
        event = "admin.document_versions.listed",
        sources = listing.sources.len(),
        versions = version_count,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "admin document versions listed"
    );

    Ok(listing)
}

/// Publish an already-retained document version after the caller has passed admin authorization.
fn execute_document_version_rollback(
    state: &AppState,
    request: DocumentVersionRollbackRequest,
) -> Result<DocumentVersionRollbackResponse, ApiError> {
    let started = Instant::now();
    let requested_source = request.source.clone();
    let requested_version_label = request.version_label.clone();
    if let Err(source) = request.validate(state.config.server.max_ingest_source_chars) {
        warn!(
            event = "admin.document_version_rollback.validation_failed",
            source_path = %requested_source,
            version_label = %requested_version_label,
            max_ingest_source_chars = state.config.server.max_ingest_source_chars,
            source_chars = requested_source.chars().count(),
            status = source.status_u16(),
            error_kind = source.error_kind(),
            error = %source,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "admin document version rollback validation failed"
        );
        return Err(source);
    }
    let storage = match state.storage() {
        Ok(storage) => storage,
        Err(source) => {
            error!(
                event = "admin.document_version_rollback.failed",
                source_path = %requested_source,
                version_label = %requested_version_label,
                stage = "storage_runtime",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "admin document version rollback failed"
            );
            return Err(source);
        }
    };
    let rollback = match storage.rollback_document_version(&request.source, &request.version_label)
    {
        Ok(rollback) => rollback,
        Err(source) => {
            error!(
                event = "admin.document_version_rollback.failed",
                source_path = %requested_source,
                version_label = %requested_version_label,
                stage = "storage_rollback",
                status = source.status_u16(),
                error_kind = source.error_kind(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "admin document version rollback failed"
            );
            return Err(source);
        }
    };
    info!(
        event = "admin.document_version_rollback.completed",
        source_path = %rollback.source_path,
        active_version_label = %rollback.active_version_label,
        published_at_ms = rollback.published_at_ms,
        vector_count = rollback.vector_count,
        elapsed_ms = started.elapsed().as_millis() as u64,
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
