use std::{sync::Arc, time::Instant};

use axum::{
    Extension, Json, Router,
    extract::{
        DefaultBodyLimit, FromRequestParts, MatchedPath, Path, Query, Request, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{HeaderMap, Method, StatusCode, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{debug, error, info, warn};

use crate::maintenance::MaintenancePermit;
use crate::util::LogContext;

use crate::assembly::model::EvidencePack;
use crate::inference::{ColbertCandidateScore, RerankerCandidateScore};
use crate::model::SnapshotType;
use crate::model::{
    ContentUnit, DeletionEvidence, Operation, ParseRun, SourceLocation, SourceLocationStatus,
    SourceObject, UnitRelationship,
};
use crate::query::execute::{QueryPipelineOutcome, QueryRequestContext, execute_query};
use crate::query::model::RetrievalHit;
use crate::query::passages::{PassageCandidate, SearchResult};
use crate::query::profile::{ScopeInput, active_profile, resolve_scope};
use crate::query::request::{QueryRequest, ValidatedQuery};

// Axum is confined to this transport module: it owns routing, bearer auth,
// request-body limits, and the spawn_blocking seams that keep synchronous
// domain work off the async runtime. Handlers translate between HTTP and the
// synchronous domain; no async runtime type escapes this boundary.
use crate::{
    error::{ApiError, OperationErrorDetail},
    model::OperationType,
    state::{AppState, SyncHealth},
    types::HealthResponse,
};

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

/// Preserve Axum's path response contract while recording pre-handler failures.
struct DiagnosticPath<T>(T);

impl<S, T> FromRequestParts<S> for DiagnosticPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = PathRejection;

    /// Forward Axum's original rejection so its status and body remain unchanged.
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .inspect_err(|rejection| {
                log_extraction_failed("path_extraction", rejection, rejection.status())
            })
    }
}

/// Preserve direct query extraction while attributing failures to the request.
struct DiagnosticQuery<T>(T);

impl<S, T> FromRequestParts<S> for DiagnosticQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = QueryRejection;

    /// Log the cause without consuming, rewriting, or replacing Axum's rejection.
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .inspect_err(|rejection| {
                log_extraction_failed("query_extraction", rejection, rejection.status())
            })
    }
}

/// Record extractor errors that return before a handler can log their cause.
fn log_extraction_failed(
    stage: &'static str,
    rejection: &dyn std::error::Error,
    status: StatusCode,
) {
    let error_chain = crate::util::error_chain(rejection);
    if status.is_server_error() {
        error!(event = "http.extraction.failed", stage, status = status.as_u16(),
            reason = %rejection, error_chain, "HTTP request extraction failed");
    } else {
        warn!(event = "http.extraction.failed", stage, status = status.as_u16(),
            reason = %rejection, error_chain, "HTTP request extraction rejected");
    }
}

/// Build the Axum router for supported health and operation-control routes.
pub fn build_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.server.max_request_body_bytes;

    // Protection class is enforced per-handler by the `authorize_request` guard
    // (called first in every protected handler), NOT by a router-level layer:
    // the split is fine-grained (some GETs public, some protected) and axum
    // route-scoped middleware would need per-route wiring anyway, so a single
    // in-handler guard keeps the split auditable at each handler head.
    //
    // PUBLIC (no bearer): /v1/health, /query, GET /units/{id}(/relationships),
    // GET /sources/{id}, GET /sync/status. PROTECTED (bearer required): every
    // mutating admin POST, POST /shutdown, GET /parses?status=held, and
    // GET /operations/{operationId}. See §34 protection split (plan resolution 4).
    Router::new()
        .route("/v1/health", get(get_health))
        // §34.1 spec-literal query path. The synchronous retrieval + assembly
        // pipeline runs on a blocking thread inside `post_query` (R12).
        .route("/query", post(post_query))
        // Mutating admin routes (§34): each returns an Operation id immediately
        // and runs the work asynchronously (queue-coupled via the drain, or on a
        // detached spawn_blocking task). All protected.
        .route("/sources", post(post_sources))
        .route("/sources/{sourceId}/parses", post(post_source_parses))
        .route(
            "/sources/{sourceId}/parses/{parseId}/activate",
            post(post_activate),
        )
        .route("/parses/{parseId}/accept", post(post_accept))
        .route("/parses/{parseId}/discard", post(post_discard))
        .route("/snapshots", post(post_snapshots))
        .route("/restore", post(post_restore))
        .route("/rebuild-all", post(post_rebuild_all))
        // Control action (§34): immediate confirmation then signal; NOT an
        // Operation row (it is not async work). Protected.
        .route("/shutdown", post(post_shutdown))
        // Held-parse listing (§13.4 disposition surface). Protected.
        .route("/parses", get(get_parses))
        // Annotation vocabulary inspection (CA2 ruling 8): the operator surface
        // for authoring the corpus-dependent policy rulesets. Protected.
        .route("/annotations/vocabulary", get(get_annotation_vocabulary))
        // Inspection reads (§14/§10): public, and unit reads honor the
        // active-parse gate so a non-active parse's units are never served.
        .route("/units/{unitId}", get(get_unit))
        .route("/units/{unitId}/relationships", get(get_unit_relationships))
        .route("/sources/{sourceId}", get(get_source))
        .route("/sync/status", get(get_sync_status))
        // Operation polling (§34.6). Protected.
        .route("/operations/{operationId}", get(get_operation))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            storage_admission,
        ))
        .layer(middleware::from_fn(request_diagnostics))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

/// Build the REDUCED Axum router for the CA2-P5 annotation dry-run mode (ruling
/// 9). The mode runs a one-shot scan → parse pass inline, then serves ONLY the
/// inspection surface until shutdown, so this router exposes exactly four routes:
///
///   - `GET /v1/health` — mode readiness (inference reports not-ready by design;
///     the mode initializes no model runtimes, so its `inference` component
///     carries the explicit "annotation dry-run mode" error detail).
///   - `GET /annotations/vocabulary` — the P4 inspection route, the dry run's
///     whole point. Its `scope=all` reads annotations on NON-ACTIVE parses, which
///     is exactly the dry-run pass's output (the parses are left ready, never
///     activated). Protected (admin bearer).
///   - `GET /operations/{operationId}` — Operation polling, protected. Served so
///     an operator can inspect any Operation row that predates this run.
///   - `POST /shutdown` — protected; ends the inspection phase cleanly.
///
/// The FULL router is deliberately NOT served: there is no scheduler thread, no
/// annotation worker, and no inference in this mode, so the query path, the
/// mutating admin routes (POST /sources, /parses/*, /snapshots, /restore, …), and
/// the sync/unit/source inspection reads would accept work that nothing drains
/// (or that has no live retrieval planes to answer). Reusing the existing handlers
/// unchanged keeps auth, validation, and the spawn-blocking discipline identical
/// to the full server. The same body-limit layer applies.
// Consumed by the main-loop dry-run wiring (the `--annotation-dry-run` mode
// branch main.rs adds serves this router instead of `build_router`); dead until
// that branch lands.
#[allow(dead_code)]
pub fn build_dry_run_router(state: Arc<AppState>) -> Router {
    let max_request_body_bytes = state.config.server.max_request_body_bytes;
    Router::new()
        .route("/v1/health", get(get_health))
        .route("/annotations/vocabulary", get(get_annotation_vocabulary))
        .route("/shutdown", post(post_shutdown))
        .route("/operations/{operationId}", get(get_operation))
        .layer(middleware::from_fn(request_diagnostics))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

/// Correlate admission, extractor rejection, blocking work, and response rendering.
/// Instrument the future instead of entering a span across an async suspension.
async fn request_diagnostics(request: Request, next: Next) -> Response {
    let request_id = crate::ids::new_request_id();
    let context = LogContext::new("request", &request_id);
    context.record("trigger", "http_request");
    context.record("method", request.method().as_str());
    context.record("request_path", request.uri().path());
    context.record(
        "route",
        request
            .extensions()
            .get::<MatchedPath>()
            .map(|path| path.as_str())
            .unwrap_or("<unmatched>"),
    );
    context
        .instrument(async move {
            let started = Instant::now();
            let response = next.run(request).await;
            // This records response construction, not successful delivery to a client.
            // Extractor failures can bypass handlers, so this boundary covers them too.
            if response.status().is_server_error() {
                error!(
                    event = "http.response.ready",
                    status = response.status().as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "HTTP error response ready"
                );
            } else if response.status().is_client_error() {
                warn!(
                    event = "http.response.ready",
                    status = response.status().as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "HTTP request rejected"
                );
            } else {
                debug!(
                    event = "http.response.ready",
                    status = response.status().as_u16(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "HTTP response ready"
                );
            }
            response
        })
        .await
}

/// Retain admission through the complete handler even if its client disconnects:
/// spawn_blocking work cannot be cancelled by dropping the HTTP future. Detached
/// admin tasks additionally inherit the request's lease through Extension.
async fn storage_admission(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str())
        .unwrap_or("<unmatched>")
        .to_owned();
    let control = matches!(
        (request.method(), route.as_str()),
        (&Method::GET, "/v1/health" | "/operations/{operationId}")
            | (&Method::POST, "/shutdown" | "/rebuild-all")
    );
    let permit = if control {
        None
    } else {
        match state.maintenance().enter() {
            Ok(permit) => {
                request.extensions_mut().insert(permit.clone());
                Some(permit)
            }
            Err(mut failure) => {
                // Preserve protected-route auth precedence during maintenance.
                let public = matches!(
                    (request.method(), route.as_str()),
                    (&Method::POST, "/query")
                        | (
                            &Method::GET,
                            "/units/{unitId}"
                                | "/units/{unitId}/relationships"
                                | "/sources/{sourceId}"
                                | "/sync/status"
                        )
                );
                if !public
                    && let Err(auth_error) = bearer_token_from_headers(request.headers())
                        .and_then(|token| state.authorize_admin_token(token))
                {
                    failure = auth_error;
                }
                warn!(event = "http.maintenance_rejected", route,
                    error = %failure, status = failure.status_u16(), "request rejected before storage admission");
                return failure.into_response();
            }
        }
    };
    let task_route = route.clone();
    match tokio::spawn(LogContext::current().instrument(async move {
        let _permit = permit;
        let response = next.run(request).await;
        debug!(
            event = "http.admitted_task_finished",
            route = task_route,
            status = response.status().as_u16(),
            "admitted HTTP task finished"
        );
        response
    }))
    .await
    {
        Ok(response) => response,
        Err(source) => {
            error!(event = "http.admitted_task_failed", route, %source, "HTTP task failed to join");
            ApiError::InternalIo {
                message: format!("HTTP task failed to join: {source}"),
            }
            .into_response()
        }
    }
}

/// Capture request correlation before a closure leaves its instrumented future.
fn spawn_blocking_with_context<F, T>(work: F) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(LogContext::current().wrap(work))
}

/// Drain and accept rebuild-all on a blocking boundary, then detach clearing.
/// The middleware shields the initial wait from client cancellation. Acceptance
/// waits for existing writers before persisting an Operation; health remains live.
async fn post_rebuild_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/rebuild-all";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;
    let reserve_state = Arc::clone(&state);
    let operation_id = spawn_blocking_with_context(move || crate::reset::reserve(&reserve_state))
        .await
        .map_err(|source| ApiError::InternalIo {
            message: format!("rebuild-all acceptance task failed to join: {source}"),
        })
        .inspect_err(|source| log_route_failed(route, "acceptance_join", source, &started))?
        .inspect_err(|source| log_route_failed(route, "accepting", source, &started))?;
    LogContext::current().record("operation_id", operation_id.as_str());
    let task_id = operation_id.clone();
    spawn_blocking_with_context(move || crate::reset::run(&state, &task_id));
    log_operation_accepted(route, "rebuild_all", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Return service readiness and startup diagnostics.
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let started = log_route_started("/v1/health", "health_reading");
    let response = state.health();
    debug!(
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

/// Log the accepted boundary for one route-specific HTTP request.
fn log_route_started(route: &'static str, stage: &'static str) -> Instant {
    let started = Instant::now();
    // Polling traces belong at DEBUG; query and admin lifecycle starts remain INFO.
    if matches!(
        route,
        "/v1/health" | "/operations/{operationId}" | "/sync/status"
    ) {
        debug!(
            event = "http.route.started",
            route,
            stage,
            elapsed_ms = 0_u64,
            "HTTP route started"
        );
    } else {
        info!(
            event = "http.route.started",
            route,
            stage,
            elapsed_ms = 0_u64,
            "HTTP route started"
        );
    }

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
        let error_chain = crate::util::error_chain(&self);
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
                error_chain,
                stage = "error_response_rendering",
                "API error response constructed; work outcome is recorded at its owning boundary"
            );
        } else {
            warn!(
                event = "api.error_response",
                status = status.as_u16(),
                error_kind,
                error = %message,
                error_chain,
                stage = "error_response_rendering",
                "API error response constructed"
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
    let error_chain = crate::util::error_chain(error);
    if status >= 500 {
        error!(
            event = "http.route.failed",
            route,
            stage,
            status,
            error_kind,
            error = %error_message,
            error_chain,
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
            error_chain,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "HTTP route failed"
        );
    }
}

/// §34.1 `/query` response envelope. Carries the assembled `EvidencePack` and,
/// only when the request set `debug`, the raw per-stage retrieval diagnostics.
///
/// DEVIATION FROM §34.1 (deliberate MVP narrowing, R1): the spec-literal response
/// is "the EvidencePack plus the queryExecutionRecordId". The `queryExecutionRecordId`
/// is OMITTED here because the QueryExecutionRecord audit tier is deferred this
/// cluster (no QER is written). This is a recorded narrowing, not an oversight —
/// the field can be added additively when the QER tier lands. `query_id` on the
/// pack is a per-query correlation handle only (see `post_query`), not a QER id.
///
/// Wire conventions mirror `crate::query::model` (§16.2): camelCase,
/// `skip_serializing_if` so `diagnostics` is absent (not null) unless `debug`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    /// Ranked, cited passages ready for presentation by any client.
    results: Vec<SearchResult>,
    /// Canonical records underlying the selected passages, for inspection.
    evidence_pack: EvidencePack,
    /// Raw stage diagnostics, present only when the request set `debug` (R6).
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics: Option<QueryDiagnostics>,
}

/// Raw per-stage retrieval diagnostics, attached only under `debug`. Projects the
/// pipeline's in-memory stage outputs into a serializable shape: the inference
/// candidate-score types (`ColbertCandidateScore`/`RerankerCandidateScore`) are
/// not `Serialize` and are owned by another module, so their decision-relevant
/// fields are projected into local DTOs here rather than serialized directly.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryDiagnostics {
    /// Individual eligible channel records before rank fusion.
    channel_hits: Vec<RetrievalHit>,
    /// Passage text and membership offered to the final reranker.
    passage_candidates: Vec<PassageCandidate>,
    /// The fused dense+lexical+graph candidate pool the rerankers scored over.
    fused_pool: Vec<RetrievalHit>,
    /// ColBERT MaxSim scores over the fused pool, best-first.
    maxsim: Vec<MaxsimScoreView>,
    /// Final passage reranker scores, keyed by representative anchor, best-first.
    reranked: Vec<RerankerScoreView>,
    /// Per-stage wall-clock latencies (milliseconds).
    latencies: StageLatencyView,
}

/// Serializable projection of one ColBERT MaxSim candidate score (§debug).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MaxsimScoreView {
    unit_id: String,
    score: f32,
    rank: usize,
}

/// Serializable projection of one final-reranker candidate score (§debug). The
/// raw `logit`/`token_count` are surfaced when the backend supplied them (the
/// local ModernBERT backend does; absent values stay omitted, never synthesized).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RerankerScoreView {
    unit_id: String,
    score: f32,
    rank: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    logit: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_count: Option<usize>,
}

/// Serializable projection of the pipeline's per-stage latencies (§debug).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StageLatencyView {
    open_transaction_ms: u64,
    capture_ms: u64,
    query_embed_ms: u64,
    dense_lexical_fusion_ms: u64,
    graph_ms: u64,
    maxsim_ms: u64,
    passage_build_ms: u64,
    rerank_ms: u64,
    assembly_ms: u64,
    snapshot_held_ms: u64,
}

impl QueryDiagnostics {
    /// Copy stage records only when debug output was requested, allowing the
    /// larger canonical evidence pack to move into the response without cloning.
    fn from_outcome(outcome: &QueryPipelineOutcome) -> Self {
        let maxsim = outcome
            .maxsim
            .iter()
            .map(|score: &ColbertCandidateScore| MaxsimScoreView {
                unit_id: score.unit_id.clone(),
                score: score.score,
                rank: score.rank,
            })
            .collect();
        let reranked = outcome
            .reranked
            .iter()
            .map(|score: &RerankerCandidateScore| RerankerScoreView {
                unit_id: score.unit_id.clone(),
                score: score.score,
                rank: score.rank,
                logit: score.logit,
                token_count: score.token_count,
            })
            .collect();
        let latencies = StageLatencyView {
            open_transaction_ms: outcome.latencies.open_transaction_ms,
            capture_ms: outcome.latencies.capture_ms,
            query_embed_ms: outcome.latencies.query_embed_ms,
            dense_lexical_fusion_ms: outcome.latencies.dense_lexical_fusion_ms,
            graph_ms: outcome.latencies.graph_ms,
            maxsim_ms: outcome.latencies.maxsim_ms,
            passage_build_ms: outcome.latencies.passage_build_ms,
            rerank_ms: outcome.latencies.rerank_ms,
            assembly_ms: outcome.latencies.assembly_ms,
            snapshot_held_ms: outcome.latencies.snapshot_held_ms,
        };
        Self {
            channel_hits: outcome.channel_hits.clone(),
            passage_candidates: outcome.passage_candidates.clone(),
            fused_pool: outcome.fused_pool.clone(),
            maxsim,
            reranked,
            latencies,
        }
    }
}

/// Handle `POST /query` (§34.1): validate the request, admit it into the
/// single-search window, and run the synchronous retrieval + assembly pipeline
/// on a blocking thread, returning the assembled `EvidencePack`.
///
/// ADMISSION ORDER (R12): `try_acquire_search` FIRST (the permit is bound for the
/// WHOLE call and released on drop after the pipeline returns), THEN
/// `spawn_blocking`. Saturation surfaces the existing `ServiceUnavailable` (503),
/// no dedicated error (R7).
///
/// BLOCKING BOUNDARY (R12, PRINCIPLES.md Async rules): this is `http.rs`'s FIRST
/// `spawn_blocking` seam. The retrieval pipeline is SQLite + local-accelerator
/// work — blocking by contract (`execute_query`'s DP2 doc) — so it must run on a
/// blocking thread, never a Tokio worker. The async boundary is confined to this
/// transport shell; everything the pipeline touches stays synchronous. A
/// `spawn_blocking` join failure (a panic in the blocking closure) is mapped to a
/// logged `InternalIo` (500), never a silent drop.
async fn post_query(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<QueryResponse>, ApiError> {
    let route = "/query";
    let started = log_route_started(route, "request_decoding");

    // Decode: normalize the axum JSON rejection into this service's error shape
    // (413 for body-too-large, 400 otherwise) exactly like the control route.
    let Json(request) = match payload.map_err(json_rejection_to_api_error) {
        Ok(request) => request,
        Err(error) => {
            log_route_failed(route, "request_decoding", &error, &started);
            return Err(error);
        }
    };
    info!(
        event = "query.request_decoded",
        route,
        stage = "request_decoded",
        has_constraints = request.constraints.is_some(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "query request decoded"
    );

    // Validate against the sealed retrieval profile and the server query-length
    // cap. `active_profile()` is the FIRST functional profile read on this path;
    // `max_search_query_chars` is this request layer's first functional reader of
    // that config key. A validation failure is a client boundary (field + cap).
    let profile = match active_profile() {
        Ok(profile) => profile,
        Err(error) => {
            log_route_failed(route, "profile_sealing", &error, &started);
            return Err(error);
        }
    };
    let max_search_query_chars = state.config.server.max_search_query_chars;
    let validated = match request.validate(profile, max_search_query_chars) {
        Ok(validated) => validated,
        Err(error) => {
            log_route_failed(route, "request_validating", &error, &started);
            return Err(error);
        }
    };

    // Admission FIRST (R12): hold the permit for the whole call. Saturation is the
    // existing 503 `ServiceUnavailable`. Bound to `_permit` so it drops only when
    // this handler returns (after the blocking pipeline completes).
    let _permit = match state.try_acquire_search("query") {
        Ok(permit) => permit,
        Err(error) => {
            log_route_failed(route, "admission", &error, &started);
            return Err(error);
        }
    };

    // Run the synchronous pipeline on a blocking thread (see fn doc). The handles
    // the pipeline needs are cloned/derived from `AppState` here (on the async
    // thread) and moved into the closure; `execute_query` reaches no globals (DP2).
    let state_for_blocking = Arc::clone(&state);
    let debug_requested = validated.debug;
    let join =
        spawn_blocking_with_context(move || run_query_pipeline(&state_for_blocking, validated))
            .await;

    let outcome = match join {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            log_route_failed(route, "pipeline", &error, &started);
            return Err(error);
        }
        Err(join_error) => {
            // A panic/cancel inside the blocking closure. Surface it as a logged
            // 500, never a silent drop of the request.
            let error = ApiError::InternalIo {
                message: format!("query pipeline task failed to join: {join_error}"),
            };
            log_route_failed(route, "pipeline_join", &error, &started);
            return Err(error);
        }
    };

    // Split the pack out for the primary response; attach raw diagnostics only
    // under `debug` (R6). `queryExecutionRecordId` is omitted (R1 — see
    // `QueryResponse`).
    let diagnostics = if debug_requested {
        Some(QueryDiagnostics::from_outcome(&outcome))
    } else {
        None
    };

    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        evidence_units = outcome.evidence_pack.evidence_units.len(),
        results = outcome.results.len(),
        debug_requested,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );

    Ok(Json(QueryResponse {
        results: outcome.results,
        evidence_pack: outcome.evidence_pack,
        diagnostics,
    }))
}

/// Run the synchronous retrieval + assembly pipeline for one validated query on
/// the current (blocking) thread. Threads the pipeline handles out of `AppState`
/// (DP2: explicit handles, no reach-through inside `execute_query`) and mints the
/// per-query correlation id.
///
/// This runs INSIDE `spawn_blocking` (see `post_query`): it does SQLite +
/// local-accelerator work and must not run on a Tokio worker thread.
fn run_query_pipeline(
    state: &AppState,
    validated: ValidatedQuery,
) -> Result<QueryPipelineOutcome, ApiError> {
    // The per-query correlation id. `crate::ids` exposes no dedicated `query_id`
    // minter and the QueryExecutionRecord is deferred (R1), so the id-generation
    // primitive `new_query_execution_record_id()` is reused purely as a
    // correlation handle for logs and the pack's `queryId` — NO QueryExecutionRecord
    // is written this cluster. (Amendment 5.) The minter is fallible (it hashes
    // random bytes); a mint failure fails the query loudly rather than falling
    // back to a non-unique id.
    let query_id = crate::ids::new_query_execution_record_id()?;
    LogContext::current().record("query_id", query_id.as_str());

    // Resolve scope from the validated constraints (R3: both-present conjoins).
    let scope = resolve_scope(ScopeInput {
        source_ids: validated.source_ids.as_deref(),
        governance_domains: validated.governance_domains.as_deref(),
    });

    let inference = state.inference()?;
    let gate = state.model_call_gate_handle();
    let index_root = state.config.storage.index_root.as_path();
    // The runtime ColBERT projection width the multivector decoder validates
    // against, threaded from config exactly as the build path does (§scheduler).
    let colbert_expected_dimension = state.config.models.colbert.dimension as usize;
    let profile = active_profile()?;

    let request_ctx = QueryRequestContext {
        max_final_evidence_units: validated.max_final_evidence_units,
        evidence_options: validated.evidence_options,
    };

    execute_query(
        state.cutover_registry(),
        state.dense_cache(),
        inference,
        &gate,
        index_root,
        profile,
        // CA2 D9 amendment: the operator-loaded entity-match policy governs the
        // graph channel's fuzzy entry classes (ships disabled = exact-only).
        state.entity_match_policy(),
        colbert_expected_dimension,
        &query_id,
        &validated.query_text,
        &scope,
        request_ctx,
    )
}

// ===========================================================================
// §34 administrative + inspection surface (C10a).
//
// Every mutating admin route is an async Operation (§34.6): the handler writes
// a durable `pending` Operation row, returns the id immediately, and the actual
// work completes asynchronously. Two completion models:
//
//   * QUEUE-COUPLED (POST /sources, POST /sources/{id}/parses): the handler
//     enqueues via `scheduler::enqueue_coalesced` with the operation id threaded
//     into `sync_queue.operation_id`. The DRAIN owns the full running→terminal
//     lifecycle — it flips the still-`pending` row to `running` at dispatch and
//     drives `succeeded`/`failed` at `scheduler::complete()`/`fail()` (C10r
//     wiring). The handler NEVER marks running/terminal for these.
//
//   * DETACHED (activate, accept, discard, snapshots, restore): the handler
//     spawns a detached `spawn_blocking` task (never awaited) whose closure
//     flips `pending → running`, runs the domain call, and writes the terminal
//     state — including a panic-to-`failed` conversion so a panicking admin task
//     can never leave an operation stuck at `running` with no durable record.
//     `spawn_admin_operation` pins that contract in one place.
//
// Inspection reads (§14/§10) are public and non-mutating. Unit/relationship
// reads honor the §14 active-parse gate in SQL: a unit is served only when its
// parse-scoped id's parse equals the owning source's active_parse_id, so a
// non-active or held parse's units are never served (indistinguishable from
// absence at the API — a 404).
// ===========================================================================

/// Bearer-auth guard for every protected route: extract the token from the
/// Authorization header and validate it constant-time against the admin token.
/// Called FIRST in each protected handler; an auth failure is logged through the
/// route-failure boundary (never logging the token itself) and short-circuits
/// before any work. Public routes never call this.
fn authorize_request(
    state: &AppState,
    headers: &HeaderMap,
    route: &'static str,
    started: &Instant,
) -> Result<(), ApiError> {
    let token = match bearer_token_from_headers(headers) {
        Ok(token) => token,
        Err(error) => {
            // The token value is never in the error or the log — only the failure.
            log_route_failed(route, "authorizing", &error, started);
            return Err(error);
        }
    };
    if let Err(error) = state.authorize_admin_token(token) {
        log_route_failed(route, "authorizing", &error, started);
        return Err(error);
    }
    Ok(())
}

/// The acceptance body for an async admin Operation (§34.6): the
/// caller polls `GET /operations/{operationId}` with this id. Returned under
/// HTTP 202 (Accepted) — the work has NOT completed when this returns.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationAcceptedBody {
    operation_id: String,
}

/// Insert the `pending` Operation row synchronously and return its id. The row
/// MUST exist before the handler returns the id (a caller polling immediately
/// must find it), so the insert — a SQLite write — runs on a blocking thread and
/// is awaited here, unlike the detached work task. `insert_pending` opens its own
/// write connection (operations-store contract).
async fn insert_pending_operation(
    index_root: std::path::PathBuf,
    operation_type: OperationType,
    target_object_type: &'static str,
    target_object_id: String,
) -> Result<String, ApiError> {
    let started = Instant::now();
    let context = LogContext::current();
    context.record("target_object_type", target_object_type);
    context.record("target_object_id", target_object_id.as_str());
    let join = spawn_blocking_with_context(move || {
        crate::operations::insert_pending(
            &index_root,
            operation_type,
            target_object_type,
            &target_object_id,
        )
    })
    .await;
    let result = match join {
        Ok(result) => result,
        Err(join_error) => Err(ApiError::InternalIo {
            message: format!("pending-operation insert task failed to join: {join_error}"),
        }),
    };
    match &result {
        Ok(operation_id) => context.record("operation_id", operation_id.as_str()),
        Err(source) => error!(event = "http.operation_insert.failed",
            stage = "insert_pending_operation", error = %source,
            error_chain = %crate::util::error_chain(source),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "operation acceptance failed; no operation ID returned"),
    }
    result
}

/// Spawn the DETACHED work for one async admin Operation and pin the durability
/// contract (DIAGNOSTICS spawned-task rule). The returned id was already written
/// `pending` by the handler; this task owns the rest of the lifecycle:
///   1. `mark_running` (pending → running, stamps started_at),
///   2. run `work` inside `catch_unwind` so a panic is converted to a bounded
///      `mark_failed` instead of unwinding out of the detached task and leaving
///      the row stuck at `running`,
///   3. `mark_succeeded` on Ok, `mark_failed(bounded)` on Err or panic.
///
/// The handle is NOT awaited (the caller already returned the id), so every
/// terminal write happens INSIDE the closure — a JoinError would otherwise be
/// unobservable. `mark_*` failures (e.g. a status-guard miss) are logged and
/// dropped: the task cannot surface them anywhere, and the row's last durable
/// state plus this log line are the record.
fn spawn_admin_operation<F>(
    permit: MaintenancePermit,
    index_root: std::path::PathBuf,
    operation_id: String,
    operation_type: &'static str,
    work: F,
) where
    F: FnOnce() -> Result<(), ApiError> + Send + 'static,
{
    spawn_blocking_with_context(move || {
        // The HTTP response may already be gone; retain admission through the
        // terminal Operation write so rebuild-all cannot erase this task's data.
        let _permit = permit;
        let started = Instant::now();
        // pending → running. A failure here means the row vanished or was already
        // advanced; there is nowhere to surface it (detached task), so log and stop
        // — running the work without a running row would break the audit contract.
        if let Err(error) = crate::operations::mark_running(&index_root, &operation_id) {
            error!(
                event = "operation.task.mark_running_failed",
                operation_type,
                operation_id = %operation_id,
                stage = "mark_running",
                status = error.status_u16(),
                error_kind = error.error_kind(),
                error = %error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "detached admin operation could not be marked running"
            );
            return;
        }
        info!(
            event = "operation.task.running",
            operation_type,
            operation_id = %operation_id,
            stage = "running",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "detached admin operation running"
        );

        // Convert a panic in the domain work into a bounded `failed` terminal
        // state. Terminal persistence may itself fail; its failure log preserves
        // that unresolved outcome. `AssertUnwindSafe` is sound
        // here because a panic ends the task — no caught-across state is observed
        // after unwinding; the only post-panic action is the durable mark_failed.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        match outcome {
            Ok(Ok(())) => match crate::operations::mark_succeeded(&index_root, &operation_id) {
                Ok(()) => info!(
                    event = "operation.task.succeeded",
                    operation_type,
                    operation_id = %operation_id,
                    stage = "succeeded",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "detached admin operation succeeded"
                ),
                Err(error) => log_terminal_mark_failure(
                    operation_type,
                    &operation_id,
                    "mark_succeeded",
                    &error,
                    &started,
                ),
            },
            Ok(Err(error)) => {
                warn!(
                    event = "operation.task.failed",
                    operation_type,
                    operation_id = %operation_id,
                    stage = "failed",
                    status = error.status_u16(),
                    error_kind = error.error_kind(),
                    error = %error,
                    error_chain = %crate::util::error_chain(&error),
                    last_confirmed_operation_status = "running",
                    work_effects = "see_domain_boundary_logs",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "admin work failed; recording terminal operation failure"
                );
                mark_operation_failed(
                    operation_type,
                    &index_root,
                    &operation_id,
                    &error.to_string(),
                    &started,
                );
            }
            Err(panic) => {
                let panic_message = panic_message(panic.as_ref());
                error!(
                    event = "operation.task.panicked",
                    operation_type,
                    operation_id = %operation_id,
                    stage = "panicked",
                    panic = %panic_message,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "detached admin operation panicked; recording failed"
                );
                mark_operation_failed(
                    operation_type,
                    &index_root,
                    &operation_id,
                    &format!("operation task panicked: {panic_message}"),
                    &started,
                );
            }
        }
    });
}

/// Drive the terminal `failed` write for a detached task, logging a
/// double-fault if the mark itself fails (there is nowhere else to surface it).
fn mark_operation_failed(
    operation_type: &'static str,
    index_root: &std::path::Path,
    operation_id: &str,
    detail: &str,
    started: &Instant,
) {
    if let Err(error) = crate::operations::mark_failed(index_root, operation_id, detail) {
        log_terminal_mark_failure(operation_type, operation_id, "mark_failed", &error, started);
    }
}

/// Log the double-fault where a terminal transition (`mark_succeeded`/
/// `mark_failed`) itself failed inside a detached task.
fn log_terminal_mark_failure(
    operation_type: &'static str,
    operation_id: &str,
    stage: &'static str,
    error: &ApiError,
    started: &Instant,
) {
    error!(
        event = "operation.task.terminal_mark_failed",
        operation_type,
        operation_id = %operation_id,
        stage,
        status = error.status_u16(),
        error_kind = error.error_kind(),
        error = %error,
        error_chain = %crate::util::error_chain(error),
        last_confirmed_operation_status = "running",
        terminal_operation_status = "unknown",
        next_action = "inspect_operation_and_domain_boundary_logs",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "detached admin operation terminal transition failed"
    );
}

/// Extract a bounded, human-readable message from a caught panic payload. Only
/// the panic's own string is surfaced (never captured local state), and it is
/// bounded before persistence by `mark_failed`.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Read one parse run's owning source id on a fresh read connection, for the
/// held-disposition cleanup which needs the source id to gate over the run's own
/// pre_activation snapshot (`SupersededCleanupMode::HeldSupersession`). The
/// run→source binding is immutable (§12), so a plain read suffices; an absent
/// run is a caller-contract 404. Mirrors the pre-barrier source lookup in
/// activation.rs, kept here because that lookup is private to that module.
const SELECT_PARSE_RUN_SOURCE_SQL: &str = "SELECT source_id FROM parse_runs WHERE id = ?1";

fn lookup_source_id_for_parse(
    index_root: &std::path::Path,
    parse_run_id: &str,
) -> Result<String, ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let source_id: Option<String> = connection
        .query_row(SELECT_PARSE_RUN_SOURCE_SQL, params![parse_run_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to look up source for parse run {parse_run_id}: {source}"),
        })?;
    source_id.ok_or_else(|| ApiError::NotFound {
        message: format!("no parse run {parse_run_id}"),
    })
}

// --- Mutating admin routes -------------------------------------------------

/// Request body for `POST /sources` and `POST /sources/{sourceId}/parses`: the
/// connector-scoped identity of the content to ingest/re-parse. Both queue-coupled
/// routes enqueue this coordinate; the drain performs acquisition and the parse
/// chain. `deny_unknown_fields` matches the model serde standard.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IngestRequest {
    source_system: String,
    native_uri: String,
}

/// Handle `POST /sources` (§34, `source_ingest`): QUEUE-COUPLED. Writes a
/// `pending` Operation, enqueues the ingest coordinate with the operation id
/// threaded into `sync_queue.operation_id`, and returns the id. The DRAIN owns
/// the running→terminal lifecycle for this operation (C10r wiring); this handler
/// never marks running/terminal.
async fn post_sources(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Result<Json<IngestRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/sources";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let Json(request) = decode_body(route, payload, &started)?;
    LogContext::current().record("source_paths", request.native_uri.as_str());

    // Lexical containment prescreen (ruled 2026-07-17): a nativeUri that is
    // not an absolute path under the corpus root, or that carries traversal
    // components, can never be staged by any scan — reject it 400 here
    // instead of minting a pending Operation whose failure would wait on the
    // next scan cycle. CPU-only lexical check, so no spawn_blocking; the
    // drain's missing-bundle policy and the parse-dispatch containment
    // authority (crate::source) remain authoritative.
    if let Err(error) = crate::source::prescreen_operator_native_uri(
        &state.config.storage.corpus_root,
        &request.native_uri,
    ) {
        log_route_failed(route, "coordinate_validating", &error, &started);
        return Err(error);
    }

    let index_root = state.config.storage.index_root.clone();
    let native_uri = request.native_uri.clone();

    // pending row first (target = the source coordinate), then enqueue with the
    // id. The drain completes the Operation when the ingest finishes.
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::SourceIngest,
        "source",
        native_uri.clone(),
    )
    .await?;
    enqueue_ingest(
        index_root,
        request.source_system,
        request.native_uri,
        "http_source_ingest",
        &operation_id,
        route,
        &started,
    )
    .await?;

    log_operation_accepted(route, "source_ingest", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Existence probe for the path `sourceId` on a re-parse. Bounded (`LIMIT 1`),
/// read-only; returns the id only to confirm the row exists.
const SELECT_SOURCE_EXISTS_SQL: &str = "SELECT id FROM source_objects WHERE id = ?1 LIMIT 1";

/// Probe that the body coordinate `(source_system, native_uri)` is a `current`
/// location of the path source. Bounded (`LIMIT 1`), read-only. `source_id` is
/// matched too so a coordinate that is current for a *different* source does not
/// satisfy the check for this source.
const SELECT_CURRENT_LOCATION_SQL: &str = "
SELECT id FROM source_locations
WHERE source_id = ?1 AND source_system = ?2 AND native_uri = ?3 AND status = 'current'
LIMIT 1";

/// Validate that a force-reparse body coordinate names a current location of the
/// path source, on a fresh read connection (blocking; SQLite hot plane).
///
/// INVARIANT (Operation targetObjectId integrity / Option 1 ruling 2026-07-16):
/// `post_source_parses` stamps the path `sourceId` as the Operation's target
/// (`targetObjectId`) while the body `(source_system, native_uri)` alone drives
/// the enqueue. Without this check a mismatched body would re-parse one source
/// while the Operation record claims to target another — audit-trail corruption.
/// This closes that hazard before the pending Operation is written.
///
/// Two-step for diagnosability:
///   - source row absent           -> `NotFound` (404), names the source id.
///   - no matching `current` row    -> `BadRequest` (400), the coordinate does not
///     name a current location of the source.
///
/// Discloses only ids/URIs the caller already supplied.
fn validate_parse_coordinate(
    index_root: &std::path::Path,
    source_id: &str,
    source_system: &str,
    native_uri: &str,
) -> Result<(), ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let source_exists: Option<String> = connection
        .query_row(SELECT_SOURCE_EXISTS_SQL, params![source_id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to confirm source {source_id}: {source}"),
        })?;
    if source_exists.is_none() {
        return Err(ApiError::NotFound {
            message: format!("no source {source_id}"),
        });
    }

    let location_exists: Option<String> = connection
        .query_row(
            SELECT_CURRENT_LOCATION_SQL,
            params![source_id, source_system, native_uri],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to confirm current location of source {source_id}: {source}"),
        })?;
    if location_exists.is_none() {
        return Err(ApiError::BadRequest {
            message: format!("coordinate does not name a current location of source {source_id}"),
        });
    }

    Ok(())
}

/// Handle `POST /sources/{sourceId}/parses` (§34, `parser_execution`):
/// QUEUE-COUPLED force re-parse. Same shape as ingest — a `pending` Operation is
/// written and the coordinate enqueued with the operation id; the drain runs the
/// parse chain and completes the Operation. A force re-parse whose parser
/// identity and content are unchanged completes with the drain's recorded
/// identical-identity outcome (§13.5); the handler just enqueues.
///
/// Before the pending Operation is written, the body coordinate is VALIDATED
/// against the path source (Option 1 ruling 2026-07-16, `validate_parse_coordinate`):
/// the source must exist and the `(source_system, native_uri)` must be one of its
/// `current` locations, so the stamped `targetObjectId` cannot disagree with the
/// coordinate actually re-parsed.
async fn post_source_parses(
    State(state): State<Arc<AppState>>,
    DiagnosticPath(source_id): DiagnosticPath<String>,
    headers: HeaderMap,
    payload: Result<Json<IngestRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/sources/{sourceId}/parses";
    LogContext::current().record("source_id", source_id.as_str());
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let Json(request) = decode_body(route, payload, &started)?;
    LogContext::current().record("source_paths", request.native_uri.as_str());
    let index_root = state.config.storage.index_root.clone();

    // Operation targetObjectId integrity (Option 1 ruling 2026-07-16): the path
    // source id becomes the Operation target below, so the body coordinate must
    // first be confirmed a current location of that source. A mismatch here is a
    // request-validation boundary rejection (404 absent source / 400 stale or
    // foreign coordinate), logged and returned before any row is written.
    {
        let validate_index_root = index_root.clone();
        let validate_source_id = source_id.clone();
        let validate_source_system = request.source_system.clone();
        let validate_native_uri = request.native_uri.clone();
        let validation = spawn_blocking_with_context(move || {
            validate_parse_coordinate(
                &validate_index_root,
                &validate_source_id,
                &validate_source_system,
                &validate_native_uri,
            )
        })
        .await
        .map_err(|join_error| ApiError::InternalIo {
            message: format!("parse-coordinate validation task failed to join: {join_error}"),
        })
        .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?;
        if let Err(error) = validation {
            log_route_failed(route, "coordinate_validating", &error, &started);
            return Err(error);
        }
    }

    // The Operation targets the source; the coordinate re-parsed is the request's
    // (source_system, native_uri), now validated as a current location of it.
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::ParserExecution,
        "source",
        source_id,
    )
    .await?;
    enqueue_ingest(
        index_root,
        request.source_system,
        request.native_uri,
        "http_force_reparse",
        &operation_id,
        route,
        &started,
    )
    .await?;

    log_operation_accepted(route, "parser_execution", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Enqueue one queue-coupled coordinate with its operation id, on a blocking
/// thread (SQLite write). `enqueue_coalesced` persists the operation id into
/// `sync_queue.operation_id` so the drain can complete the paired Operation.
///
/// DURABILITY on enqueue failure: the drain owns the queue-coupled Operation
/// lifecycle, but only once a queue row exists. If the enqueue itself fails, no
/// row exists and the drain would never complete the already-written `pending`
/// Operation — an orphan. So on enqueue failure this drives the Operation
/// terminal here (pending → running → failed), mirroring the drain's own
/// `drive_operation_failed` (mark_failed is `running`-guarded, so the row is
/// advanced through `running` first). Only after that does the request error
/// return to the client.
async fn enqueue_ingest(
    index_root: std::path::PathBuf,
    source_system: String,
    native_uri: String,
    reason: &'static str,
    operation_id: &str,
    route: &'static str,
    started: &Instant,
) -> Result<(), ApiError> {
    let enqueue_index_root = index_root.clone();
    let enqueue_operation_id = operation_id.to_owned();
    let join = spawn_blocking_with_context(move || {
        crate::scheduler::enqueue_coalesced(
            &enqueue_index_root,
            &source_system,
            &native_uri,
            reason,
            Some(&enqueue_operation_id),
        )
    })
    .await;
    let result = match join {
        Ok(result) => result,
        Err(join_error) => Err(ApiError::InternalIo {
            message: format!("enqueue task failed to join: {join_error}"),
        }),
    };
    if let Err(error) = &result {
        log_route_failed(route, "enqueue", error, started);
        fail_orphaned_operation(index_root, operation_id, &error.to_string()).await;
    }
    result
}

/// Drive a queue-coupled Operation terminal after an enqueue failure left no
/// queue row for the drain to complete (pending → running → failed). Best-effort
/// on its own blocking boundary: a store fault here is logged and dropped — the
/// enqueue error is already the client's response, and the durable record is the
/// Operation row's last state plus this log.
async fn fail_orphaned_operation(index_root: std::path::PathBuf, operation_id: &str, detail: &str) {
    let operation_id = operation_id.to_owned();
    let detail = detail.to_owned();
    // Await within the cancellation-shielded HTTP task so its storage lease
    // also covers this last orphan-repair write.
    let join = spawn_blocking_with_context(move || {
        // mark_failed is `running`-guarded, so advance through `running` first —
        // the same sequence the drain's fail path uses for a still-pending row.
        if let Err(error) = crate::operations::mark_running(&index_root, &operation_id) {
            warn!(
                event = "operation.enqueue_orphan.mark_running_failed",
                operation_id = %operation_id,
                error = %error,
                "could not advance orphaned operation to running after enqueue failure"
            );
            return;
        }
        if let Err(error) = crate::operations::mark_failed(&index_root, &operation_id, &detail) {
            warn!(
                event = "operation.enqueue_orphan.mark_failed_failed",
                operation_id = %operation_id,
                error = %error,
                "could not fail orphaned operation after enqueue failure"
            );
        }
    })
    .await;
    if let Err(source) = join {
        error!(event = "operation.enqueue_orphan.join_failed", %source, "orphan repair task failed to join");
    }
}

/// Handle `POST /sources/{sourceId}/parses/{parseId}/activate` (§34,
/// `parse_activation`): DETACHED task. Force-activate the addressed parse through
/// the unattended gate (`gate_and_activate`), then drive the §31.2 predecessor
/// cleanup and the Ruling-1 held-supersession cleanup the decision carries, all
/// inside the detached task.
async fn post_activate(
    State(state): State<Arc<AppState>>,
    Extension(permit): Extension<MaintenancePermit>,
    DiagnosticPath((source_id, parse_id)): DiagnosticPath<(String, String)>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/sources/{sourceId}/parses/{parseId}/activate";
    LogContext::current().record("source_id", source_id.as_str());
    LogContext::current().record("parse_id", parse_id.as_str());
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let index_root = state.config.storage.index_root.clone();
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::ParseActivation,
        "parse",
        parse_id.clone(),
    )
    .await?;

    let state_for_task = Arc::clone(&state);
    let task_index_root = index_root.clone();
    spawn_admin_operation(
        permit,
        index_root,
        operation_id.clone(),
        "parse_activation",
        move || {
            let dense_dimension = state_for_task.config.models.dense.dimension as usize;
            let store = crate::artifact_store::ArtifactStore::open(&task_index_root)?;
            let decision = crate::activation::gate_and_activate(
                &task_index_root,
                &store,
                state_for_task.cutover_registry(),
                state_for_task.dense_cache(),
                dense_dimension,
                &parse_id,
            )?;
            drive_activation_cleanup(&task_index_root, &source_id, &parse_id, &decision)?;
            Ok(())
        },
    );

    log_operation_accepted(route, "parse_activation", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Handle `POST /parses/{parseId}/accept` (§34, `parse_activation`): DETACHED
/// task. Force-activate a held parse (`accept_held_parse`), then drive the same
/// predecessor + held-supersession cleanup the scheduler arms run — the accepted
/// run is never in `superseded_held_ids`.
async fn post_accept(
    State(state): State<Arc<AppState>>,
    Extension(permit): Extension<MaintenancePermit>,
    DiagnosticPath(parse_id): DiagnosticPath<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/parses/{parseId}/accept";
    LogContext::current().record("parse_id", parse_id.as_str());
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let index_root = state.config.storage.index_root.clone();
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::ParseActivation,
        "parse",
        parse_id.clone(),
    )
    .await?;

    let state_for_task = Arc::clone(&state);
    let task_index_root = index_root.clone();
    spawn_admin_operation(
        permit,
        index_root,
        operation_id.clone(),
        "parse_activation",
        move || {
            let dense_dimension = state_for_task.config.models.dense.dimension as usize;
            let store = crate::artifact_store::ArtifactStore::open(&task_index_root)?;
            let decision = crate::activation::accept_held_parse(
                &task_index_root,
                &store,
                state_for_task.cutover_registry(),
                state_for_task.dense_cache(),
                dense_dimension,
                &parse_id,
            )?;
            // The accepted run's own source, for both the predecessor and the
            // held-supersession cleanups (the decision carries ids, not the source).
            let source_id = lookup_source_id_for_parse(&task_index_root, &parse_id)?;
            drive_activation_cleanup(&task_index_root, &source_id, &parse_id, &decision)?;
            Ok(())
        },
    );

    log_operation_accepted(route, "parse_activation", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Handle `POST /parses/{parseId}/discard` (§34, `parse_discard`): DETACHED task.
/// `discard_held_parse` moves the run to `archiving` (no cutover barrier); the
/// task then drives the Ruling-1 `HeldSupersession` cleanup on the discarded run
/// itself, gating over its own pre_activation snapshot.
async fn post_discard(
    State(state): State<Arc<AppState>>,
    Extension(permit): Extension<MaintenancePermit>,
    DiagnosticPath(parse_id): DiagnosticPath<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/parses/{parseId}/discard";
    LogContext::current().record("parse_id", parse_id.as_str());
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let index_root = state.config.storage.index_root.clone();
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::ParseDiscard,
        "parse",
        parse_id.clone(),
    )
    .await?;

    let task_index_root = index_root.clone();
    spawn_admin_operation(
        permit,
        index_root,
        operation_id.clone(),
        "parse_discard",
        move || {
            // Source id resolved BEFORE the discard: the run→source binding is
            // immutable, and discard leaves the row present at `archiving`, so either
            // ordering reads the same source — resolving first keeps the cleanup
            // driver symmetric with accept.
            let source_id = lookup_source_id_for_parse(&task_index_root, &parse_id)?;
            crate::activation::discard_held_parse(&task_index_root, &parse_id)?;
            // Ruling 1: the discarded run's own pre_activation snapshot gates its
            // terminal archive-verify-delete. A gate failure halts/retains and
            // surfaces into the Operation row via the task's mark_failed — no retry.
            crate::restore::complete_superseded_parse(
                &task_index_root,
                &source_id,
                &parse_id,
                crate::restore::SupersededCleanupMode::HeldSupersession,
            )?;
            Ok(())
        },
    );

    log_operation_accepted(route, "parse_discard", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Request body for `POST /snapshots` (§30.6 manual/incident): whether to mint a
/// manual (default) or incident snapshot, plus optional operator attribution.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SnapshotRequest {
    /// True for an incident snapshot; absent/false mints a manual snapshot.
    #[serde(default)]
    incident: bool,
    #[serde(default)]
    created_by: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

/// Handle `POST /snapshots` (§34, `snapshot_creation`): DETACHED task. Mints a
/// corpus-wide manual or incident snapshot via `request_snapshot`.
async fn post_snapshots(
    State(state): State<Arc<AppState>>,
    Extension(permit): Extension<MaintenancePermit>,
    headers: HeaderMap,
    payload: Result<Json<SnapshotRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/snapshots";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let Json(request) = decode_body(route, payload, &started)?;
    let index_root = state.config.storage.index_root.clone();
    let snapshot_type = if request.incident {
        SnapshotType::Incident
    } else {
        SnapshotType::Manual
    };
    // Corpus-wide snapshot: the target is the corpus, not one object.
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::SnapshotCreation,
        "corpus",
        "corpus".to_string(),
    )
    .await?;

    let state_for_task = Arc::clone(&state);
    let task_index_root = index_root.clone();
    spawn_admin_operation(
        permit,
        index_root,
        operation_id.clone(),
        "snapshot_creation",
        move || {
            crate::snapshot::request_snapshot(
                &task_index_root,
                state_for_task.application_identity(),
                snapshot_type,
                request.created_by.as_deref(),
                request.notes.as_deref(),
            )
            .map(|_snapshot| ())
        },
    );

    log_operation_accepted(route, "snapshot_creation", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Request body for `POST /restore` (§31.3 rollback-as-restore): the source and
/// the archived parse to restore.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RestoreRequest {
    source_id: String,
    parse_id: String,
}

/// Handle `POST /restore` (§34, `restore`): DETACHED task. Completes an
/// operator rollback-as-restore (§31.3): restore the source's archived parse
/// from its snapshot, THEN clear `source_objects.deactivated_at` and append the
/// `source.reactivated` event. This completion contract — restore, then
/// reactivation, then event — runs through the shared
/// `deletion::restore_and_reactivate_source` path, so this route leaves the
/// SAME durable end state as the autonomous §11.4 caller. A `succeeded` restore
/// that only re-imported the parse without clearing the flag would strand the
/// source gated out of All-scope queries (the `deactivated_at IS NULL` filter).
/// Domain Err marks the Operation failed exactly as before; the barrier/publish
/// semantics live inside the restore and are unchanged.
async fn post_restore(
    State(state): State<Arc<AppState>>,
    Extension(permit): Extension<MaintenancePermit>,
    headers: HeaderMap,
    payload: Result<Json<RestoreRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<OperationAcceptedBody>), ApiError> {
    let route = "/restore";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let Json(request) = decode_body(route, payload, &started)?;
    let index_root = state.config.storage.index_root.clone();
    let source_id = request.source_id.clone();
    let parse_id = request.parse_id.clone();
    let operation_id = insert_pending_operation(
        index_root.clone(),
        OperationType::Restore,
        "source",
        source_id.clone(),
    )
    .await?;

    let state_for_task = Arc::clone(&state);
    let task_index_root = index_root.clone();
    spawn_admin_operation(
        permit,
        index_root,
        operation_id.clone(),
        "restore",
        move || {
            let dense_dimension = state_for_task.config.models.dense.dimension as usize;
            // One source of truth for the restore completion end state: this shared
            // path restores AND clears `deactivated_at` + appends `source.reactivated`,
            // so the HTTP rollback and the autonomous §11.4 reappearance leave the
            // same durable state. Domain Err propagates → spawn_admin_operation marks
            // the Operation failed (lifecycle unchanged).
            crate::deletion::restore_and_reactivate_source(
                &task_index_root,
                state_for_task.cutover_registry(),
                state_for_task.dense_cache(),
                dense_dimension,
                &source_id,
                &parse_id,
                "operator_rollback_restore",
            )
        },
    );

    log_operation_accepted(route, "restore", &operation_id, &started);
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAcceptedBody { operation_id }),
    ))
}

/// Handle `POST /shutdown` (§34, protected, extra-spec recorded additive): a
/// control action, NOT an async Operation. It returns an immediate confirmation
/// and signals the shutdown latch; there is no Operation row because shutting
/// down is a control signal, not tracked async work (distinct from the
/// async-operation routes). The §34.6 operationType enum has no shutdown value,
/// which corroborates the non-Operation reading.
async fn post_shutdown(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let route = "/shutdown";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    if let Err(error) = state.request_shutdown() {
        log_route_failed(route, "signaling", &error, &started);
        return Err(error);
    }
    info!(
        event = "http.route.result_ready",
        route,
        stage = "signaled",
        status = 202_u16,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "shutdown signalled"
    );
    Ok(StatusCode::ACCEPTED)
}

/// Complete the post-activation cleanup an `Activated` decision owes: the §31.2
/// predecessor supersession (`ActivationSupersession`) and the Ruling-1
/// held-supersession cleanup of each superseded held candidate. Runs inside the
/// detached activate/accept task AFTER the domain call returns (its cutover
/// barrier has released), mirroring the scheduler arms. A `Held` decision owes no
/// predecessor cleanup (nothing was activated) but may still have superseded held
/// candidates, so both arms feed the held-supersession loop. Any cleanup failure
/// propagates so the task records the operation `failed` (§31.2 halt/retain, no
/// auto-retry).
fn drive_activation_cleanup(
    index_root: &std::path::Path,
    source_id: &str,
    activated_parse_id: &str,
    decision: &crate::activation::ActivationDecision,
) -> Result<(), ApiError> {
    match decision {
        crate::activation::ActivationDecision::Activated {
            superseded_predecessor_id,
            superseded_held_ids,
        } => {
            if let Some(predecessor_id) = superseded_predecessor_id {
                crate::restore::complete_superseded_parse(
                    index_root,
                    source_id,
                    predecessor_id,
                    crate::restore::SupersededCleanupMode::ActivationSupersession {
                        activated_parse_id: activated_parse_id.to_owned(),
                    },
                )?;
            }
            clean_superseded_held(index_root, source_id, superseded_held_ids)?;
        }
        crate::activation::ActivationDecision::Held {
            superseded_held_ids,
            ..
        } => {
            clean_superseded_held(index_root, source_id, superseded_held_ids)?;
        }
    }
    Ok(())
}

/// Drive `HeldSupersession` cleanup over each superseded held candidate id
/// (Ruling 1), gating over each candidate's own pre_activation snapshot. Mirrors
/// the scheduler's post-barrier held cleanup. Normally empty or one id.
fn clean_superseded_held(
    index_root: &std::path::Path,
    source_id: &str,
    superseded_held_ids: &[String],
) -> Result<(), ApiError> {
    for held_id in superseded_held_ids {
        crate::restore::complete_superseded_parse(
            index_root,
            source_id,
            held_id,
            crate::restore::SupersededCleanupMode::HeldSupersession,
        )?;
    }
    Ok(())
}

/// Log the immediate acceptance of one async admin Operation.
fn log_operation_accepted(
    route: &'static str,
    operation_type: &'static str,
    operation_id: &str,
    started: &Instant,
) {
    info!(
        event = "operation.accepted",
        route,
        operation_type,
        operation_id = %operation_id,
        stage = "accepted",
        status = 202_u16,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "async admin operation accepted"
    );
}

/// Decode a JSON request body into `T`, normalizing the axum rejection and
/// logging a decode failure through the route-failure boundary.
fn decode_body<T>(
    route: &'static str,
    payload: Result<Json<T>, JsonRejection>,
    started: &Instant,
) -> Result<Json<T>, ApiError> {
    payload.map_err(|rejection| {
        let error = json_rejection_to_api_error(rejection);
        log_route_failed(route, "request_decoding", &error, started);
        error
    })
}

// --- Held-parse listing ----------------------------------------------------

/// Query params for `GET /parses`: only `status=held` is supported (the §13.4
/// held-disposition listing). Any other value is a client boundary.
#[derive(Debug, Deserialize)]
struct ParsesQuery {
    status: String,
}

/// SELECT every held parse (status `ready` with a held_reason), source-ordered
/// for deterministic listing. Held parses are the §13.4 disposition backlog.
const SELECT_HELD_PARSES_SQL: &str = "
SELECT id, source_id, parser_name, parser_version, parser_config_hash,
       capability_profile_hash, status, held_reason, conformance_report_json,
       started_at, completed_at, activated_at, archived_at,
       artifact_bundle_uri, artifact_bundle_hash, parser_raw_output_uri,
       warnings_json, metrics_json, error, created_at
FROM parse_runs
WHERE status = 'ready' AND held_reason IS NOT NULL
ORDER BY source_id, id";

/// Response envelope for `GET /parses?status=held`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HeldParsesResponse {
    parses: Vec<ParseRun>,
}

/// Handle `GET /parses?status=held` (§13.4, protected): list the held parses
/// awaiting disposition. Only `status=held` is accepted.
///
/// The query extractor is `Result`-wrapped so authorization runs BEFORE
/// validation: a missing/invalid `status` param must not leak a plain-text
/// framework 400 ahead of the bearer check. Auth-first, and every response on
/// this surface carries the standard JSON error envelope (query rejections are
/// mapped through the same boundary as body rejections).
async fn get_parses(
    State(state): State<Arc<AppState>>,
    params: Result<Query<ParsesQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Json<HeldParsesResponse>, ApiError> {
    let route = "/parses";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    // Only after successful auth do we examine the query extractor result, so a
    // malformed query is reported through the JSON envelope, not the framework's
    // plain-text pre-auth rejection.
    let Query(params) = params.map_err(|rejection| {
        let error = ApiError::BadRequest {
            message: rejection.body_text(),
        };
        log_route_failed(route, "validating", &error, &started);
        error
    })?;

    if params.status != "held" {
        let error = ApiError::BadRequest {
            message: format!(
                "GET /parses supports only status=held; got status={:?}",
                params.status
            ),
        };
        log_route_failed(route, "validating", &error, &started);
        return Err(error);
    }

    let index_root = state.config.storage.index_root.clone();
    let parses = spawn_blocking_with_context(move || read_held_parses(&index_root))
        .await
        .map_err(|join_error| ApiError::InternalIo {
            message: format!("held-parse listing task failed to join: {join_error}"),
        })
        .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
        .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;

    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        held_count = parses.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(HeldParsesResponse { parses }))
}

/// Read every held parse into the model shape on a blocking connection. The
/// column-string fields (status, held_reason, and the JSON payload columns) are
/// re-typed through their model shapes in `parse_run_from_row`, so a corrupt
/// persisted value fails loudly rather than being served wrong.
fn read_held_parses(index_root: &std::path::Path) -> Result<Vec<ParseRun>, ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let mut statement = connection
        .prepare(SELECT_HELD_PARSES_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare held-parse listing: {source}"),
        })?;
    let raw_rows = statement
        .query_map([], parse_run_row)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query held parses: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read held parse row: {source}"),
        })?;
    raw_rows.into_iter().map(parse_run_from_row).collect()
}

/// One parse_runs row as read from SQLite, before its enum/JSON columns are
/// re-typed into the ParseRun model shape.
struct ParseRunRow {
    id: String,
    source_id: String,
    parser_name: String,
    parser_version: String,
    parser_config_hash: String,
    capability_profile_hash: String,
    status: String,
    held_reason: Option<String>,
    conformance_report_json: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    activated_at: Option<String>,
    archived_at: Option<String>,
    artifact_bundle_uri: Option<String>,
    artifact_bundle_hash: Option<String>,
    parser_raw_output_uri: Option<String>,
    warnings_json: Option<String>,
    metrics_json: Option<String>,
    error: Option<String>,
    created_at: String,
}

/// rusqlite row projector for the held-parse listing SELECT (column order fixed
/// by `SELECT_HELD_PARSES_SQL`).
fn parse_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ParseRunRow> {
    Ok(ParseRunRow {
        id: row.get(0)?,
        source_id: row.get(1)?,
        parser_name: row.get(2)?,
        parser_version: row.get(3)?,
        parser_config_hash: row.get(4)?,
        capability_profile_hash: row.get(5)?,
        status: row.get(6)?,
        held_reason: row.get(7)?,
        conformance_report_json: row.get(8)?,
        started_at: row.get(9)?,
        completed_at: row.get(10)?,
        activated_at: row.get(11)?,
        archived_at: row.get(12)?,
        artifact_bundle_uri: row.get(13)?,
        artifact_bundle_hash: row.get(14)?,
        parser_raw_output_uri: row.get(15)?,
        warnings_json: row.get(16)?,
        metrics_json: row.get(17)?,
        error: row.get(18)?,
        created_at: row.get(19)?,
    })
}

/// Re-type one parse_runs row into a ParseRun. The status/held_reason wire
/// strings re-type through their model enums and the *_json columns through
/// their model shapes, so a corrupt persisted value fails loudly with the row's
/// identity rather than being served wrong.
fn parse_run_from_row(row: ParseRunRow) -> Result<ParseRun, ApiError> {
    let status: crate::model::ParseRunStatus = wire_value(&row.status, "parse run status")?;
    let held_reason = row
        .held_reason
        .map(|value| wire_value::<crate::model::ParseHeldReason>(&value, "parse held reason"))
        .transpose()?;
    let conformance_report = row
        .conformance_report_json
        .map(|json| {
            parse_json_column::<crate::model::ConformanceReport>(&json, "conformance report")
        })
        .transpose()?;
    let warnings = row
        .warnings_json
        .map(|json| parse_json_column::<Vec<crate::model::ParseWarning>>(&json, "parse warnings"))
        .transpose()?;
    let metrics = row
        .metrics_json
        .map(|json| parse_json_column::<crate::model::ParseMetrics>(&json, "parse metrics"))
        .transpose()?;
    Ok(ParseRun {
        id: row.id,
        source_id: row.source_id,
        parser_name: row.parser_name,
        parser_version: row.parser_version,
        parser_config_hash: row.parser_config_hash,
        capability_profile_hash: row.capability_profile_hash,
        status,
        held_reason,
        conformance_report,
        started_at: row.started_at,
        completed_at: row.completed_at,
        activated_at: row.activated_at,
        archived_at: row.archived_at,
        artifact_bundle_uri: row.artifact_bundle_uri,
        artifact_bundle_hash: row.artifact_bundle_hash,
        parser_raw_output_uri: row.parser_raw_output_uri,
        warnings,
        metrics,
        created_at: row.created_at,
        error: row.error,
    })
}

// --- Annotation vocabulary inspection (CA2 ruling 8) ------------------------
//
// The operator-facing surface for authoring the corpus-dependent policy
// rulesets (entity-match, annotator-naming): it exposes the entity names and
// relation predicates the annotation models actually produced. Protected (admin
// bearer), bounded reads with explicit truncation, and — unlike the projection
// builders — tolerant of imperfect data because it EXISTS to reveal anomalies
// (see the aggregation module banner, `crate::annotations::vocabulary`).

/// Query params for `GET /annotations/vocabulary`. `annotationType` is required
/// and MUST be `entity` or `relation` (the only two producers whose vocabulary
/// is authored against). `scope` defaults to `active` (annotations of each
/// source's active parse); `all` includes never-activated dry-run output.
///
/// Kept as raw strings (not enums) so an unknown value produces the standard
/// JSON 400 envelope in the handler, AFTER auth — a serde enum reject would leak
/// a plain-text framework 400 ahead of the bearer check (the same auth-first
/// discipline `ParsesQuery` documents).
#[derive(Debug, Deserialize)]
struct VocabularyQuery {
    #[serde(rename = "annotationType")]
    annotation_type: Option<String>,
    scope: Option<String>,
}

/// Response envelope for `GET /annotations/vocabulary?annotationType=entity`.
/// Sorted by normalized name ascending so spelling variants sit adjacent.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EntityVocabularyResponse {
    annotation_type: String,
    scope: String,
    groups: Vec<EntityVocabularyGroupDto>,
    /// Empty-marker rows (body exactly `[]`) skipped and counted, not grouped.
    skipped_marker_count: usize,
    /// Malformed rows counted, not grouped, never fatal (inspection reveals them).
    malformed_row_count: usize,
    /// True when the row-read cap OR the group cap clipped the result.
    truncated: bool,
    /// Rows read (bounded), for operator sizing of the truncation.
    rows_read: usize,
    /// Groups returned (== groups.len(), surfaced for quick reading).
    group_count: usize,
}

/// One entity vocabulary group in the response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EntityVocabularyGroupDto {
    normalized_name: String,
    raw_forms: Vec<RawFormDto>,
    entity_types: Vec<String>,
    source_count: usize,
    model_counts: Vec<ModelCountDto>,
    total_count: usize,
}

/// Response envelope for `GET /annotations/vocabulary?annotationType=relation`.
/// Sorted by predicate.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelationVocabularyResponse {
    annotation_type: String,
    scope: String,
    groups: Vec<RelationVocabularyGroupDto>,
    skipped_marker_count: usize,
    malformed_row_count: usize,
    truncated: bool,
    rows_read: usize,
    group_count: usize,
}

/// One relation (predicate) vocabulary group in the response. `predicate` is
/// the NORMALIZED group key; `raw_forms` carries the pre-normalization
/// predicate strings with counts, mirroring the entity group's raw-forms shape.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelationVocabularyGroupDto {
    predicate: String,
    raw_forms: Vec<RawFormDto>,
    total_count: usize,
    source_count: usize,
    model_counts: Vec<ModelCountDto>,
}

/// One raw form and its count within an entity group.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawFormDto {
    raw_form: String,
    count: usize,
}

/// One model name and its occurrence count within a group.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelCountDto {
    model_name: String,
    count: usize,
}

/// Either vocabulary response, so one handler serves both annotation types
/// through one auth/validation/spawn-blocking path. axum serializes the enum's
/// inner value transparently (untagged), so the wire shape is exactly the
/// entity or relation envelope with no wrapper.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum VocabularyResponse {
    Entity(EntityVocabularyResponse),
    Relation(RelationVocabularyResponse),
}

/// Handle `GET /annotations/vocabulary?annotationType=entity|relation&scope=active|all`
/// (CA2 ruling 8, PROTECTED). Auth-first: the query extractor is `Result`-wrapped
/// so a malformed query is reported through the JSON envelope, not the
/// framework's plain-text pre-auth rejection (same discipline as `get_parses`).
/// A missing/invalid `annotationType` is a 400; `scope` defaults to `active`.
/// The aggregation is SQLite work, so it runs in `spawn_blocking` and opens its
/// own read-only connection at the boundary (mirroring `get_parses`).
async fn get_annotation_vocabulary(
    State(state): State<Arc<AppState>>,
    params: Result<Query<VocabularyQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Json<VocabularyResponse>, ApiError> {
    let route = "/annotations/vocabulary";
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    // Only after auth do we examine the query extractor result (auth-first).
    let Query(params) = params.map_err(|rejection| {
        let error = ApiError::BadRequest {
            message: rejection.body_text(),
        };
        log_route_failed(route, "validating", &error, &started);
        error
    })?;

    // annotationType is required and closed to {entity, relation}.
    let annotation_type = match params.annotation_type.as_deref() {
        Some("entity") => crate::model::SemanticAnnotationType::Entity,
        Some("relation") => crate::model::SemanticAnnotationType::Relation,
        other => {
            let error = ApiError::BadRequest {
                message: format!(
                    "GET /annotations/vocabulary requires annotationType=entity|relation; got {other:?}"
                ),
            };
            log_route_failed(route, "validating", &error, &started);
            return Err(error);
        }
    };

    // scope defaults to active; only active|all are accepted.
    let scope = match params.scope.as_deref() {
        None | Some("active") => crate::annotations::vocabulary::VocabularyScope::Active,
        Some("all") => crate::annotations::vocabulary::VocabularyScope::All,
        Some(other) => {
            let error = ApiError::BadRequest {
                message: format!(
                    "GET /annotations/vocabulary scope must be active|all; got {other:?}"
                ),
            };
            log_route_failed(route, "validating", &error, &started);
            return Err(error);
        }
    };

    let index_root = state.config.storage.index_root.clone();
    let response =
        spawn_blocking_with_context(move || read_vocabulary(&index_root, annotation_type, scope))
            .await
            .map_err(|join_error| ApiError::InternalIo {
                message: format!("vocabulary aggregation task failed to join: {join_error}"),
            })
            .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
            .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;

    // Handler-boundary log: bounded facts only — NEVER vocabulary text. Scope,
    // type, group/row counts, skips, truncation, elapsed (CA2 ruling 8, item 4).
    let (group_count, rows_read, skipped, malformed, truncated) = match &response {
        VocabularyResponse::Entity(entity) => (
            entity.group_count,
            entity.rows_read,
            entity.skipped_marker_count,
            entity.malformed_row_count,
            entity.truncated,
        ),
        VocabularyResponse::Relation(relation) => (
            relation.group_count,
            relation.rows_read,
            relation.skipped_marker_count,
            relation.malformed_row_count,
            relation.truncated,
        ),
    };
    let annotation_type_wire = match annotation_type {
        crate::model::SemanticAnnotationType::Entity => "entity",
        crate::model::SemanticAnnotationType::Relation => "relation",
        // Unreachable: the match above admits only entity/relation.
        _ => "unknown",
    };
    let scope_wire = match scope {
        crate::annotations::vocabulary::VocabularyScope::Active => "active",
        crate::annotations::vocabulary::VocabularyScope::All => "all",
    };
    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        annotation_type = annotation_type_wire,
        scope = scope_wire,
        group_count,
        rows_read,
        skipped_marker_count = skipped,
        malformed_row_count = malformed,
        truncated,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(response))
}

/// Open a read-only connection at the boundary and aggregate the requested
/// vocabulary (mirrors `read_held_parses`: the read function owns `open_read`;
/// the handler runs it in `spawn_blocking`). Maps the aggregation module's typed
/// result into the camelCase wire DTOs.
fn read_vocabulary(
    index_root: &std::path::Path,
    annotation_type: crate::model::SemanticAnnotationType,
    scope: crate::annotations::vocabulary::VocabularyScope,
) -> Result<VocabularyResponse, ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let scope_wire = match scope {
        crate::annotations::vocabulary::VocabularyScope::Active => "active",
        crate::annotations::vocabulary::VocabularyScope::All => "all",
    }
    .to_owned();

    match annotation_type {
        crate::model::SemanticAnnotationType::Entity => {
            let vocabulary = crate::annotations::vocabulary::entity_vocabulary(&connection, scope)?;
            let groups: Vec<EntityVocabularyGroupDto> = vocabulary
                .groups
                .into_iter()
                .map(|group| EntityVocabularyGroupDto {
                    normalized_name: group.normalized_name,
                    raw_forms: group
                        .raw_forms
                        .into_iter()
                        .map(|raw| RawFormDto {
                            raw_form: raw.raw_form,
                            count: raw.count,
                        })
                        .collect(),
                    entity_types: group.entity_types,
                    source_count: group.source_count,
                    model_counts: group
                        .model_counts
                        .into_iter()
                        .map(|model| ModelCountDto {
                            model_name: model.model_name,
                            count: model.count,
                        })
                        .collect(),
                    total_count: group.total_count,
                })
                .collect();
            Ok(VocabularyResponse::Entity(EntityVocabularyResponse {
                annotation_type: "entity".to_owned(),
                scope: scope_wire,
                group_count: groups.len(),
                groups,
                skipped_marker_count: vocabulary.skipped_markers,
                malformed_row_count: vocabulary.malformed_rows,
                truncated: vocabulary.truncated,
                rows_read: vocabulary.rows_read,
            }))
        }
        crate::model::SemanticAnnotationType::Relation => {
            let vocabulary =
                crate::annotations::vocabulary::relation_vocabulary(&connection, scope)?;
            let groups: Vec<RelationVocabularyGroupDto> = vocabulary
                .groups
                .into_iter()
                .map(|group| RelationVocabularyGroupDto {
                    predicate: group.predicate,
                    raw_forms: group
                        .raw_forms
                        .into_iter()
                        .map(|raw| RawFormDto {
                            raw_form: raw.raw_form,
                            count: raw.count,
                        })
                        .collect(),
                    total_count: group.total_count,
                    source_count: group.source_count,
                    model_counts: group
                        .model_counts
                        .into_iter()
                        .map(|model| ModelCountDto {
                            model_name: model.model_name,
                            count: model.count,
                        })
                        .collect(),
                })
                .collect();
            Ok(VocabularyResponse::Relation(RelationVocabularyResponse {
                annotation_type: "relation".to_owned(),
                scope: scope_wire,
                group_count: groups.len(),
                groups,
                skipped_marker_count: vocabulary.skipped_markers,
                malformed_row_count: vocabulary.malformed_rows,
                truncated: vocabulary.truncated,
                rows_read: vocabulary.rows_read,
            }))
        }
        // The handler admits only entity/relation into this function; any other
        // type is a caller bug surfaced loudly rather than served empty.
        other => Err(ApiError::InternalIo {
            message: format!(
                "vocabulary aggregation received unsupported annotation type {other:?}"
            ),
        }),
    }
}

// --- Inspection read helpers -----------------------------------------------

/// SELECT one ContentUnit by id, GATED on §14: the row is returned ONLY when its
/// parse is the owning source's active parse (the correlated subselect on
/// source_objects.active_parse_id). A non-active/held parse's unit therefore
/// yields no row — the same as absence, which the handler maps to 404. Mirrors
/// the annotations-store active-parse subselect (store.rs:102).
const SELECT_ACTIVE_UNIT_SQL: &str = "
SELECT id, source_id, parse_id, content_type, body_hash, text_hash,
       structure_hash, primary_parent_id, sequence_index, locators_json,
       body_json, created_at
FROM content_units
WHERE id = ?1
  AND parse_id = (SELECT active_parse_id FROM source_objects WHERE id = content_units.source_id)";

/// Read one active-parse-gated ContentUnit into the model shape; `None` when no
/// such active unit exists (absent, or its parse is not active — §14).
fn read_active_unit(
    index_root: &std::path::Path,
    unit_id: &str,
) -> Result<Option<ContentUnit>, ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let row = connection
        .query_row(SELECT_ACTIVE_UNIT_SQL, params![unit_id], |row| {
            Ok(ContentUnitRow {
                id: row.get(0)?,
                source_id: row.get(1)?,
                parse_id: row.get(2)?,
                content_type: row.get(3)?,
                body_hash: row.get(4)?,
                text_hash: row.get(5)?,
                structure_hash: row.get(6)?,
                primary_parent_id: row.get(7)?,
                sequence_index: row.get(8)?,
                locators_json: row.get(9)?,
                body_json: row.get(10)?,
                created_at: row.get(11)?,
            })
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit {unit_id}: {source}"),
        })?;
    row.map(content_unit_from_row).transpose()
}

/// One content_units row before its typed columns are re-typed into ContentUnit.
struct ContentUnitRow {
    id: String,
    source_id: String,
    parse_id: String,
    content_type: String,
    body_hash: String,
    text_hash: Option<String>,
    structure_hash: Option<String>,
    primary_parent_id: Option<String>,
    sequence_index: Option<u64>,
    locators_json: Option<String>,
    body_json: String,
    created_at: String,
}

/// Re-type one content_units row into a ContentUnit. content_type re-types
/// through its enum and the JSON columns through their model shapes; a corrupt
/// value fails loudly. `deleted_at` is always None here (hot cleanup hard-deletes
/// units, never soft-deletes them — schema §15 comment).
fn content_unit_from_row(row: ContentUnitRow) -> Result<ContentUnit, ApiError> {
    let content_type: crate::model::ContentType =
        wire_value(&row.content_type, "content unit content_type")?;
    let locators = row
        .locators_json
        .map(|json| parse_json_column::<Vec<crate::model::Locator>>(&json, "unit locators"))
        .transpose()?;
    let body: serde_json::Value = parse_json_column(&row.body_json, "unit body")?;
    Ok(ContentUnit {
        id: row.id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        content_type,
        body_hash: row.body_hash,
        text_hash: row.text_hash,
        structure_hash: row.structure_hash,
        primary_parent_id: row.primary_parent_id,
        sequence_index: row.sequence_index,
        locators,
        body,
        created_at: row.created_at,
        deleted_at: None,
    })
}

/// Edge direction filter for the relationships inspection route.
#[derive(Debug, Clone, Copy)]
enum RelationshipDirection {
    Out,
    In,
    Both,
}

/// Parse the `direction` query param into the filter enum; an unknown value is a
/// client boundary (`BadRequest`). Absent means both directions.
fn parse_direction(value: Option<&str>) -> Result<RelationshipDirection, ApiError> {
    match value {
        None => Ok(RelationshipDirection::Both),
        Some("out") => Ok(RelationshipDirection::Out),
        Some("in") => Ok(RelationshipDirection::In),
        Some(other) => Err(ApiError::BadRequest {
            message: format!("direction must be 'out' or 'in'; got {other:?}"),
        }),
    }
}

/// SELECT structural relationships touching one unit within its ACTIVE parse
/// (§14 gate via the active_parse_id subselect on the anchor unit's source). The
/// `?2`/`?3` direction toggles and the `?4` type filter (NULL = any) let one
/// statement serve every direction/type combination. `parse_id` is bound to the
/// anchor unit's active parse so only active-parse edges are ever returned.
const SELECT_ACTIVE_UNIT_RELATIONSHIPS_SQL: &str = "
SELECT r.id, r.source_id, r.parse_id, r.from_unit_id, r.to_unit_id,
       r.relationship_type, r.relationship_role, r.sequence_index, r.confidence,
       r.provenance_json, r.created_at
FROM unit_relationships r
JOIN content_units anchor ON anchor.id = ?1
WHERE r.parse_id = (SELECT active_parse_id FROM source_objects WHERE id = anchor.source_id)
  AND r.parse_id = anchor.parse_id
  AND (
        (?2 AND r.from_unit_id = ?1)
     OR (?3 AND r.to_unit_id = ?1)
      )
  AND (?4 IS NULL OR r.relationship_type = ?4)
ORDER BY r.sequence_index, r.id";

/// Read the direction/type-filtered relationships of one active-parse unit.
/// Returns `None` when the anchor unit is not served under §14 (absent or its
/// parse is not active), so the handler maps that to 404 — matching the unit read.
fn read_active_unit_relationships(
    index_root: &std::path::Path,
    unit_id: &str,
    direction: RelationshipDirection,
    relationship_type: Option<&str>,
) -> Result<Option<Vec<UnitRelationship>>, ApiError> {
    // First confirm the anchor unit is itself served under §14; if it is not,
    // the relationships route 404s exactly like the unit route (no edges of a
    // non-active parse are ever exposed, even if edge rows exist).
    if read_active_unit(index_root, unit_id)?.is_none() {
        return Ok(None);
    }
    let connection = crate::hot_plane::open_read(index_root)?;
    let (include_out, include_in) = match direction {
        RelationshipDirection::Out => (true, false),
        RelationshipDirection::In => (false, true),
        RelationshipDirection::Both => (true, true),
    };
    let mut statement = connection
        .prepare(SELECT_ACTIVE_UNIT_RELATIONSHIPS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare unit relationships read: {source}"),
        })?;
    let raw_rows = statement
        .query_map(
            params![unit_id, include_out, include_in, relationship_type],
            unit_relationship_row,
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query relationships for unit {unit_id}: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read relationship row for unit {unit_id}: {source}"),
        })?;
    let relationships = raw_rows
        .into_iter()
        .map(unit_relationship_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(relationships))
}

/// One unit_relationships row before its typed columns are re-typed.
struct UnitRelationshipRow {
    id: String,
    source_id: String,
    parse_id: String,
    from_unit_id: String,
    to_unit_id: String,
    relationship_type: String,
    relationship_role: Option<String>,
    sequence_index: Option<u64>,
    confidence: Option<f64>,
    provenance_json: Option<String>,
    created_at: String,
}

/// rusqlite row projector for the relationships SELECT.
fn unit_relationship_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<UnitRelationshipRow> {
    Ok(UnitRelationshipRow {
        id: row.get(0)?,
        source_id: row.get(1)?,
        parse_id: row.get(2)?,
        from_unit_id: row.get(3)?,
        to_unit_id: row.get(4)?,
        relationship_type: row.get(5)?,
        relationship_role: row.get(6)?,
        sequence_index: row.get(7)?,
        confidence: row.get(8)?,
        provenance_json: row.get(9)?,
        created_at: row.get(10)?,
    })
}

/// Re-type one unit_relationships row into a UnitRelationship. `deleted_at` is
/// always None (relationships are hard-deleted by hot cleanup, never soft).
fn unit_relationship_from_row(row: UnitRelationshipRow) -> Result<UnitRelationship, ApiError> {
    let relationship_type: crate::model::UnitRelationshipType =
        wire_value(&row.relationship_type, "unit relationship_type")?;
    let provenance = row
        .provenance_json
        .map(|json| parse_json_column::<crate::model::Provenance>(&json, "relationship provenance"))
        .transpose()?;
    Ok(UnitRelationship {
        id: row.id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        from_unit_id: row.from_unit_id,
        to_unit_id: row.to_unit_id,
        relationship_type,
        relationship_role: row.relationship_role,
        sequence_index: row.sequence_index,
        confidence: row.confidence,
        provenance,
        created_at: row.created_at,
        deleted_at: None,
    })
}

/// SELECT one source_objects row by id.
const SELECT_SOURCE_OBJECT_SQL: &str = "
SELECT id, source_hash, active_parse_id, mime_type, size_bytes, storage_uri,
       event_time, ingest_time, created_at, deactivated_at
FROM source_objects
WHERE id = ?1";

/// SELECT every location of one source, ordered for deterministic listing.
const SELECT_SOURCE_LOCATIONS_SQL: &str = "
SELECT id, source_system, native_uri, native_id, governance_domain,
       first_seen_at, last_seen_at, status, deletion_evidence_json, metadata_json
FROM source_locations
WHERE source_id = ?1
ORDER BY source_system, native_uri";

/// Read one source with its full location set (§10 inspection). `None` when the
/// source id is absent. Locations carry freshness (last_seen_at) and status, so
/// the assembled SourceObject is the §10 inspection view.
fn read_source(
    index_root: &std::path::Path,
    source_id: &str,
) -> Result<Option<SourceObject>, ApiError> {
    let connection = crate::hot_plane::open_read(index_root)?;
    let object_row = connection
        .query_row(SELECT_SOURCE_OBJECT_SQL, params![source_id], |row| {
            Ok(SourceObjectRow {
                id: row.get(0)?,
                source_hash: row.get(1)?,
                active_parse_id: row.get(2)?,
                mime_type: row.get(3)?,
                size_bytes: row.get(4)?,
                storage_uri: row.get(5)?,
                event_time: row.get(6)?,
                ingest_time: row.get(7)?,
                created_at: row.get(8)?,
                deactivated_at: row.get(9)?,
            })
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read source {source_id}: {source}"),
        })?;
    let Some(object_row) = object_row else {
        return Ok(None);
    };

    let mut statement = connection
        .prepare(SELECT_SOURCE_LOCATIONS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare source locations read: {source}"),
        })?;
    let raw_locations = statement
        .query_map(params![source_id], source_location_row)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query locations for source {source_id}: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read location row for source {source_id}: {source}"),
        })?;
    let locations = raw_locations
        .into_iter()
        .map(source_location_from_row)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(SourceObject {
        id: object_row.id,
        active_parse_id: object_row.active_parse_id,
        mime_type: object_row.mime_type,
        size_bytes: object_row.size_bytes,
        source_hash: object_row.source_hash,
        storage_uri: object_row.storage_uri,
        locations,
        event_time: object_row.event_time,
        ingest_time: object_row.ingest_time,
        created_at: object_row.created_at,
        deactivated_at: object_row.deactivated_at,
    }))
}

/// One source_objects row before assembly into the SourceObject inspection view.
struct SourceObjectRow {
    id: String,
    source_hash: String,
    active_parse_id: Option<String>,
    mime_type: String,
    size_bytes: Option<u64>,
    storage_uri: String,
    event_time: Option<String>,
    ingest_time: String,
    created_at: String,
    deactivated_at: Option<String>,
}

/// One source_locations row before its typed columns are re-typed.
struct SourceLocationRow {
    id: String,
    source_system: String,
    native_uri: String,
    native_id: Option<String>,
    governance_domain: String,
    first_seen_at: String,
    last_seen_at: String,
    status: String,
    deletion_evidence_json: Option<String>,
    metadata_json: Option<String>,
}

/// rusqlite row projector for the locations SELECT.
fn source_location_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SourceLocationRow> {
    Ok(SourceLocationRow {
        id: row.get(0)?,
        source_system: row.get(1)?,
        native_uri: row.get(2)?,
        native_id: row.get(3)?,
        governance_domain: row.get(4)?,
        first_seen_at: row.get(5)?,
        last_seen_at: row.get(6)?,
        status: row.get(7)?,
        deletion_evidence_json: row.get(8)?,
        metadata_json: row.get(9)?,
    })
}

/// Re-type one source_locations row into a SourceLocation. status re-types
/// through its enum; the JSON columns through their model shapes.
fn source_location_from_row(row: SourceLocationRow) -> Result<SourceLocation, ApiError> {
    let status: SourceLocationStatus = wire_value(&row.status, "source location status")?;
    let deletion_evidence = row
        .deletion_evidence_json
        .map(|json| parse_json_column::<DeletionEvidence>(&json, "deletion evidence"))
        .transpose()?;
    let metadata = row
        .metadata_json
        .map(|json| {
            parse_json_column::<serde_json::Map<String, serde_json::Value>>(
                &json,
                "location metadata",
            )
        })
        .transpose()?;
    Ok(SourceLocation {
        id: row.id,
        source_system: row.source_system,
        native_uri: row.native_uri,
        native_id: row.native_id,
        governance_domain: row.governance_domain,
        first_seen_at: row.first_seen_at,
        last_seen_at: row.last_seen_at,
        status,
        deletion_evidence,
        metadata,
    })
}

/// Re-type one persisted wire string through its model enum (mirror of the
/// annotations-store `wire_value`): a corrupt value fails loudly with context.
fn wire_value<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> Result<T, ApiError> {
    serde_json::from_value(serde_json::Value::String(text.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!("persisted {what} value {text:?} is not a known variant: {source}"),
        }
    })
}

/// Parse one persisted *_json column back into its model shape (mirror of the
/// annotations-store `parse_json_column`): unparseable persisted state is a
/// surfaced corruption, never silently dropped.
fn parse_json_column<T: serde::de::DeserializeOwned>(
    json: &str,
    what: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
        message: format!("persisted {what} is unparseable: {source}"),
    })
}

// --- Operation polling -----------------------------------------------------

/// Handle `GET /operations/{operationId}` (§34.6, protected): read the Operation
/// row via the operations store; an absent id maps to a 404 (`NotFound`).
async fn get_operation(
    State(state): State<Arc<AppState>>,
    DiagnosticPath(operation_id): DiagnosticPath<String>,
    headers: HeaderMap,
) -> Result<Json<Operation>, ApiError> {
    let route = "/operations/{operationId}";
    LogContext::current().record("operation_id", operation_id.as_str());
    let started = log_route_started(route, "authorizing");
    authorize_request(&state, &headers, route, &started)?;

    let index_root = state.config.storage.index_root.clone();
    let lookup_id = operation_id.clone();
    let operation =
        spawn_blocking_with_context(move || crate::operations::get(&index_root, &lookup_id))
            .await
            .map_err(|join_error| ApiError::InternalIo {
                message: format!("operation read task failed to join: {join_error}"),
            })
            .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
            .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;
    let Some(operation) = operation else {
        let error = ApiError::NotFound {
            message: format!("no operation {operation_id}"),
        };
        log_route_failed(route, "reading", &error, &started);
        return Err(error);
    };

    debug!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        operation_id = %operation_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(operation))
}

// --- Inspection reads (public) ---------------------------------------------

/// Handle `GET /units/{unitId}` (§14, public): serve one ContentUnit ONLY when
/// the parse derived from its parse-scoped id (`<parseId>:unit:N`) is the owning
/// source's active parse. A non-active/held parse's unit is not queryable (§14),
/// which is indistinguishable from absence at the API — both map to 404.
async fn get_unit(
    State(state): State<Arc<AppState>>,
    DiagnosticPath(unit_id): DiagnosticPath<String>,
) -> Result<Json<ContentUnit>, ApiError> {
    let route = "/units/{unitId}";
    LogContext::current().record("target_object_type", "unit");
    LogContext::current().record("target_object_id", unit_id.as_str());
    let started = log_route_started(route, "reading");

    let index_root = state.config.storage.index_root.clone();
    let lookup_id = unit_id.clone();
    let unit = spawn_blocking_with_context(move || read_active_unit(&index_root, &lookup_id))
        .await
        .map_err(|join_error| ApiError::InternalIo {
            message: format!("unit read task failed to join: {join_error}"),
        })
        .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
        .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;
    let Some(unit) = unit else {
        return Err(not_found_logged(
            route,
            "reading",
            format!("no active unit {unit_id}"),
            &started,
        ));
    };

    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        unit_id = %unit_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(unit))
}

/// Query params for `GET /units/{unitId}/relationships`: optional direction and
/// relationship-type filters.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RelationshipsQuery {
    /// `out` (from this unit), `in` (to this unit), or absent (both).
    #[serde(default)]
    direction: Option<String>,
    /// Restrict to one relationship type wire name (e.g. `contains`).
    #[serde(default)]
    relationship_type: Option<String>,
}

/// Response envelope for `GET /units/{unitId}/relationships`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelationshipsResponse {
    relationships: Vec<UnitRelationship>,
}

/// Handle `GET /units/{unitId}/relationships` (§14/§19, public): the structural
/// edges touching one unit, direction/type-filtered, served ONLY when the derived
/// parse is active on its source (same §14 gate as the unit read).
async fn get_unit_relationships(
    State(state): State<Arc<AppState>>,
    DiagnosticPath(unit_id): DiagnosticPath<String>,
    DiagnosticQuery(params): DiagnosticQuery<RelationshipsQuery>,
) -> Result<Json<RelationshipsResponse>, ApiError> {
    let route = "/units/{unitId}/relationships";
    LogContext::current().record("target_object_type", "unit");
    LogContext::current().record("target_object_id", unit_id.as_str());
    let started = log_route_started(route, "reading");

    let direction = match parse_direction(params.direction.as_deref()) {
        Ok(direction) => direction,
        Err(error) => {
            log_route_failed(route, "validating", &error, &started);
            return Err(error);
        }
    };
    let index_root = state.config.storage.index_root.clone();
    let lookup_id = unit_id.clone();
    let relationship_type = params.relationship_type.clone();
    let relationships = spawn_blocking_with_context(move || {
        read_active_unit_relationships(
            &index_root,
            &lookup_id,
            direction,
            relationship_type.as_deref(),
        )
    })
    .await
    .map_err(|join_error| ApiError::InternalIo {
        message: format!("relationships read task failed to join: {join_error}"),
    })
    .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
    .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;
    let Some(relationships) = relationships else {
        return Err(not_found_logged(
            route,
            "reading",
            format!("no active unit {unit_id}"),
            &started,
        ));
    };

    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        unit_id = %unit_id,
        relationship_count = relationships.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(RelationshipsResponse { relationships }))
}

/// Handle `GET /sources/{sourceId}` (§10, public): the source with its full
/// location set and freshness. An absent source is a 404.
async fn get_source(
    State(state): State<Arc<AppState>>,
    DiagnosticPath(source_id): DiagnosticPath<String>,
) -> Result<Json<SourceObject>, ApiError> {
    let route = "/sources/{sourceId}";
    LogContext::current().record("source_id", source_id.as_str());
    let started = log_route_started(route, "reading");

    let index_root = state.config.storage.index_root.clone();
    let lookup_id = source_id.clone();
    let source = spawn_blocking_with_context(move || read_source(&index_root, &lookup_id))
        .await
        .map_err(|join_error| ApiError::InternalIo {
            message: format!("source read task failed to join: {join_error}"),
        })
        .inspect_err(|error| log_route_failed(route, "blocking_join", error, &started))?
        .inspect_err(|error| log_route_failed(route, "storage_read", error, &started))?;
    let Some(source) = source else {
        return Err(not_found_logged(
            route,
            "reading",
            format!("no source {source_id}"),
            &started,
        ));
    };

    info!(
        event = "http.route.result_ready",
        route,
        stage = "result_ready",
        status = 200_u16,
        source_id = %source_id,
        location_count = source.locations.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Ok(Json(source))
}

/// Public projection of the published `SyncHealth` snapshot for `GET
/// /sync/status` (§9.5–§9.6). The internal `SyncHealth` is not `Serialize`, so
/// its operator-facing fields are projected into this camelCase DTO.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncStatusResponse {
    fabric_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    pending: u64,
    in_flight: u64,
    failed: u64,
    coalesced_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cadence_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_success_at: Option<String>,
}

impl From<SyncHealth> for SyncStatusResponse {
    fn from(health: SyncHealth) -> Self {
        Self {
            fabric_ready: health.fabric_ready,
            detail: health.detail,
            pending: health.pending,
            in_flight: health.in_flight,
            failed: health.failed,
            coalesced_total: health.coalesced_total,
            cadence_ms: health.cadence_ms,
            last_success_at: health.last_success_at,
        }
    }
}

/// Handle `GET /sync/status` (§9.5–§9.6, public): the last-published sync
/// scheduler health snapshot.
async fn get_sync_status(State(state): State<Arc<AppState>>) -> Json<SyncStatusResponse> {
    let started = log_route_started("/sync/status", "reading");
    let response = SyncStatusResponse::from(state.sync_health_snapshot());
    debug!(
        event = "http.route.result_ready",
        route = "/sync/status",
        stage = "result_ready",
        status = 200_u16,
        fabric_ready = response.fabric_ready,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "HTTP route result ready"
    );
    Json(response)
}

/// Build and log a `NotFound` (404) for an inspection read.
fn not_found_logged(
    route: &'static str,
    stage: &'static str,
    message: String,
    started: &Instant,
) -> ApiError {
    let error = ApiError::NotFound { message };
    log_route_failed(route, stage, &error, started);
    error
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
