use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};

use crate::{
    error::ApiError,
    state::AppState,
    types::{HealthResponse, IngestRequest, IngestResponse, SearchRequest, SearchResponse},
};

/// Build the Axum router for the versioned HTTP API.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/ingest", post(post_ingest))
        .route("/v1/search", post(post_search))
        .with_state(state)
}

/// Return service readiness and startup diagnostics.
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(state.health())
}

/// Validate an ingest request and report that ingest is not implemented yet.
async fn post_ingest(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    request.validate()?;

    Err(ApiError::NotImplemented {
        message: "document ingestion is not implemented in this scaffold".to_string(),
    })
}

/// Validate a search request and report that retrieval is not implemented yet.
async fn post_search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    request.validate(state.config.retrieval.max_top_k)?;

    Err(ApiError::NotImplemented {
        message: "document retrieval is not implemented in this scaffold".to_string(),
    })
}
