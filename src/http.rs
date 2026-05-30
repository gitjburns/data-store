use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use tracing::info;

use crate::{
    docling::convert_source_to_markdown,
    error::ApiError,
    source::resolve_source_reference,
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

/// Run the Phase 5 source-resolution and Docling conversion path for one ingest request.
async fn post_ingest(
    State(state): State<Arc<AppState>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    request.validate()?;
    let source = resolve_source_reference(&state.config.storage, &request.source)?;
    let conversion = convert_source_to_markdown(
        &state.config.docling,
        &state.config.storage.index_root,
        &request,
        source,
    )
    .await?;

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
        "Docling conversion completed; unit splitting and indexing are not implemented yet"
    );

    Err(ApiError::NotImplemented {
        message: format!(
            "Docling conversion succeeded for {}; unit splitting and indexing are not implemented yet",
            conversion.source.relative_path.display()
        ),
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
