use std::sync::Arc;

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
    source::resolve_source_reference,
    state::AppState,
    storage::UnitDenseVector,
    types::{
        HealthResponse, IngestRequest, IngestResponse, SearchRequest, SearchResponse,
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

/// Run source-resolution, Docling conversion, and unit splitting for one ingest request.
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
