use serde::{Deserialize, Serialize};

use crate::error::ApiError;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub service: String,
    pub ready: bool,
    pub components: Vec<HealthComponent>,
}

#[derive(Debug, Serialize)]
pub struct HealthComponent {
    pub name: String,
    pub ready: bool,
    pub details: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LimitsResponse {
    pub request: RequestLimitsResponse,
    pub retrieval: RetrievalLimitsResponse,
}

#[derive(Debug, Serialize)]
pub struct RequestLimitsResponse {
    #[serde(rename = "maxRequestBodyBytes")]
    pub max_request_body_bytes: usize,

    #[serde(rename = "maxIngestSourceChars")]
    pub max_ingest_source_chars: u32,

    #[serde(rename = "maxSearchQueryChars")]
    pub max_search_query_chars: u32,
}

#[derive(Debug, Serialize)]
pub struct RetrievalLimitsResponse {
    #[serde(rename = "defaultTopK")]
    pub default_top_k: u32,

    #[serde(rename = "maxTopK")]
    pub max_top_k: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestRequest {
    pub source: String,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    #[serde(rename = "documentId")]
    pub document_id: String,

    #[serde(rename = "versionLabel")]
    pub version_label: String,

    #[serde(rename = "unitsIngested")]
    pub units_ingested: u32,

    pub status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub query: String,

    #[serde(rename = "topK")]
    pub top_k: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,

    #[serde(rename = "latencyMs")]
    pub latency_ms: u64,

    pub raw: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    #[serde(rename = "unitId")]
    pub unit_id: String,

    pub score: f32,
    pub content: String,

    #[serde(rename = "headingPath")]
    pub heading_path: Vec<String>,

    #[serde(rename = "sourcePath")]
    pub source_path: String,

    #[serde(rename = "pageNumbers")]
    pub page_numbers: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub struct ShutdownResponse {
    pub status: String,
}

impl IngestRequest {
    /// Validate ingest request fields before the runtime pipeline consumes them.
    pub fn validate(&self, max_source_chars: u32) -> Result<(), ApiError> {
        if self.source.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "source must be a non-empty corpus-relative reference".to_string(),
            });
        }

        if char_count_exceeds(&self.source, max_source_chars) {
            return Err(ApiError::BadRequest {
                message: format!("source must be at most {max_source_chars} characters"),
            });
        }

        Ok(())
    }
}

impl SearchRequest {
    /// Validate search request fields against configured API limits.
    pub fn validate(&self, max_query_chars: u32, max_top_k: u32) -> Result<(), ApiError> {
        if self.query.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "query must be non-empty".to_string(),
            });
        }

        if char_count_exceeds(&self.query, max_query_chars) {
            return Err(ApiError::BadRequest {
                message: format!("query must be at most {max_query_chars} characters"),
            });
        }

        if let Some(top_k) = self.top_k {
            if top_k == 0 || top_k > max_top_k {
                return Err(ApiError::BadRequest {
                    message: format!("topK must be between 1 and {max_top_k}"),
                });
            }
        }

        Ok(())
    }
}

/// Check a character limit without counting an entire hostile string when the limit is already exceeded.
fn char_count_exceeds(value: &str, max_chars: u32) -> bool {
    value.chars().take(max_chars as usize + 1).count() > max_chars as usize
}
