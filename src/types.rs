use serde::{Deserialize, Serialize};

use crate::error::{ApiError, OperationErrorDetail};

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
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentVersionRollbackRequest {
    pub source: String,

    #[serde(rename = "versionLabel")]
    pub version_label: String,
}

#[derive(Debug, Serialize)]
pub struct DocumentVersionRollbackResponse {
    #[serde(rename = "sourcePath")]
    pub source_path: String,

    #[serde(rename = "activeVersionLabel")]
    pub active_version_label: String,

    #[serde(rename = "publishedAtMs")]
    pub published_at_ms: u64,

    #[serde(rename = "vectorCount")]
    pub vector_count: usize,

    pub status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    #[serde(rename = "operationId")]
    pub operation_id: Option<String>,

    pub operation: String,

    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum OperationEvent {
    #[serde(rename = "status")]
    Status {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "progress")]
    #[allow(dead_code)]
    Progress {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        current: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total: Option<u64>,
    },
    #[serde(rename = "result")]
    Result {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        payload: serde_json::Value,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(rename = "operationId")]
        operation_id: String,
        sequence: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        error: OperationErrorDetail,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationControlRequest {
    #[serde(rename = "type")]
    pub control_type: String,
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

impl DocumentVersionRollbackRequest {
    /// Validate rollback target fields before the admin operation mutates active-version state.
    pub fn validate(&self, max_source_chars: u32) -> Result<(), ApiError> {
        if self.source.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "source must be a non-empty corpus-relative reference".to_string(),
            });
        }

        if self.source.trim() != self.source {
            return Err(ApiError::BadRequest {
                message: "source must not contain leading or trailing whitespace".to_string(),
            });
        }

        if char_count_exceeds(&self.source, max_source_chars) {
            return Err(ApiError::BadRequest {
                message: format!("source must be at most {max_source_chars} characters"),
            });
        }

        if self.version_label.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "versionLabel must be non-empty".to_string(),
            });
        }

        if self.version_label.trim() != self.version_label {
            return Err(ApiError::BadRequest {
                message: "versionLabel must not contain leading or trailing whitespace".to_string(),
            });
        }

        Ok(())
    }
}

/// Check a character limit without counting an entire hostile string when the limit is already exceeded.
fn char_count_exceeds(value: &str, max_chars: u32) -> bool {
    value.chars().take(max_chars as usize + 1).count() > max_chars as usize
}
