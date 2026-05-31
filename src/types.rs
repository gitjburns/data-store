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

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub source: String,
    pub options: Option<IngestOptions>,
}

#[derive(Debug, Deserialize)]
pub struct IngestOptions {
    #[serde(rename = "pdfBackend")]
    pub pdf_backend: Option<String>,

    #[serde(rename = "ocrMode")]
    pub ocr_mode: Option<String>,

    #[serde(rename = "pageBatchSize")]
    pub page_batch_size: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    #[serde(rename = "documentId")]
    pub document_id: String,

    #[serde(rename = "unitsIngested")]
    pub units_ingested: u32,

    pub status: String,
}

#[derive(Debug, Deserialize)]
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
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.source.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "source must be a non-empty corpus-relative reference".to_string(),
            });
        }

        if let Some(options) = &self.options {
            options.validate()?;
        }

        Ok(())
    }
}

impl IngestOptions {
    /// Validate optional ingest settings that are known at the HTTP boundary.
    fn validate(&self) -> Result<(), ApiError> {
        if let Some(pdf_backend) = &self.pdf_backend {
            require_non_empty("options.pdfBackend", pdf_backend)?;
        }

        if let Some(ocr_mode) = &self.ocr_mode {
            require_non_empty("options.ocrMode", ocr_mode)?;
        }

        if self.page_batch_size == Some(0) {
            return Err(ApiError::BadRequest {
                message: "options.pageBatchSize must be greater than zero".to_string(),
            });
        }

        Ok(())
    }
}

impl SearchRequest {
    /// Validate search request fields against configured API limits.
    pub fn validate(&self, max_top_k: u32) -> Result<(), ApiError> {
        if self.query.trim().is_empty() {
            return Err(ApiError::BadRequest {
                message: "query must be non-empty".to_string(),
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

/// Ensure an optional request string is not blank when present.
fn require_non_empty(label: &str, value: &str) -> Result<(), ApiError> {
    if !value.trim().is_empty() {
        return Ok(());
    }

    Err(ApiError::BadRequest {
        message: format!("{label} must be non-empty"),
    })
}
