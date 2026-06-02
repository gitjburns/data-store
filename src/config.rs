use std::{env, fs, net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::error::ApiError;

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub server: ServerConfig,
    pub inference: InferenceConfig,
    pub storage: StorageConfig,
    pub docling: DoclingConfig,
    pub models: ModelConfig,
    pub retrieval: RetrievalConfig,
}

#[derive(Debug, Clone)]
pub struct CliOptions {
    pub config_path: PathBuf,
    pub smoke_dense: bool,
    pub setup_storage: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind_address: SocketAddr,
    pub max_request_body_bytes: usize,
    pub max_ingest_source_chars: u32,
    pub max_search_query_chars: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InferenceConfig {
    pub device: InferenceDeviceKind,
    pub device_index: usize,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDeviceKind {
    Cuda,
    Metal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub corpus_root: PathBuf,
    pub index_root: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DoclingConfig {
    pub python_path: PathBuf,
    pub docling_path: PathBuf,
    pub default_pdf_backend: String,
    pub default_ocr_mode: String,
    pub page_batch_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    pub dense: DenseModelConfig,
    pub colbert: ColbertModelConfig,
    pub reranker: RerankerModelConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DenseModelConfig {
    pub path: PathBuf,
    pub dimension: u32,
    pub max_tokens: u32,
    pub pooling: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColbertModelConfig {
    pub path: PathBuf,
    pub dimension: u32,
    pub query_max_tokens: u32,
    pub document_max_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RerankerModelConfig {
    pub path: PathBuf,
    pub max_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetrievalConfig {
    pub default_top_k: u32,
    pub max_top_k: u32,
    pub rrf_k: u32,
    pub candidate_overfetch_multiplier: u32,
    #[serde(default = "default_colbert_candidate_pool_size")]
    pub colbert_candidate_pool_size: u32,
    pub min_search_unit_chars: u32,
    pub max_unit_tokens: u32,
}

impl ServiceConfig {
    /// Load and validate service configuration from one TOML file.
    pub fn load(path: PathBuf) -> Result<Self, ApiError> {
        let raw = fs::read_to_string(&path).map_err(|source| ApiError::ConfigRead {
            path: path.clone(),
            source,
        })?;
        let config: Self = toml::from_str(&raw).map_err(|source| ApiError::ConfigParse {
            path: path.clone(),
            source,
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Return the configured socket address for the HTTP server.
    pub fn bind_address(&self) -> SocketAddr {
        self.server.bind_address
    }

    /// Validate cross-field config invariants that TOML deserialization cannot express.
    fn validate(&self) -> Result<(), ApiError> {
        require_positive_usize(
            "server.max_request_body_bytes",
            self.server.max_request_body_bytes,
        )?;
        require_positive(
            "server.max_ingest_source_chars",
            self.server.max_ingest_source_chars,
        )?;
        require_positive(
            "server.max_search_query_chars",
            self.server.max_search_query_chars,
        )?;
        require_absolute_path("storage.corpus_root", &self.storage.corpus_root)?;
        require_absolute_path("storage.index_root", &self.storage.index_root)?;
        require_absolute_path("docling.python_path", &self.docling.python_path)?;
        require_absolute_path("docling.docling_path", &self.docling.docling_path)?;
        require_absolute_path("models.dense.path", &self.models.dense.path)?;
        require_absolute_path("models.colbert.path", &self.models.colbert.path)?;
        require_absolute_path("models.reranker.path", &self.models.reranker.path)?;

        require_non_empty(
            "docling.default_pdf_backend",
            &self.docling.default_pdf_backend,
        )?;
        require_non_empty("docling.default_ocr_mode", &self.docling.default_ocr_mode)?;
        if !matches!(self.docling.default_ocr_mode.trim(), "auto" | "on" | "off") {
            return Err(ApiError::InvalidConfig {
                message: "docling.default_ocr_mode must be one of auto, on, or off".to_string(),
            });
        }
        if self.docling.page_batch_size == Some(0) {
            return Err(ApiError::InvalidConfig {
                message: "docling.page_batch_size must be greater than zero when set".to_string(),
            });
        }
        require_non_empty("models.dense.pooling", &self.models.dense.pooling)?;
        require_positive("models.dense.dimension", self.models.dense.dimension)?;
        require_positive("models.dense.max_tokens", self.models.dense.max_tokens)?;
        require_positive("models.colbert.dimension", self.models.colbert.dimension)?;
        require_positive(
            "models.colbert.query_max_tokens",
            self.models.colbert.query_max_tokens,
        )?;
        require_positive(
            "models.colbert.document_max_tokens",
            self.models.colbert.document_max_tokens,
        )?;
        require_positive(
            "models.reranker.max_tokens",
            self.models.reranker.max_tokens,
        )?;
        require_positive("retrieval.default_top_k", self.retrieval.default_top_k)?;
        require_positive("retrieval.max_top_k", self.retrieval.max_top_k)?;
        require_positive("retrieval.rrf_k", self.retrieval.rrf_k)?;
        require_positive(
            "retrieval.candidate_overfetch_multiplier",
            self.retrieval.candidate_overfetch_multiplier,
        )?;
        require_positive(
            "retrieval.colbert_candidate_pool_size",
            self.retrieval.colbert_candidate_pool_size,
        )?;
        acknowledge_non_negative(
            "retrieval.min_search_unit_chars",
            self.retrieval.min_search_unit_chars,
        )?;
        require_positive("retrieval.max_unit_tokens", self.retrieval.max_unit_tokens)?;

        if self.retrieval.default_top_k > self.retrieval.max_top_k {
            return Err(ApiError::InvalidConfig {
                message:
                    "retrieval.default_top_k must be less than or equal to retrieval.max_top_k"
                        .to_string(),
            });
        }
        if self.retrieval.colbert_candidate_pool_size < self.retrieval.max_top_k {
            return Err(ApiError::InvalidConfig {
                message:
                    "retrieval.colbert_candidate_pool_size must be greater than or equal to retrieval.max_top_k"
                        .to_string(),
            });
        }

        Ok(())
    }
}

/// Return the Phase 11F default bounded ColBERT reranking pool size.
fn default_colbert_candidate_pool_size() -> u32 {
    100
}

/// Resolve supported CLI options, falling back to `config.toml`.
pub fn resolve_cli_options_from_args() -> Result<CliOptions, ApiError> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from("config.toml");
    let mut smoke_dense = false;
    let mut setup_storage = false;

    while let Some(arg) = args.next() {
        if arg == "--config" {
            let Some(value) = args.next() else {
                return Err(ApiError::InvalidCli {
                    message: "--config requires a path".to_string(),
                });
            };
            config_path = PathBuf::from(value);
            continue;
        }

        if arg == "--smoke-dense" {
            smoke_dense = true;
            continue;
        }

        if arg == "--setup-storage" {
            setup_storage = true;
            continue;
        }

        return Err(ApiError::InvalidCli {
            message: format!("unknown argument: {arg}"),
        });
    }

    Ok(CliOptions {
        config_path,
        smoke_dense,
        setup_storage,
    })
}

/// Ensure a path field uses an absolute path.
fn require_absolute_path(label: &str, path: &PathBuf) -> Result<(), ApiError> {
    if path.is_absolute() {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be an absolute path"),
    })
}

/// Ensure a string field is non-empty after trimming.
fn require_non_empty(label: &str, value: &str) -> Result<(), ApiError> {
    if !value.trim().is_empty() {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be a non-empty string"),
    })
}

/// Ensure a numeric field is greater than zero.
fn require_positive(label: &str, value: u32) -> Result<(), ApiError> {
    if value > 0 {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be greater than zero"),
    })
}

/// Ensure a usize numeric field is greater than zero.
fn require_positive_usize(label: &str, value: usize) -> Result<(), ApiError> {
    if value > 0 {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be greater than zero"),
    })
}

/// Make intentionally non-negative u32 fields part of validation and readiness.
fn acknowledge_non_negative(_label: &str, _value: u32) -> Result<(), ApiError> {
    Ok(())
}
