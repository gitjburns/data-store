use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::error::ApiError;

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    /// HTTP bind address, request limits, and synchronous admission limits.
    pub server: ServerConfig,
    /// File-backed service logging settings used after bootstrap stdout output.
    pub logging: LoggingConfig,
    /// Startup-scoped admin credential handoff settings.
    pub admin: AdminConfig,
    /// Accelerator selection for all model runtimes.
    pub inference: InferenceConfig,
    /// Corpus and durable index/artifact paths owned by the service.
    pub storage: StorageConfig,
    /// Explicit Docling executable and PDF conversion controls.
    pub docling: DoclingConfig,
    /// Local model artifact locations and runtime shape limits.
    pub models: ModelConfig,
    /// Retrieval ranking, candidate-pool, and unit-sizing parameters.
    pub retrieval: RetrievalConfig,
}

#[derive(Debug, Clone)]
pub struct CliOptions {
    /// TOML config path supplied on the command line or defaulted to config.toml.
    pub config_path: PathBuf,
    /// Run inference readiness smoke checks without binding HTTP.
    pub smoke_dense: bool,
    /// Create or validate the development SQLite schema through the explicit setup path.
    pub setup_storage: bool,
    /// Keep the HTTP service attached to the current terminal instead of daemonizing.
    pub foreground: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Socket address where Axum binds the standalone service.
    pub bind_address: SocketAddr,
    /// HTTP body limit applied before request JSON is accepted.
    pub max_request_body_bytes: usize,
    /// Maximum length of an ingest source reference after JSON parsing.
    pub max_ingest_source_chars: u32,
    /// Maximum length of a search query after JSON parsing.
    pub max_search_query_chars: u32,
    /// Non-queueing limit for concurrent synchronous ingest operations.
    pub max_in_flight_ingest: u32,
    /// Non-queueing limit for concurrent synchronous search operations.
    pub max_in_flight_search: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Service log file path; relative paths are resolved against the Rust service root.
    pub file_path: PathBuf,
    /// Minimum event level written to the service log file.
    pub level: LoggingLevel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdminConfig {
    /// Runtime file where the service writes the current startup-scoped admin bearer token.
    pub token_file_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingLevel {
    /// Include every tracing event, including very verbose diagnostics.
    Trace,
    /// Include debug, info, warning, and error events.
    Debug,
    /// Include normal operational events, warnings, and errors.
    Info,
    /// Include warnings and errors only.
    Warn,
    /// Include errors only.
    Error,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InferenceConfig {
    /// Explicit accelerator backend. CPU fallback is intentionally unsupported.
    pub device: InferenceDeviceKind,
    /// Device index passed to the selected accelerator backend.
    pub device_index: usize,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDeviceKind {
    /// NVIDIA CUDA backend selected by the cuda Cargo feature.
    Cuda,
    /// Apple Silicon Metal backend selected by the metal Cargo feature.
    Metal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Root directory for corpus-relative source references.
    pub corpus_root: PathBuf,
    /// Service-owned root for SQLite storage and generated conversion artifacts.
    pub index_root: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DoclingConfig {
    /// Python executable recorded for the configured Docling environment.
    pub python_path: PathBuf,
    /// Docling executable launched directly for PDF conversion.
    pub docling_path: PathBuf,
    /// Per-document Docling conversion timeout in seconds.
    pub document_timeout_seconds: u64,
    /// PDF backend selected by service config rather than callers.
    pub pdf_backend: String,
    /// OCR behavior selected by service config: auto, on, or off.
    pub ocr_mode: String,
    /// Accelerator device selected for Docling's Python-side processing.
    pub device: String,
    /// Worker thread count passed to Docling.
    pub num_threads: u32,
    /// Docling page batch size for conversion resource control.
    pub page_batch_size: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    /// Dense embedding model configuration.
    pub dense: DenseModelConfig,
    /// ColBERT late-interaction model configuration.
    pub colbert: ColbertModelConfig,
    /// ModernBERT sequence-classification reranker model configuration.
    pub reranker: RerankerModelConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DenseModelConfig {
    /// Local model artifact directory for Qwen3 dense embeddings.
    pub path: PathBuf,
    /// Expected dense vector width.
    pub dimension: u32,
    /// Runtime token cap for dense embedding inputs.
    pub max_tokens: u32,
    /// Pooling contract validated by the service-local dense adapter.
    pub pooling: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColbertModelConfig {
    /// Local model artifact directory for ColBERT-Zero.
    pub path: PathBuf,
    /// Expected ColBERT token-vector width.
    pub dimension: u32,
    /// Runtime token cap for ColBERT query inputs.
    pub query_max_tokens: u32,
    /// Runtime token cap for ColBERT document/unit inputs.
    pub document_max_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RerankerModelConfig {
    /// Explicit reranker backend selection. There is no fallback between backends.
    pub backend: RerankerBackendKind,
    /// Local model artifact directory for ModernBERT sequence-classification
    /// reranking. Required when backend = "local"; forbidden otherwise.
    pub path: Option<PathBuf>,
    /// Runtime token cap for reranker query/document pairs. Required when
    /// backend = "local"; forbidden otherwise.
    pub max_tokens: Option<u32>,
    /// Cohere-compatible rerank endpoint URL. Required when backend = "http";
    /// forbidden otherwise.
    pub endpoint: Option<String>,
    /// Model name sent in HTTP rerank requests. Required when backend = "http";
    /// forbidden otherwise.
    pub model: Option<String>,
    /// HTTP rerank request timeout in seconds. Required when backend = "http";
    /// forbidden otherwise.
    pub timeout_seconds: Option<u64>,
    /// Optional owner-only file holding the HTTP rerank API key. Allowed only
    /// when backend = "http".
    pub api_key_file_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RerankerBackendKind {
    /// In-process Candle ModernBERT sequence-classification runtime.
    Local,
    /// HTTP client speaking the Cohere-compatible rerank contract.
    Http,
}

impl RerankerModelConfig {
    /// Return the local model artifact directory. Config validation guarantees
    /// presence for the local backend; the http backend has no local path.
    pub fn local_path(&self) -> Result<&Path, ApiError> {
        match (self.backend, self.path.as_deref()) {
            (RerankerBackendKind::Local, Some(path)) => Ok(path),
            _ => Err(ApiError::InvalidConfig {
                message: "models.reranker has no local model path unless backend = \"local\""
                    .to_string(),
            }),
        }
    }

    /// Return the local runtime token cap. Config validation guarantees
    /// presence for the local backend; the http backend has no local token cap.
    pub fn local_max_tokens(&self) -> Result<u32, ApiError> {
        match (self.backend, self.max_tokens) {
            (RerankerBackendKind::Local, Some(max_tokens)) => Ok(max_tokens),
            _ => Err(ApiError::InvalidConfig {
                message: "models.reranker has no local max_tokens unless backend = \"local\""
                    .to_string(),
            }),
        }
    }

    /// Return the HTTP rerank endpoint. Config validation guarantees presence
    /// for the HTTP backend; the local backend has no endpoint.
    pub fn http_endpoint(&self) -> Result<&str, ApiError> {
        match (self.backend, self.endpoint.as_deref()) {
            (RerankerBackendKind::Http, Some(endpoint)) => Ok(endpoint.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.reranker has no endpoint unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Return the HTTP rerank model name sent in provider requests.
    pub fn http_model(&self) -> Result<&str, ApiError> {
        match (self.backend, self.model.as_deref()) {
            (RerankerBackendKind::Http, Some(model)) => Ok(model.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.reranker has no model unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Return the configured HTTP rerank timeout in seconds.
    pub fn http_timeout_seconds(&self) -> Result<u64, ApiError> {
        match (self.backend, self.timeout_seconds) {
            (RerankerBackendKind::Http, Some(timeout_seconds)) => Ok(timeout_seconds),
            _ => Err(ApiError::InvalidConfig {
                message: "models.reranker has no timeout_seconds unless backend = \"http\""
                    .to_string(),
            }),
        }
    }

    /// Resolve the optional HTTP API-key file path against the Rust service root.
    pub fn resolved_http_api_key_file_path(&self) -> Option<PathBuf> {
        let path = self.api_key_file_path.as_ref()?;
        if path.is_absolute() {
            return Some(path.clone());
        }

        Some(service_root().join(path))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetrievalConfig {
    /// Default public result count when callers omit topK.
    pub default_top_k: u32,
    /// Maximum public result count allowed for one search request.
    pub max_top_k: u32,
    /// Reciprocal Rank Fusion constant for dense and BM25 candidate lists.
    pub rrf_k: u32,
    /// First-stage dense/BM25 over-fetch multiplier before RRF and reranking.
    pub candidate_overfetch_multiplier: u32,
    #[serde(default = "default_colbert_candidate_pool_size")]
    /// Bounded RRF candidate pool size sent into ColBERT MaxSim.
    pub colbert_candidate_pool_size: u32,
    #[serde(default = "default_reranker_candidate_pool_size")]
    /// Bounded ColBERT-ranked candidate pool size sent into the reranker; the
    /// effective pool is max(this value, requested topK), clamped to available
    /// ColBERT-ranked candidates.
    pub reranker_candidate_pool_size: u32,
    /// Minimum unit text length retained as searchable content.
    pub min_search_unit_chars: u32,
    /// Unit tokenizer cap aligned to ColBERT document capacity.
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
        require_positive(
            "server.max_in_flight_ingest",
            self.server.max_in_flight_ingest,
        )?;
        require_positive(
            "server.max_in_flight_search",
            self.server.max_in_flight_search,
        )?;
        require_non_empty_path("logging.file_path", &self.logging.file_path)?;
        require_non_empty_path("admin.token_file_path", &self.admin.token_file_path)?;
        require_absolute_path("storage.corpus_root", &self.storage.corpus_root)?;
        require_absolute_path("storage.index_root", &self.storage.index_root)?;
        require_absolute_path("docling.python_path", &self.docling.python_path)?;
        require_absolute_path("docling.docling_path", &self.docling.docling_path)?;
        require_positive_u64(
            "docling.document_timeout_seconds",
            self.docling.document_timeout_seconds,
        )?;
        require_absolute_path("models.dense.path", &self.models.dense.path)?;
        require_absolute_path("models.colbert.path", &self.models.colbert.path)?;

        require_non_empty("docling.pdf_backend", &self.docling.pdf_backend)?;
        if !matches!(
            self.docling.pdf_backend.trim(),
            "pypdfium2" | "docling_parse" | "dlparse_v1" | "dlparse_v2" | "dlparse_v4"
        ) {
            return Err(ApiError::InvalidConfig {
                message:
                    "docling.pdf_backend must be one of pypdfium2, docling_parse, dlparse_v1, dlparse_v2, or dlparse_v4"
                        .to_string(),
            });
        }
        require_non_empty("docling.ocr_mode", &self.docling.ocr_mode)?;
        if !matches!(self.docling.ocr_mode.trim(), "auto" | "on" | "off") {
            return Err(ApiError::InvalidConfig {
                message: "docling.ocr_mode must be one of auto, on, or off".to_string(),
            });
        }
        require_non_empty("docling.device", &self.docling.device)?;
        if !matches!(
            self.docling.device.trim(),
            "auto" | "cpu" | "cuda" | "mps" | "xpu"
        ) {
            return Err(ApiError::InvalidConfig {
                message: "docling.device must be one of auto, cpu, cuda, mps, or xpu".to_string(),
            });
        }
        require_positive("docling.num_threads", self.docling.num_threads)?;
        require_positive("docling.page_batch_size", self.docling.page_batch_size)?;
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
        validate_reranker_backend_fields(&self.models.reranker)?;
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
        require_positive(
            "retrieval.reranker_candidate_pool_size",
            self.retrieval.reranker_candidate_pool_size,
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

impl LoggingConfig {
    /// Resolve the configured log path against the Rust service root when it is relative.
    pub fn resolved_file_path(&self) -> PathBuf {
        if self.file_path.is_absolute() {
            return self.file_path.clone();
        }

        service_root().join(&self.file_path)
    }
}

impl AdminConfig {
    /// Resolve the configured token file path against the Rust service root when it is relative.
    pub fn resolved_token_file_path(&self) -> PathBuf {
        if self.token_file_path.is_absolute() {
            return self.token_file_path.clone();
        }

        service_root().join(&self.token_file_path)
    }
}

impl LoggingLevel {
    /// Return the lowercase config spelling for bootstrap output and log diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Return the Rust crate root used as the base for service-relative paths.
fn service_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Return the Phase 11F default bounded ColBERT reranking pool size.
fn default_colbert_candidate_pool_size() -> u32 {
    100
}

/// Return the default reranker candidate pool size, preserving the previous
/// hard topK cap for configs that omit the value.
fn default_reranker_candidate_pool_size() -> u32 {
    10
}

/// Resolve supported CLI options, falling back to `config.toml`.
pub fn resolve_cli_options_from_args() -> Result<CliOptions, ApiError> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from("config.toml");
    let mut smoke_dense = false;
    let mut setup_storage = false;
    let mut foreground = false;

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

        if arg == "--foreground" {
            foreground = true;
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
        foreground,
    })
}

/// Validate per-backend required and forbidden [models.reranker] fields. Each
/// backend's required fields must be present and valid, and the other
/// backend's fields must be absent so misconfiguration fails at startup
/// instead of being silently ignored.
fn validate_reranker_backend_fields(reranker: &RerankerModelConfig) -> Result<(), ApiError> {
    match reranker.backend {
        RerankerBackendKind::Local => {
            let Some(path) = reranker.path.as_ref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.path is required when backend = \"local\""
                        .to_string(),
                });
            };
            require_absolute_path("models.reranker.path", path)?;
            let Some(max_tokens) = reranker.max_tokens else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.max_tokens is required when backend = \"local\""
                        .to_string(),
                });
            };
            require_positive("models.reranker.max_tokens", max_tokens)?;
            if reranker.endpoint.is_some()
                || reranker.model.is_some()
                || reranker.timeout_seconds.is_some()
                || reranker.api_key_file_path.is_some()
            {
                return Err(ApiError::InvalidConfig {
                    message:
                        "models.reranker with backend = \"local\" must not set endpoint, model, timeout_seconds, or api_key_file_path"
                            .to_string(),
                });
            }
        }
        RerankerBackendKind::Http => {
            if reranker.path.is_some() || reranker.max_tokens.is_some() {
                return Err(ApiError::InvalidConfig {
                    message:
                        "models.reranker with backend = \"http\" must not set path or max_tokens"
                            .to_string(),
                });
            }
            let Some(endpoint) = reranker.endpoint.as_deref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.endpoint is required when backend = \"http\""
                        .to_string(),
                });
            };
            require_non_empty("models.reranker.endpoint", endpoint)?;
            let trimmed_endpoint = endpoint.trim();
            if !trimmed_endpoint.starts_with("http://") && !trimmed_endpoint.starts_with("https://")
            {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.endpoint must start with http:// or https://"
                        .to_string(),
                });
            }
            let Some(model) = reranker.model.as_deref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.model is required when backend = \"http\""
                        .to_string(),
                });
            };
            require_non_empty("models.reranker.model", model)?;
            let Some(timeout_seconds) = reranker.timeout_seconds else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.timeout_seconds is required when backend = \"http\""
                        .to_string(),
                });
            };
            require_positive_u64("models.reranker.timeout_seconds", timeout_seconds)?;
            if let Some(api_key_file_path) = reranker.api_key_file_path.as_ref() {
                require_non_empty_path("models.reranker.api_key_file_path", api_key_file_path)?;
            }
        }
    }

    Ok(())
}

/// Ensure a path field uses an absolute path.
fn require_absolute_path(label: &str, path: &Path) -> Result<(), ApiError> {
    if path.is_absolute() {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be an absolute path"),
    })
}

/// Ensure a path field is not the empty path.
fn require_non_empty_path(label: &str, path: &Path) -> Result<(), ApiError> {
    if !path.as_os_str().is_empty() {
        return Ok(());
    }

    Err(ApiError::InvalidConfig {
        message: format!("{label} must be a non-empty path"),
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

/// Ensure an unsigned 64-bit integer field is greater than zero.
fn require_positive_u64(label: &str, value: u64) -> Result<(), ApiError> {
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
