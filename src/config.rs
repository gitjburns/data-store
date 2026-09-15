use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::error::ApiError;
use crate::limits::{
    ClientLimits, DiagnosticLimits, EpubLimits, IndexingLimits, ParsingLimits, ResourceLimits,
    RetrievalLimits, RuntimeLimits, SchedulingLimits, SqliteLimits, WorkerLimits,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// HTTP bind address, startup hold, and request-shape limits.
    pub server: ServerConfig,
    /// File-backed service logging settings used after bootstrap stdout output.
    pub logging: LoggingConfig,
    /// Startup-scoped admin credential handoff settings.
    pub admin: AdminConfig,
    /// CLI client settings sharing this config file. The server parses and
    /// validates this section so `deny_unknown_fields` accepts the shared
    /// file, but never reads it at runtime; it is client-owned.
    pub client: ClientLimits,
    /// Query candidate selection, evidence, and passage budgets.
    pub retrieval: RetrievalLimits,
    /// Construction limits recorded with newly published projection artifacts.
    pub indexing: IndexingLimits,
    /// File, matrix, inventory, and allocation guards.
    pub resources: ResourceLimits,
    /// Synchronous worker batch, admission, and polling limits.
    pub workers: WorkerLimits,
    /// Independent lock-wait and SQL execution deadlines.
    pub sqlite: SqliteLimits,
    /// Parser acceptance limits for in-process parse workers.
    pub parsing: ParsingLimits,
    /// EPUB archive, member, document, image, and nesting admission budgets.
    pub epub: EpubLimits,
    /// Filesystem sweep cadence and lifecycle observation intervals.
    pub scheduling: SchedulingLimits,
    /// Bounded diagnostics and previews.
    pub diagnostics: DiagnosticLimits,
    /// Accelerator selection for all model runtimes.
    pub inference: InferenceConfig,
    /// Corpus and durable index/artifact paths owned by the service.
    pub storage: StorageConfig,
    /// Acquisition connector settings; external facts per spec §35, never
    /// internal capacity guesses.
    pub connectors: ConnectorsConfig,
    /// Local model artifact locations and runtime shape limits.
    pub models: ModelConfig,
    /// External policy document paths. Entity-match enable flags are composed
    /// with retrieval.entity_matching limits before effective-policy hashing.
    pub policies: PoliciesConfig,
    /// Directory of the loaded config file; the base every relative config
    /// path resolves against. Set by `load`, never deserialized.
    #[serde(skip)]
    config_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CliOptions {
    /// TOML config path supplied on the command line or defaulted to config.toml.
    pub config_path: PathBuf,
    /// Run inference readiness smoke checks without binding HTTP.
    pub smoke_dense: bool,
    /// Create or validate the fabric hot-plane SQLite schema through the
    /// explicit setup path.
    pub setup_storage: bool,
    /// Keep the HTTP service attached to the current terminal instead of daemonizing.
    pub foreground: bool,
    /// Annotation dry-run mode (CA2-P5): sample-annotate the first N section
    /// groups per source per type (entity/relation), then serve the
    /// inspection surface until shutdown. Service binary only; the
    /// `data-store` client rejects it like `--setup-storage`.
    pub annotation_dry_run: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Socket address where Axum binds the standalone service.
    pub bind_address: SocketAddr,
    /// Seconds to hold ordinary corpus access while rebuild-all remains available;
    /// zero disables the startup wait.
    pub startup_delay_seconds: u64,
    /// HTTP body limit applied before request JSON is accepted.
    pub max_request_body_bytes: usize,
    /// Maximum length of an ingest source reference after JSON parsing.
    pub max_ingest_source_chars: u32,
    /// Maximum length of a search query after JSON parsing.
    pub max_search_query_chars: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Service log file path; relative paths resolve against the config file's directory.
    pub file_path: PathBuf,
    /// Minimum event level written to the service log file.
    pub level: LoggingLevel,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Runtime file where the service writes the current startup-scoped admin bearer token.
    /// Relative paths resolve against the config file's directory.
    pub token_file_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoliciesConfig {
    /// Entity-match enable flags, resolved relative to the config directory.
    /// Flags combine with retrieval.entity_matching limits before startup
    /// assigns the effective policy's content hash and version.
    pub entity_match_file_path: PathBuf,
    /// Annotator naming-rules policy document, composed into the entity and
    /// relation producer prompts (producer-identity-bearing: an edit changes
    /// promptHash and invalidates memo reuse). Relative paths resolve against
    /// the config file's directory.
    pub annotator_naming_file_path: PathBuf,
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
#[serde(deny_unknown_fields)]
pub struct InferenceConfig {
    /// Accelerator used only when a local retrieval model is selected.
    /// Local model inference has no CPU fallback.
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
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Root directory for corpus-relative source references.
    pub corpus_root: PathBuf,
    /// Service-owned root for SQLite storage and generated conversion artifacts.
    pub index_root: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorsConfig {
    /// Filesystem connector acquiring source files from the corpus root.
    pub filesystem: FilesystemConnectorConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConnectorConfig {
    /// Governance domain stamped on every SourceLocation this connector
    /// acquires (spec §6 reservation 3): an external governance fact
    /// assigned at acquisition, retaggable without re-parse or re-index.
    pub governance_domain: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// Dense embedding model configuration.
    pub dense: DenseModelConfig,
    /// ColBERT late-interaction model configuration.
    pub colbert: ColbertModelConfig,
    /// ModernBERT sequence-classification reranker model configuration.
    pub reranker: RerankerModelConfig,
    /// External OpenAI-compatible annotation-producer endpoint configuration.
    pub annotator: AnnotatorModelConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenseModelConfig {
    /// Explicit dense-embedding backend selection. There is no fallback
    /// between backends.
    pub backend: DenseBackendKind,
    /// Expected dense vector width. Common to both backends; HTTP responses
    /// are validated against it per call.
    pub dimension: u32,
    /// Dense pooling contract of the model. The local adapter validates it;
    /// for the HTTP backend it records the served model's pooling as an
    /// operator-stated external fact (the server owns pooling).
    pub pooling: String,
    /// Local model artifact directory for Qwen3 dense embeddings. Required
    /// when backend = "local"; forbidden otherwise.
    pub path: Option<PathBuf>,
    /// Encoded input capacity; HTTP must match the engine and never truncates.
    pub max_tokens: u32,
    /// Maximum inputs per HTTP embeddings call; local inference does not batch here.
    pub http_batch_size: usize,
    /// Maximum concurrent HTTP embeddings requests from one projection build.
    pub http_concurrent_requests: usize,
    /// Transient HTTP failure retries; zero permits only the initial request.
    pub http_max_retries: usize,
    /// First transient-failure retry delay in milliseconds.
    pub http_retry_initial_delay_ms: u64,
    /// Maximum transient-failure retry delay in milliseconds.
    pub http_retry_max_delay_ms: u64,
    /// OpenAI-compatible embeddings endpoint URL. Required when
    /// backend = "http"; forbidden otherwise.
    pub endpoint: Option<String>,
    /// Model name sent in HTTP embeddings requests. Required when
    /// backend = "http"; forbidden otherwise.
    pub model: Option<String>,
    /// HTTP embeddings request timeout in seconds. Required when
    /// backend = "http"; forbidden otherwise.
    pub timeout_seconds: Option<u64>,
    /// Optional owner-only file holding the HTTP embeddings API key. Allowed
    /// only when backend = "http".
    pub api_key_file_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DenseBackendKind {
    /// In-process Candle Qwen3 embedding runtime.
    Local,
    /// HTTP client speaking the OpenAI-compatible embeddings contract.
    Http,
}

impl DenseModelConfig {
    /// Return the local model artifact directory. Config validation guarantees
    /// presence for the local backend; the http backend has no local path.
    pub fn local_path(&self) -> Result<&Path, ApiError> {
        match (self.backend, self.path.as_deref()) {
            (DenseBackendKind::Local, Some(path)) => Ok(path),
            _ => Err(ApiError::InvalidConfig {
                message: "models.dense has no local model path unless backend = \"local\""
                    .to_string(),
            }),
        }
    }

    /// Expose the shared input capacity only to the selected local runtime.
    pub fn local_max_tokens(&self) -> Result<u32, ApiError> {
        match (self.backend, self.max_tokens) {
            (DenseBackendKind::Local, max_tokens) => Ok(max_tokens),
            _ => Err(ApiError::InvalidConfig {
                message: "models.dense has no local max_tokens unless backend = \"local\""
                    .to_string(),
            }),
        }
    }

    /// Return the HTTP embeddings endpoint. Config validation guarantees
    /// presence for the HTTP backend; the local backend has no endpoint.
    pub fn http_endpoint(&self) -> Result<&str, ApiError> {
        match (self.backend, self.endpoint.as_deref()) {
            (DenseBackendKind::Http, Some(endpoint)) => Ok(endpoint.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.dense has no endpoint unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Return the HTTP embeddings model name sent in provider requests.
    pub fn http_model(&self) -> Result<&str, ApiError> {
        match (self.backend, self.model.as_deref()) {
            (DenseBackendKind::Http, Some(model)) => Ok(model.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.dense has no model unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Return the configured HTTP embeddings timeout in seconds.
    pub fn http_timeout_seconds(&self) -> Result<u64, ApiError> {
        match (self.backend, self.timeout_seconds) {
            (DenseBackendKind::Http, Some(timeout_seconds)) => Ok(timeout_seconds),
            _ => Err(ApiError::InvalidConfig {
                message: "models.dense has no timeout_seconds unless backend = \"http\""
                    .to_string(),
            }),
        }
    }

    /// Resolve the optional HTTP API-key file path against the config file's
    /// directory when it is relative.
    pub fn resolved_http_api_key_file_path(&self, config_root: &Path) -> Option<PathBuf> {
        let path = self.api_key_file_path.as_ref()?;
        if path.is_absolute() {
            return Some(path.clone());
        }

        Some(config_root.join(path))
    }
}

/// Bind ColBERT token limits and width to one explicitly configured inference backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColbertModelConfig {
    /// Explicit late-interaction backend selection, with no fallback.
    pub backend: ColbertBackendKind,
    /// Local ColBERT-Zero artifact directory. Required for the local backend;
    /// forbidden for HTTP, which loads only the tokenizer locally.
    pub path: Option<PathBuf>,
    /// Expected ColBERT token-vector width.
    pub dimension: u32,
    /// Checkpoint/engine encoded sequence capacity, including special tokens.
    pub max_tokens: u32,
    /// Runtime token cap for ColBERT query inputs.
    pub query_max_tokens: u32,
    /// Runtime token cap for ColBERT document/unit inputs.
    pub document_max_tokens: u32,
    /// Maximum document windows per embedding call.
    pub document_batch_size: usize,
    /// Maximum document length admitted to the local padded batch path.
    pub local_batch_max_tokens: usize,
    /// Full vLLM pooling route URL. Required only for the HTTP backend.
    pub endpoint: Option<String>,
    /// Served ColBERT model name sent in pooling requests. HTTP only.
    pub model: Option<String>,
    /// Absolute tokenizer JSON path matching the served checkpoint. HTTP only;
    /// local tokenization preserves passage budgets and ColBERT input markers.
    pub tokenizer_file_path: Option<PathBuf>,
    /// Whole-request pooling timeout in seconds. Required for HTTP only.
    pub timeout_seconds: Option<u64>,
    /// Optional owner-only HTTP bearer-key file; forbidden for local inference.
    pub api_key_file_path: Option<PathBuf>,
}

/// Select who produces token matrices; both backends retain local tokenization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColbertBackendKind {
    /// In-process Candle ColBERT-Zero inference.
    Local,
    /// vLLM token-embedding pooling endpoint.
    Http,
}

impl ColbertModelConfig {
    /// Expose model weights only to the explicitly selected local backend.
    pub fn local_path(&self) -> Result<&Path, ApiError> {
        match (self.backend, self.path.as_deref()) {
            (ColbertBackendKind::Local, Some(path)) => Ok(path),
            _ => Err(ApiError::InvalidConfig {
                message: "models.colbert has no local model path unless backend = \"local\""
                    .to_string(),
            }),
        }
    }

    /// Return the validated full pooling URL without adding route components.
    pub fn http_endpoint(&self) -> Result<&str, ApiError> {
        match (self.backend, self.endpoint.as_deref()) {
            (ColbertBackendKind::Http, Some(endpoint)) => Ok(endpoint.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.colbert has no endpoint unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Return the provider's model identifier for pooling requests.
    pub fn http_model(&self) -> Result<&str, ApiError> {
        match (self.backend, self.model.as_deref()) {
            (ColbertBackendKind::Http, Some(model)) => Ok(model.trim()),
            _ => Err(ApiError::InvalidConfig {
                message: "models.colbert has no model unless backend = \"http\"".to_string(),
            }),
        }
    }

    /// Locate the served checkpoint's tokenizer without requiring local weights.
    pub fn http_tokenizer_file_path(&self) -> Result<&Path, ApiError> {
        match (self.backend, self.tokenizer_file_path.as_deref()) {
            (ColbertBackendKind::Http, Some(path)) => Ok(path),
            _ => Err(ApiError::InvalidConfig {
                message: "models.colbert has no tokenizer_file_path unless backend = \"http\""
                    .to_string(),
            }),
        }
    }

    /// Return the whole-request deadline used by the blocking pooling client.
    pub fn http_timeout_seconds(&self) -> Result<u64, ApiError> {
        match (self.backend, self.timeout_seconds) {
            (ColbertBackendKind::Http, Some(timeout_seconds)) => Ok(timeout_seconds),
            _ => Err(ApiError::InvalidConfig {
                message: "models.colbert has no timeout_seconds unless backend = \"http\""
                    .to_string(),
            }),
        }
    }

    /// Resolve relative credential paths against the configuration directory.
    pub fn resolved_http_api_key_file_path(&self, config_root: &Path) -> Option<PathBuf> {
        let path = self.api_key_file_path.as_ref()?;
        if path.is_absolute() {
            return Some(path.clone());
        }

        Some(config_root.join(path))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RerankerModelConfig {
    /// Explicit reranker backend selection. There is no fallback between backends.
    pub backend: RerankerBackendKind,
    /// Local model artifact directory for ModernBERT sequence-classification
    /// reranking. Required when backend = "local"; forbidden otherwise.
    pub path: Option<PathBuf>,
    /// Encoded query/document capacity; HTTP must match the engine and never truncates.
    pub max_tokens: u32,
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

    /// Expose the shared pair capacity only to the selected local runtime.
    pub fn local_max_tokens(&self) -> Result<u32, ApiError> {
        match (self.backend, self.max_tokens) {
            (RerankerBackendKind::Local, max_tokens) => Ok(max_tokens),
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

    /// Resolve the optional HTTP API-key file path against the config file's
    /// directory when it is relative.
    pub fn resolved_http_api_key_file_path(&self, config_root: &Path) -> Option<PathBuf> {
        let path = self.api_key_file_path.as_ref()?;
        if path.is_absolute() {
            return Some(path.clone());
        }

        Some(config_root.join(path))
    }
}

/// External OpenAI-compatible chat-completions endpoint used by the
/// annotation producers (entity, relation, summary). The endpoint is
/// exclusive: producer failures park annotations as failed for later retry
/// by the annotation worker; there is no fallback model or endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotatorModelConfig {
    /// Full chat-completions route URL (external fact, spec §35).
    pub endpoint: String,
    /// Model name sent in request bodies.
    pub model: String,
    /// Whole-request timeout for one producer call, in seconds.
    pub timeout_seconds: u64,
    /// Optional owner-only file holding the bearer API key.
    pub api_key_file_path: Option<PathBuf>,
    /// Maximum source-excerpt length in Unicode characters. Prompts and prior
    /// stage outputs are additional; this is not a model token-budget estimate.
    pub max_input_chars: usize,
    /// Provider completion-token allowance per call, including reasoning tokens.
    pub max_completion_tokens: u64,
    /// Malformed-output retry allowance per annotation; zero permits only the
    /// initial attempt. This budget also determines the temperature ramp.
    pub annotation_max_retries: u32,
    /// Fixed delay after malformed output, independent of execution backoff.
    pub annotation_retry_interval_seconds: u64,
    /// Execution-failure retry allowance, independent of malformed outputs;
    /// zero disables retries for execution failures.
    pub execution_max_retries: u32,
    /// Delay after the first execution failure; later failures double it.
    pub execution_retry_initial_delay_seconds: u64,
    /// Ceiling for execution backoff only; must cover the initial delay.
    pub execution_retry_max_delay_seconds: u64,
}

impl AnnotatorModelConfig {
    /// Resolve the optional API-key file path against the config file's
    /// directory when it is relative.
    pub fn resolved_api_key_file_path(&self, config_root: &Path) -> Option<PathBuf> {
        let path = self.api_key_file_path.as_ref()?;
        if path.is_absolute() {
            return Some(path.clone());
        }

        Some(config_root.join(path))
    }
}

impl ServiceConfig {
    /// Load and validate service configuration from one TOML file.
    pub fn load(path: PathBuf) -> Result<Self, ApiError> {
        let raw = fs::read_to_string(&path).map_err(|source| ApiError::ConfigRead {
            path: path.clone(),
            source,
        })?;
        let mut config: Self = toml::from_str(&raw).map_err(|source| ApiError::ConfigParse {
            path: path.clone(),
            source,
        })?;
        config.config_root = config_root_for(&path)?;

        config.validate()?;
        Ok(config)
    }

    /// Return the directory relative config paths resolve against: the loaded
    /// config file's parent directory.
    pub fn config_root(&self) -> &Path {
        &self.config_root
    }

    /// Return the configured socket address for the HTTP server.
    pub fn bind_address(&self) -> SocketAddr {
        self.server.bind_address
    }

    /// Copy validated leaf settings into the startup-owned runtime context.
    pub fn runtime_limits(&self) -> RuntimeLimits {
        RuntimeLimits {
            retrieval: self.retrieval,
            indexing: self.indexing,
            resources: self.resources,
            workers: self.workers,
            sqlite: self.sqlite,
            parsing: self.parsing,
            epub: self.epub,
            scheduling: self.scheduling,
            diagnostics: self.diagnostics,
            client: self.client,
        }
    }

    /// Validate cross-field config invariants that TOML deserialization cannot express.
    fn validate(&self) -> Result<(), ApiError> {
        self.runtime_limits()
            .validate()
            .map_err(|message| ApiError::InvalidConfig { message })?;
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
        require_non_empty_path("logging.file_path", &self.logging.file_path)?;
        require_non_empty_path("admin.token_file_path", &self.admin.token_file_path)?;
        require_absolute_path("storage.corpus_root", &self.storage.corpus_root)?;
        require_absolute_path("storage.index_root", &self.storage.index_root)?;
        require_non_empty(
            "connectors.filesystem.governance_domain",
            &self.connectors.filesystem.governance_domain,
        )?;
        require_non_empty("models.dense.pooling", &self.models.dense.pooling)?;
        require_positive("models.dense.dimension", self.models.dense.dimension)?;
        validate_dense_backend_fields(&self.models.dense)?;
        validate_colbert_backend_fields(&self.models.colbert)?;
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
        require_non_empty("models.annotator.endpoint", &self.models.annotator.endpoint)?;
        require_non_empty("models.annotator.model", &self.models.annotator.model)?;
        require_positive_u64(
            "models.annotator.timeout_seconds",
            self.models.annotator.timeout_seconds,
        )?;
        require_positive_usize(
            "models.annotator.max_input_chars",
            self.models.annotator.max_input_chars,
        )?;
        require_positive_u64(
            "models.annotator.max_completion_tokens",
            self.models.annotator.max_completion_tokens,
        )?;
        // Zero retry allowances are valid, but enabled retry paths must never
        // become a busy loop. The annotation interval has no execution ceiling.
        require_positive_u64(
            "models.annotator.annotation_retry_interval_seconds",
            self.models.annotator.annotation_retry_interval_seconds,
        )?;
        require_positive_u64(
            "models.annotator.execution_retry_initial_delay_seconds",
            self.models.annotator.execution_retry_initial_delay_seconds,
        )?;
        require_positive_u64(
            "models.annotator.execution_retry_max_delay_seconds",
            self.models.annotator.execution_retry_max_delay_seconds,
        )?;
        if self.models.annotator.execution_retry_max_delay_seconds
            < self.models.annotator.execution_retry_initial_delay_seconds
        {
            return Err(ApiError::InvalidConfig {
                message: "models.annotator.execution_retry_max_delay_seconds must be at least models.annotator.execution_retry_initial_delay_seconds".to_string(),
            });
        }
        // Optional, but an explicit empty path would resolve to the config
        // directory itself; fail at startup like the reranker's key path.
        if let Some(api_key_file_path) = self.models.annotator.api_key_file_path.as_ref() {
            require_non_empty_path("models.annotator.api_key_file_path", api_key_file_path)?;
        }
        require_non_empty_path(
            "policies.entity_match_file_path",
            &self.policies.entity_match_file_path,
        )?;
        require_non_empty_path(
            "policies.annotator_naming_file_path",
            &self.policies.annotator_naming_file_path,
        )?;

        Ok(())
    }
}

impl LoggingConfig {
    /// Resolve the configured log path against the config file's directory when it is relative.
    pub fn resolved_file_path(&self, config_root: &Path) -> PathBuf {
        if self.file_path.is_absolute() {
            return self.file_path.clone();
        }

        config_root.join(&self.file_path)
    }
}

impl AdminConfig {
    /// Resolve the configured token file path against the config file's directory when it is relative.
    pub fn resolved_token_file_path(&self, config_root: &Path) -> PathBuf {
        if self.token_file_path.is_absolute() {
            return self.token_file_path.clone();
        }

        config_root.join(&self.token_file_path)
    }
}

impl PoliciesConfig {
    /// Resolve the entity-match policy document path against the config
    /// file's directory when it is relative.
    pub fn resolved_entity_match_file_path(&self, config_root: &Path) -> PathBuf {
        if self.entity_match_file_path.is_absolute() {
            return self.entity_match_file_path.clone();
        }

        config_root.join(&self.entity_match_file_path)
    }

    /// Resolve the annotator naming-rules policy document path against the
    /// config file's directory when it is relative.
    pub fn resolved_annotator_naming_file_path(&self, config_root: &Path) -> PathBuf {
        if self.annotator_naming_file_path.is_absolute() {
            return self.annotator_naming_file_path.clone();
        }

        config_root.join(&self.annotator_naming_file_path)
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

/// Resolve the base directory for relative config paths: the canonicalized
/// parent directory of the loaded config file. Canonicalization makes the
/// base stable regardless of the process working directory or how the
/// `--config` argument was spelled.
fn config_root_for(config_path: &Path) -> Result<PathBuf, ApiError> {
    let canonical = fs::canonicalize(config_path).map_err(|source| ApiError::ConfigRead {
        path: config_path.to_path_buf(),
        source,
    })?;
    match canonical.parent() {
        Some(parent) => Ok(parent.to_path_buf()),
        None => Err(ApiError::InvalidConfig {
            message: format!(
                "config path {} has no parent directory to resolve relative paths against",
                canonical.display()
            ),
        }),
    }
}

/// Resolve supported CLI options, falling back to `config.toml`.
pub fn resolve_cli_options_from_args() -> Result<CliOptions, ApiError> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from("config.toml");
    let mut smoke_dense = false;
    let mut setup_storage = false;
    let mut foreground = false;
    let mut annotation_dry_run = None;

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

        if arg == "--annotation-dry-run" {
            let Some(value) = args.next() else {
                return Err(ApiError::InvalidCli {
                    message: "--annotation-dry-run requires a groups-per-source count".to_string(),
                });
            };
            // Positive by requirement: a zero-group sample would parse the
            // corpus but annotate nothing, which is not the ruled mode.
            let groups: usize = value
                .parse()
                .ok()
                .filter(|count| *count > 0)
                .ok_or_else(|| ApiError::InvalidCli {
                    message: format!(
                        "--annotation-dry-run expects a positive integer, got {value}"
                    ),
                })?;
            annotation_dry_run = Some(groups);
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
        annotation_dry_run,
    })
}

/// Validate per-backend required and forbidden [models.dense] fields. Each
/// backend's required fields must be present and valid, and the other
/// backend's fields must be absent so misconfiguration fails at startup
/// instead of being silently ignored.
fn validate_dense_backend_fields(dense: &DenseModelConfig) -> Result<(), ApiError> {
    require_positive("models.dense.max_tokens", dense.max_tokens)?;
    require_positive_usize("models.dense.http_batch_size", dense.http_batch_size)?;
    require_positive_usize(
        "models.dense.http_concurrent_requests",
        dense.http_concurrent_requests,
    )?;
    require_positive_u64(
        "models.dense.http_retry_initial_delay_ms",
        dense.http_retry_initial_delay_ms,
    )?;
    require_positive_u64(
        "models.dense.http_retry_max_delay_ms",
        dense.http_retry_max_delay_ms,
    )?;
    if dense.http_retry_initial_delay_ms > dense.http_retry_max_delay_ms {
        return Err(ApiError::InvalidConfig {
            message:
                "models.dense.http_retry_initial_delay_ms must not exceed http_retry_max_delay_ms"
                    .into(),
        });
    }
    match dense.backend {
        DenseBackendKind::Local => {
            let Some(path) = dense.path.as_ref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense.path is required when backend = \"local\"".to_string(),
                });
            };
            require_absolute_path("models.dense.path", path)?;
            if dense.endpoint.is_some()
                || dense.model.is_some()
                || dense.timeout_seconds.is_some()
                || dense.api_key_file_path.is_some()
            {
                return Err(ApiError::InvalidConfig {
                    message:
                        "models.dense with backend = \"local\" must not set endpoint, model, timeout_seconds, or api_key_file_path"
                            .to_string(),
                });
            }
        }
        DenseBackendKind::Http => {
            if dense.path.is_some() {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense with backend = \"http\" must not set path".to_string(),
                });
            }
            let Some(endpoint) = dense.endpoint.as_deref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense.endpoint is required when backend = \"http\""
                        .to_string(),
                });
            };
            require_non_empty("models.dense.endpoint", endpoint)?;
            let trimmed_endpoint = endpoint.trim();
            if !trimmed_endpoint.starts_with("http://") && !trimmed_endpoint.starts_with("https://")
            {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense.endpoint must start with http:// or https://"
                        .to_string(),
                });
            }
            let Some(model) = dense.model.as_deref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense.model is required when backend = \"http\"".to_string(),
                });
            };
            require_non_empty("models.dense.model", model)?;
            let Some(timeout_seconds) = dense.timeout_seconds else {
                return Err(ApiError::InvalidConfig {
                    message: "models.dense.timeout_seconds is required when backend = \"http\""
                        .to_string(),
                });
            };
            require_positive_u64("models.dense.timeout_seconds", timeout_seconds)?;
            if let Some(api_key_file_path) = dense.api_key_file_path.as_ref() {
                require_non_empty_path("models.dense.api_key_file_path", api_key_file_path)?;
            }
        }
    }

    Ok(())
}

/// Reject missing or mixed ColBERT backend settings before model initialization;
/// HTTP keeps a matching tokenizer locally but must never require model weights.
fn validate_colbert_backend_fields(colbert: &ColbertModelConfig) -> Result<(), ApiError> {
    require_positive("models.colbert.max_tokens", colbert.max_tokens)?;
    require_positive_usize(
        "models.colbert.document_batch_size",
        colbert.document_batch_size,
    )?;
    require_positive_usize(
        "models.colbert.local_batch_max_tokens",
        colbert.local_batch_max_tokens,
    )?;
    // Backend capacity is distinct from window policy, but no configured input
    // path may exceed the capacity that initialization verifies with the model.
    if colbert.query_max_tokens > colbert.max_tokens
        || colbert.document_max_tokens > colbert.max_tokens
        || colbert.local_batch_max_tokens > colbert.max_tokens as usize
    {
        return Err(ApiError::InvalidConfig { message: "models.colbert query_max_tokens, document_max_tokens, and local_batch_max_tokens must not exceed max_tokens".into() });
    }
    match colbert.backend {
        ColbertBackendKind::Local => {
            let Some(path) = colbert.path.as_ref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.colbert.path is required when backend = \"local\"".to_string(),
                });
            };
            require_absolute_path("models.colbert.path", path)?;
            if colbert.endpoint.is_some()
                || colbert.model.is_some()
                || colbert.tokenizer_file_path.is_some()
                || colbert.timeout_seconds.is_some()
                || colbert.api_key_file_path.is_some()
            {
                return Err(ApiError::InvalidConfig {
                    message: "models.colbert with backend = \"local\" must not set endpoint, model, tokenizer_file_path, timeout_seconds, or api_key_file_path"
                        .to_string(),
                });
            }
        }
        ColbertBackendKind::Http => {
            if colbert.path.is_some() {
                return Err(ApiError::InvalidConfig {
                    message: "models.colbert with backend = \"http\" must not set path".to_string(),
                });
            }
            let endpoint = colbert.http_endpoint()?;
            let endpoint_url =
                reqwest::Url::parse(endpoint).map_err(|error| ApiError::InvalidConfig {
                    message: format!("models.colbert.endpoint is not a valid URL: {error}"),
                })?;
            // Endpoint identity is logged and hashed. Authentication belongs in
            // the owner-only key file, never in URL components exposed there.
            if !endpoint_url.username().is_empty()
                || endpoint_url.password().is_some()
                || endpoint_url.query().is_some()
                || endpoint_url.fragment().is_some()
            {
                return Err(ApiError::InvalidConfig {
                    message: "models.colbert.endpoint must not contain user information, query parameters, or a fragment; use api_key_file_path for authentication".to_string(),
                });
            }
            if !matches!(endpoint_url.scheme(), "http" | "https")
                || endpoint_url.host_str().is_none()
                || !endpoint_url
                    .path()
                    .trim_end_matches('/')
                    .ends_with("/pooling")
            {
                return Err(ApiError::InvalidConfig {
                    message: "models.colbert.endpoint must be a full http:// or https:// pooling route URL ending in /pooling"
                        .to_string(),
                });
            }
            require_non_empty("models.colbert.model", colbert.http_model()?)?;
            require_absolute_path(
                "models.colbert.tokenizer_file_path",
                colbert.http_tokenizer_file_path()?,
            )?;
            require_positive_u64(
                "models.colbert.timeout_seconds",
                colbert.http_timeout_seconds()?,
            )?;
            if let Some(api_key_file_path) = colbert.api_key_file_path.as_ref() {
                require_non_empty_path("models.colbert.api_key_file_path", api_key_file_path)?;
            }
        }
    }

    Ok(())
}

/// Validate per-backend required and forbidden [models.reranker] fields. Each
/// backend's required fields must be present and valid, and the other
/// backend's fields must be absent so misconfiguration fails at startup
/// instead of being silently ignored.
fn validate_reranker_backend_fields(reranker: &RerankerModelConfig) -> Result<(), ApiError> {
    require_positive("models.reranker.max_tokens", reranker.max_tokens)?;
    match reranker.backend {
        RerankerBackendKind::Local => {
            let Some(path) = reranker.path.as_ref() else {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker.path is required when backend = \"local\""
                        .to_string(),
                });
            };
            require_absolute_path("models.reranker.path", path)?;
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
            if reranker.path.is_some() {
                return Err(ApiError::InvalidConfig {
                    message: "models.reranker with backend = \"http\" must not set path"
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
