//! Application identity (spec §30.2 line "Application identity", §30.7 replay
//! environment): the recorded system version, spec version, active build
//! features, and the aggregate configuration hash that together pin the
//! code-and-config half of an isolated replay environment. A ForensicSnapshot
//! embeds these so a restore drill can reconstruct the exact application
//! identity a snapshot was taken under, independent of container or VM imaging.
//!
//! `ApplicationIdentity::capture` assembles the value ONCE at startup (in
//! `main`, right after config load) and it is moved into the scheduler thread
//! via `scheduler::start`, which threads it explicitly to every snapshot-minting
//! site it reaches — including the deletion path, reached through that same
//! scheduler thread — per the 2026-07-16 ruling, never a global. The accessors
//! are pure and never log: identity is captured once, not at a lifecycle
//! boundary of its own.

use std::path::Path;

use serde::Serialize;

use crate::canonical;
use crate::config::ServiceConfig;
use crate::error::ApiError;
use crate::limits::{
    DiagnosticLimits, EpubLimits, IndexingLimits, ParsingLimits, ResourceLimits, RetrievalLimits,
    SchedulingLimits, SqliteLimits, WorkerLimits,
};

/// Spec version this build implements, stamped into every `ForensicSnapshot`
/// as `specVersion` (§30.3). A constant, not derived from the spec file: the
/// running binary asserts which contract revision it honors.
pub(crate) const SPEC_VERSION: &str = "0.4";

/// The captured §30.2 "Application identity": the code-and-config half of an
/// isolated replay environment (§30.7), assembled ONCE at startup and threaded
/// explicitly to every snapshot-minting site (user ruling 2026-07-16 — no
/// global, no `OnceLock`). Holding it by value means the aggregate
/// `configuration_hash` is computed a single time from the loaded config and
/// then carried, so no trigger signature needs a `&ServiceConfig` and no minting
/// site re-hashes.
///
/// SECRET BOUNDARY: `configuration_hash` covers secret file PATHS only, never
/// secret VALUES (see `configuration_hash`). The captured identity therefore
/// changes when a credential file is repointed but never encodes a secret.
///
/// `Clone` because the identity is moved into the scheduler thread once and
/// reused at each snapshot-minting site within that thread, including the
/// deletion path reached via `propagate_deletions`; `main` keeps no copy.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApplicationIdentity {
    /// §30.3 `systemVersion` — the running binary's package version.
    pub(crate) system_version: String,
    /// §30.3 `specVersion` — the contract revision this build honors.
    pub(crate) spec_version: String,
    /// §30.7 replay environment — the accelerator/build features compiled in,
    /// sorted for order-independent comparison (see `build_features`).
    pub(crate) build_features: Vec<String>,
    /// §30.7 aggregate `configurationHash` — lowercase-hex SHA-256 over the
    /// audit-relevant configuration (secret PATHS only; see `configuration_hash`).
    pub(crate) configuration_hash: String,
    /// Effective entity-match hash combines file enable flags and configured
    /// numeric limits. Replay must reconstruct the same rules, not only a path.
    pub(crate) entity_match_policy_hash: String,
    /// Content hash of the loaded annotator naming-rules policy document
    /// (same rationale as `entity_match_policy_hash`; this one is also
    /// producer-identity-bearing via promptHash composition).
    pub(crate) annotator_naming_policy_hash: String,
}

impl ApplicationIdentity {
    /// Capture the full application identity from the loaded configuration. The
    /// aggregate configuration hash is computed here, exactly once at startup,
    /// so the value can be threaded to every snapshot-minting site without any
    /// trigger holding a `&ServiceConfig` (user ruling 2026-07-16). Reuses the
    /// existing `system_version`/`build_features`/`configuration_hash` sources
    /// and the `SPEC_VERSION` constant so the captured shape never drifts from
    /// what a snapshot header stamps.
    /// The two policy content hashes come from the documents loaded moments
    /// earlier in startup (`policy::load_*`); capture takes the hashes rather
    /// than the paths so the identity records what was actually loaded.
    pub(crate) fn capture(
        config: &ServiceConfig,
        entity_match_policy_hash: &str,
        annotator_naming_policy_hash: &str,
    ) -> Result<Self, ApiError> {
        Ok(ApplicationIdentity {
            system_version: system_version().to_owned(),
            spec_version: SPEC_VERSION.to_owned(),
            build_features: build_features().into_iter().map(str::to_owned).collect(),
            configuration_hash: configuration_hash(config)?,
            entity_match_policy_hash: entity_match_policy_hash.to_owned(),
            annotator_naming_policy_hash: annotator_naming_policy_hash.to_owned(),
        })
    }
}

/// The application's own version string, stamped into every `ForensicSnapshot`
/// as `systemVersion` (§30.3) and recorded in the replay environment (§30.7).
/// Sourced from `CARGO_PKG_VERSION` so the recorded identity always matches the
/// built binary's package version with no separate constant to drift.
pub(crate) fn system_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The Cargo build features compiled into this binary, in a stable sorted
/// order, recorded as part of the §30.7 replay environment. Determined by
/// `cfg!` at compile time so the record reflects the actual binary rather than
/// runtime configuration: `cuda` and `metal` select mutually-exclusive
/// accelerator backends (Cargo.toml `[features]`), and which one is present
/// changes model execution and therefore replay fidelity. Returned sorted so
/// the value is order-independent for hashing and comparison.
pub(crate) fn build_features() -> Vec<&'static str> {
    let mut features = Vec::new();
    if cfg!(feature = "cuda") {
        features.push("cuda");
    }
    if cfg!(feature = "metal") {
        features.push("metal");
    }
    features.sort_unstable();
    features
}

/// Aggregate configuration hash for the §30.7 replay environment: a
/// lowercase-hex SHA-256 over the canonical serialization of the
/// audit-relevant configuration, stamped into snapshots so a replay
/// environment can assert it was reconstructed under the same operational
/// configuration.
///
/// SECRET BOUNDARY: this hash covers secret file PATHS only, never secret
/// VALUES. `ConfigurationIdentity` is built to carry the resolved *paths* of
/// credential files (the annotator/reranker API-key files) and never their
/// contents, so the hash changes when a credential file is repointed but the
/// snapshot record never encodes a secret. Per-connector and per-parser config
/// hashes are recorded elsewhere in the fabric; this is the process-wide
/// aggregate the replay environment pins.
pub(crate) fn configuration_hash(config: &ServiceConfig) -> Result<String, ApiError> {
    let identity = ConfigurationIdentity::from_config(config);
    canonical::canonical_sha256_hex_of(&identity)
}

/// The audit-relevant projection of `ServiceConfig` that `configuration_hash`
/// hashes. This is a deliberate, reviewable subset — the operational settings
/// that change replay behavior — not the whole config: the client section is
/// presentation-only and excluded, and every credential field is captured as a
/// resolved PATH string, never a value (see the `configuration_hash` secret
/// boundary). Keeping this a single struct makes the hashed shape the one
/// source of truth for what the configuration hash covers.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigurationIdentity {
    startup_delay_seconds: u64,
    max_request_body_bytes: usize,
    max_ingest_source_chars: u32,
    max_search_query_chars: u32,

    // Persist all operational groups; client presentation cannot affect replay.
    retrieval: RetrievalLimits,
    indexing: IndexingLimits,
    resources: ResourceLimits,
    workers: WorkerLimits,
    sqlite: SqliteLimits,
    parsing: ParsingLimits,
    epub: EpubLimits,
    scheduling: SchedulingLimits,
    diagnostics: DiagnosticLimits,

    logging_level: String,

    inference_device: String,
    inference_device_index: usize,

    corpus_root: String,
    index_root: String,

    filesystem_governance_domain: String,

    models: ModelIdentity,

    /// PATHS of the operator-editable policy documents (D3 amendment, CA2).
    /// Paths only — the documents' CONTENT identity is carried by the two
    /// content-hash fields on `ApplicationIdentity` itself, because the
    /// content is operator-mutable state outside this config projection.
    policy_entity_match_file_path: String,
    policy_annotator_naming_file_path: String,
}

/// Model runtime settings that affect embedding/ranking output and therefore
/// replay fidelity. Credential files appear only as resolved PATH strings via
/// `Option<String>`; their contents are never read here.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelIdentity {
    dense_backend: String,
    dense_dimension: u32,
    dense_pooling: String,
    /// Local-backend facts: present only when `dense_backend` is local.
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_path: Option<String>,
    dense_max_tokens: u32,
    dense_http_batch_size: usize,
    dense_http_concurrent_requests: usize,
    dense_http_max_retries: usize,
    dense_http_retry_initial_delay_ms: u64,
    dense_http_retry_max_delay_ms: u64,
    /// HTTP-backend facts: present only when `dense_backend` is http.
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_timeout_seconds: Option<u64>,
    /// Resolved PATH of the dense HTTP API-key file, never its contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_api_key_file_path: Option<String>,

    colbert_backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_path: Option<String>,
    colbert_dimension: u32,
    colbert_max_tokens: u32,
    colbert_query_max_tokens: u32,
    colbert_document_max_tokens: u32,
    colbert_document_batch_size: usize,
    colbert_local_batch_max_tokens: usize,
    /// HTTP facts identify the remote model and its matching local tokenizer.
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_tokenizer_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_timeout_seconds: Option<u64>,
    /// Resolved PATH of the ColBERT API-key file, never its contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    colbert_api_key_file_path: Option<String>,

    reranker_backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reranker_path: Option<String>,
    reranker_max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reranker_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reranker_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reranker_timeout_seconds: Option<u64>,
    /// Resolved PATH of the reranker API-key file, never its contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    reranker_api_key_file_path: Option<String>,

    /// Resolved PATH of the annotator API-key file, never its contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    annotator_api_key_file_path: Option<String>,
    annotator_max_input_chars: usize,
    annotator_max_completion_tokens: u64,
    // Operational retry policy belongs to the audit identity, not producer
    // memo identity. Each call's actual retry temperature is recorded in provenance.
    annotator_annotation_max_retries: u32,
    annotator_annotation_retry_interval_seconds: u64,
    annotator_execution_max_retries: u32,
    annotator_execution_retry_initial_delay_seconds: u64,
    annotator_execution_retry_max_delay_seconds: u64,
}

impl ConfigurationIdentity {
    /// Project the audit-relevant configuration out of `ServiceConfig`. Lossy
    /// by design (see the struct doc): only settings that change replay
    /// behavior are captured, credential files as resolved paths only.
    fn from_config(config: &ServiceConfig) -> Self {
        ConfigurationIdentity {
            startup_delay_seconds: config.server.startup_delay_seconds,
            max_request_body_bytes: config.server.max_request_body_bytes,
            max_ingest_source_chars: config.server.max_ingest_source_chars,
            max_search_query_chars: config.server.max_search_query_chars,
            retrieval: config.retrieval,
            indexing: config.indexing,
            resources: config.resources,
            workers: config.workers,
            sqlite: config.sqlite,
            parsing: config.parsing,
            epub: config.epub,
            scheduling: config.scheduling,
            diagnostics: config.diagnostics,
            logging_level: format!("{:?}", config.logging.level),
            inference_device: format!("{:?}", config.inference.device),
            inference_device_index: config.inference.device_index,
            corpus_root: path_string(&config.storage.corpus_root),
            index_root: path_string(&config.storage.index_root),
            filesystem_governance_domain: config.connectors.filesystem.governance_domain.clone(),
            models: ModelIdentity {
                // Input capacity is shared; optional connection/model paths
                // identify only the selected backend. No secret values enter here.
                dense_backend: format!("{:?}", config.models.dense.backend),
                dense_dimension: config.models.dense.dimension,
                dense_pooling: config.models.dense.pooling.clone(),
                dense_path: config.models.dense.path.as_deref().map(path_string),
                dense_max_tokens: config.models.dense.max_tokens,
                dense_http_batch_size: config.models.dense.http_batch_size,
                dense_http_concurrent_requests: config.models.dense.http_concurrent_requests,
                dense_http_max_retries: config.models.dense.http_max_retries,
                dense_http_retry_initial_delay_ms: config.models.dense.http_retry_initial_delay_ms,
                dense_http_retry_max_delay_ms: config.models.dense.http_retry_max_delay_ms,
                dense_endpoint: config.models.dense.endpoint.clone(),
                dense_model: config.models.dense.model.clone(),
                dense_timeout_seconds: config.models.dense.timeout_seconds,
                // PATH only — never the key inside the file.
                dense_api_key_file_path: config
                    .models
                    .dense
                    .api_key_file_path
                    .as_deref()
                    .map(path_string),
                // Validation guarantees the optional fields describe only the
                // selected backend; credentials remain paths, never key contents.
                colbert_backend: format!("{:?}", config.models.colbert.backend),
                colbert_path: config.models.colbert.path.as_deref().map(path_string),
                colbert_dimension: config.models.colbert.dimension,
                colbert_max_tokens: config.models.colbert.max_tokens,
                colbert_query_max_tokens: config.models.colbert.query_max_tokens,
                colbert_document_max_tokens: config.models.colbert.document_max_tokens,
                colbert_document_batch_size: config.models.colbert.document_batch_size,
                colbert_local_batch_max_tokens: config.models.colbert.local_batch_max_tokens,
                colbert_endpoint: config.models.colbert.endpoint.clone(),
                colbert_model: config.models.colbert.model.clone(),
                colbert_tokenizer_file_path: config
                    .models
                    .colbert
                    .tokenizer_file_path
                    .as_deref()
                    .map(path_string),
                colbert_timeout_seconds: config.models.colbert.timeout_seconds,
                colbert_api_key_file_path: config
                    .models
                    .colbert
                    .api_key_file_path
                    .as_deref()
                    .map(path_string),
                reranker_backend: format!("{:?}", config.models.reranker.backend),
                reranker_path: config.models.reranker.path.as_deref().map(path_string),
                reranker_max_tokens: config.models.reranker.max_tokens,
                reranker_endpoint: config.models.reranker.endpoint.clone(),
                reranker_model: config.models.reranker.model.clone(),
                reranker_timeout_seconds: config.models.reranker.timeout_seconds,
                // PATH only — never the key inside the file.
                reranker_api_key_file_path: config
                    .models
                    .reranker
                    .api_key_file_path
                    .as_deref()
                    .map(path_string),
                // PATH only — never the key inside the file.
                annotator_api_key_file_path: config
                    .models
                    .annotator
                    .api_key_file_path
                    .as_deref()
                    .map(path_string),
                annotator_annotation_max_retries: config.models.annotator.annotation_max_retries,
                annotator_max_input_chars: config.models.annotator.max_input_chars,
                annotator_max_completion_tokens: config.models.annotator.max_completion_tokens,
                annotator_annotation_retry_interval_seconds: config
                    .models
                    .annotator
                    .annotation_retry_interval_seconds,
                annotator_execution_max_retries: config.models.annotator.execution_max_retries,
                annotator_execution_retry_initial_delay_seconds: config
                    .models
                    .annotator
                    .execution_retry_initial_delay_seconds,
                annotator_execution_retry_max_delay_seconds: config
                    .models
                    .annotator
                    .execution_retry_max_delay_seconds,
            },
            policy_entity_match_file_path: path_string(&config.policies.entity_match_file_path),
            policy_annotator_naming_file_path: path_string(
                &config.policies.annotator_naming_file_path,
            ),
        }
    }
}

/// Render a path as a UTF-8 string for the configuration-identity projection.
/// `to_string_lossy` is acceptable here: the value feeds a hash and an audit
/// record, not a filesystem operation, and config validation already requires
/// these paths, so a non-UTF-8 component degrades to a stable replacement
/// rather than failing snapshot identity.
fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
