use std::{io, path::PathBuf};

use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("failed to read config at {path}: {source}")]
    ConfigRead { path: PathBuf, source: io::Error },

    #[error("failed to parse config at {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[error("invalid config: {message}")]
    InvalidConfig { message: String },

    #[error("invalid command line: {message}")]
    InvalidCli { message: String },

    #[error("inference initialization failed: {message}")]
    InferenceInit { message: String },

    // Raised when resolving/containing a parser-input source path fails
    // (src/source.rs path containment, and the scheduler's source reads).
    #[error("{message}")]
    SourceResolution { message: String },

    #[error("{message}")]
    InternalIo { message: String },

    // Constructed during chunk token counting (chunk::build_chunks ->
    // count_tokens in projections/chunk.rs) when a unit cannot be split.
    #[error("{message}")]
    UnitSplitting { message: String },

    #[error("{message}")]
    StorageInit { message: String },

    #[error("{message}")]
    StorageOperation { message: String },

    // Annotation-producer boundary (CA): external LLM call failures and
    // malformed model output; the message preserves endpoint/model context.
    #[error("{message}")]
    AnnotationProducer { message: String },

    #[error("{message}")]
    BadRequest { message: String },

    #[error("{message}")]
    PayloadTooLarge { message: String },

    #[error("{message}")]
    Unauthorized { message: String },

    // NotFound-class (HTTP 404). The C10a inspection and polling surface maps an
    // absent addressed resource to this variant: GET /operations/{id} when the
    // operation row is absent, GET /units/{id}(/relationships) when the derived
    // parse is not the source's active parse or the unit has no row (§14 makes a
    // non-active parse's units non-queryable — indistinguishable from absence at
    // the API), and GET /sources/{id} when no such source exists. It is a client
    // boundary, not an internal fault, so it carries no server-error logging.
    #[error("{message}")]
    NotFound { message: String },

    #[error("{message}")]
    ServiceUnavailable { message: String },

    // Retryable query rejection when the targeted source is mid-cutover
    // (spec §31.1): the query is rejected, never partially executed. Client
    // retry is sufficient because barrier holds last milliseconds. The
    // `From<crate::state::CutoverBarrierActive>` mapping that builds this
    // variant lives in `state.rs` (beside the source type and its `Display`),
    // so `error.rs` stays free of a `crate::state` reference — the
    // `colbert-diagnostic` bin `#[path]`-includes `error.rs` WITHOUT a `state`
    // module, and a `crate::state` path here would not resolve in that crate.
    #[error("{message}")]
    CutoverBarrierActive { message: String },

    // Snapshot verification failure (spec §30.5 / §31.2 step 4). Constructed by
    // the mechanical and deletion-gate tiers in `snapshot::verify` when a
    // manifest is incomplete, a referenced artifact's bytes no longer hash to
    // their recorded hash, or a deterministic index rebuild disagrees with the
    // captured hot state. It is an internal lifecycle-integrity failure (500),
    // like the other internal integrity errors above: the gate halts the
    // affected source's lifecycle before superseded state is deleted. Retention
    // of superseded state and the no-auto-retry rule are the CALLER's duties
    // (C9d), not this variant's.
    #[error("{message}")]
    SnapshotVerificationFailed { message: String },

    // Restore failure (spec §31.3 rollback-is-restore). Constructed in
    // restore.rs: re-import of canonical rows and projection payloads, a
    // deterministic index rebuild, or the snapshot lookup/manifest read failing
    // on the restore path.
    #[error("{message}")]
    RestoreFailed { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationErrorDetail {
    pub status: u16,
    pub kind: String,
    /// The transport may apply its configured presentation bound without altering the original error.
    pub(crate) message: String,
}

impl ApiError {
    /// Return the numeric HTTP status code for this error without depending on transport types.
    pub fn status_u16(&self) -> u16 {
        match self {
            Self::BadRequest { .. } | Self::SourceResolution { .. } => 400,
            Self::PayloadTooLarge { .. } => 413,
            Self::Unauthorized { .. } => 401,
            Self::NotFound { .. } => 404,
            Self::ServiceUnavailable { .. } | Self::CutoverBarrierActive { .. } => 503,
            Self::ConfigRead { .. }
            | Self::ConfigParse { .. }
            | Self::InvalidConfig { .. }
            | Self::InvalidCli { .. }
            | Self::InferenceInit { .. }
            | Self::InternalIo { .. }
            | Self::UnitSplitting { .. }
            | Self::StorageInit { .. }
            | Self::StorageOperation { .. }
            | Self::AnnotationProducer { .. }
            | Self::SnapshotVerificationFailed { .. }
            | Self::RestoreFailed { .. } => 500,
        }
    }

    /// Return a stable error-kind label for service logs.
    pub fn error_kind(&self) -> &'static str {
        match self {
            Self::ConfigRead { .. } => "config_read",
            Self::ConfigParse { .. } => "config_parse",
            Self::InvalidConfig { .. } => "invalid_config",
            Self::InvalidCli { .. } => "invalid_cli",
            Self::InferenceInit { .. } => "inference_init",
            Self::SourceResolution { .. } => "source_resolution",
            Self::InternalIo { .. } => "internal_io",
            Self::UnitSplitting { .. } => "unit_splitting",
            Self::StorageInit { .. } => "storage_init",
            Self::StorageOperation { .. } => "storage_operation",
            Self::AnnotationProducer { .. } => "annotation_producer",
            Self::BadRequest { .. } => "bad_request",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Unauthorized { .. } => "unauthorized",
            Self::NotFound { .. } => "not_found",
            Self::ServiceUnavailable { .. } => "service_unavailable",
            Self::CutoverBarrierActive { .. } => "cutover_barrier_active",
            Self::SnapshotVerificationFailed { .. } => "snapshot_verification_failed",
            Self::RestoreFailed { .. } => "restore_failed",
        }
    }

    /// Build the structured error payload used by HTTP errors and operation streams.
    pub fn operation_error_detail(&self) -> OperationErrorDetail {
        OperationErrorDetail {
            status: self.status_u16(),
            kind: self.error_kind().to_string(),
            message: self.to_string(),
        }
    }
}
