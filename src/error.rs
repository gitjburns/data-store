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

    #[error("{message}")]
    SourceResolution { message: String },

    #[error("{message}")]
    DoclingUnavailable { message: String },

    #[error("{message}")]
    DoclingConversion { message: String },

    #[error("{message}")]
    InternalIo { message: String },

    #[error("{message}")]
    UnitSplitting { message: String },

    #[error("{message}")]
    StorageInit { message: String },

    #[error("{message}")]
    StorageOperation { message: String },

    #[error("{message}")]
    BadRequest { message: String },

    #[error("{message}")]
    SourceAlreadyIngested { message: String },

    #[error("{message}")]
    PayloadTooLarge { message: String },

    #[error("{message}")]
    Unauthorized { message: String },

    #[error("{message}")]
    ServiceUnavailable { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationErrorDetail {
    pub status: u16,
    pub kind: String,
    message: String,
}

impl ApiError {
    /// Return the numeric HTTP status code for this error without depending on transport types.
    pub fn status_u16(&self) -> u16 {
        match self {
            Self::BadRequest { .. } | Self::SourceResolution { .. } => 400,
            Self::SourceAlreadyIngested { .. } => 409,
            Self::PayloadTooLarge { .. } => 413,
            Self::Unauthorized { .. } => 401,
            Self::ServiceUnavailable { .. } => 503,
            Self::DoclingConversion { .. } => 422,
            Self::ConfigRead { .. }
            | Self::ConfigParse { .. }
            | Self::InvalidConfig { .. }
            | Self::InvalidCli { .. }
            | Self::InferenceInit { .. }
            | Self::DoclingUnavailable { .. }
            | Self::InternalIo { .. }
            | Self::UnitSplitting { .. }
            | Self::StorageInit { .. }
            | Self::StorageOperation { .. } => 500,
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
            Self::DoclingUnavailable { .. } => "docling_unavailable",
            Self::DoclingConversion { .. } => "docling_conversion",
            Self::InternalIo { .. } => "internal_io",
            Self::UnitSplitting { .. } => "unit_splitting",
            Self::StorageInit { .. } => "storage_init",
            Self::StorageOperation { .. } => "storage_operation",
            Self::BadRequest { .. } => "bad_request",
            Self::SourceAlreadyIngested { .. } => "source_already_ingested",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Unauthorized { .. } => "unauthorized",
            Self::ServiceUnavailable { .. } => "service_unavailable",
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
