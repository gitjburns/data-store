use std::{io, path::PathBuf};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;
use tracing::{error, warn};

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
    PayloadTooLarge { message: String },

    #[error("{message}")]
    Unauthorized { message: String },

    #[error("{message}")]
    ServiceUnavailable { message: String },
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationErrorDetail {
    pub status: u16,
    pub kind: String,
    message: String,
}

type ErrorDetail = OperationErrorDetail;

impl ApiError {
    /// Return the HTTP status code that corresponds to this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest { .. } | Self::SourceResolution { .. } => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::DoclingConversion { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::ConfigRead { .. }
            | Self::ConfigParse { .. }
            | Self::InvalidConfig { .. }
            | Self::InvalidCli { .. }
            | Self::InferenceInit { .. }
            | Self::DoclingUnavailable { .. }
            | Self::InternalIo { .. }
            | Self::UnitSplitting { .. }
            | Self::StorageInit { .. }
            | Self::StorageOperation { .. } => StatusCode::INTERNAL_SERVER_ERROR,
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
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Unauthorized { .. } => "unauthorized",
            Self::ServiceUnavailable { .. } => "service_unavailable",
        }
    }

    /// Build the structured error payload used by HTTP errors and operation streams.
    pub fn operation_error_detail(&self) -> OperationErrorDetail {
        OperationErrorDetail {
            status: self.status_code().as_u16(),
            kind: self.error_kind().to_string(),
            message: self.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    /// Render service errors as explicit JSON API responses.
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_kind = self.error_kind();
        let message = self.to_string();
        // Central response logging guarantees every failed HTTP request is
        // visible even when the failing stage returned before its completion log.
        if status.is_server_error() {
            error!(
                event = "api.error_response",
                status = status.as_u16(),
                error_kind,
                error = %message,
                "API error response"
            );
        } else {
            warn!(
                event = "api.error_response",
                status = status.as_u16(),
                error_kind,
                error = %message,
                "API error response"
            );
        }
        let body = ErrorBody {
            error: ErrorDetail {
                status: status.as_u16(),
                kind: error_kind.to_string(),
                message,
            },
        };

        (status, Json(body)).into_response()
    }
}
