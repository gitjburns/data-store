use std::{io, path::PathBuf};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
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
    Unauthorized { message: String },
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Serialize)]
struct ErrorDetail {
    message: String,
}

impl ApiError {
    /// Return the HTTP status code that corresponds to this error.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest { .. } | Self::SourceResolution { .. } => StatusCode::BAD_REQUEST,
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
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
}

impl IntoResponse for ApiError {
    /// Render service errors as explicit JSON API responses.
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorBody {
            error: ErrorDetail {
                message: self.to_string(),
            },
        };

        (status, Json(body)).into_response()
    }
}
