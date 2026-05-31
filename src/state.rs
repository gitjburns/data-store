use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::{
    config::ServiceConfig,
    error::ApiError,
    inference::InferenceRuntime,
    storage::StorageRuntime,
    types::{HealthComponent, HealthResponse},
};

#[derive(Debug)]
pub struct AppState {
    pub config: ServiceConfig,
    inference: Result<InferenceRuntime, ApiError>,
    storage: Result<StorageRuntime, ApiError>,
    admin_shutdown_token: String,
    shutdown_sender: Mutex<Option<oneshot::Sender<()>>>,
}

impl AppState {
    /// Build shared application state for HTTP handlers.
    pub fn new(
        config: ServiceConfig,
        inference: Result<InferenceRuntime, ApiError>,
        storage: Result<StorageRuntime, ApiError>,
        admin_shutdown_token: String,
        shutdown_sender: oneshot::Sender<()>,
    ) -> Self {
        Self {
            config,
            inference,
            storage,
            admin_shutdown_token,
            shutdown_sender: Mutex::new(Some(shutdown_sender)),
        }
    }

    /// Return the initialized inference runtime or an explicit readiness error.
    pub fn inference(&self) -> Result<&InferenceRuntime, ApiError> {
        self.inference
            .as_ref()
            .map_err(|source| ApiError::InferenceInit {
                message: source.to_string(),
            })
    }

    /// Return the initialized storage runtime or an explicit readiness error.
    pub fn storage(&self) -> Result<&StorageRuntime, ApiError> {
        self.storage
            .as_ref()
            .map_err(|source| ApiError::StorageInit {
                message: source.to_string(),
            })
    }

    /// Validate the admin shutdown token without exposing the expected value.
    pub fn authorize_admin_token(&self, candidate: &str) -> Result<(), ApiError> {
        if constant_time_eq(candidate.as_bytes(), self.admin_shutdown_token.as_bytes()) {
            return Ok(());
        }

        Err(ApiError::Unauthorized {
            message: "invalid admin shutdown token".to_string(),
        })
    }

    /// Signal the HTTP server to drain active work and exit.
    pub fn request_shutdown(&self) -> Result<(), ApiError> {
        let mut sender = self
            .shutdown_sender
            .lock()
            .map_err(|source| ApiError::InternalIo {
                message: format!("shutdown signal lock is poisoned: {source}"),
            })?;
        let Some(sender) = sender.take() else {
            return Ok(());
        };

        sender.send(()).map_err(|_| ApiError::InternalIo {
            message: "failed to signal service shutdown".to_string(),
        })
    }

    /// Return current service health and readiness diagnostics.
    pub fn health(&self) -> HealthResponse {
        let inference_component = match &self.inference {
            Ok(runtime) => HealthComponent {
                name: "inference".to_string(),
                ready: true,
                details: runtime.health_details(),
            },
            Err(error) => HealthComponent {
                name: "inference".to_string(),
                ready: false,
                details: vec![error.to_string()],
            },
        };
        let storage_component = match &self.storage {
            Ok(runtime) => HealthComponent {
                name: "storage_cache".to_string(),
                ready: true,
                details: runtime.health_details(),
            },
            Err(error) => HealthComponent {
                name: "storage_cache".to_string(),
                ready: false,
                details: vec![error.to_string()],
            },
        };
        let components = vec![inference_component, storage_component];
        let ready = components.iter().all(|component| component.ready);

        HealthResponse {
            service: "data-store".to_string(),
            ready,
            components,
        }
    }
}

/// Compare secrets without data-dependent early return.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        diff |= left_value ^ right_value;
    }

    diff == 0
}
