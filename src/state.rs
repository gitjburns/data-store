use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

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
    ingest_admission: AdmissionGate,
    search_admission: AdmissionGate,
    admin_shutdown_token: String,
    shutdown_sender: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Debug)]
pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub struct AdmissionSnapshot {
    pub max_in_flight: usize,
    pub in_flight: usize,
}

#[derive(Debug)]
struct AdmissionGate {
    max_in_flight: usize,
    semaphore: Arc<Semaphore>,
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
        let ingest_admission = AdmissionGate::new(config.server.max_in_flight_ingest);
        let search_admission = AdmissionGate::new(config.server.max_in_flight_search);

        Self {
            config,
            inference,
            storage,
            ingest_admission,
            search_admission,
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

    /// Admit one ingest request without queuing when the configured concurrency budget is saturated.
    pub fn try_acquire_ingest_admission(&self) -> Result<AdmissionPermit, ApiError> {
        self.ingest_admission.try_acquire("ingest")
    }

    /// Admit one search request without queuing when the configured concurrency budget is saturated.
    pub fn try_acquire_search_admission(&self) -> Result<AdmissionPermit, ApiError> {
        self.search_admission.try_acquire("search")
    }

    /// Return current ingest admission counters for health and request diagnostics.
    pub fn ingest_admission_snapshot(&self) -> AdmissionSnapshot {
        self.ingest_admission.snapshot()
    }

    /// Return current search admission counters for health and request diagnostics.
    pub fn search_admission_snapshot(&self) -> AdmissionSnapshot {
        self.search_admission.snapshot()
    }

    /// Validate the startup-scoped admin token without exposing the expected value.
    pub fn authorize_admin_token(&self, candidate: &str) -> Result<(), ApiError> {
        if constant_time_eq(candidate.as_bytes(), self.admin_shutdown_token.as_bytes()) {
            return Ok(());
        }

        Err(ApiError::Unauthorized {
            message: "invalid admin token".to_string(),
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
        let ingest_admission = self.ingest_admission_snapshot();
        let search_admission = self.search_admission_snapshot();
        let admission_component = HealthComponent {
            name: "admission".to_string(),
            ready: true,
            details: vec![
                format!(
                    "ingest in-flight {}/{}",
                    ingest_admission.in_flight, ingest_admission.max_in_flight
                ),
                format!(
                    "search in-flight {}/{}",
                    search_admission.in_flight, search_admission.max_in_flight
                ),
            ],
        };
        let components = vec![inference_component, storage_component, admission_component];
        let ready = components.iter().all(|component| component.ready);

        HealthResponse {
            service: "data-store".to_string(),
            ready,
            components,
        }
    }
}

impl AdmissionGate {
    /// Create a concurrency gate with a fixed positive capacity from validated config.
    fn new(max_in_flight: u32) -> Self {
        let max_in_flight = max_in_flight as usize;

        Self {
            max_in_flight,
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
        }
    }

    /// Acquire one permit immediately or fail with a visible saturation diagnostic.
    fn try_acquire(&self, operation: &'static str) -> Result<AdmissionPermit, ApiError> {
        let permit = self.semaphore.clone().try_acquire_owned().map_err(|_| {
            ApiError::ServiceUnavailable {
                message: format!(
                    "{operation} admission saturated: in-flight {}/{}",
                    self.in_flight(),
                    self.max_in_flight
                ),
            }
        })?;

        Ok(AdmissionPermit { _permit: permit })
    }

    /// Capture exact current gate counters without mutating admission state.
    fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            max_in_flight: self.max_in_flight,
            in_flight: self.in_flight(),
        }
    }

    /// Compute current in-flight work from the semaphore's remaining permits.
    fn in_flight(&self) -> usize {
        self.max_in_flight
            .saturating_sub(self.semaphore.available_permits())
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
