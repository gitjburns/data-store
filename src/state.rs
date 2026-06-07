use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::{error, info};

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
    model_call_gate: Arc<Semaphore>,
    admin_shutdown_token: String,
    shutdown_sender: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Debug)]
pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub struct ModelCallPermit {
    _permit: OwnedSemaphorePermit,
    operation_id: String,
    model_role: &'static str,
    call_purpose: &'static str,
    acquired_at: Instant,
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
        let model_call_gate = Arc::new(Semaphore::new(1));

        Self {
            config,
            inference,
            storage,
            ingest_admission,
            search_admission,
            model_call_gate,
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

    /// Wait for exclusive access to the shared accelerator-backed model runtimes.
    pub async fn acquire_model_call_gate(
        &self,
        operation_id: &str,
        model_role: &'static str,
        call_purpose: &'static str,
    ) -> Result<ModelCallPermit, ApiError> {
        let wait_started = Instant::now();
        info!(
            event = "model_gate.waiting",
            operation_id, model_role, call_purpose, "model execution gate wait started"
        );
        let permit = match self.model_call_gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(source) => {
                let error = ApiError::InferenceInit {
                    message: format!(
                        "model execution gate closed before {model_role} {call_purpose}: {source}"
                    ),
                };
                error!(
                    event = "model_gate.failed",
                    operation_id,
                    model_role,
                    call_purpose,
                    error = %error,
                    wait_ms = wait_started.elapsed().as_millis() as u64,
                    "model execution gate acquisition failed"
                );
                return Err(error);
            }
        };
        info!(
            event = "model_gate.acquired",
            operation_id,
            model_role,
            call_purpose,
            wait_ms = wait_started.elapsed().as_millis() as u64,
            "model execution gate acquired"
        );

        Ok(ModelCallPermit {
            _permit: permit,
            operation_id: operation_id.to_string(),
            model_role,
            call_purpose,
            acquired_at: Instant::now(),
        })
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
        info!(
            event = "shutdown.signal.requested",
            stage = "signal_requesting",
            "shutdown signal requested"
        );
        let mut sender = match self.shutdown_sender.lock() {
            Ok(sender) => sender,
            Err(source) => {
                error!(
                    event = "shutdown.signal.failed",
                    stage = "signal_locking",
                    error = %source,
                    "shutdown signal lock failed"
                );
                return Err(ApiError::InternalIo {
                    message: format!("shutdown signal lock is poisoned: {source}"),
                });
            }
        };
        let Some(sender) = sender.take() else {
            info!(
                event = "shutdown.signal.already_requested",
                stage = "signal_sender_absent",
                "shutdown signal was already requested"
            );
            return Ok(());
        };

        if sender.send(()).is_err() {
            error!(
                event = "shutdown.signal.failed",
                stage = "signal_sending",
                "shutdown signal receiver was unavailable"
            );
            return Err(ApiError::InternalIo {
                message: "failed to signal service shutdown".to_string(),
            });
        }
        info!(
            event = "shutdown.signal.sent",
            stage = "signal_sent",
            "shutdown signal sent"
        );
        Ok(())
    }

    /// Return current service health and readiness diagnostics.
    pub fn health(&self) -> HealthResponse {
        let inference_component = match &self.inference {
            Ok(runtime) => HealthComponent {
                name: "inference".to_string(),
                ready: true,
                details: readiness_details("readiness-critical", runtime.health_details()),
            },
            Err(error) => HealthComponent {
                name: "inference".to_string(),
                ready: false,
                details: readiness_details("readiness-critical", vec![error.to_string()]),
            },
        };
        let storage_component = match &self.storage {
            Ok(runtime) => HealthComponent {
                name: "storage_cache".to_string(),
                ready: true,
                details: readiness_details("readiness-critical", runtime.health_details()),
            },
            Err(error) => HealthComponent {
                name: "storage_cache".to_string(),
                ready: false,
                details: readiness_details("readiness-critical", vec![error.to_string()]),
            },
        };
        let ingest_admission = self.ingest_admission_snapshot();
        let search_admission = self.search_admission_snapshot();
        let admission_component = HealthComponent {
            name: "admission".to_string(),
            ready: true,
            details: readiness_details("diagnostic-only", vec![
                format!(
                    "ingest in-flight {}/{}",
                    ingest_admission.in_flight, ingest_admission.max_in_flight
                ),
                format!(
                    "search in-flight {}/{}",
                    search_admission.in_flight, search_admission.max_in_flight
                ),
                "saturated ingest/search requests fail fast with 503 instead of changing service readiness".to_string(),
            ]),
        };
        let logging_component = HealthComponent {
            name: "logging".to_string(),
            ready: true,
            details: readiness_details(
                "diagnostic-only",
                vec![
                    format!(
                        "file logging initialized before HTTP bind: {}",
                        self.config.logging.resolved_file_path().display()
                    ),
                    format!("level {}", self.config.logging.level.as_str()),
                ],
            ),
        };
        // Only components that gate ingest/search readiness determine the
        // top-level flag. Diagnostic-only components remain visible without
        // making a running service appear unavailable.
        let ready = inference_component.ready && storage_component.ready;
        let components = vec![
            inference_component,
            storage_component,
            admission_component,
            logging_component,
        ];

        HealthResponse {
            service: "data-store".to_string(),
            ready,
            components,
        }
    }
}

impl Drop for ModelCallPermit {
    /// Log the release side of the model-call boundary when the exclusive permit leaves scope.
    fn drop(&mut self) {
        info!(
            event = "model_gate.released",
            operation_id = %self.operation_id,
            model_role = self.model_role,
            call_purpose = self.call_purpose,
            held_ms = self.acquired_at.elapsed().as_millis() as u64,
            "model execution gate released"
        );
    }
}

/// Prefix health details with the component's readiness role for operators.
fn readiness_details(role: &str, details: Vec<String>) -> Vec<String> {
    let mut output = Vec::with_capacity(details.len() + 1);
    output.push(format!("role: {role}"));
    output.extend(details);
    output
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

/// Compare one submitted token against the expected token without short-circuiting mismatched bytes.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for (index, right_value) in right.iter().enumerate() {
        let left_value = left.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_value ^ right_value);
    }

    diff == 0
}
