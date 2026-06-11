use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

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
    model_call_gate: Arc<ExclusiveGate>,
    admin_shutdown_token: String,
    shutdown_signal: Arc<ShutdownSignal>,
}

#[derive(Debug)]
pub struct AdmissionPermit {
    in_flight: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct ModelCallPermit {
    gate: Arc<ExclusiveGate>,
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
    in_flight: Arc<AtomicUsize>,
}

/// Cross-thread shutdown latch signaled once by the protected shutdown operation.
#[derive(Debug, Default)]
pub struct ShutdownSignal {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Exclusive waiting gate that serializes accelerator-backed model calls across operations.
#[derive(Debug)]
struct ExclusiveGate {
    busy: Mutex<bool>,
    released: Condvar,
}

impl AppState {
    /// Build shared application state for HTTP handlers.
    pub fn new(
        config: ServiceConfig,
        inference: Result<InferenceRuntime, ApiError>,
        storage: Result<StorageRuntime, ApiError>,
        admin_shutdown_token: String,
        shutdown_signal: Arc<ShutdownSignal>,
    ) -> Self {
        let ingest_admission = AdmissionGate::new(config.server.max_in_flight_ingest);
        let search_admission = AdmissionGate::new(config.server.max_in_flight_search);
        let model_call_gate = Arc::new(ExclusiveGate::new());

        Self {
            config,
            inference,
            storage,
            ingest_admission,
            search_admission,
            model_call_gate,
            admin_shutdown_token,
            shutdown_signal,
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
    pub fn acquire_model_call_gate(
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
        if let Err(source) = self.model_call_gate.acquire() {
            let error = ApiError::InferenceInit {
                message: format!(
                    "model execution gate failed before {model_role} {call_purpose}: {source}"
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
        info!(
            event = "model_gate.acquired",
            operation_id,
            model_role,
            call_purpose,
            wait_ms = wait_started.elapsed().as_millis() as u64,
            "model execution gate acquired"
        );

        Ok(ModelCallPermit {
            gate: Arc::clone(&self.model_call_gate),
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
        match self.shutdown_signal.request() {
            Ok(true) => {
                info!(
                    event = "shutdown.signal.sent",
                    stage = "signal_sent",
                    "shutdown signal sent"
                );
                Ok(())
            }
            Ok(false) => {
                info!(
                    event = "shutdown.signal.already_requested",
                    stage = "signal_already_requested",
                    "shutdown signal was already requested"
                );
                Ok(())
            }
            Err(source) => {
                error!(
                    event = "shutdown.signal.failed",
                    stage = "signal_locking",
                    error = %source,
                    "shutdown signal lock failed"
                );
                Err(ApiError::InternalIo { message: source })
            }
        }
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

impl Drop for AdmissionPermit {
    /// Return the held admission slot when the permit leaves scope.
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for ModelCallPermit {
    /// Release exclusive model access and log the release side of the model-call boundary.
    fn drop(&mut self) {
        info!(
            event = "model_gate.released",
            operation_id = %self.operation_id,
            model_role = self.model_role,
            call_purpose = self.call_purpose,
            held_ms = self.acquired_at.elapsed().as_millis() as u64,
            "model execution gate released"
        );
        self.gate.release();
    }
}

impl ShutdownSignal {
    /// Mark shutdown as requested and wake all waiters; returns false when already requested.
    pub fn request(&self) -> Result<bool, String> {
        let mut requested = self
            .requested
            .lock()
            .map_err(|source| format!("shutdown signal lock is poisoned: {source}"))?;
        if *requested {
            return Ok(false);
        }
        *requested = true;
        self.changed.notify_all();
        Ok(true)
    }

    /// Block the calling thread until shutdown has been requested.
    pub fn wait(&self) {
        // A poisoned latch means a holder panicked while flipping the flag.
        // Proceeding to shutdown keeps the failure visible instead of leaving
        // the server waiting forever on a broken latch.
        let mut requested = match self.requested.lock() {
            Ok(requested) => requested,
            Err(_poisoned) => {
                error!(
                    event = "shutdown.signal.lock_poisoned",
                    stage = "signal_waiting",
                    "shutdown signal lock was poisoned while waiting; proceeding to shutdown"
                );
                return;
            }
        };
        while !*requested {
            match self.changed.wait(requested) {
                Ok(guard) => requested = guard,
                Err(_poisoned) => {
                    error!(
                        event = "shutdown.signal.lock_poisoned",
                        stage = "signal_waiting",
                        "shutdown signal wait was poisoned; proceeding to shutdown"
                    );
                    return;
                }
            }
        }
    }
}

impl ExclusiveGate {
    /// Create an idle gate.
    fn new() -> Self {
        Self {
            busy: Mutex::new(false),
            released: Condvar::new(),
        }
    }

    /// Block the calling thread until exclusive access is acquired.
    ///
    /// The Result is kept for the caller's diagnostic error path even though
    /// poison is recovered on every branch.
    fn acquire(&self) -> Result<(), String> {
        // The bool guarded by this lock stays valid after a holder panic, so
        // poison is recovered on acquire and release alike; std poison is
        // sticky, and failing here would turn one panic into permanent
        // model-call failures.
        let mut busy = match self.busy.lock() {
            Ok(busy) => busy,
            Err(poisoned) => {
                error!(
                    event = "model_gate.lock_poisoned",
                    stage = "gate_acquiring",
                    "model gate lock was poisoned during acquire; recovering"
                );
                poisoned.into_inner()
            }
        };
        while *busy {
            busy = match self.released.wait(busy) {
                Ok(busy) => busy,
                Err(poisoned) => {
                    error!(
                        event = "model_gate.lock_poisoned",
                        stage = "gate_waiting",
                        "model gate wait was poisoned; recovering"
                    );
                    poisoned.into_inner()
                }
            };
        }
        *busy = true;
        Ok(())
    }

    /// Release exclusive access and wake one waiting acquirer.
    fn release(&self) {
        let mut busy = match self.busy.lock() {
            Ok(busy) => busy,
            Err(poisoned) => {
                // The poisoned lock still holds a valid bool; recover it so a
                // panicked holder cannot deadlock every later model call.
                error!(
                    event = "model_gate.lock_poisoned",
                    stage = "gate_releasing",
                    "model gate lock was poisoned during release; recovering"
                );
                poisoned.into_inner()
            }
        };
        *busy = false;
        self.released.notify_one();
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
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Acquire one permit immediately or fail with a visible saturation diagnostic.
    fn try_acquire(&self, operation: &'static str) -> Result<AdmissionPermit, ApiError> {
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.max_in_flight {
                return Err(ApiError::ServiceUnavailable {
                    message: format!(
                        "{operation} admission saturated: in-flight {current}/{}",
                        self.max_in_flight
                    ),
                });
            }
            // Compare-exchange keeps admission fail-fast and lock-free: a lost
            // race retries against the observed count instead of waiting.
            match self.in_flight.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(AdmissionPermit {
                        in_flight: Arc::clone(&self.in_flight),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Capture exact current gate counters without mutating admission state.
    fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            max_in_flight: self.max_in_flight,
            in_flight: self.in_flight.load(Ordering::Acquire),
        }
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
