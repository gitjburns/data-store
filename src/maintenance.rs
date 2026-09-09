//! Process-wide storage admission for an explicit rebuild-all. A lease covers
//! the whole operation, including detached descendants, rather than one SQL call.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing::{error, info};

use crate::{error::ApiError, state::ShutdownSignal};

/// Shared admission and exclusive rebuild ownership, independent of corpus storage.
#[derive(Debug, Default)]
pub(crate) struct MaintenanceGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct GateState {
    active: usize,
    rebuilding: bool,
    stopped: bool,
    detail: Option<String>,
    generation: u64,
}

/// Clones share one counted admission; the final descendant releases it.
#[derive(Debug, Clone)]
pub(crate) struct MaintenancePermit(Arc<Lease>);

#[derive(Debug)]
struct Lease {
    gate: Arc<MaintenanceGate>,
    generation: u64,
}

impl MaintenanceGate {
    /// Fail closed on lock poison: uncertain admission accounting cannot permit deletion.
    fn lock(&self) -> Result<MutexGuard<'_, GateState>, ApiError> {
        self.state.lock().map_err(|source| ApiError::InternalIo {
            message: format!("maintenance admission lock poisoned: {source}"),
        })
    }

    /// Admit a new HTTP operation only while normal storage access is enabled.
    pub(crate) fn enter(self: &Arc<Self>) -> Result<MaintenancePermit, ApiError> {
        let mut state = self.lock()?;
        if state.stopped || state.detail.is_some() {
            return Err(ApiError::ServiceUnavailable {
                message: state
                    .detail
                    .clone()
                    .unwrap_or_else(|| "service is shutting down".into()),
            });
        }
        Ok(self.admit(&mut state))
    }

    /// Count admission under the same lock that closes the gate for rebuild-all.
    fn admit(self: &Arc<Self>, state: &mut GateState) -> MaintenancePermit {
        state.active += 1;
        MaintenancePermit(Arc::new(Lease {
            gate: Arc::clone(self),
            generation: state.generation,
        }))
    }

    /// Sleep between cycles, park during maintenance, and wake immediately on a
    /// successful reset. Shutdown is polled because its latch has a separate condvar.
    pub(crate) fn worker_permit(
        self: &Arc<Self>,
        worker: &'static str,
        delay: Duration,
        generation: u64,
        shutdown: &ShutdownSignal,
    ) -> Result<Option<MaintenancePermit>, ApiError> {
        let started = Instant::now();
        let mut state = self.lock()?;
        let mut parked = false;
        loop {
            if state.stopped || shutdown.wait_timeout(Duration::ZERO) {
                return Ok(None);
            }
            if state.detail.is_none()
                && (state.generation != generation || started.elapsed() >= delay)
            {
                if parked {
                    info!(
                        event = "maintenance.worker_resumed",
                        worker, "worker storage access resumed"
                    );
                }
                return Ok(Some(self.admit(&mut state)));
            }
            if state.detail.is_some() && !parked {
                info!(
                    event = "maintenance.worker_parked",
                    worker, "worker parked between cycles"
                );
                parked = true;
            }
            let interval = if state.detail.is_some() {
                Duration::from_millis(100)
            } else {
                delay
                    .saturating_sub(started.elapsed())
                    .min(Duration::from_millis(100))
            };
            state = self
                .changed
                .wait_timeout(state, interval)
                .map_err(|source| ApiError::InternalIo {
                    message: format!("maintenance worker wait poisoned: {source}"),
                })?
                .0;
        }
    }

    /// Reserve the only rebuild owner and close admission before its durable marker is written.
    pub(crate) fn begin(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        if state.rebuilding || state.stopped {
            return Err(ApiError::ServiceUnavailable {
                message: "rebuild-all is already running or shutdown has started".into(),
            });
        }
        state.rebuilding = true;
        state.detail = Some("rebuild-all: draining storage work".into());
        self.changed.notify_all();
        Ok(())
    }

    /// Wait for admitted work to release all leases; no async executor is blocked.
    pub(crate) fn drain(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        while state.active != 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|source| ApiError::InternalIo {
                    message: format!("maintenance drain wait poisoned: {source}"),
                })?;
        }
        if state.stopped {
            return Err(ApiError::ServiceUnavailable {
                message: "shutdown interrupted rebuild-all before storage clearing".into(),
            });
        }
        state.detail = Some("rebuild-all: clearing stored data".into());
        Ok(())
    }

    /// Preflight publication before recording success, then reopen infallibly.
    /// The lock spans one bounded terminal SQL update so shutdown cannot win
    /// between the durable success marker and admission reopening.
    pub(crate) fn complete(
        &self,
        record_success: impl FnOnce() -> Result<(), ApiError>,
    ) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        if state.stopped {
            return Err(ApiError::ServiceUnavailable {
                message: "shutdown interrupted rebuild-all; explicit retry required".into(),
            });
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| ApiError::InternalIo {
                message: "maintenance generation exhausted".into(),
            })?;
        record_success()?;
        state.generation = generation;
        state.detail = None;
        state.rebuilding = false;
        self.changed.notify_all();
        Ok(())
    }

    /// Retain a failed or interrupted rebuild as a closed gate until an explicit retry.
    pub(crate) fn fail(&self, detail: String) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(source) => {
                error!(event = "maintenance.lock_poisoned", %source, "maintenance remains closed");
                source.into_inner()
            }
        };
        state.detail = Some(format!("rebuild-all requires explicit retry: {detail}"));
        state.rebuilding = false;
        self.changed.notify_all();
    }

    /// Expose maintenance without accessing the storage being cleared.
    pub(crate) fn detail(&self) -> Option<String> {
        match self.lock() {
            Ok(state) => state.detail.clone(),
            Err(error) => Some(error.to_string()),
        }
    }

    /// Stop admission and wake parked workers; an accepted rebuild retains its owner.
    pub(crate) fn stop(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        state.stopped = true;
        self.changed.notify_all();
        Ok(())
    }

    /// Keep main alive until an accepted destructive action has reached a terminal boundary.
    pub(crate) fn wait_for_rebuild(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        while state.rebuilding {
            state = self
                .changed
                .wait(state)
                .map_err(|source| ApiError::InternalIo {
                    message: format!("maintenance shutdown wait poisoned: {source}"),
                })?;
        }
        Ok(())
    }
}

impl MaintenancePermit {
    /// Workers discard local bookkeeping when the storage generation changes.
    pub(crate) fn generation(&self) -> u64 {
        self.0.generation
    }
}

impl Drop for Lease {
    /// Release admission only when both the request and all detached descendants have finished.
    fn drop(&mut self) {
        let mut state = match self.gate.state.lock() {
            Ok(state) => state,
            Err(source) => {
                error!(event = "maintenance.release_poisoned", %source, "maintenance remains closed after lease release");
                source.into_inner()
            }
        };
        state.active -= 1;
        self.gate.changed.notify_all();
    }
}
