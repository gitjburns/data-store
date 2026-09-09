//! Process-wide storage admission for an explicit rebuild-all. A lease covers
//! the whole operation, including detached descendants, rather than one SQL call.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{error, info};

use crate::{error::ApiError, state::ShutdownSignal};

/// Shared admission and exclusive rebuild ownership, independent of corpus storage.
#[derive(Debug)]
pub(crate) struct MaintenanceGate {
    state: Mutex<GateState>,
    changed: Condvar,
    annotation_cancellation: watch::Sender<Option<AnnotationCancelReason>>,
}

/// Cancellation is control flow, distinct from provider failures and retry debt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnnotationCancelReason {
    Rebuild,
    Shutdown,
    StoragePaused,
    OwnerDropped,
}

impl AnnotationCancelReason {
    /// Stable operator labels preserve the reason even across request/thread boundaries.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::Shutdown => "shutdown",
            Self::StoragePaused => "storage_paused",
            Self::OwnerDropped => "cancellation_owner_dropped",
        }
    }
}

/// Each request clones a receiver so cancellation wakes an otherwise idle HTTP
/// future. Storage leases still govern when it is safe to clear persisted data.
#[derive(Debug, Clone)]
pub(crate) struct AnnotationCancellation {
    receiver: watch::Receiver<Option<AnnotationCancelReason>>,
}

impl AnnotationCancellation {
    /// Check without blocking; a lost owner fails closed instead of leaving a request running.
    pub(crate) fn reason(&self) -> Option<AnnotationCancelReason> {
        if self.receiver.has_changed().is_err() {
            return Some(AnnotationCancelReason::OwnerDropped);
        }
        *self.receiver.borrow()
    }

    /// Await the same control state polled by synchronous worker checkpoints.
    pub(crate) async fn cancelled(&mut self) -> AnnotationCancelReason {
        loop {
            if let Some(reason) = self.reason() {
                return reason;
            }
            if self.receiver.changed().await.is_err() {
                return AnnotationCancelReason::OwnerDropped;
            }
        }
    }
}

impl Default for MaintenanceGate {
    /// The sender retains state even before an annotation client subscribes.
    fn default() -> Self {
        let (annotation_cancellation, _) = watch::channel(None);
        Self {
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
            annotation_cancellation,
        }
    }
}

#[derive(Debug, Default)]
struct GateState {
    active: usize,
    rebuilding: bool,
    stopped: bool,
    detail: Option<String>,
    // Startup and rebuild have independent owners. Completing either hold must
    // never reopen admission while the other still protects corpus state.
    startup_detail: Option<String>,
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
    /// Hold ordinary access before publishing HTTP or starting corpus workers.
    pub(crate) fn begin_startup(&self, delay_seconds: u64) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        state.startup_detail = Some(format!(
            "startup: waiting {delay_seconds} seconds before corpus initialization"
        ));
        Ok(())
    }

    /// Wait for the startup delay or a rebuild request, then admit initialization
    /// once maintenance allows it. Rebuild ends the timer; its completion wakes
    /// this same condvar, so no remaining delay postpones normal work.
    pub(crate) fn startup_permit(
        self: &Arc<Self>,
        delay_seconds: u64,
        shutdown: &ShutdownSignal,
    ) -> Result<Option<MaintenancePermit>, ApiError> {
        let started = Instant::now();
        let delay = Duration::from_secs(delay_seconds);
        let mut delay_pending = true;
        info!(
            event = "startup.delay_started",
            delay_seconds, "HTTP controls available; waiting before ordinary corpus work"
        );
        let mut state = self.lock()?;
        loop {
            if state.stopped || shutdown.wait_timeout(Duration::ZERO) {
                info!(
                    event = "startup.delay_cancelled",
                    reason = "shutdown",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "startup wait cancelled"
                );
                return Ok(None);
            }
            // Generation catches a rebuild that finished before this task ran;
            // a failed/interrupted rebuild also takes over startup's pause.
            let rebuild_seen = state.rebuilding || state.generation != 0;
            let remaining = delay.saturating_sub(started.elapsed());
            if delay_pending && (remaining.is_zero() || rebuild_seen || state.detail.is_some()) {
                delay_pending = false;
                let reason = if rebuild_seen {
                    "rebuild_all"
                } else if state.detail.is_some() {
                    "storage_paused"
                } else {
                    "elapsed"
                };
                info!(
                    event = "startup.delay_completed",
                    reason,
                    delay_seconds,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "startup delay ended; corpus admission follows maintenance state"
                );
            }
            if !delay_pending && state.detail.is_none() {
                state.startup_detail = Some("startup: initializing corpus".into());
                return Ok(Some(self.admit(&mut state)));
            }
            // Poll shutdown's separate latch without building an Instant deadline
            // from the unbounded configured seconds. Rebuild wakes this wait directly.
            let interval = if delay_pending {
                remaining.min(Duration::from_millis(100))
            } else {
                Duration::from_millis(100)
            };
            state = self
                .changed
                .wait_timeout(state, interval)
                .map_err(|source| ApiError::InternalIo {
                    message: format!("startup admission wait poisoned: {source}"),
                })?
                .0;
        }
    }

    /// Release startup alone; a concurrently accepted or failed rebuild retains
    /// its own hold and keeps both workers and ordinary HTTP requests paused.
    pub(crate) fn complete_startup(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        state.startup_detail = None;
        self.changed.notify_all();
        Ok(())
    }

    /// Subscribe without taking a storage lease; admitted work owns its lease separately.
    pub(crate) fn annotation_cancellation(&self) -> AnnotationCancellation {
        AnnotationCancellation {
            receiver: self.annotation_cancellation.subscribe(),
        }
    }

    /// Fail closed on lock poison: uncertain admission accounting cannot permit deletion.
    fn lock(&self) -> Result<MutexGuard<'_, GateState>, ApiError> {
        self.state.lock().map_err(|source| ApiError::InternalIo {
            message: format!("maintenance admission lock poisoned: {source}"),
        })
    }

    /// Admit a new HTTP operation only while normal storage access is enabled.
    pub(crate) fn enter(self: &Arc<Self>) -> Result<MaintenancePermit, ApiError> {
        let mut state = self.lock()?;
        if state.stopped || state.detail.is_some() || state.startup_detail.is_some() {
            return Err(ApiError::ServiceUnavailable {
                message: state
                    .detail
                    .clone()
                    .or_else(|| state.startup_detail.clone())
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
                && state.startup_detail.is_none()
                && (state.generation != generation || started.elapsed() >= delay)
            {
                if parked {
                    info!(
                        event = "maintenance.worker_resumed",
                        worker,
                        generation = state.generation,
                        wait_ms = started.elapsed().as_millis() as u64,
                        "worker storage access resumed"
                    );
                }
                return Ok(Some(self.admit(&mut state)));
            }
            if (state.detail.is_some() || state.startup_detail.is_some()) && !parked {
                info!(
                    event = "maintenance.worker_parked",
                    worker,
                    generation = state.generation,
                    active_storage_leases = state.active,
                    reason = state.detail.as_deref().or(state.startup_detail.as_deref()),
                    "worker parked between cycles"
                );
                parked = true;
            }
            let interval = if state.detail.is_some() || state.startup_detail.is_some() {
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
        self.annotation_cancellation
            .send_replace(Some(AnnotationCancelReason::Rebuild));
        info!(
            event = "maintenance.annotation_cancellation_requested",
            reason = "rebuild",
            "cancelling annotation requests before draining storage work"
        );
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
        // Drain proved every old lease was released. Only the new storage
        // generation may dispatch again after this reset of cancellation state.
        self.annotation_cancellation.send_replace(None);
        self.changed.notify_all();
        Ok(())
    }

    /// Pause after a startup storage failure while the caller holds its lease.
    /// An accepted rebuild may already be draining that lease: it owns recovery,
    /// and startup must never clear or replace its exclusive ownership.
    pub(crate) fn fail_startup_storage(&self, detail: String) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        if !state.rebuilding && !state.stopped {
            state.detail = Some(format!("rebuild-all requires explicit retry: {detail}"));
            self.annotation_cancellation
                .send_replace(Some(AnnotationCancelReason::StoragePaused));
        }
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
        self.annotation_cancellation
            .send_replace(Some(if state.stopped {
                AnnotationCancelReason::Shutdown
            } else {
                AnnotationCancelReason::StoragePaused
            }));
        self.changed.notify_all();
    }

    /// Expose the controlling hold without accessing storage. Rebuild/failure
    /// details take precedence over startup so operators know what blocks resume.
    pub(crate) fn detail(&self) -> Option<String> {
        match self.lock() {
            Ok(state) => state
                .detail
                .clone()
                .or_else(|| state.startup_detail.clone()),
            Err(error) => Some(error.to_string()),
        }
    }

    /// Stop admission and wake parked workers; an accepted rebuild retains its owner.
    pub(crate) fn stop(&self) -> Result<(), ApiError> {
        let mut state = self.lock()?;
        state.stopped = true;
        self.annotation_cancellation
            .send_replace(Some(AnnotationCancelReason::Shutdown));
        info!(
            event = "maintenance.annotation_cancellation_requested",
            reason = "shutdown",
            "cancelling outstanding annotation requests for shutdown"
        );
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
