//! Worker-owned coverage accounting. Health and logs consume these observations;
//! they never decide whether an annotation is satisfied or should be scheduled.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::{
    error::ApiError,
    model::SemanticAnnotationType,
    state::AnnotationHealth,
    types::{
        AnnotationActivity, AnnotationDocumentProgress, AnnotationProgressCount,
        AnnotationTypeProgress, AnnotationWorkCounts,
    },
};

use super::worker;

/// A current-plan key belongs to exactly one state, independently of output rows.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkState {
    Completed,
    Pending,
    Running,
    Failed,
    RetryWaiting {
        started_at: Instant,
        duration: Duration,
    },
    Exhausted,
}

/// Per-source item identities stay on the serial worker; only compact snapshots
/// cross into its existing shared health slot. No model thread mutates coverage.
pub(super) struct DocumentProgress<'slot> {
    slot: &'slot Mutex<AnnotationHealth>,
    snapshot: AnnotationDocumentProgress,
    items: BTreeMap<String, (usize, WorkState)>,
    /// Index only waits, so live publication does not rescan all completed work.
    retry_waiting: BTreeSet<String>,
}

/// Represent an observed source whose plan has not yet been measured.
pub(super) fn unmeasured(
    source_id: &str,
    parse_id: &str,
    source_paths: &str,
) -> Result<AnnotationDocumentProgress, ApiError> {
    let paths = serde_json::from_str(source_paths).map_err(|source| ApiError::InternalIo {
        message: format!("invalid annotation source paths for {source_id}: {source}"),
    })?;
    Ok(AnnotationDocumentProgress {
        source_id: source_id.to_string(),
        parse_id: parse_id.to_string(),
        source_paths: paths,
        plan_id: None,
        measured_at: None,
        progress: AnnotationProgressCount::default(),
        work: AnnotationWorkCounts::default(),
        by_type: Vec::new(),
        activity: AnnotationActivity::Discovering,
        detail: None,
    })
}

impl<'slot> DocumentProgress<'slot> {
    /// Reconstruct coverage from the same content keys and required types used
    /// by dispatch. Duplicate keys are an accounting error, never extra work.
    pub(super) fn new(
        slot: &'slot Mutex<AnnotationHealth>,
        mut snapshot: AnnotationDocumentProgress,
        required_types: &[SemanticAnnotationType],
        planned: Vec<(String, SemanticAnnotationType, WorkState)>,
    ) -> Result<Self, ApiError> {
        for annotation_type in required_types {
            let wire =
                serde_json::to_value(annotation_type).map_err(|source| ApiError::InternalIo {
                    message: format!("cannot encode progress annotation type: {source}"),
                })?;
            let name = wire.as_str().ok_or_else(|| ApiError::InternalIo {
                message: "annotation type did not serialize as a name".to_string(),
            })?;
            snapshot.by_type.push(AnnotationTypeProgress {
                annotation_type: name.to_string(),
                progress: count(0, 0),
                work: AnnotationWorkCounts::default(),
            });
        }
        let mut items = BTreeMap::new();
        for (key, annotation_type, state) in planned {
            let index = required_types
                .iter()
                .position(|value| *value == annotation_type)
                .ok_or_else(|| ApiError::InternalIo {
                    message: format!(
                        "unrequired annotation type in progress plan for {}",
                        snapshot.parse_id
                    ),
                })?;
            if items.insert(key, (index, state)).is_some() {
                return Err(ApiError::InternalIo {
                    message: format!(
                        "duplicate annotation coverage key in progress plan for {}",
                        snapshot.parse_id
                    ),
                });
            }
            let group = &mut snapshot.by_type[index];
            group.progress.total = Some(group.progress.total.unwrap_or(0) + 1);
            *bucket(group, state) += 1;
        }
        // Identity follows required coverage, not model-call counts or producer
        // identity; changing a prompt does not invalidate committed coverage.
        snapshot.plan_id = Some(crate::canonical::canonical_sha256_hex_of(&(
            &snapshot.parse_id,
            required_types,
            items.keys().collect::<Vec<_>>(),
        ))?);
        let retry_waiting = items
            .iter()
            .filter(|(_, (_, state))| matches!(state, WorkState::RetryWaiting { .. }))
            .map(|(key, _)| key.clone())
            .collect();
        let mut progress = Self {
            slot,
            snapshot,
            items,
            retry_waiting,
        };
        progress.refresh();
        progress.settle();
        progress.publish();
        Ok(progress)
    }

    /// Carry an immutable committed-count snapshot into an entire producer wave.
    pub(super) fn count(&self) -> AnnotationProgressCount {
        self.snapshot.progress
    }

    /// Change one known plan item's state. Completion transitions are called only
    /// after the transaction commits, including successful empty and memo results.
    pub(super) fn transition(&mut self, key: &str, state: WorkState) -> Result<(), ApiError> {
        let (index, previous) = self
            .items
            .get_mut(key)
            .ok_or_else(|| ApiError::InternalIo {
                message: format!(
                    "annotation progress key is absent from parse {}",
                    self.snapshot.parse_id
                ),
            })?;
        if *previous != state {
            let group = &mut self.snapshot.by_type[*index];
            // Every key was inserted once; its prior bucket therefore owns one
            // item. Move, rather than accumulate, so retries cannot inflate totals.
            *bucket(group, *previous) -= 1;
            *bucket(group, state) += 1;
            *previous = state;
        }
        if matches!(state, WorkState::RetryWaiting { .. }) {
            self.retry_waiting.insert(key.to_string());
        } else {
            self.retry_waiting.remove(key);
        }
        self.refresh();
        if state == WorkState::Running {
            self.snapshot.activity = AnnotationActivity::Running;
            self.snapshot.detail = None;
        } else if self.snapshot.work.running == 0 {
            self.settle();
        }
        self.publish();
        Ok(())
    }

    /// Explain a boundary wait without changing any durable completion count.
    pub(super) fn activity(&mut self, activity: AnnotationActivity, detail: Option<String>) {
        self.snapshot.activity = activity;
        self.snapshot.detail = detail;
        self.publish();
    }

    /// Sum type counts instead of keeping an independently maintained document total.
    fn refresh(&mut self) {
        // Eligibility can expire while another chain runs. Reclassify observed
        // waits at each existing boundary before advancing the measurement time.
        // This index owns no work; non-waiting keys simply leave the index.
        self.retry_waiting.retain(|key| {
            let Some((index, state)) = self.items.get_mut(key) else {
                return false;
            };
            let WorkState::RetryWaiting {
                started_at,
                duration,
            } = *state
            else {
                return false;
            };
            if duration.saturating_sub(started_at.elapsed()).is_zero() {
                self.snapshot.by_type[*index].work.retry_waiting -= 1;
                self.snapshot.by_type[*index].work.failed += 1;
                *state = WorkState::Failed;
                false
            } else {
                true
            }
        });
        let mut completed = 0;
        let mut total = 0;
        let mut work = AnnotationWorkCounts::default();
        for group in &mut self.snapshot.by_type {
            let group_total = group.progress.total.unwrap_or(0);
            group.progress = count(group.progress.completed, group_total);
            completed += group.progress.completed;
            total += group_total;
            work.pending += group.work.pending;
            work.running += group.work.running;
            work.failed += group.work.failed;
            work.retry_waiting += group.work.retry_waiting;
            work.exhausted += group.work.exhausted;
        }
        self.snapshot.progress = count(completed, total);
        self.snapshot.work = work;
    }

    /// Classify idle work from measured coverage, without implying graph publication.
    fn settle(&mut self) {
        self.snapshot.activity = if self.snapshot.progress.total == Some(0) {
            AnnotationActivity::NoWork
        } else if self.snapshot.progress.total == Some(self.snapshot.progress.completed) {
            AnnotationActivity::Complete
        } else if self.snapshot.work.pending > 0 || self.snapshot.work.failed > 0 {
            AnnotationActivity::Pending
        } else if self.snapshot.work.retry_waiting > 0 {
            AnnotationActivity::RetryWait
        } else {
            AnnotationActivity::Exhausted
        };
        self.snapshot.detail = None;
    }

    /// Publish only this document under the existing worker health lock. Rebuild
    /// cannot clear the inventory until this worker releases storage admission.
    fn publish(&mut self) {
        self.refresh();
        if matches!(
            self.snapshot.activity,
            AnnotationActivity::Pending
                | AnnotationActivity::RetryWait
                | AnnotationActivity::Exhausted
        ) {
            self.settle();
        }
        self.snapshot.measured_at = worker::annotation_measured_at();
        worker::update_annotation_health(self.slot, |health| {
            if let Some(document) = health.documents.as_mut().and_then(|documents| {
                documents.iter_mut().find(|document| {
                    document.source_id == self.snapshot.source_id
                        && document.parse_id == self.snapshot.parse_id
                })
            }) {
                *document = self.snapshot.clone();
            }
        });
    }
}

impl Drop for DocumentProgress<'_> {
    /// An early error, cancellation, or unwind must not leave abandoned chains
    /// looking active. Durable completion is retained; unfinished work is recoverable.
    fn drop(&mut self) {
        let mut interrupted = false;
        for (index, state) in self.items.values_mut() {
            if *state == WorkState::Running {
                let group = &mut self.snapshot.by_type[*index];
                group.work.running -= 1;
                group.work.pending += 1;
                *state = WorkState::Pending;
                interrupted = true;
            }
        }
        if interrupted {
            self.refresh();
            self.snapshot.activity = AnnotationActivity::Stopped;
            self.snapshot.detail =
                Some("Uncommitted work stopped; coverage remains pending.".to_string());
            self.publish();
        }
    }
}

/// Resolve one exclusive bucket for initialization and state transitions.
fn bucket(group: &mut AnnotationTypeProgress, state: WorkState) -> &mut u64 {
    match state {
        WorkState::Completed => &mut group.progress.completed,
        WorkState::Pending => &mut group.work.pending,
        WorkState::Running => &mut group.work.running,
        WorkState::Failed => &mut group.work.failed,
        WorkState::RetryWaiting { .. } => &mut group.work.retry_waiting,
        WorkState::Exhausted => &mut group.work.exhausted,
    }
}

/// Floor to one decimal using integer arithmetic: incomplete work never rounds
/// to 100%, and an empty plan has no mathematically defined percentage.
fn count(completed: u64, total: u64) -> AnnotationProgressCount {
    AnnotationProgressCount {
        completed,
        total: Some(total),
        percentage: (total > 0)
            .then(|| ((u128::from(completed) * 1000 / u128::from(total)) as f64) / 10.0),
    }
}
