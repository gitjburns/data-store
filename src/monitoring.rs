//! Worker-owned transient observations. The monitor never reads SQLite, parses
//! logs, drives scheduling, or treats a model response as a durable commit.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

pub use crate::monitoring_types::PUBLICATION_WORKER;
use crate::monitoring_types::*;
use crate::primitives::{current_time_ms, format_utc_timestamp_ms};
use crate::types::{
    AnnotationDocumentProgress, AnnotationProgressCount, AnnotationWorkCounts,
    ProjectionDocumentProgress, ProjectionPublicationCounts,
};

/// Inventory replacement retires issues only for this annotation observation owner.
pub const ANNOTATION_WORKER: &str = "annotations";

/// One storage context owns this observer; scoped worker handles share it without
/// extending any database, model-admission, or maintenance lease lifetime.
pub struct Monitoring {
    run_id: String,
    started: Instant,
    started_epoch_ms: Option<u64>,
    // One short lock makes a snapshot coherent across work, calls, and counters.
    // Database work, network I/O, and normal diagnostic writes stay outside it.
    data: Mutex<MonitorData>,
}

struct MonitorData {
    generation: u64,
    generation_started: Instant,
    next_id: u64,
    ingestion: IngestionMetrics,
    scan_wait_started: Option<Instant>,
    queue_measurement_started: Option<Instant>,
    source_measurement_started: Option<Instant>,
    annotations: AnnotationAggregate,
    publication: PublicationAggregate,
    work: BTreeMap<u64, ActiveWork>,
    calls: BTreeMap<u64, ActiveCall>,
    finished_calls: VecDeque<CallRecord>,
    model_stats: BTreeMap<(String, String), ModelAggregate>,
    rates: BTreeMap<String, VecDeque<RateBucket>>,
    issues: BTreeMap<String, OutstandingIssue>,
    recent: VecDeque<MonitorEvent>,
}

struct ActiveWork {
    observation: WorkObservation,
    started: Instant,
    progress_published: Instant,
    pending_progress: Option<AnnotationProgressCount>,
}

struct ActiveCall {
    work_id: u64,
    identity: WorkIdentity,
    role: String,
    model: String,
    stage: String,
    started: Instant,
    started_at: Option<String>,
    timeout_ms: Option<u64>,
    // Response metadata stays private until terminal accounting; it is not
    // a progress update or a claim that validation/persistence succeeded.
    received_usage: TokenUsage,
}

impl ActiveCall {
    /// Response usage stays unpublished until terminal accounting, even when the
    /// provider has returned but the owning call has not yet reported its result.
    fn observation(&self, id: u64, now: Instant) -> CallRecord {
        CallRecord {
            id,
            identity: self.identity.clone(),
            role: self.role.clone(),
            model: self.model.clone(),
            stage: self.stage.clone(),
            state: MonitorState::Running,
            started_at: self.started_at.clone(),
            ended_at: None,
            elapsed_ms: duration_ms(now.saturating_duration_since(self.started)),
            timeout_ms: self.timeout_ms,
            usage: TokenUsage::default(),
            detail: None,
        }
    }
}

/// Group only calls with identical ownership, provider role, stage, and deadline.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct CallGroupKey {
    identity: WorkIdentity,
    role: String,
    model: String,
    stage: String,
    timeout_ms: Option<u64>,
}

#[derive(Default)]
struct ModelAggregate {
    statistics: ModelStatistics,
    duration_ms: u128,
}

struct RateBucket {
    tick: u64,
    count: u64,
}

struct OutstandingIssue {
    observation: MonitorIssue,
    first_seen: Instant,
    recorded: Instant,
}

/// A guard owns the terminal outcome; cloned handles can report intermediate
/// boundaries but cannot keep a finished or pre-rebuild operation on screen.
pub struct WorkGuard {
    handle: WorkHandle,
    finished: bool,
}

/// Clones explicitly carry monitoring identity through existing scoped threads.
#[derive(Clone)]
pub struct WorkHandle {
    monitoring: Arc<Monitoring>,
    generation: u64,
    id: u64,
    // Canonical IDs become known during ingestion and must remain available to
    // cloned handles after the active row ends. Never hold this lock with data.
    identity: Arc<Mutex<WorkIdentity>>,
}

/// Each request reports exactly one terminal outcome, including unwinding and
/// early returns. This guard has no ownership of the real provider request.
pub struct CallGuard {
    monitoring: Arc<Monitoring>,
    generation: u64,
    id: Option<u64>,
}

#[derive(Default)]
struct CountAggregate {
    completed: u64,
    total: u64,
    unmeasured: u64,
}

#[derive(Default)]
struct TypeAggregate {
    contributors: u64,
    counts: CountAggregate,
    work: AnnotationWorkCounts,
}

// Retain only subtraction inputs, never whole documents, paths, or plan payloads.
struct AnnotationContribution {
    progress: AnnotationProgressCount,
    work: AnnotationWorkCounts,
    by_type: Vec<AnnotationTypeMetrics>,
}

#[derive(Default)]
struct AnnotationAggregate {
    measured: bool,
    documents: BTreeMap<String, AnnotationContribution>,
    counts: CountAggregate,
    work: AnnotationWorkCounts,
    by_type: BTreeMap<String, TypeAggregate>,
    memoized: u64,
    parked_reason: Option<String>,
    measured_at: Option<String>,
}

struct PublicationContribution {
    graph: ProjectionPublicationCounts,
    summary: ProjectionPublicationCounts,
    embeddings: Option<ProjectionPublicationCounts>,
}

#[derive(Default)]
struct PublicationAggregate {
    measured: bool,
    documents: BTreeMap<String, PublicationContribution>,
    graph: ProjectionPublicationCounts,
    summary: ProjectionPublicationCounts,
    embeddings: ProjectionPublicationCounts,
    unmeasured_embeddings: u64,
    measured_at: Option<String>,
}

impl CountAggregate {
    /// Signed replacement deltas keep per-document updates independent of corpus size.
    fn apply(&mut self, counts: &AnnotationProgressCount, add: bool) {
        adjust(&mut self.completed, counts.completed, add);
        match counts.total {
            Some(total) => adjust(&mut self.total, total, add),
            None => adjust(&mut self.unmeasured, 1, add),
        }
    }

    /// Unknown plans hide only the denominator; observed committed work remains visible.
    fn snapshot(&self, additional_unknown: bool) -> AnnotationProgressCount {
        progress_count(
            self.completed,
            (self.unmeasured == 0 && !additional_unknown).then_some(self.total),
        )
    }
}

impl AnnotationAggregate {
    /// Store one compact replacement contribution and adjust each affected type once.
    fn document(&mut self, document: &AnnotationDocumentProgress) {
        if let Some(previous) = self.documents.remove(&document.source_id) {
            self.apply(&previous, false);
        }
        let contribution = AnnotationContribution {
            progress: document.progress,
            work: document.work.clone(),
            by_type: document
                .by_type
                .iter()
                .map(|entry| AnnotationTypeMetrics {
                    annotation_type: entry.annotation_type.clone(),
                    progress: entry.progress,
                    work: entry.work.clone(),
                })
                .collect(),
        };
        self.apply(&contribution, true);
        self.documents
            .insert(document.source_id.clone(), contribution);
        self.measured = true;
        self.measured_at = document.measured_at.clone();
    }

    /// Apply the old and new contribution symmetrically, including unknown-plan counts.
    fn apply(&mut self, contribution: &AnnotationContribution, add: bool) {
        self.counts.apply(&contribution.progress, add);
        apply_work(&mut self.work, &contribution.work, add);
        for entry in &contribution.by_type {
            let aggregate = self
                .by_type
                .entry(entry.annotation_type.clone())
                .or_default();
            adjust(&mut aggregate.contributors, 1, add);
            aggregate.counts.apply(&entry.progress, add);
            apply_work(&mut aggregate.work, &entry.work, add);
            if aggregate.contributors == 0 {
                self.by_type.remove(&entry.annotation_type);
            }
        }
    }

    /// Copy only aggregate/type counts, so 200 ms polling never clones the corpus.
    fn snapshot(&self) -> AnnotationMetrics {
        AnnotationMetrics {
            documents: self.measured.then_some(self.documents.len() as u64),
            unmeasured_documents: self.counts.unmeasured,
            progress: self.counts.snapshot(!self.measured),
            work: self.work.clone(),
            by_type: self
                .by_type
                .iter()
                .map(|(name, aggregate)| AnnotationTypeMetrics {
                    annotation_type: name.clone(),
                    progress: aggregate.counts.snapshot(self.counts.unmeasured > 0),
                    work: aggregate.work.clone(),
                })
                .collect(),
            memoized: self.memoized,
            parked_reason: self.parked_reason.clone(),
            measured_at: self.measured_at.clone(),
        }
    }
}

impl PublicationAggregate {
    /// Replacing a captured source also replaces its parse's entire publication denominator.
    fn document(&mut self, document: &ProjectionDocumentProgress) {
        if let Some(previous) = self.documents.remove(&document.source_id) {
            self.apply(&previous, false);
        }
        let contribution = PublicationContribution {
            graph: document.graph,
            summary: document.summary,
            embeddings: document.embeddings,
        };
        self.apply(&contribution, true);
        self.documents
            .insert(document.source_id.clone(), contribution);
        self.measured = true;
        self.measured_at = document.measured_at.clone();
    }

    /// Missing embedding inventories do not erase measured cohorts or imply completion.
    fn apply(&mut self, contribution: &PublicationContribution, add: bool) {
        apply_publication(&mut self.graph, &contribution.graph, add);
        apply_publication(&mut self.summary, &contribution.summary, add);
        match contribution.embeddings {
            Some(counts) => apply_publication(&mut self.embeddings, &counts, add),
            None => adjust(&mut self.unmeasured_embeddings, 1, add),
        }
    }

    /// Preserve partial embedding counts while withholding a corpus denominator until measured.
    fn snapshot(&self) -> PublicationMetrics {
        let embeddings_measured = self.measured
            && (self.documents.is_empty()
                || self.unmeasured_embeddings < self.documents.len() as u64);
        PublicationMetrics {
            documents: self.measured.then_some(self.documents.len() as u64),
            graph: self.graph,
            graph_progress: publication_progress(&self.graph, self.measured),
            summary: self.summary,
            summary_progress: publication_progress(&self.summary, self.measured),
            embeddings: embeddings_measured.then_some(self.embeddings),
            embeddings_progress: embeddings_measured
                .then(|| publication_progress(&self.embeddings, self.unmeasured_embeddings == 0)),
            unmeasured_embedding_documents: self.unmeasured_embeddings,
            measured_at: self.measured_at.clone(),
        }
    }
}

/// Round to one decimal without ever claiming complete coverage for unfinished work.
pub(crate) fn progress_count(completed: u64, total: Option<u64>) -> AnnotationProgressCount {
    AnnotationProgressCount {
        completed,
        total,
        percentage: total.and_then(|total| {
            if total == 0 {
                None
            } else if completed >= total {
                Some(100.0)
            } else {
                Some(((completed as f64 / total as f64 * 1_000.0).round() / 10.0).min(99.9))
            }
        }),
    }
}

/// Publication states are mutually exclusive; only committed publication fills its bar.
fn publication_progress(
    counts: &ProjectionPublicationCounts,
    measured: bool,
) -> AnnotationProgressCount {
    progress_count(
        counts.published,
        measured.then_some(
            counts
                .published
                .saturating_add(counts.pending)
                .saturating_add(counts.failed),
        ),
    )
}

/// Arithmetic remains non-panicking even if a caller supplies counts near numeric limits.
fn adjust(target: &mut u64, amount: u64, add: bool) {
    *target = if add {
        target.saturating_add(amount)
    } else {
        target.saturating_sub(amount)
    };
}

/// Keep every unfinished annotation state on the same replacement-delta path.
fn apply_work(target: &mut AnnotationWorkCounts, value: &AnnotationWorkCounts, add: bool) {
    adjust(&mut target.pending, value.pending, add);
    adjust(&mut target.running, value.running, add);
    adjust(&mut target.failed, value.failed, add);
    adjust(&mut target.retry_waiting, value.retry_waiting, add);
    adjust(&mut target.exhausted, value.exhausted, add);
}

/// Coverage replacement never records throughput: remeasurement is not new publication.
fn apply_publication(
    target: &mut ProjectionPublicationCounts,
    value: &ProjectionPublicationCounts,
    add: bool,
) {
    adjust(&mut target.published, value.published, add);
    adjust(&mut target.pending, value.pending, add);
    adjust(&mut target.failed, value.failed, add);
}

/// Durations saturate at the wire type's limit instead of narrowing a u128 counter.
fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

impl MonitorData {
    /// A new generation starts unmeasured; cleared coverage must never resemble an empty corpus.
    fn new(generation: u64, now: Instant) -> Self {
        Self {
            generation,
            generation_started: now,
            next_id: 0,
            ingestion: IngestionMetrics::default(),
            scan_wait_started: None,
            queue_measurement_started: None,
            source_measurement_started: None,
            annotations: AnnotationAggregate::default(),
            publication: PublicationAggregate::default(),
            work: BTreeMap::new(),
            calls: BTreeMap::new(),
            finished_calls: VecDeque::new(),
            model_stats: BTreeMap::new(),
            rates: BTreeMap::new(),
            issues: BTreeMap::new(),
            recent: VecDeque::new(),
        }
    }

    /// IDs distinguish simultaneous identical work; a generation fence handles reset reuse.
    fn next_id(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }

    /// Recent events are explicitly expendable; outstanding work and problems are not.
    fn event(&mut self, mut event: MonitorEvent) {
        event.message = clean_text(&event.message);
        event.stage = clean_text(&event.stage);
        event.identity = clean_identity(event.identity);
        self.recent.push_front(event);
        self.recent.truncate(RECENT_EVENT_LIMIT);
    }

    /// Preserve an unresolved key's original age while accepting its latest retry
    /// schedule and detail. Clearing the key is the only way to start a new lifetime.
    fn issue(
        &mut self,
        key: String,
        mut observation: MonitorIssue,
        now: Instant,
        at: Option<String>,
    ) {
        let first_seen = if let Some(previous) = self.issues.get(&key) {
            observation.observed_since = previous.observation.observed_since.clone();
            previous.first_seen
        } else {
            observation.observed_since = at;
            now
        };
        observation.elapsed_ms = duration_ms(now.saturating_duration_since(first_seen));
        self.issues.insert(
            key,
            OutstandingIssue {
                observation,
                first_seen,
                recorded: now,
            },
        );
    }

    /// Terminal ownership is removed before accounting so a late/drop report cannot double count.
    fn finish_call(
        &mut self,
        id: u64,
        state: MonitorState,
        usage: &TokenUsage,
        detail: Option<&str>,
        now: Instant,
        at: Option<String>,
    ) -> Option<ActiveCall> {
        let call = self.calls.remove(&id)?;
        self.issues.remove(&call_issue_key(id));
        let usage = merge_usage(usage, &call.received_usage);
        let aggregate = self
            .model_stats
            .entry((call.role.clone(), call.model.clone()))
            .or_insert_with(|| ModelAggregate {
                statistics: ModelStatistics {
                    role: call.role.clone(),
                    model: call.model.clone(),
                    ..Default::default()
                },
                duration_ms: 0,
            });
        let stats = &mut aggregate.statistics;
        match state {
            MonitorState::Complete => stats.succeeded = stats.succeeded.saturating_add(1),
            MonitorState::Cancelled => stats.cancelled = stats.cancelled.saturating_add(1),
            _ => stats.failed = stats.failed.saturating_add(1),
        }
        let elapsed = duration_ms(now.saturating_duration_since(call.started));
        aggregate.duration_ms = aggregate.duration_ms.saturating_add(u128::from(elapsed));
        let calls = stats
            .succeeded
            .saturating_add(stats.failed)
            .saturating_add(stats.cancelled);
        stats.mean_duration_ms = Some(
            (aggregate.duration_ms / u128::from(calls.max(1))).min(u128::from(u64::MAX)) as u64,
        );
        stats.max_duration_ms = Some(
            stats
                .max_duration_ms
                .map_or(elapsed, |old| old.max(elapsed)),
        );
        add_usage(&mut stats.usage.prompt, usage.prompt);
        add_usage(&mut stats.usage.completion, usage.completion);
        add_usage(&mut stats.usage.reasoning, usage.reasoning);
        add_usage(&mut stats.usage.total, usage.total);
        // A missing field remains missing rather than becoming an invented zero.
        // Embedding responses have no generated completion/reasoning population.
        // Missing applicable fields still mark these displayed sums as partial.
        let requires_completion = !matches!(call.role.as_str(), "dense" | "colbert");
        if usage.prompt.is_none()
            || usage.total.is_none()
            || (requires_completion && usage.completion.is_none())
        {
            stats.usage_unavailable = stats.usage_unavailable.saturating_add(1);
        }
        // The call was removed under this same lock before accounting. Retain
        // exactly one terminal record even for calls that finish between polls.
        let mut record = call.observation(id, now);
        record.state = state;
        record.ended_at = at.clone();
        record.usage = usage;
        record.detail = detail.map(clean_text);
        self.finished_calls.push_front(record);
        self.finished_calls.truncate(CALL_HISTORY_LIMIT);
        self.event(MonitorEvent {
            at,
            identity: call.identity.clone(),
            stage: call.stage.clone(),
            state,
            message: detail.map(clean_text).unwrap_or_else(|| {
                format!(
                    "{} / {} request ended after {elapsed} ms",
                    call.role, call.model
                )
            }),
        });
        Some(call)
    }
}

impl WorkIdentity {
    /// Carry known canonical identities without inventing IDs before ingestion assigns them.
    pub fn new(
        worker: &str,
        document: &str,
        source_id: Option<&str>,
        parse_id: Option<&str>,
    ) -> Self {
        Self {
            worker: worker.to_owned(),
            document: document.to_owned(),
            source_id: source_id.map(str::to_owned),
            parse_id: parse_id.map(str::to_owned),
        }
    }
}

impl Monitoring {
    /// Initialize transient service-run state without requiring new operational configuration.
    pub fn new() -> Self {
        let started = Instant::now();
        let started_epoch_ms = match current_time_ms() {
            Ok(now) => Some(now),
            Err(source) => {
                error!(event = "monitor.clock_unavailable", error = %source,
                    "monitor wall-clock timestamps unavailable; monotonic elapsed times remain valid");
                None
            }
        };
        // Instant's opaque representation plus PID distinguishes successive runs
        // even when the wall clock moves backwards or is unavailable.
        Self {
            run_id: format!("{}:{started:?}", std::process::id()),
            started,
            started_epoch_ms,
            data: Mutex::new(MonitorData::new(1, started)),
        }
    }

    /// Give ingestion submeasurements the same clock/failure semantics as the monitor snapshot.
    pub fn observed_at(&self) -> Option<String> {
        self.timestamp(Instant::now())
    }

    /// Use a captured epoch plus monotonic elapsed time so retries/rates never follow clock jumps.
    fn timestamp(&self, now: Instant) -> Option<String> {
        let epoch = self.started_epoch_ms?;
        match format_utc_timestamp_ms(
            epoch.saturating_add(duration_ms(now.saturating_duration_since(self.started))),
        ) {
            Ok(timestamp) => Some(timestamp),
            Err(source) => {
                error!(event = "monitor.timestamp_failed", error = %source,
                    "monitor observation timestamp unavailable");
                None
            }
        }
    }

    /// Poison means an aggregate mutation may be incomplete. Reset observations visibly
    /// rather than presenting internally inconsistent counters or failing real work.
    fn lock(&self) -> MutexGuard<'_, MonitorData> {
        match self.data.lock() {
            Ok(data) => data,
            Err(poisoned) => {
                let mut data = poisoned.into_inner();
                let generation = data.generation.saturating_add(1);
                *data = MonitorData::new(generation, Instant::now());
                self.data.clear_poison();
                let now = Instant::now();
                data.issue("monitor:poisoned".to_owned(), MonitorIssue {
                        identity: monitor_identity(), stage: "observation state".to_owned(),
                        state: MonitorState::Unavailable,
                        message: "Monitoring state reset after a reporting panic; coverage awaits remeasurement".to_owned(),
                        affected: 1, observed_since: None, elapsed_ms: 0,
                        retry_in_ms: None, attempt: None, retry_limit: None,
                    }, now, self.timestamp(now));
                error!(
                    event = "monitor.lock_poisoned",
                    generation,
                    "monitor reporting lock poisoned; observations reset without changing worker outcomes"
                );
                data
            }
        }
    }

    /// Sample after lock acquisition so concurrent event publishers cannot append
    /// old rate buckets behind newer ones or appear newer than the returned snapshot.
    fn lock_observed(&self) -> (MutexGuard<'_, MonitorData>, Instant) {
        let data = self.lock();
        (data, Instant::now())
    }

    /// Rebuild callers must first park workers; outstanding observer handles are fenced
    /// from the new corpus even if their scope later unwinds or finishes.
    pub fn reset(&self) {
        let now = Instant::now();
        let (old, generation) = {
            let mut data = self.lock();
            let generation = data.generation.saturating_add(1);
            (
                std::mem::replace(&mut *data, MonitorData::new(generation, now)),
                generation,
            )
        };
        info!(
            event = "monitor.reset",
            generation,
            discarded_work = old.work.len(),
            discarded_calls = old.calls.len(),
            "monitor corpus observations and generation statistics reset"
        );
        // Potentially large inventory destruction happens after releasing the snapshot lock.
        drop(old);
    }

    /// Called after the failure reset commits and workers drain. Keep coverage and
    /// call history; refreshed inventories repopulate any still-applicable problems.
    pub fn clear_failure_observations(&self) {
        let removed = {
            let mut data = self.lock();
            let before = data.issues.len();
            data.issues.retain(|_, issue| {
                !matches!(
                    issue.observation.state,
                    MonitorState::Failed | MonitorState::Waiting
                )
            });
            before - data.issues.len()
        };
        info!(
            event = "monitor.failures_cleared",
            removed, "failure observations cleared after retry reset"
        );
    }

    /// Build only the bounded/active display state; no document inventory or database is read.
    pub fn snapshot(&self) -> MonitorSnapshot {
        let (mut data, now) = self.lock_observed();
        let sampled_at = self.timestamp(now);
        let work = data
            .work
            .values_mut()
            .map(|work| {
                work.publish_progress(now);
                let mut observation = work.observation.clone();
                observation.elapsed_ms = duration_ms(now.saturating_duration_since(work.started));
                observation
            })
            .collect();
        let calls = grouped_calls(data.calls.values());
        // Active calls cannot be evicted by completed traffic. Newest starts
        // lead the active block; terminal records follow in completion order.
        // Concurrent starts can acquire IDs out of timestamp order.
        let mut active_calls: Vec<_> = data.calls.iter().collect();
        active_calls.sort_unstable_by_key(|(id, call)| std::cmp::Reverse((call.started, **id)));
        let call_log = active_calls
            .into_iter()
            .map(|(id, call)| call.observation(*id, now))
            .chain(data.finished_calls.iter().cloned())
            .collect();
        let elapsed = duration_ms(now.saturating_duration_since(data.generation_started));
        let tick = elapsed / REFRESH_INTERVAL_MS;
        let rates = data
            .rates
            .iter_mut()
            .map(|(label, buckets)| {
                trim_rate(buckets, tick);
                let completed = buckets
                    .iter()
                    .fold(0_u64, |total, bucket| total.saturating_add(bucket.count));
                // Bucket alignment shortens the trailing window by at most 199 ms;
                // expose that actual duration instead of counting older work as 60 s.
                let window_ms = elapsed
                    .saturating_sub(oldest_rate_tick(tick) * REFRESH_INTERVAL_MS)
                    .max(1);
                WorkRate {
                    label: label.clone(),
                    completed,
                    window_ms,
                    per_minute: completed as f64 * 60_000.0 / window_ms as f64,
                }
            })
            .collect();
        let issues = data
            .issues
            .values()
            .map(|issue| {
                let mut observation = issue.observation.clone();
                observation.elapsed_ms =
                    duration_ms(now.saturating_duration_since(issue.first_seen));
                observation.retry_in_ms = observation.retry_in_ms.map(|wait| {
                    wait.saturating_sub(duration_ms(now.saturating_duration_since(issue.recorded)))
                });
                observation
            })
            .collect();
        let mut ingestion = data.ingestion.clone();
        if let Some(started) = data.scan_wait_started {
            ingestion.next_scan_wait_ms = ingestion.next_scan_wait_ms.map(|delay| {
                delay.saturating_sub(duration_ms(now.saturating_duration_since(started)))
            });
        }
        MonitorSnapshot {
            run_id: self.run_id.clone(),
            generation: data.generation,
            started_at: self.timestamp(self.started),
            sampled_at,
            uptime_ms: duration_ms(now.saturating_duration_since(self.started)),
            generation_elapsed_ms: elapsed,
            ready: false,
            status: "Service state not sampled".to_owned(),
            headline: String::new(),
            activity: String::new(),
            ingestion,
            annotations: data.annotations.snapshot(),
            publication: data.publication.snapshot(),
            work,
            calls,
            call_log,
            model_stats: data
                .model_stats
                .values()
                .map(|aggregate| aggregate.statistics.clone())
                .collect(),
            rates,
            issues,
            recent: data.recent.iter().cloned().collect(),
            corpus: Vec::new(),
            queries_in_flight: 0,
            query_limit: 0,
        }
    }

    /// Inventory replacement runs on its owning worker. Build outside the snapshot lock,
    /// and never convert rediscovered committed coverage into completion throughput.
    pub fn replace_annotations(
        &self,
        documents: &[AnnotationDocumentProgress],
        parked_reason: Option<String>,
    ) {
        let generation = self.lock().generation;
        let inventory: BTreeSet<_> = documents
            .iter()
            .map(|document| (document.source_id.as_str(), document.parse_id.as_str()))
            .collect();
        let mut replacement = AnnotationAggregate {
            measured: true,
            parked_reason: parked_reason.as_deref().map(clean_text),
            ..Default::default()
        };
        for document in documents {
            replacement.document(document);
        }
        replacement.measured_at = documents
            .iter()
            .filter_map(|document| document.measured_at.as_ref())
            .max()
            .cloned()
            .or_else(|| self.timestamp(Instant::now()));
        let old = {
            let mut data = self.lock();
            if data.generation != generation {
                return;
            }
            replacement.memoized = data.annotations.memoized;
            retain_inventory_issues(&mut data.issues, ANNOTATION_WORKER, &inventory);
            std::mem::replace(&mut data.annotations, replacement)
        };
        drop(old);
    }

    /// Update committed coverage in O(annotation types), independent of document inventory size.
    pub fn annotation_document(&self, document: &AnnotationDocumentProgress) {
        self.lock().annotations.document(document);
    }

    /// Preserve measured coverage while reporting why the annotation worker cannot advance.
    pub fn annotation_parked(&self, reason: Option<String>) {
        self.lock().annotations.parked_reason = reason.as_deref().map(clean_text);
    }

    /// Count actual memo reuse once at its owning boundary, not when health is reread.
    pub fn annotation_memoized(&self, count: u64) {
        let mut data = self.lock();
        data.annotations.memoized = data.annotations.memoized.saturating_add(count);
    }

    /// Replace publication inventory outside the hot lock; a reset invalidates an in-progress build.
    pub fn replace_publications(&self, documents: &[ProjectionDocumentProgress]) {
        let generation = self.lock().generation;
        let inventory: BTreeSet<_> = documents
            .iter()
            .map(|document| (document.source_id.as_str(), document.parse_id.as_str()))
            .collect();
        let mut replacement = PublicationAggregate {
            measured: true,
            ..Default::default()
        };
        for document in documents {
            replacement.document(document);
        }
        replacement.measured_at = documents
            .iter()
            .filter_map(|document| document.measured_at.as_ref())
            .max()
            .cloned()
            .or_else(|| self.timestamp(Instant::now()));
        let old = {
            let mut data = self.lock();
            if data.generation != generation {
                return;
            }
            retain_inventory_issues(&mut data.issues, PUBLICATION_WORKER, &inventory);
            std::mem::replace(&mut data.publication, replacement)
        };
        drop(old);
    }

    /// Apply one measured publication contribution without scanning completed documents.
    pub fn publication_document(&self, document: &ProjectionDocumentProgress) {
        self.lock().publication.document(document);
    }

    /// The closure must only mutate compact observation fields, never perform I/O or lock workers.
    pub fn update_ingestion(&self, update: impl FnOnce(&mut IngestionMetrics)) {
        let measured_at = self.timestamp(Instant::now());
        let mut data = self.lock();
        update(&mut data.ingestion);
        // Starting the next scan clears its sleep deadline. Queue/source updates
        // leave an ongoing sleep untouched, so unrelated measurements cannot extend it.
        if data.ingestion.next_scan_wait_ms.is_none() {
            data.scan_wait_started = None;
        }
        let counts = &mut data.ingestion.active_sources;
        *counts = progress_count(counts.completed, counts.total);
        data.ingestion.measured_at = measured_at;
    }

    /// The scheduler reports an actual sleep boundary once; snapshots derive remaining
    /// time without requiring timer callbacks or extending it on unrelated updates.
    pub fn schedule_scan_wait(&self, delay_ms: u64) {
        let now = Instant::now();
        let measured_at = self.timestamp(now);
        let mut data = self.lock();
        data.ingestion.next_scan_wait_ms = Some(delay_ms);
        data.scan_wait_started = Some(now);
        data.ingestion.measured_at = measured_at;
    }

    /// Coalesce optional autonomous enqueue measurements; actual claim/terminal and
    /// external-operation boundaries can still force their final authoritative sample.
    pub fn try_queue_measurement(&self) -> bool {
        let (mut data, now) = self.lock_observed();
        claim_measurement(&mut data.queue_measurement_started, now)
    }

    /// Source replay has a separate measurement clock so queue traffic cannot postpone
    /// source coverage. This gate never performs or schedules the database read itself.
    pub fn try_source_measurement(&self) -> bool {
        let (mut data, now) = self.lock_observed();
        claim_measurement(&mut data.source_measurement_started, now)
    }

    /// Retain outstanding work until its owner clears it; retry deadlines use elapsed time.
    pub fn set_issue(&self, key: String, mut issue: MonitorIssue) {
        issue.identity = clean_identity(issue.identity);
        issue.stage = clean_text(&issue.stage);
        issue.message = clean_text(&issue.message);
        let (mut data, now) = self.lock_observed();
        data.issue(key, issue, now, self.timestamp(now));
    }

    /// Resolving a real outstanding condition removes its persistent display row explicitly.
    pub fn clear_issue(&self, key: &str) {
        self.lock().issues.remove(key);
    }

    /// Owners retire work identities excluded by a newly measured plan. The closure
    /// must consult only that in-memory plan, never perform I/O under the monitor lock.
    pub fn retain_issues(&self, prefix: &str, mut keep: impl FnMut(&str) -> bool) {
        self.lock()
            .issues
            .retain(|key, _| !key.starts_with(prefix) || keep(key));
    }

    /// Count actual completed work, called only after the relevant commit/publication boundary.
    pub fn record_completed(
        &self,
        identity: WorkIdentity,
        stage: &str,
        message: &str,
        rate_label: &str,
        count: u64,
    ) {
        let (mut data, now) = self.lock_observed();
        let at = self.timestamp(now);
        data.event(MonitorEvent {
            at,
            identity,
            stage: clean_text(stage),
            state: MonitorState::Complete,
            message: clean_text(message),
        });
        let tick = duration_ms(now.saturating_duration_since(data.generation_started))
            / REFRESH_INTERVAL_MS;
        let buckets = data.rates.entry(rate_label.to_owned()).or_default();
        trim_rate(buckets, tick);
        match buckets.back_mut() {
            Some(bucket) if bucket.tick == tick => {
                bucket.count = bucket.count.saturating_add(count)
            }
            _ => buckets.push_back(RateBucket { tick, count }),
        }
    }

    /// Start one explicitly scoped operation; identical concurrent identities receive separate guards.
    pub fn work(
        self: &Arc<Self>,
        identity: WorkIdentity,
        stage: &str,
        total: Option<u64>,
        unit: &str,
    ) -> WorkGuard {
        let now = Instant::now();
        let started_at = self.timestamp(now);
        let identity = clean_identity(identity);
        let handle_identity = Arc::new(Mutex::new(identity.clone()));
        let mut data = self.lock();
        let id = data.next_id();
        data.work.insert(
            id,
            ActiveWork {
                observation: WorkObservation {
                    identity,
                    stage: clean_text(stage),
                    state: MonitorState::Running,
                    progress: Some(StageProgress {
                        counts: progress_count(0, total),
                        unit: clean_text(unit),
                    }),
                    started_at,
                    elapsed_ms: 0,
                    detail: None,
                },
                started: now,
                progress_published: now,
                pending_progress: None,
            },
        );
        WorkGuard {
            handle: WorkHandle {
                monitoring: Arc::clone(self),
                generation: data.generation,
                id,
                identity: handle_identity,
            },
            finished: false,
        }
    }
}

impl Default for Monitoring {
    /// Default construction has the same unmeasured semantics as explicit startup.
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Monitoring {
    /// Runtime settings diagnostics identify the observer without locking or dumping its content.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Monitoring")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Adding an absent provider field preserves absence until a response actually reports it.
fn add_usage(total: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *total = Some(total.unwrap_or(0).saturating_add(value));
    }
}

/// Both observations describe the same provider response. Explicit terminal fields
/// win; filling only absent fields avoids adding the same usage twice.
fn merge_usage(primary: &TokenUsage, received: &TokenUsage) -> TokenUsage {
    TokenUsage {
        prompt: primary.prompt.or(received.prompt),
        completion: primary.completion.or(received.completion),
        reasoning: primary.reasoning.or(received.reasoning),
        total: primary.total.or(received.total),
    }
}

/// A fixed number of 200 ms buckets bounds memory independently of completion frequency.
fn trim_rate(buckets: &mut VecDeque<RateBucket>, tick: u64) {
    let oldest = oldest_rate_tick(tick);
    while buckets.front().is_some_and(|bucket| bucket.tick < oldest) {
        buckets.pop_front();
    }
}

/// Include the current bucket and at most 299 earlier buckets in a 60 s window.
fn oldest_rate_tick(tick: u64) -> u64 {
    tick.saturating_sub((RATE_WINDOW_SECONDS * 1_000 / REFRESH_INTERVAL_MS).saturating_sub(1))
}

/// Monitoring failures carry their own identity and never masquerade as corpus failures.
fn monitor_identity() -> WorkIdentity {
    WorkIdentity {
        worker: "monitor".to_owned(),
        document: "service".to_owned(),
        source_id: None,
        parse_id: None,
    }
}

/// Group all active calls without retaining an unbounded history of completed request identities.
fn grouped_calls<'a>(calls: impl Iterator<Item = &'a ActiveCall>) -> Vec<CallGroup> {
    let mut groups: BTreeMap<CallGroupKey, (Instant, CallGroup)> = BTreeMap::new();
    for call in calls {
        let key = CallGroupKey {
            identity: call.identity.clone(),
            role: call.role.clone(),
            model: call.model.clone(),
            stage: call.stage.clone(),
            timeout_ms: call.timeout_ms,
        };
        let (oldest, group) = groups.entry(key).or_insert_with(|| {
            (
                call.started,
                CallGroup {
                    identity: call.identity.clone(),
                    role: call.role.clone(),
                    model: call.model.clone(),
                    stage: call.stage.clone(),
                    running: 0,
                    oldest_started_at: call.started_at.clone(),
                    timeout_ms: call.timeout_ms,
                },
            )
        });
        group.running = group.running.saturating_add(1);
        // Concurrent callers can acquire the observation lock out of start order.
        if call.started < *oldest {
            *oldest = call.started;
            group.oldest_started_at = call.started_at.clone();
        }
    }
    groups.into_values().map(|(_, group)| group).collect()
}

/// Advance only the selected measurement clock when its fixed publication period
/// elapsed; repeated denied attempts cannot indefinitely postpone the next sample.
fn claim_measurement(last_started: &mut Option<Instant>, now: Instant) -> bool {
    if last_started.is_none_or(|previous| {
        now.saturating_duration_since(previous) >= Duration::from_millis(REFRESH_INTERVAL_MS)
    }) {
        *last_started = Some(now);
        true
    } else {
        false
    }
}

impl ActiveWork {
    /// Coalesce rapid batch counters, but publish a completed measured stage immediately.
    fn publish_progress(&mut self, now: Instant) {
        let Some(pending) = self.pending_progress else {
            return;
        };
        let complete = pending
            .total
            .is_some_and(|total| pending.completed >= total);
        if complete
            || now.saturating_duration_since(self.progress_published)
                >= Duration::from_millis(REFRESH_INTERVAL_MS)
        {
            if let Some(progress) = &mut self.observation.progress {
                progress.counts = pending;
            }
            self.pending_progress = None;
            self.progress_published = now;
        }
    }
}

impl WorkHandle {
    /// Cloned scoped workers retain the latest known identity even after terminal observation.
    pub fn identity(&self) -> WorkIdentity {
        self.identity_lock().clone()
    }

    /// Identity replacement is one whole-value assignment, so poison recovery can safely
    /// retain the previous or replacement identity while leaving a durable diagnostic.
    fn identity_lock(&self) -> MutexGuard<'_, WorkIdentity> {
        match self.identity.lock() {
            Ok(identity) => identity,
            Err(poisoned) => {
                self.identity.clear_poison();
                error!(
                    event = "monitor.identity_lock_poisoned",
                    work_id = self.id,
                    generation = self.generation,
                    "monitor identity lock poisoned; retaining assigned identity"
                );
                poisoned.into_inner()
            }
        }
    }

    /// Attach identities at the point ingestion learns them; callers serialize assignments
    /// before dispatching dependent work, and display labels remain unchanged.
    pub fn identify(&self, source_id: Option<&str>, parse_id: Option<&str>) {
        let mut identity = self.identity();
        identity.source_id = source_id.map(str::to_owned);
        identity.parse_id = parse_id.map(str::to_owned);
        *self.identity_lock() = identity.clone();
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        if let Some(work) = data.work.get_mut(&self.id) {
            work.observation.identity = identity.clone();
        }
        for call in data
            .calls
            .values_mut()
            .filter(|call| call.work_id == self.id)
        {
            call.identity = identity.clone();
        }
    }

    /// A scanner can report its current pathname without starting a second work lifecycle.
    pub fn document(&self, document: &str) {
        let mut identity = self.identity();
        identity.document = clean_text(document);
        *self.identity_lock() = identity.clone();
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        if let Some(work) = data.work.get_mut(&self.id) {
            work.observation.identity = identity.clone();
        }
        for call in data
            .calls
            .values_mut()
            .filter(|call| call.work_id == self.id)
        {
            call.identity = identity.clone();
        }
    }

    /// A stage change resets its own measured unit/counts and cannot inherit queued progress.
    pub fn stage(&self, stage: &str, total: Option<u64>, unit: &str) {
        let now = Instant::now();
        let started_at = self.monitoring.timestamp(now);
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        let Some(work) = data.work.get_mut(&self.id) else {
            return;
        };
        work.observation.stage = clean_text(stage);
        work.observation.state = MonitorState::Running;
        work.observation.detail = None;
        work.observation.progress = Some(StageProgress {
            counts: progress_count(0, total),
            unit: clean_text(unit),
        });
        work.observation.started_at = started_at;
        work.started = now;
        work.progress_published = now;
        work.pending_progress = None;
    }

    /// Scoped parallel batches can report out of lock order. Within one unchanged
    /// denominator progress never regresses; stage() explicitly starts a new counter.
    pub fn progress(&self, completed: u64, total: Option<u64>) {
        let now = Instant::now();
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        let Some(work) = data.work.get_mut(&self.id) else {
            return;
        };
        let previous = work
            .pending_progress
            .or_else(|| work.observation.progress.as_ref().map(|value| value.counts));
        let completed = previous
            .filter(|value| value.total == total)
            .map_or(completed, |value| value.completed.max(completed));
        work.pending_progress = Some(progress_count(completed, total));
        work.publish_progress(now);
    }

    /// Waiting is owned by the actual admission/commit boundary, not inferred from elapsed time.
    pub fn waiting(&self, detail: &str) {
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        if let Some(work) = data.work.get_mut(&self.id) {
            work.observation.state = MonitorState::Waiting;
            work.observation.detail = Some(clean_text(detail));
        }
    }

    /// Clear an observed wait only after its owning boundary grants progress.
    pub fn running(&self) {
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        if let Some(work) = data.work.get_mut(&self.id) {
            work.observation.state = MonitorState::Running;
            work.observation.detail = None;
        }
    }

    /// Model calls report start and terminal outcome only; opaque internal generation
    /// does not become a percentage or require a monitor publication timer.
    pub fn call(&self, role: &str, model: &str, stage: &str, timeout_ms: Option<u64>) -> CallGuard {
        let now = Instant::now();
        let started_at = self.monitoring.timestamp(now);
        let mut guard = CallGuard {
            monitoring: Arc::clone(&self.monitoring),
            generation: self.generation,
            id: None,
        };
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return guard;
        }
        let Some(work) = data.work.get(&self.id) else {
            return guard;
        };
        let identity = work.observation.identity.clone();
        let id = data.next_id();
        data.calls.insert(
            id,
            ActiveCall {
                work_id: self.id,
                identity,
                role: clean_text(role),
                model: clean_text(model),
                stage: clean_text(stage),
                started: now,
                started_at,
                timeout_ms,
                received_usage: TokenUsage::default(),
            },
        );
        guard.id = Some(id);
        guard
    }
}

impl WorkGuard {
    /// Share reporting identity across existing scoped threads without sharing terminal ownership.
    pub fn handle(&self) -> WorkHandle {
        self.handle.clone()
    }

    /// Report the scope's outcome before relinquishing terminal ownership. A worker
    /// scope may end idle or deferred without claiming any underlying work completed.
    pub fn finish(mut self, state: MonitorState, message: &str) {
        self.end(state, message, false);
        self.finished = true;
    }

    /// Retire calls orphaned by an ended scope; late provider guards cannot resurrect or
    /// double count them. Diagnostic output occurs after releasing the observation lock.
    fn end(&self, state: MonitorState, message: &str, interrupted: bool) {
        let now = Instant::now();
        let monitoring = &self.handle.monitoring;
        let at = monitoring.timestamp(now);
        let (work, orphaned) = {
            let mut data = monitoring.lock();
            if data.generation != self.handle.generation {
                return;
            }
            let Some(work) = data.work.remove(&self.handle.id) else {
                return;
            };
            let call_ids: Vec<u64> = data
                .calls
                .iter()
                .filter(|(_, call)| call.work_id == self.handle.id)
                .map(|(id, _)| *id)
                .collect();
            let orphaned: Vec<ActiveCall> = call_ids
                .into_iter()
                .filter_map(|id| {
                    data.finish_call(
                        id,
                        MonitorState::Cancelled,
                        &TokenUsage::default(),
                        Some("Owning work ended before request outcome was reported"),
                        now,
                        at.clone(),
                    )
                })
                .collect();
            // Empty rediscovery is routine worker polling, not a useful recent outcome.
            if state != MonitorState::Idle {
                data.event(MonitorEvent {
                    at,
                    identity: work.observation.identity.clone(),
                    stage: work.observation.stage.clone(),
                    state,
                    message: clean_text(message),
                });
            }
            (work, orphaned)
        };
        if interrupted {
            warn!(event = "monitor.work_interrupted", worker = %work.observation.identity.worker,
                source_id = ?work.observation.identity.source_id, parse_id = ?work.observation.identity.parse_id,
                document = %work.observation.identity.document, stage = %work.observation.stage,
                elapsed_ms = duration_ms(now.saturating_duration_since(work.started)),
                "work observation ended without a terminal outcome; real operation outcome remains with its owner");
        }
        for call in orphaned {
            log_interrupted_call(&call, now);
        }
    }
}

impl Drop for WorkGuard {
    /// Early returns and unwinding cannot leave a permanently running dashboard row.
    fn drop(&mut self) {
        if !self.finished {
            self.end(
                MonitorState::Cancelled,
                "Work scope ended without a terminal outcome",
                true,
            );
        }
    }
}

impl CallGuard {
    /// Capture provider metadata before response validation may fail. It appears only
    /// at the terminal outcome and never changes a running call's visible progress.
    pub fn response_usage(&self, usage: TokenUsage) {
        let Some(id) = self.id else {
            return;
        };
        let mut data = self.monitoring.lock();
        if data.generation != self.generation {
            return;
        }
        if let Some(call) = data.calls.get_mut(&id) {
            call.received_usage = merge_usage(&usage, &call.received_usage);
        }
    }

    /// Only the first terminal report consumes this request's accounting identity.
    pub fn finish(mut self, state: MonitorState, usage: TokenUsage, detail: Option<&str>) {
        self.end(terminal_state(state), &usage, detail, false);
    }

    /// Finish a synchronous embedding outcome with any captured response metadata;
    /// absent fields remain absent and the actual Result propagates unchanged.
    pub fn finish_result<T>(self, result: &Result<T, crate::error::ApiError>) {
        match result {
            Ok(_) => self.finish(MonitorState::Complete, TokenUsage::default(), None),
            Err(source) => self.finish(
                MonitorState::Failed,
                TokenUsage::default(),
                Some(&source.to_string()),
            ),
        }
    }

    /// Backoff belongs to this logical request, not every concurrent batch in its
    /// document. Existing provider code owns the actual delay and retry decisions.
    pub fn retry_wait(&self, message: &str, attempt: u64, retry_limit: u64, delay_ms: u64) {
        let Some(id) = self.id else {
            return;
        };
        let (mut data, now) = self.monitoring.lock_observed();
        if data.generation != self.generation {
            return;
        }
        let Some(call) = data.calls.get(&id) else {
            return;
        };
        let issue = MonitorIssue {
            identity: call.identity.clone(),
            stage: call.stage.clone(),
            state: MonitorState::Waiting,
            message: clean_text(message),
            affected: 1,
            retry_in_ms: Some(delay_ms),
            observed_since: None,
            elapsed_ms: 0,
            attempt: Some(attempt),
            retry_limit: Some(retry_limit),
        };
        data.issue(
            call_issue_key(id),
            issue,
            now,
            self.monitoring.timestamp(now),
        );
    }

    /// Clear backoff when the provider attempt actually resumes; late guards cannot
    /// clear another generation's issues after an ID is reused by a rebuild.
    pub fn retry_started(&self) {
        let Some(id) = self.id else {
            return;
        };
        let mut data = self.monitoring.lock();
        if data.generation == self.generation {
            data.issues.remove(&call_issue_key(id));
        }
    }

    /// The call map owns exact-once terminal accounting, including parent-scope retirement.
    fn end(
        &mut self,
        state: MonitorState,
        usage: &TokenUsage,
        detail: Option<&str>,
        interrupted: bool,
    ) {
        let Some(id) = self.id.take() else {
            return;
        };
        let now = Instant::now();
        let at = self.monitoring.timestamp(now);
        let call = {
            let mut data = self.monitoring.lock();
            if data.generation != self.generation {
                return;
            }
            data.finish_call(id, state, usage, detail, now, at)
        };
        if interrupted && let Some(call) = call {
            log_interrupted_call(&call, now);
        }
    }
}

impl Drop for CallGuard {
    /// Unreported request outcomes are cancellation/interruptions, never inferred successes.
    fn drop(&mut self) {
        self.end(
            MonitorState::Cancelled,
            &TokenUsage::default(),
            Some("Request scope ended without a terminal outcome"),
            true,
        );
    }
}

/// A misuse of the observer API is visible but cannot turn into a scheduling failure.
fn terminal_state(state: MonitorState) -> MonitorState {
    match state {
        MonitorState::Complete
        | MonitorState::Failed
        | MonitorState::Cancelled
        | MonitorState::Unavailable => state,
        _ => {
            error!(
                event = "monitor.invalid_terminal_state",
                ?state,
                "monitor terminal reporter received a nonterminal state; reporting observation unavailable"
            );
            MonitorState::Unavailable
        }
    }
}

/// Missing observation outcomes get compact durable diagnostics without copying payloads.
fn log_interrupted_call(call: &ActiveCall, now: Instant) {
    warn!(event = "monitor.call_interrupted", worker = %call.identity.worker,
        source_id = ?call.identity.source_id, parse_id = ?call.identity.parse_id,
        document = %call.identity.document, role = %call.role, model = %call.model,
        stage = %call.stage, elapsed_ms = duration_ms(now.saturating_duration_since(call.started)),
        "model call observation ended without a terminal outcome; provider outcome remains with its owner");
}

/// Single-screen observations carry compact safe summaries, not terminal controls or
/// unbounded errors. Callers still own secret redaction and payload prohibitions.
fn clean_text(text: &str) -> String {
    const DETAIL_CHARACTER_LIMIT: usize = 2_048;
    let mut characters = text.chars();
    let mut result: String = characters
        .by_ref()
        .take(DETAIL_CHARACTER_LIMIT)
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if characters.next().is_some() {
        result.push_str(" …[truncated]");
    }
    result
}

/// Display labels are untrusted path text; canonical IDs stay byte-for-byte identifiable.
fn clean_identity(mut identity: WorkIdentity) -> WorkIdentity {
    identity.worker = clean_text(&identity.worker);
    identity.document = clean_text(&identity.document);
    identity
}

/// Retirement follows the owner's captured active parse inventory, never an age
/// heuristic. Worker-wide issues without a corpus identity remain outstanding.
fn retain_inventory_issues(
    issues: &mut BTreeMap<String, OutstandingIssue>,
    worker: &str,
    inventory: &BTreeSet<(&str, &str)>,
) {
    issues.retain(|_, issue| {
        let identity = &issue.observation.identity;
        if identity.worker != worker {
            return true;
        }
        match (identity.source_id.as_deref(), identity.parse_id.as_deref()) {
            (Some(source), Some(parse)) => inventory.contains(&(source, parse)),
            _ => true,
        }
    });
}

/// Call-local backoff issues are retired through the same ID as terminal accounting.
fn call_issue_key(id: u64) -> String {
    format!("monitor-call:{id}")
}
