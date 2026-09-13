//! Shared, presentation-independent monitoring contract. Counts describe observed
//! work; request completion never implies a database commit or publication.

use serde::{Deserialize, Serialize};

use crate::types::{AnnotationProgressCount, AnnotationWorkCounts, ProjectionPublicationCounts};

/// The monitor's presentation cadence is deliberately fixed, not operational configuration.
pub const REFRESH_INTERVAL_MS: u64 = 200;
/// Rates describe this trailing observation window, shortened during startup.
pub const RATE_WINDOW_SECONDS: u64 = 60;
/// Recent outcomes supplement durable logs; active work and totals are never age-evicted.
pub const RECENT_EVENT_LIMIT: usize = 32;
/// Finished calls roll off independently of recent work events; running calls never do.
pub const CALL_HISTORY_LIMIT: usize = 32;
/// Publication issues have an independent lifecycle from annotation coverage.
pub const PUBLICATION_WORKER: &str = "projections";

/// One coherent monitoring read, identified across service restarts and corpus rebuilds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub run_id: String,
    pub generation: u64,
    pub started_at: Option<String>,
    pub sampled_at: Option<String>,
    pub uptime_ms: u64,
    pub generation_elapsed_ms: u64,
    pub ready: bool,
    pub status: String,
    /// Server-owned explanation keeps ingestion readiness separate from query readiness.
    #[serde(default)]
    pub headline: String,
    /// Explains actual work or the reason no work is currently eligible.
    #[serde(default)]
    pub activity: String,
    pub ingestion: IngestionMetrics,
    pub annotations: AnnotationMetrics,
    pub publication: PublicationMetrics,
    pub work: Vec<WorkObservation>,
    pub calls: Vec<CallGroup>,
    /// Running calls followed by newest terminal outcomes, retained across client polls.
    #[serde(default)]
    pub call_log: Vec<CallRecord>,
    pub model_stats: Vec<ModelStatistics>,
    pub rates: Vec<WorkRate>,
    pub issues: Vec<MonitorIssue>,
    pub recent: Vec<MonitorEvent>,
    pub corpus: Vec<CorpusCount>,
    pub queries_in_flight: usize,
    pub query_limit: usize,
}

/// Files and content-deduplicated sources are separate populations. Queue values
/// and source coverage remain unavailable until their owning worker measures them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestionMetrics {
    pub enumerated_files: Option<u64>,
    pub enumeration_complete: bool,
    pub skipped_files: u64,
    pub staged_files: u64,
    pub scan_failures: u64,
    pub last_scan_ms: Option<u64>,
    pub next_scan_wait_ms: Option<u64>,
    pub pending: Option<u64>,
    pub in_flight: Option<u64>,
    pub failed: Option<u64>,
    /// Sources whose failed parse blocks another automatic attempt.
    #[serde(default)]
    pub blocked_sources: Option<u64>,
    pub active_sources: AnnotationProgressCount,
    /// Independent owners' timestamps prevent a queue refresh from making old scans look fresh.
    pub queue_measured_at: Option<String>,
    pub source_measured_at: Option<String>,
    pub scan_measured_at: Option<String>,
    pub measured_at: Option<String>,
}

/// Aggregate existing committed coverage without treating unknown document plans
/// as zero required work. Type counts use the same accounting as document health.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnnotationMetrics {
    pub documents: Option<u64>,
    pub unmeasured_documents: u64,
    pub progress: AnnotationProgressCount,
    pub work: AnnotationWorkCounts,
    pub by_type: Vec<AnnotationTypeMetrics>,
    pub memoized: u64,
    pub parked_reason: Option<String>,
    pub measured_at: Option<String>,
}

/// A required annotation type contributes committed coverage, not output-row counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnotationTypeMetrics {
    pub annotation_type: String,
    pub progress: AnnotationProgressCount,
    pub work: AnnotationWorkCounts,
}

/// Publication covers the worker's currently declared inputs; fresh annotation
/// additions may increase its denominator independently of annotation completion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PublicationMetrics {
    pub documents: Option<u64>,
    pub graph: ProjectionPublicationCounts,
    pub graph_progress: AnnotationProgressCount,
    pub summary: ProjectionPublicationCounts,
    pub summary_progress: AnnotationProgressCount,
    pub embeddings: Option<ProjectionPublicationCounts>,
    pub embeddings_progress: Option<AnnotationProgressCount>,
    /// Partial embedding inventory retains measured counts but has no corpus percentage.
    pub unmeasured_embedding_documents: u64,
    pub measured_at: Option<String>,
}

/// Worker-owned state is rendered verbatim; clients never classify work from counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorState {
    Running,
    Waiting,
    Complete,
    Failed,
    Cancelled,
    Unavailable,
    Idle,
}

/// Progress within a stage counts its actual work unit. The owning stage supplies
/// a denominator only after it has measured the corresponding input population.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageProgress {
    pub counts: AnnotationProgressCount,
    pub unit: String,
}

/// Identity accompanies explicit observer handles across scoped worker threads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkIdentity {
    pub worker: String,
    pub document: String,
    pub source_id: Option<String>,
    pub parse_id: Option<String>,
}

/// Active work and its latest boundary observation; elapsed time is not a claim
/// of internal progress in an opaque parser or model request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkObservation {
    pub identity: WorkIdentity,
    pub stage: String,
    pub state: MonitorState,
    pub progress: Option<StageProgress>,
    pub started_at: Option<String>,
    pub elapsed_ms: u64,
    pub detail: Option<String>,
}

/// Concurrent calls sharing a document, role, model, and stage occupy one row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallGroup {
    pub identity: WorkIdentity,
    pub role: String,
    pub model: String,
    pub stage: String,
    pub running: u64,
    pub oldest_started_at: Option<String>,
    pub timeout_ms: Option<u64>,
}

/// A call keeps its ID through terminal reporting, scoped by snapshot run and generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    pub id: u64,
    pub identity: WorkIdentity,
    pub role: String,
    pub model: String,
    pub stage: String,
    pub state: MonitorState,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub elapsed_ms: u64,
    pub timeout_ms: Option<u64>,
    pub usage: TokenUsage,
    pub detail: Option<String>,
}

/// Provider-reported usage remains optional; reasoning tokens, when supplied,
/// are part of completion tokens and must never be added a second time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt: Option<u64>,
    pub completion: Option<u64>,
    pub reasoning: Option<u64>,
    pub total: Option<u64>,
}

/// Run/generation statistics count terminal calls once. Token totals aggregate
/// only supplied fields and retain unavailable-response counts alongside them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelStatistics {
    pub role: String,
    pub model: String,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub mean_duration_ms: Option<u64>,
    pub max_duration_ms: Option<u64>,
    pub usage: TokenUsage,
    pub usage_unavailable: u64,
}

/// Rates have explicit work units and an observed window, including startup warmup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRate {
    pub label: String,
    pub completed: u64,
    pub window_ms: u64,
    pub per_minute: f64,
}

/// Outstanding failures/waits stay separate from the bounded recent-event list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorIssue {
    pub identity: WorkIdentity,
    pub stage: String,
    pub state: MonitorState,
    pub message: String,
    pub affected: u64,
    /// First unresolved observation of this issue key; retry scheduling does not reset its age.
    pub observed_since: Option<String>,
    pub elapsed_ms: u64,
    pub retry_in_ms: Option<u64>,
    pub attempt: Option<u64>,
    pub retry_limit: Option<u64>,
}

/// A compact outcome at its real boundary, supplementing the durable service log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorEvent {
    pub at: Option<String>,
    pub identity: WorkIdentity,
    pub stage: String,
    pub state: MonitorState,
    pub message: String,
}

/// Diagnostic corpus exceptions retain the scheduler's measurement time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusCount {
    pub source_system: String,
    pub label: String,
    pub value: u64,
    pub measured_at: Option<String>,
}
