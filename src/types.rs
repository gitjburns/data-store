use serde::{Deserialize, Serialize};

/// Service readiness and both views of the same component observations.
#[derive(Debug, Deserialize, Serialize)]
pub struct HealthResponse {
    pub service: String,
    pub ready: bool,
    pub components: Vec<HealthComponent>,
}

/// Detailed diagnostics remain available alongside the server-owned summary.
#[derive(Debug, Deserialize, Serialize)]
pub struct HealthComponent {
    pub name: String,
    pub ready: bool,
    pub details: Vec<String>,
    /// Additive typed diagnostic counters (C10b, spec §9.5–§9.6, §13.4–13.5,
    /// §21, §30.5). Each count carries its own as-of marker so a value is never
    /// presented as current without saying when it was measured. Kept as a
    /// typed vec — NOT strings parsed out of `details` — so consumers read the
    /// numbers directly. Empty for components that publish no counters (e.g.
    /// inference, logging), which serializes as an empty array.
    #[serde(default)]
    pub counts: Vec<HealthCount>,
    /// Absent on older servers; clients report the summary as unavailable rather
    /// than inferring operational state from free-form diagnostic text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<HealthSummary>,
}

/// Display priority is independent of readiness: annotation faults need attention
/// even while the service remains able to answer queries.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Attention,
    Unreported,
    Normal,
}

/// Specific problems and typed observations are derived from one component read.
#[derive(Debug, Deserialize, Serialize)]
pub struct HealthSummary {
    pub status: HealthStatus,
    pub problems: Vec<String>,
    pub observations: HealthObservations,
}

/// Compact observations keep clients out of diagnostic-string parsing and domain
/// status inference. Cycle values are explicitly historical, never live progress.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthObservations {
    Ingestion {
        pending: u64,
        in_flight: u64,
        failed: u64,
        last_success_at: Option<String>,
    },
    Annotations {
        parked: bool,
        last_cycle: Option<AnnotationSummary>,
        measured_at: Option<String>,
        /// None before inventory or on an older server; an empty list is a measured empty scope.
        #[serde(default)]
        documents: Option<Vec<AnnotationDocumentProgress>>,
        #[serde(default)]
        inventory_measured_at: Option<String>,
    },
    Projections {
        activity: ProjectionActivity,
        measured_at: Option<String>,
        /// None before inventory; an empty list is a measured empty active corpus.
        documents: Option<Vec<ProjectionDocumentProgress>>,
    },
    Corpus {
        source_systems: Vec<String>,
        measured_at: Option<String>,
    },
    Queries {
        in_flight: usize,
        max_in_flight: usize,
    },
    Models {
        initialized: bool,
        dense: HealthBackend,
        colbert: HealthBackend,
        reranker: HealthBackend,
    },
    Logging {
        level: String,
        file_path: String,
    },
}

/// Mutually exclusive counts for the current measured input version. An older
/// publication does not satisfy a newer cohort; it remains available to queries
/// while its replacement is pending or failed.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct ProjectionPublicationCounts {
    pub published: u64,
    pub pending: u64,
    pub failed: u64,
}

/// Projection coverage belongs to one captured source/parse and remains separate
/// from annotation generation progress. Embeddings are unmeasured without inference.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProjectionDocumentProgress {
    pub source_id: String,
    pub parse_id: String,
    pub source_paths: Vec<String>,
    pub measured_at: Option<String>,
    pub graph: ProjectionPublicationCounts,
    pub summary: ProjectionPublicationCounts,
    pub embeddings: Option<ProjectionPublicationCounts>,
    pub activity: ProjectionActivity,
    pub detail: Option<String>,
}

/// Classification is supplied by the publication owner, never inferred by clients.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionActivity {
    #[default]
    Discovering,
    Pending,
    Building,
    AwaitingCommit,
    Complete,
    RetryWait,
    Stopped,
    Unavailable,
}

impl std::fmt::Display for ProjectionActivity {
    /// Keep CLI and operational activity labels aligned with server classifications.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Discovering => "discovering",
            Self::Pending => "pending",
            Self::Building => "building",
            Self::AwaitingCommit => "awaiting commit",
            Self::Complete => "projections published",
            Self::RetryWait => "waiting to retry",
            Self::Stopped => "stopped",
            Self::Unavailable => "unavailable",
        })
    }
}

/// Counts describe work examined in a completed cycle. Eligible missing work
/// excludes retry-waiting/exhausted items and cannot establish completion.
#[derive(Debug, Deserialize, Serialize)]
pub struct AnnotationSummary {
    pub sources_examined: u64,
    pub planned: u64,
    pub eligible_missing: u64,
    pub new_failures: u64,
    pub exhausted: u64,
}

/// Committed coverage of a measured plan. The server supplies the percentage;
/// clients only format it, and an unknown denominator is never treated as zero.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct AnnotationProgressCount {
    pub completed: u64,
    pub total: Option<u64>,
    pub percentage: Option<f64>,
}

impl std::fmt::Display for AnnotationProgressCount {
    /// Keep CLI and log rendering identical without rounding incomplete work to 100%.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.total, self.percentage) {
            (Some(0), _) => write!(formatter, "{} / 0 (no required work)", self.completed),
            (Some(total), Some(percentage)) => {
                write!(formatter, "{} / {total} ({percentage:.1}%)", self.completed)
            }
            _ => formatter.write_str("unavailable"),
        }
    }
}

/// Mutually exclusive states of unfinished plan items. Running covers a chain
/// from preparation through persistence; document activity identifies storage waits.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AnnotationWorkCounts {
    pub pending: u64,
    pub running: u64,
    pub failed: u64,
    pub retry_waiting: u64,
    pub exhausted: u64,
}

/// Per-type coverage uses the same required work and state accounting as the document.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnotationTypeProgress {
    pub annotation_type: String,
    pub progress: AnnotationProgressCount,
    pub work: AnnotationWorkCounts,
}

/// A worker observation tied to one captured active parse and required plan.
/// Activity and completion describe annotation work, not retrieval publication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnotationDocumentProgress {
    pub source_id: String,
    pub parse_id: String,
    pub source_paths: Vec<String>,
    pub plan_id: Option<String>,
    pub measured_at: Option<String>,
    pub progress: AnnotationProgressCount,
    pub work: AnnotationWorkCounts,
    pub by_type: Vec<AnnotationTypeProgress>,
    pub activity: AnnotationActivity,
    pub detail: Option<String>,
}

/// The worker owns activity classification; health clients must not infer it from counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationActivity {
    Discovering,
    Pending,
    Running,
    AwaitingCommit,
    WaitingForStorage,
    RetryWait,
    Exhausted,
    Complete,
    NoWork,
    Stopped,
    Unavailable,
}

impl std::fmt::Display for AnnotationActivity {
    /// Render the server's classification as readable operator text.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Discovering => "discovering",
            Self::Pending => "pending",
            Self::Running => "running",
            Self::AwaitingCommit => "awaiting commit",
            Self::WaitingForStorage => "waiting for storage",
            Self::RetryWait => "waiting to retry",
            Self::Exhausted => "retries exhausted",
            Self::Complete => "annotations complete",
            Self::NoWork => "no required annotation work",
            Self::Stopped => "stopped",
            Self::Unavailable => "unavailable",
        })
    }
}

/// Config-selected retrieval execution location, not a live endpoint probe.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthBackend {
    Local,
    Http,
}

/// One additive diagnostic counter on a `HealthComponent` (C10b). `label`
/// names the measured quantity (e.g. `"held"`, `"stuck_building"`),
/// `source_system` scopes fabric counts to their owning source-system (spec
/// resolution 6: a per-source-system shape, not a single global bucket) and is
/// `None` for corpus-aggregate counters, and `as_of` is the RFC3339 timestamp
/// or cycle marker the count was measured at — accuracy over convenience: a
/// count without its as-of time is a guess presented as fact.
#[derive(Debug, Deserialize, Serialize)]
pub struct HealthCount {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_system: Option<String>,
    pub value: u64,
    pub as_of: String,
}
