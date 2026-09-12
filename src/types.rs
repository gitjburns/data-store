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
