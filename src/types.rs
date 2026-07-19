use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub service: String,
    pub ready: bool,
    pub components: Vec<HealthComponent>,
}

#[derive(Debug, Serialize)]
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
    pub counts: Vec<HealthCount>,
}

/// One additive diagnostic counter on a `HealthComponent` (C10b). `label`
/// names the measured quantity (e.g. `"held"`, `"stuck_building"`),
/// `source_system` scopes fabric counts to their owning source-system (spec
/// resolution 6: a per-source-system shape, not a single global bucket) and is
/// `None` for corpus-aggregate counters, and `as_of` is the RFC3339 timestamp
/// or cycle marker the count was measured at — accuracy over convenience: a
/// count without its as-of time is a guess presented as fact.
#[derive(Debug, Serialize)]
pub struct HealthCount {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_system: Option<String>,
    pub value: u64,
    pub as_of: String,
}
