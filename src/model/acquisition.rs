//! Acquisition-layer records (spec §9.2–§9.3): the durable outcome of every
//! acquisition attempt and each connector's statically declared
//! change-detection capability profile.

use serde::{Deserialize, Serialize};

/// Spec §9.2. Durable record of one acquisition attempt, successful or
/// failed. A source system that cannot be read is operationally meaningful
/// state, so failed attempts are recorded with the same fidelity as
/// successes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AcquisitionRecord {
    pub(crate) id: String,

    pub(crate) connector_name: String,
    pub(crate) connector_version: String,
    pub(crate) connector_config_hash: String,

    pub(crate) source_system: String,
    pub(crate) native_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) native_modified_at: Option<String>,

    pub(crate) governance_domain: String,

    pub(crate) outcome: AcquisitionOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_class: Option<AcquisitionFailureClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_detail: Option<String>,

    // Success-path linkage: only present when the bundle was validated and a
    // SourceObject/SourceLocation was created or refreshed (§9.1 rule 3).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_location_id: Option<String>,

    pub(crate) acquired_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) elapsed_ms: Option<u64>,
}

/// Spec §9.2 `outcome`. Whether the acquisition attempt produced a validated
/// bundle or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcquisitionOutcome {
    Succeeded,
    Failed,
}

/// Spec §9.2 `failureClass`. Closed classification of acquisition failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcquisitionFailureClass {
    Unreachable,
    AccessDenied,
    NotFound,
    Timeout,
    Malformed,
    ResourceLimit,
    Other,
}

/// Spec §9.3. A connector's statically declared, versioned change-detection
/// capability. Records externally imposed facts only; internal tuning
/// guesses must not be encoded here (§35).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConnectorCapabilityProfile {
    pub(crate) connector_name: String,
    pub(crate) connector_version: String,

    pub(crate) detection_mode: DetectionMode,

    pub(crate) supports_explicit_delete_events: bool,
    pub(crate) supports_complete_enumeration: bool,
    pub(crate) supports_native_versioning: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider_constraints: Option<serde_json::Map<String, serde_json::Value>>,

    pub(crate) profile_hash: String,
}

/// Spec §9.3 `detectionMode`. How the connector observes change in its
/// source system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DetectionMode {
    ChangeFeed,
    IncrementalPoll,
    FullScan,
}
