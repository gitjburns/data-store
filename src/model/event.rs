//! System event model (spec §33): the internal, audit-facing events that
//! drive the autonomous pipeline. Persisted by the appender in
//! `crate::events`; internal audit state, distinct from any client-facing
//! response surface.

use serde::{Deserialize, Serialize};

/// Spec §33. One durable pipeline/audit event about a fabric object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SystemEvent {
    pub(crate) id: String,

    pub(crate) event_type: SystemEventType,

    pub(crate) object_type: String,
    pub(crate) object_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<serde_json::Map<String, serde_json::Value>>,

    pub(crate) created_at: String,
}

/// Spec §33 `eventType`. Closed set of system event types. Wire names carry
/// a `subsystem.` prefix with a dot, so every variant needs an explicit
/// rename instead of `rename_all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SystemEventType {
    #[serde(rename = "acquisition.succeeded")]
    AcquisitionSucceeded,
    #[serde(rename = "acquisition.failed")]
    AcquisitionFailed,
    #[serde(rename = "source.ingested")]
    SourceIngested,
    #[serde(rename = "source.location_added")]
    SourceLocationAdded,
    #[serde(rename = "source.location_deleted")]
    SourceLocationDeleted,
    #[serde(rename = "source.access_lost")]
    SourceAccessLost,
    #[serde(rename = "source.access_restored")]
    SourceAccessRestored,
    #[serde(rename = "source.deactivated")]
    SourceDeactivated,
    #[serde(rename = "source.reactivated")]
    SourceReactivated,
    #[serde(rename = "parse.started")]
    ParseStarted,
    #[serde(rename = "parse.ready")]
    ParseReady,
    #[serde(rename = "parse.held")]
    ParseHeld,
    #[serde(rename = "parse.hold_superseded")]
    ParseHoldSuperseded,
    #[serde(rename = "parse.accepted")]
    ParseAccepted,
    #[serde(rename = "parse.discarded")]
    ParseDiscarded,
    #[serde(rename = "parse.activated")]
    ParseActivated,
    #[serde(rename = "parse.failed")]
    ParseFailed,
    #[serde(rename = "parse.archived")]
    ParseArchived,
    #[serde(rename = "sync.backpressure_entered")]
    SyncBackpressureEntered,
    #[serde(rename = "sync.backpressure_exited")]
    SyncBackpressureExited,
    // The annotation.* variants are a recorded ADDITIVE extension of the
    // spec §33 closed enumeration (CA cluster, 2026-07-13): the spec defines
    // SemanticAnnotations (§21) but omits their lifecycle events. They mirror
    // the projection.* lifecycle vocabulary.
    #[serde(rename = "annotation.requested")]
    AnnotationRequested,
    #[serde(rename = "annotation.completed")]
    AnnotationCompleted,
    #[serde(rename = "annotation.failed")]
    AnnotationFailed,
    #[serde(rename = "annotation.stale")]
    AnnotationStale,
    #[serde(rename = "projection.requested")]
    ProjectionRequested,
    #[serde(rename = "projection.completed")]
    ProjectionCompleted,
    #[serde(rename = "projection.failed")]
    ProjectionFailed,
    #[serde(rename = "projection.stale")]
    ProjectionStale,
    // projection.superseded is a recorded ADDITIVE extension of the spec §33
    // closed enumeration (C6 cluster, 2026-07-14), on the same precedent as the
    // annotation.* additions above: spec §22 defines the fresh → superseded
    // lifecycle transition (a projection's parse superseded at cutover) but §33
    // omits its event. It mirrors the projection.* lifecycle vocabulary; its
    // mint-point is envelope::mark_superseded.
    #[serde(rename = "projection.superseded")]
    ProjectionSuperseded,
    #[serde(rename = "assembly_policy.changed")]
    AssemblyPolicyChanged,
    #[serde(rename = "snapshot.started")]
    SnapshotStarted,
    #[serde(rename = "snapshot.completed")]
    SnapshotCompleted,
    #[serde(rename = "snapshot.failed")]
    SnapshotFailed,
    #[serde(rename = "drill.completed")]
    DrillCompleted,
    #[serde(rename = "drill.failed")]
    DrillFailed,
    #[serde(rename = "query.executed")]
    QueryExecuted,
}
