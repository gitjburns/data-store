//! Operation model (spec §34.6): the durable record of one asynchronous
//! administrative operation, polled through `GET /operations/{operationId}`.
//!
//! An Operation is the audit-and-poll handle for async admin work (ingest,
//! parse execution, activation, snapshot, restore, and the other §34.6
//! operation types): a mutating admin route inserts it `pending`, the worker
//! flips it to `running` and then to a terminal `succeeded`/`failed`, and
//! operators poll it for status. The row IS the audit record — there is no
//! `operation.*` SystemEvent in the §33 closed vocabulary (see the operations
//! store for the deliberate absence), so unlike the derived model types this
//! shape is not paired with an event family.

// The §34 admin HTTP surface inserts, transitions, and reads Operation rows;
// the operations store in src/operations.rs is the constructor, and `Operation`
// is serialized to clients via `Json<Operation>` in the polling handler.
use serde::{Deserialize, Serialize};

/// Spec §34.6 `Operation`. The queryable record of one async admin operation:
/// `id` is the `op_` handle returned to the caller, `operationType`/`status`
/// name what ran and where it is in its lifecycle, and `targetObjectType`/
/// `targetObjectId` name the fabric object the operation acts on. The
/// milestone timestamps `startedAt`/`completedAt` and the `error` detail are
/// optional because a `pending` operation has reached neither milestone and a
/// non-failed operation carries no error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Operation {
    /// Spec §34.6 `id`: the `op_` handle the caller polls.
    pub(crate) id: String,

    /// Spec §34.6 `operationType`: which admin operation this row records.
    pub(crate) operation_type: OperationType,

    /// Spec §34.6 `status`: the operation's lifecycle position.
    pub(crate) status: OperationStatus,

    /// Spec §34.6 `targetObjectType`: the kind of fabric object acted on
    /// (e.g. `source`, `parse`), as a free string per the spec schema.
    pub(crate) target_object_type: String,

    /// Spec §34.6 `targetObjectId`: the id of the acted-on fabric object.
    pub(crate) target_object_id: String,

    /// Spec §34.6 `startedAt`: set when the operation transitions to
    /// `running`; absent while still `pending`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at: Option<String>,

    /// Spec §34.6 `completedAt`: set when the operation reaches a terminal
    /// `succeeded`/`failed` state; absent before then.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at: Option<String>,

    /// Spec §34.6 `error`: bounded failure detail, set only on a `failed`
    /// operation; absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,

    /// Spec §34.6 `createdAt`: when the `pending` row was inserted.
    pub(crate) created_at: String,
}

/// Spec §34.6 `operationType`. Closed set of the async admin operations that
/// leave an Operation row, matched exhaustively so adding a variant is a
/// compile error at every contract point.
///
/// `parse_discard` is a recorded ADDITIVE extension of the spec §34.6 closed
/// set (C10 cluster, plan resolution 1), on the same precedent as the
/// `annotation.*`/`projection.superseded` additive extensions of the §33 event
/// vocabulary in src/model/event.rs: the spec defines the held-parse discard
/// disposition (§13.4) but omits its operation type, so discard would
/// otherwise have no async-operation handle to poll. Its wire name is
/// snake_case like the spec values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationType {
    Acquisition,
    ParserExecution,
    ParseBuild,
    ParseImportValidation,
    ParseActivation,
    ProjectionBuild,
    SnapshotCreation,
    Restore,
    Drill,
    SourceIngest,
    /// Explicit destructive corpus reset; succeeds when automatic ingestion resumes.
    RebuildAll,
    // Recorded additive extension of the §34.6 closed set (see the enum doc
    // comment): the held-parse discard disposition's async-operation handle.
    ParseDiscard,
}

/// Spec §34.6 `status`. The lifecycle position of an Operation: `pending` on
/// insert, `running` once the worker starts it, then a terminal `succeeded`
/// or `failed`. Matched exhaustively so the closed set stays authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}
