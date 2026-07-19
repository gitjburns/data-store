//! Sync-queue entry model (spec §9.4): the typed counterpart of the
//! `sync_queue` hot-plane table, including the queue-state enum matching the
//! schema CHECK constraint. Implemented by work package C3c.

use serde::{Deserialize, Serialize};

/// Lifecycle state of one sync-queue entry. Wire names are the exact value
/// set of the `sync_queue.state` CHECK constraint in sql/fabric/schema.sql
/// (`'pending'`, `'in_flight'`, `'failed'`); the snake_case rename keeps the
/// two in lockstep, and adding a variant here requires a coordinated schema
/// version bump there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncQueueState {
    /// Detected change waiting to be drained. The only state
    /// `claim_pending` picks up.
    Pending,
    /// Claimed by a drain pass; a row stuck here means the drain died
    /// between claim and complete/fail (recoverable by a new detection).
    InFlight,
    /// The last drain attempt failed. Deliberately terminal for the
    /// scheduler: only a new detection re-pends it (spec §13.5 rule applied
    /// to acquisition — identical input fails identically).
    Failed,
}

/// Spec §9.4. One coalesced latest-state change per source, mirroring the
/// `sync_queue` columns of sql/fabric/schema.sql field-for-field.
///
/// Timestamp semantics (schema §9.4 comment): `created_at` stays at the
/// first detection of the currently-pending change so per-source lag is
/// answerable; `detected_at` advances to the latest coalesced observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SyncQueueEntry {
    pub(crate) id: String,

    /// UNIQUE coalescing key: `source_system` + `:` + `native_uri`,
    /// composed by the enqueuer (schema §9.4 comment).
    pub(crate) source_key: String,

    pub(crate) source_system: String,
    pub(crate) native_uri: String,

    pub(crate) detected_at: String,
    /// Why the change was enqueued (operator-facing free text, e.g.
    /// `staged_by_full_scan`).
    pub(crate) reason: String,

    pub(crate) state: SyncQueueState,

    pub(crate) attempt_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_attempt_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_error: Option<String>,

    /// How many later detections were coalesced into this row while it was
    /// queued (§9.5 health visibility).
    pub(crate) coalesced_count: u64,

    pub(crate) created_at: String,
}
