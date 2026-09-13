//! C10s: hot-plane persistence and status lifecycle for §34.6 Operation rows —
//! the durable, pollable record of one asynchronous administrative operation.
//!
//! Lifecycle (spec §34.6): an Operation is inserted `pending` (created_at set,
//! started_at/completed_at/error NULL), transitions `pending → running` when
//! the worker starts it (setting started_at), then reaches a terminal
//! `running → succeeded` or `running → failed` (setting completed_at, and the
//! error column on failure). Every UPDATE is status-guarded (`WHERE id = ?1
//! AND status = '<expected>'`) and asserted to hit exactly one row, so a
//! transition whose precondition vanished fails loudly rather than silently
//! no-opping.
//!
//! NO SystemEvent, unlike the annotations store this module mirrors: the §33
//! closed event vocabulary (src/model/event.rs `SystemEventType`) has no
//! `operation.*` family, and inventing a closed-enum variant is out of scope.
//! The durable Operation ROW plus the service log ARE the audit record for
//! async admin work (consistent with spec §33's "operators poll Operations
//! (§34.6) and health for status"). A future maintainer expecting the
//! event-atomic append these functions' template (annotations/store.rs) makes
//! must be told: there is deliberately none here.
//!
//! Transaction shape: unlike the annotations store (whose mutations take the
//! CALLER's `&Transaction` so a row change and its event commit atomically),
//! these functions open their OWN write connection per call (the scheduler
//! `complete()` precedent). Operation rows are NOT event-atomic with any
//! domain write — there is no paired event, and the C10a detached tasks and
//! the scheduler drain call these from OUTSIDE any shared domain transaction —
//! so the self-contained `open_write`-per-call form is the correct fit. Reads
//! take a fresh read-only connection. Boundary logging is deliberately left to
//! the C10a/scheduler callers (mirroring annotations/store.rs, which logs at
//! the worker boundary, not per store fn); those callers own the async-task
//! and drain boundaries these transitions ride inside.

// Fully live as of C10a: the §34 admin HTTP surface calls insert_pending from
// its queue-coupled and detached async tasks, drives mark_running/
// mark_succeeded/mark_failed from the detached tasks, the scheduler drain
// completes queue-coupled operations through the same mark_* functions, and
// GET /operations/{operationId} reads via get. The module-level dead-code allow
// this module carried while unwired is removed accordingly.
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;

use crate::error::ApiError;
use crate::hot_plane;
use crate::ids::new_operation_id;
use crate::model::{Operation, OperationStatus, OperationType};
use crate::primitives::utc_now;
use crate::util::truncate_persisted_detail;

/// Insert one Operation in the `pending` state: created_at is set, and
/// started_at/completed_at/error start NULL (a pending operation has reached
/// no milestone and carries no error). operation_type holds the snake_case
/// wire name of the closed Rust enum.
const INSERT_PENDING_SQL: &str = "
INSERT INTO operations (
  id, operation_type, status, target_object_type, target_object_id,
  started_at, completed_at, error, created_at
) VALUES (?1, ?2, 'pending', ?3, ?4, NULL, NULL, NULL, ?5)";

/// Status-guarded `pending → running` transition: stamps started_at and only
/// matches a row still `pending`. The guard makes starting a missing or
/// already-started operation a loud zero-row failure instead of a silent
/// milestone overwrite.
const MARK_RUNNING_SQL: &str = "
UPDATE operations
SET status = 'running', started_at = ?2
WHERE id = ?1 AND status = 'pending'";

/// Status-guarded `running → succeeded` transition: stamps completed_at and
/// only matches a row still `running`, so completing a non-running operation
/// fails loudly.
const MARK_SUCCEEDED_SQL: &str = "
UPDATE operations
SET status = 'succeeded', completed_at = ?2
WHERE id = ?1 AND status = 'running'";

/// Status-guarded `running → failed` transition: stamps completed_at and the
/// bounded error column, and only matches a row still `running`. The error
/// detail lives on the row (unlike the annotations store, which puts detail in
/// its event) because the Operation row is the sole audit record here.
const MARK_FAILED_SQL: &str = "
UPDATE operations
SET status = 'failed', completed_at = ?2, error = ?3
WHERE id = ?1 AND status = 'running'";

/// Read one Operation row by id for `GET /operations/{operationId}`.
const SELECT_OPERATION_SQL: &str = "
SELECT id, operation_type, status, target_object_type, target_object_id,
       started_at, completed_at, error, created_at
FROM operations
WHERE id = ?1";

/// Insert a `pending` Operation and return its freshly minted `op_` id. The
/// row records the operation type and the fabric object it targets; the worker
/// later flips it to `running` and then a terminal state. Opens its own write
/// connection (see the module transaction-shape note).
pub(crate) fn insert_pending(
    index_root: &crate::runtime::StorageContext,
    operation_type: OperationType,
    target_object_type: &str,
    target_object_id: &str,
) -> Result<String, ApiError> {
    let id = new_operation_id()?;
    let operation_type_wire = enum_wire_name(&operation_type, "operation type")?;
    let now = utc_now()?;

    let connection = hot_plane::open_write(index_root)?;
    connection
        .execute(
            INSERT_PENDING_SQL,
            params![
                id,
                operation_type_wire,
                target_object_type,
                target_object_id,
                now,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to insert pending operation {id}: {source}"),
        })?;
    Ok(id)
}

/// Transition `pending → running`, stamping started_at. The UPDATE is
/// status-guarded and asserted to hit exactly one row, so starting a missing
/// or non-pending operation is a loud failure, never a silent no-op.
pub(crate) fn mark_running(
    index_root: &crate::runtime::StorageContext,
    operation_id: &str,
) -> Result<(), ApiError> {
    let now = utc_now()?;
    let connection = hot_plane::open_write(index_root)?;
    let updated = connection
        .execute(MARK_RUNNING_SQL, params![operation_id, now])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark operation {operation_id} running: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("operation {operation_id} pending → running"),
    )
}

/// Transition `running → succeeded`, stamping completed_at. Status-guarded and
/// asserted to hit exactly one row, so completing a non-running operation
/// fails loudly.
pub(crate) fn mark_succeeded(
    index_root: &crate::runtime::StorageContext,
    operation_id: &str,
) -> Result<(), ApiError> {
    let now = utc_now()?;
    let connection = hot_plane::open_write(index_root)?;
    let updated = connection
        .execute(MARK_SUCCEEDED_SQL, params![operation_id, now])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark operation {operation_id} succeeded: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("operation {operation_id} running → succeeded"),
    )
}

/// Transition `running → failed`, stamping completed_at and the bounded error
/// column. The detail is bounded via `truncate_persisted_detail` before it is
/// persisted. Status-guarded and asserted to hit exactly one row, so failing a
/// non-running operation is a loud failure.
pub(crate) fn mark_failed(
    index_root: &crate::runtime::StorageContext,
    operation_id: &str,
    bounded_error: &str,
) -> Result<(), ApiError> {
    let detail = truncate_persisted_detail(bounded_error, &index_root.limits().diagnostics);
    let now = utc_now()?;
    let connection = hot_plane::open_write(index_root)?;
    let updated = connection
        .execute(MARK_FAILED_SQL, params![operation_id, now, detail])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark operation {operation_id} failed: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("operation {operation_id} running → failed"),
    )
}

/// Find durable evidence of an incomplete destructive reset before starting workers.
/// A successful retry deletes earlier markers, so any remaining non-success blocks admission.
pub(crate) fn unresolved_rebuild(
    index_root: &crate::runtime::StorageContext,
) -> Result<Option<String>, ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    let kind = enum_wire_name(&OperationType::RebuildAll, "operation type")?;
    connection.query_row(
        "SELECT id FROM operations WHERE operation_type = ?1 AND status != 'succeeded' ORDER BY created_at DESC LIMIT 1",
        params![kind],
        |row| row.get(0),
    ).optional().map_err(|source| ApiError::StorageOperation {
        message: format!("failed to inspect rebuild-all recovery marker: {source}"),
    })
}

/// Read one Operation by id, re-typed into the model shape, for
/// `GET /operations/{operationId}`. Returns `None` when the id is absent so
/// the handler maps that to a 404. Opens a fresh read-only connection.
pub(crate) fn get(
    index_root: &crate::runtime::StorageContext,
    operation_id: &str,
) -> Result<Option<Operation>, ApiError> {
    let connection = hot_plane::open_read(index_root)?;
    let row = connection
        .query_row(SELECT_OPERATION_SQL, params![operation_id], |row| {
            Ok(OperationRow {
                id: row.get(0)?,
                operation_type: row.get(1)?,
                status: row.get(2)?,
                target_object_type: row.get(3)?,
                target_object_id: row.get(4)?,
                started_at: row.get(5)?,
                completed_at: row.get(6)?,
                error: row.get(7)?,
                created_at: row.get(8)?,
            })
        })
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read operation {operation_id}: {source}"),
        })?;

    row.map(operation_from_row).transpose()
}

/// One operations row as read from SQLite, before its wire-string columns are
/// re-typed back into the model enums.
struct OperationRow {
    id: String,
    operation_type: String,
    status: String,
    target_object_type: String,
    target_object_id: String,
    started_at: Option<String>,
    completed_at: Option<String>,
    error: Option<String>,
    created_at: String,
}

/// Re-type one persisted row into an `Operation`. The operation_type/status
/// wire strings re-type through their model enums, so a value outside the
/// schema set (or a corrupt value) fails loudly here rather than being misread
/// into a wrong branch (same pattern as `crate::annotations::store`).
fn operation_from_row(row: OperationRow) -> Result<Operation, ApiError> {
    let operation_type: OperationType =
        wire_value(&row.operation_type, &format!("operation {} type", row.id))?;
    let status: OperationStatus = wire_value(&row.status, &format!("operation {} status", row.id))?;

    Ok(Operation {
        id: row.id,
        operation_type,
        status,
        target_object_type: row.target_object_type,
        target_object_id: row.target_object_id,
        started_at: row.started_at,
        completed_at: row.completed_at,
        error: row.error,
        created_at: row.created_at,
    })
}

/// Render one closed model enum through its serde wire name so persisted column
/// values can never drift from the Rust enum (mirror of
/// `crate::annotations::store::enum_wire_name`).
fn enum_wire_name<T: Serialize>(value: &T, what: &'static str) -> Result<String, ApiError> {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => Ok(name),
        // Unreachable for plain renamed enums; kept explicit so a future
        // representation change fails loudly instead of persisting garbage.
        other => Err(ApiError::InternalIo {
            message: format!("{what} did not serialize to a string: {other:?}"),
        }),
    }
}

/// Re-type one persisted wire string through its model enum. The value set is
/// constrained by the Rust enum round-trip on write, but re-typing keeps the
/// read half symmetric so a corrupt value fails loudly instead of being
/// string-compared into a wrong branch (mirror of the annotations store).
fn wire_value<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> Result<T, ApiError> {
    serde_json::from_value(Value::String(text.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!("persisted {what} value {text:?} is not a known variant: {source}"),
        }
    })
}

/// Enforce that a status-guarded UPDATE hit exactly one row. Zero rows means
/// the guarded state was not present — an invariant breach to surface, never a
/// silent no-op (mirror of `crate::annotations::store::expect_single_row`).
fn expect_single_row(updated: usize, what: &str) -> Result<(), ApiError> {
    if updated == 1 {
        return Ok(());
    }
    Err(ApiError::StorageOperation {
        message: format!("{what} updated {updated} rows; the status guard did not match"),
    })
}
