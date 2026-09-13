//! Durable SystemEvent append over the hot-plane event log (spec §33).
//!
//! Core invariant: `append_event` takes the CALLER's connection, so when the
//! caller has an open transaction the event row commits (or rolls back)
//! atomically with the state change it records. This module never opens its
//! own connection or transaction; the audit trail can therefore never claim
//! an event for a state change that did not durably happen, or vice versa.
//!
//! Events are internal and audit-facing (spec §33): append failures surface
//! as explicit `ApiError`s with operation context and must not be swallowed
//! by callers. No polling/reading API exists yet; it arrives with its
//! consumers.

use serde_json::{Map, Value};

use crate::error::ApiError;
use crate::ids::new_system_event_id;
use crate::model::{SystemEvent, SystemEventType};
use crate::primitives::{current_time_ms, format_utc_timestamp_ms};

/// Appends one row into the `system_events` table created by
/// `sql/fabric/schema.sql`; `payload_json` is canonical JSON or NULL when the
/// event carries no payload.
const INSERT_SYSTEM_EVENT_SQL: &str = "
INSERT INTO system_events (
  id, event_type, object_type, object_id, payload_json, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// Durably append `event` to the system event log using the caller's
/// connection. When called inside the caller's transaction, the event
/// commits atomically with the state change it records — that coupling is
/// this module's core invariant, so this function never manages transactions
/// itself. Failures are explicit `StorageOperation` errors carrying the
/// event's identity; callers must propagate them, never swallow them.
pub(crate) fn append_event(
    connection: &crate::sqlite::Connection,
    event: &SystemEvent,
) -> Result<(), ApiError> {
    let event_type = event_type_wire_name(event.event_type)?;
    let payload_json = payload_canonical_json(event)?;

    connection
        .execute(
            INSERT_SYSTEM_EVENT_SQL,
            rusqlite::params![
                event.id,
                event_type,
                event.object_type,
                event.object_id,
                payload_json,
                event.created_at,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to append system event {} ({event_type} on {} {}): {source}",
                event.id, event.object_type, event.object_id
            ),
        })?;

    Ok(())
}

/// Build a SystemEvent with a freshly minted `evt_` ID and the current time
/// as its `createdAt` (RFC3339 UTC, millisecond precision). Convenience for
/// emit sites; the result still needs `append_event` to become durable.
pub(crate) fn new_system_event(
    event_type: SystemEventType,
    object_type: &str,
    object_id: &str,
    payload: Option<Map<String, Value>>,
) -> Result<SystemEvent, ApiError> {
    Ok(SystemEvent {
        id: new_system_event_id()?,
        event_type,
        object_type: object_type.to_owned(),
        object_id: object_id.to_owned(),
        payload,
        created_at: format_utc_timestamp_ms(current_time_ms()?)?,
    })
}

/// One (key, owned string value) event-payload entry, keeping payload map
/// construction terse at the emit sites. Shared here (beside the event
/// constructor whose payloads it builds) so the emitting modules do not each
/// carry a private copy.
pub(crate) fn entry(key: &str, value: &str) -> (String, Value) {
    (key.to_owned(), Value::String(value.to_owned()))
}

/// Render the event's payload as canonical JSON text for the
/// `payload_json` column, or `None` when the event carries no payload so
/// absent payloads persist as SQL NULL, matching the wire absent-vs-null
/// convention.
fn payload_canonical_json(event: &SystemEvent) -> Result<Option<String>, ApiError> {
    let Some(payload) = &event.payload else {
        return Ok(None);
    };

    let bytes = crate::canonical::canonical_json_bytes_of(payload)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the
    // error arm keeps the panic-free Result policy instead of unwrapping.
    let text = String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!(
            "canonical payload bytes for system event {} are not UTF-8: {source}",
            event.id
        ),
    })?;

    Ok(Some(text))
}

/// Recover the spec §33 dotted wire name (e.g. `parse.activated`) from the
/// enum's serde renames, so the persisted `event_type` column always matches
/// the wire schema without a second hand-maintained name table.
fn event_type_wire_name(event_type: SystemEventType) -> Result<String, ApiError> {
    match serde_json::to_value(event_type) {
        Ok(Value::String(name)) => Ok(name),
        // Unreachable for a plain renamed enum; kept explicit so a future
        // enum representation change fails loudly instead of persisting a
        // non-spec event_type.
        other => Err(ApiError::InternalIo {
            message: format!("system event type did not serialize to a string: {other:?}"),
        }),
    }
}
