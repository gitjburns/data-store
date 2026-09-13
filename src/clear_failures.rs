//! Explicit retry permission preserves failed attempts and all successful data.

use crate::{
    error::ApiError,
    events, hot_plane,
    model::{OperationStatus, OperationType, SystemEventType},
    operations,
    sqlite::Connection,
    state::AppState,
};
use rusqlite::{OptionalExtension, params};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};
use tracing::{error, info};

const UNCLEARED_PARSES_SQL: &str = "
SELECT p.id FROM parse_runs p WHERE p.status = 'failed'
AND NOT EXISTS (SELECT 1 FROM system_events e WHERE e.event_type = ?2
AND e.object_type = 'parse' AND e.object_id = p.id)
ORDER BY p.id LIMIT ?1";

// Gather locations before writing markers. Deleted locations and healthy queue
// entries must never be revived or replaced by an operator failure clear.
const RETRY_LOCATIONS_SQL: &str = "
SELECT q.source_system, q.native_uri, l.source_id FROM sync_queue q
LEFT JOIN source_locations l ON l.source_system = q.source_system AND l.native_uri = q.native_uri
WHERE q.state = 'failed' AND (l.status IS NULL OR l.status != 'deleted')
UNION
SELECT l.source_system, l.native_uri, l.source_id FROM source_locations l
WHERE l.status IN ('current', 'access_lost') AND EXISTS (
 SELECT 1 FROM parse_runs p WHERE p.source_id = l.source_id AND p.status = 'failed'
 AND p.id = (SELECT latest.id FROM parse_runs latest WHERE latest.source_id = p.source_id
 ORDER BY latest.created_at DESC, latest.id DESC LIMIT 1)
 AND NOT EXISTS (SELECT 1 FROM system_events e WHERE e.event_type = ?2
 AND e.object_type = 'parse' AND e.object_id = p.id))
ORDER BY 1, 2 LIMIT ?1";

const RETRY_QUEUE_SQL: &str = "
INSERT INTO sync_queue (id, source_key, source_system, native_uri, detected_at, reason,
 state, attempt_count, last_attempt_at, last_error, coalesced_count, created_at, operation_id)
VALUES (?1, ?2, ?3, ?4, ?5, 'operator_clear_failures', 'pending', 0, NULL, NULL, 0, ?5, ?6)
ON CONFLICT(source_key) DO UPDATE SET state = 'pending', detected_at = excluded.detected_at,
 reason = excluded.reason, operation_id = excluded.operation_id,
 coalesced_count = sync_queue.coalesced_count + 1
WHERE sync_queue.state = 'failed'";

/// Derive the shared SQL discriminator from the closed event wire contract.
pub(crate) fn parse_failure_clear_event_name() -> Result<String, ApiError> {
    match serde_json::to_value(SystemEventType::ParseFailureCleared) {
        Ok(serde_json::Value::String(name)) => Ok(name),
        other => Err(storage_error(format!(
            "failure-clear event must serialize as a string: {other:?}"
        ))),
    }
}

/// Stop admission without cancelling healthy calls, then reserve the durable
/// admin handle after every admitted writer has reached its normal boundary.
pub(crate) fn reserve(state: &AppState) -> Result<String, ApiError> {
    state.maintenance().begin_failure_clear()?;
    let started = Instant::now();
    info!(
        event = "clear_failures.drain_started",
        "waiting for admitted work to finish safely"
    );
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        state.maintenance().drain_failure_clear()?;
        info!(
            event = "clear_failures.drained",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "all storage leases released"
        );
        operations::insert_pending(
            &state.storage,
            OperationType::ClearFailures,
            "corpus",
            &state.config.storage.corpus_root.to_string_lossy(),
        )
    }));
    let result = unwind_result(state, outcome, "acceptance");
    match &result {
        Ok(operation_id) => info!(
            event = "clear_failures.accepted",
            operation_id, "failure clear accepted; admission closed"
        ),
        Err(source) => {
            error!(event = "clear_failures.accept_failed", %source, "failure clear acceptance failed; no failure state changed");
            release_hold(state);
        }
    }
    result
}

/// Own terminal reporting through panics and distinguish committed retries from
/// later observation or operation-record failures; no destructive recovery hold exists.
pub(crate) fn run(state: &AppState, operation_id: &str) {
    let context = crate::util::LogContext::new("operation", operation_id);
    context.record("trigger", "operator_clear_failures");
    let _context = context.enter();
    let started = Instant::now();
    let mut committed = false;
    info!(
        event = "clear_failures.task_started",
        operation_id, "clearing failure blocks"
    );
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        operations::mark_running(&state.storage, operation_id)?;
        clear_persisted_failures(state, operation_id)?;
        committed = true;
        // Observational refresh must not prevent workers seeing the committed
        // retry generation, even if an observer panics after storage succeeded.
        let refresh = catch_unwind(AssertUnwindSafe(|| {
            state.refresh_after_failure_clear();
            Ok(())
        }));
        if let Err(source) = unwind_result(state, refresh, "monitor refresh") {
            error!(event = "clear_failures.monitor_refresh_failed", operation_id, %source, committed, "retry permission committed; monitor refresh failed");
        }
        state
            .maintenance()
            .complete_failure_clear(|| operations::mark_succeeded(&state.storage, operation_id))?;
        info!(
            event = "clear_failures.resumed",
            operation_id, "workers signaled; retry eligibility restored"
        );
        Ok(())
    }));
    match unwind_result(state, outcome, "execution") {
        Ok(()) => info!(
            event = "clear_failures.completed",
            operation_id,
            committed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "failure blocks cleared; ingestion continues in background"
        ),
        Err(source) => {
            let detail = format!("failure clear committed={committed}: {source}");
            if let Err(mark_error) = record_failure(state, operation_id, &detail) {
                error!(event = "clear_failures.terminal_mark_failed", operation_id, %mark_error, committed, "cannot persist failure-clear terminal outcome");
            }
            release_hold(state);
            error!(event = "clear_failures.failed", operation_id, %source, committed, elapsed_ms = started.elapsed().as_millis() as u64, "failure-clear operation failed; committed retry permission is retained");
        }
    }
    info!(
        event = "clear_failures.task_finished",
        operation_id, committed, "failure-clear task finished"
    );
}

/// Append retry permission and queue Operations atomically; rollback cannot leave
/// a marker without its corresponding retry admission.
fn clear_persisted_failures(state: &AppState, operation_id: &str) -> Result<(), ApiError> {
    let mut connection = hot_plane::open_write(&state.storage)?;
    let tx =
        hot_plane::begin_write_transaction(&mut connection, "clear_failures", "retry_permission")?;
    info!(
        event = "clear_failures.storage_started",
        operation_id, "discovering blocked attempts and staging retry permission"
    );
    let outcome = (|| {
        let parses = uncleared_parses(&tx)?;
        let locations = retry_locations(&tx)?;
        let mut requeued = 0usize;
        for location in &locations {
            requeued += usize::from(requeue_location(&tx, location)?);
        }
        for parse_id in &parses {
            let payload = [events::entry("operationId", operation_id)]
                .into_iter()
                .collect();
            events::append_event(
                &tx,
                &events::new_system_event(
                    SystemEventType::ParseFailureCleared,
                    "parse",
                    parse_id,
                    Some(payload),
                )?,
            )?;
        }
        Ok::<_, ApiError>((parses.len(), requeued))
    })();
    let (failed_parses, requeued_sources) = match outcome {
        Ok(counts) => counts,
        Err(source) => {
            return Err(hot_plane::abort_transaction(
                tx,
                "clear_failures",
                "retry_permission",
                source,
            ));
        }
    };
    hot_plane::commit_transaction(tx, "clear_failures", "retry_permission")?;
    info!(
        event = "clear_failures.storage_committed",
        operation_id,
        failed_parses,
        requeued_sources,
        "retry permission and fresh queue Operations committed; history retained"
    );
    Ok(())
}

/// Bound the administrative scan without silently accepting partial clearing.
fn uncleared_parses(connection: &Connection) -> Result<Vec<String>, ApiError> {
    let limit = connection.limits().resources.max_startup_parses;
    let mut statement = connection
        .prepare(UNCLEARED_PARSES_SQL)
        .map_err(|e| storage_error(format!("prepare failed parse scan: {e}")))?;
    let mut rows = statement
        .query(params![limit as i64 + 1, parse_failure_clear_event_name()?])
        .map_err(|e| storage_error(format!("scan failed parses: {e}")))?;
    let mut result = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| storage_error(format!("read failed parse: {e}")))?
    {
        if result.len() == limit {
            return Err(storage_error(format!(
                "failure clear exceeds resources.max_startup_parses={limit}"
            )));
        }
        check_text_cell(row, 0, connection.limits().resources.max_json_cell_bytes)?;
        result.push(
            row.get(0)
                .map_err(|e| storage_error(format!("decode failed parse ID: {e}")))?,
        );
    }
    Ok(result)
}

/// The optional source identity distinguishes acquisition retries from reparses.
struct RetryLocation {
    source_system: String,
    native_uri: String,
    source_id: Option<String>,
}

/// Discover every retry coordinate before markers hide old failures from readers.
fn retry_locations(connection: &Connection) -> Result<Vec<RetryLocation>, ApiError> {
    let limit = connection.limits().resources.max_sources;
    let mut statement = connection
        .prepare(RETRY_LOCATIONS_SQL)
        .map_err(|e| storage_error(format!("prepare failed source scan: {e}")))?;
    let mut rows = statement
        .query(params![limit as i64 + 1, parse_failure_clear_event_name()?])
        .map_err(|e| storage_error(format!("scan failed sources: {e}")))?;
    let mut result = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| storage_error(format!("read failed source: {e}")))?
    {
        if result.len() == limit {
            return Err(storage_error(format!(
                "failure clear exceeds resources.max_sources={limit}"
            )));
        }
        for column in 0..3 {
            check_text_cell(
                row,
                column,
                connection.limits().resources.max_json_cell_bytes,
            )?;
        }
        result.push(
            (|| -> rusqlite::Result<_> {
                Ok(RetryLocation {
                    source_system: row.get(0)?,
                    native_uri: row.get(1)?,
                    source_id: row.get(2)?,
                })
            })()
            .map_err(|e| storage_error(format!("decode failed source location: {e}")))?,
        );
    }
    Ok(result)
}

/// Preserve healthy pending/in-flight queue ownership and retain prior terminal
/// Operations; a fresh Operation forces unchanged-file staging on the next scan.
fn requeue_location(connection: &Connection, location: &RetryLocation) -> Result<bool, ApiError> {
    let key = crate::scheduler::source_key(&location.source_system, &location.native_uri);
    let existing: Option<String> = connection
        .query_row(
            "SELECT state FROM sync_queue WHERE source_key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| storage_error(format!("inspect retry queue {key}: {e}")))?;
    if existing.is_some_and(|state| state != "failed") {
        return Ok(false);
    }
    let (kind, target_kind, target_id) = match &location.source_id {
        Some(source_id) => (OperationType::ParserExecution, "source", source_id.as_str()),
        None => (
            OperationType::SourceIngest,
            "source_reference",
            location.native_uri.as_str(),
        ),
    };
    let operation_id = operations::insert_pending_on(connection, kind, target_kind, target_id)?;
    connection
        .execute(
            RETRY_QUEUE_SQL,
            params![
                crate::ids::new_sync_queue_entry_id()?,
                key,
                location.source_system,
                location.native_uri,
                crate::primitives::utc_now()?,
                operation_id
            ],
        )
        .map_err(|e| storage_error(format!("requeue failed source {key}: {e}")))?;
    Ok(true)
}

/// Preserve the actual storage boundary in propagated administrative errors.
fn storage_error(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}

/// Reject oversized borrowed SQL cells before allocating owned retry identities.
fn check_text_cell(row: &rusqlite::Row<'_>, column: usize, limit: usize) -> Result<(), ApiError> {
    let value = row.get_ref(column).map_err(|source| {
        storage_error(format!(
            "read failure-clear identity column {column}: {source}"
        ))
    })?;
    if let rusqlite::types::ValueRef::Text(bytes) = value
        && bytes.len() > limit
    {
        return Err(storage_error(format!(
            "failure-clear identity exceeds resources.max_json_cell_bytes={limit}"
        )));
    }
    Ok(())
}

/// Catch detached administrative panics while retaining the configured diagnostic detail.
fn unwind_result<T>(
    state: &AppState,
    outcome: std::thread::Result<Result<T, ApiError>>,
    stage: &str,
) -> Result<T, ApiError> {
    outcome.unwrap_or_else(|payload| {
        Err(ApiError::InternalIo {
            message: format!(
                "clear-failures {stage} panicked: {}",
                crate::util::panic_payload_message(payload.as_ref(), &state.config.diagnostics)
            ),
        })
    })
}

/// Release only this nondestructive hold; shutdown or rebuild admission stays closed.
fn release_hold(state: &AppState) {
    if let Err(source) = state.maintenance().abort_failure_clear() {
        error!(event = "clear_failures.release_failed", %source, "could not release failure-clear admission hold");
    }
}

/// Respect status-guarded Operation transitions if failure preceded mark_running.
fn record_failure(state: &AppState, operation_id: &str, detail: &str) -> Result<(), ApiError> {
    if let Some(operation) = operations::get(&state.storage, operation_id)?
        && operation.status == OperationStatus::Pending
    {
        operations::mark_running(&state.storage, operation_id)?;
    }
    operations::mark_failed(&state.storage, operation_id, detail)
}
