//! Worker-side measurement of durable ingestion state. HTTP monitoring only
//! reads published observations and never invokes these SQLite readers.

use rusqlite::params;
use std::collections::BTreeSet;

use crate::{
    acquisition::{MIME_TYPE_EPUB, MIME_TYPE_PLAIN_TEXT},
    error::ApiError,
    hot_plane,
    monitoring::progress_count,
    monitoring_types::{MonitorIssue, MonitorState, WorkIdentity},
    primitives::time::utc_now,
    runtime::StorageContext,
};

// Current source state and its latest parse are measured together. A cleared
// failure remains in history but no longer explains an operational retry block.
const SOURCE_OBSERVATIONS_SQL: &str = "
SELECT objects.id, objects.active_parse_id, objects.deactivated_at,
       p.id, p.status, p.held_reason, p.error, objects.mime_type,
       (SELECT MIN(l.native_uri) FROM source_locations l
        WHERE l.source_id = objects.id AND l.status = 'current'),
       EXISTS(SELECT 1 FROM system_events e WHERE e.event_type = ?2
              AND e.object_type = 'parse' AND e.object_id = p.id)
FROM source_objects AS objects
LEFT JOIN parse_runs p ON p.id = (
    SELECT latest.id FROM parse_runs latest WHERE latest.source_id = objects.id
    ORDER BY latest.created_at DESC, latest.id DESC LIMIT 1)
WHERE EXISTS (SELECT 1 FROM source_locations AS locations
              WHERE locations.source_id = objects.id AND locations.status = 'current')
ORDER BY objects.id LIMIT ?1";

const QUEUE_FAILURES_SQL: &str = "
SELECT id, native_uri, last_error FROM sync_queue WHERE state = 'failed'
ORDER BY id LIMIT ?1";

/// Reconcile queue observations after a committed queue transition. A failed
/// diagnostic read is visible but cannot turn a successful ingestion write into failure.
pub(crate) fn refresh_queue(storage: &StorageContext) {
    let outcome = crate::scheduler::queue_depths(storage).and_then(|counts| {
        let measured_at = utc_now()?;
        storage.monitoring().update_ingestion(|snapshot| {
            snapshot.pending = Some(counts.pending);
            snapshot.in_flight = Some(counts.in_flight);
            snapshot.failed = Some(counts.failed);
            snapshot.queue_measured_at = Some(measured_at.clone());
            snapshot.measured_at = Some(measured_at);
        });
        Ok(())
    });
    report_measurement(storage, "queue inventory", outcome);
}

/// A fast autonomous drain must not rescan its whole queue after every row.
/// Claim and end-of-cycle boundaries still force a final authoritative sample.
pub(crate) fn refresh_queue_throttled(storage: &StorageContext) {
    if storage.monitoring().try_queue_measurement() {
        refresh_queue(storage);
    }
}

/// Count content-deduplicated current sources independently of filesystem paths.
/// Fast recovery of unchanged sources shares a fixed observation cadence.
pub(crate) fn refresh_sources_throttled(storage: &StorageContext) {
    if storage.monitoring().try_source_measurement() {
        refresh_sources(storage);
    }
}

/// Count content-deduplicated current sources independently of filesystem paths.
/// Only source owners call this after discovery or a durable lifecycle boundary.
pub(crate) fn refresh_sources(storage: &StorageContext) {
    let outcome = (|| {
        let mut connection = hot_plane::open_read(storage)?;
        let SourceObservations {
            known,
            active,
            blocked,
            issues,
        } = source_observations(&mut connection)?;
        let measured_at = utc_now()?;
        let issue_keys: BTreeSet<_> = issues.iter().map(|(key, _)| key.clone()).collect();
        // Replace only this reader's facts; transient model/storage failures have
        // their own owners and must not vanish merely because a scan completed.
        storage
            .monitoring()
            .retain_issues("persisted-source:", |key| issue_keys.contains(key));
        storage
            .monitoring()
            .retain_issues("persisted-queue:", |key| issue_keys.contains(key));
        for (key, issue) in issues {
            if issue.identity.source_id.is_some() {
                storage
                    .monitoring()
                    .clear_issue(&format!("ingestion:{}", issue.identity.document));
            }
            storage.monitoring().set_issue(key, issue);
        }
        storage.monitoring().update_ingestion(|snapshot| {
            snapshot.active_sources = progress_count(active, Some(known));
            snapshot.blocked_sources = Some(blocked);
            snapshot.source_measured_at = Some(measured_at.clone());
            snapshot.measured_at = Some(measured_at);
        });
        Ok(())
    })();
    report_measurement(storage, "source inventory", outcome);
}

/// Capture source availability and persistent failure explanations on one WAL
/// snapshot. Restarts therefore retain the reason an inactive document is blocked.
struct SourceObservations {
    known: u64,
    active: u64,
    blocked: u64,
    issues: Vec<(String, MonitorIssue)>,
}

/// Read complete, bounded source and queue observations before publishing any result.
fn source_observations(
    connection: &mut crate::sqlite::Connection,
) -> Result<SourceObservations, ApiError> {
    let limit = connection.limits().resources.max_sources;
    let cell_limit = connection.limits().resources.max_json_cell_bytes;
    let tx = hot_plane::begin_read_transaction(connection, "monitor", "source_inventory")?;
    let mut sources = tx
        .prepare(SOURCE_OBSERVATIONS_SQL)
        .map_err(observation_error)?;
    let mut rows = sources
        .query(params![
            limit as i64 + 1,
            crate::clear_failures::parse_failure_clear_event_name()?
        ])
        .map_err(observation_error)?;
    let mut known = 0;
    let mut active = 0;
    let mut blocked = 0;
    let mut issues = Vec::new();
    while let Some(row) = rows.next().map_err(observation_error)? {
        if known == limit as u64 {
            return Err(observation_failure(
                "source inventory exceeds resources.max_sources",
            ));
        }
        for column in 0..9 {
            check_observation_cell(row, column, cell_limit)?;
        }
        let source_id: String = row.get(0).map_err(observation_error)?;
        let active_parse: Option<String> = row.get(1).map_err(observation_error)?;
        let deactivated: Option<String> = row.get(2).map_err(observation_error)?;
        let parse_id: Option<String> = row.get(3).map_err(observation_error)?;
        let status: Option<String> = row.get(4).map_err(observation_error)?;
        let held: Option<String> = row.get(5).map_err(observation_error)?;
        let error: Option<String> = row.get(6).map_err(observation_error)?;
        let mime: String = row.get(7).map_err(observation_error)?;
        let path: String = row.get(8).map_err(observation_error)?;
        let cleared: bool = row.get(9).map_err(observation_error)?;
        known += 1;
        active += u64::from(active_parse.is_some() && deactivated.is_none());
        let explanation = match status.as_deref() {
            Some("failed") if !cleared => {
                blocked += 1;
                Some((
                    MonitorState::Failed,
                    format!(
                        "Parse failed: {}. Automatic retry is blocked; run --clear-failures to retry.",
                        error.as_deref().unwrap_or("failure detail unavailable")
                    ),
                ))
            }
            Some("ready") if held.is_some() => Some((
                MonitorState::Waiting,
                format!(
                    "Parse held for operator acceptance: {}. Use --accept with parse {}.",
                    held.as_deref().unwrap_or("reason unavailable"),
                    parse_id.as_deref().unwrap_or("identity unavailable")
                ),
            )),
            // The routable MIME set is exactly the scheduler's `ParseRoute`
            // vocabulary; a stored type outside it has no parser.
            None if mime.as_str() != MIME_TYPE_PLAIN_TEXT && mime.as_str() != MIME_TYPE_EPUB => {
                Some((
                    MonitorState::Unavailable,
                    format!("No parser is registered for content type {mime}."),
                ))
            }
            _ => None,
        };
        if let Some((state, message)) = explanation {
            let identity =
                WorkIdentity::new("ingestion", &path, Some(&source_id), parse_id.as_deref());
            issues.push((
                format!("persisted-source:{source_id}"),
                persisted_issue(identity, state, message),
            ));
        }
    }
    drop(rows);
    drop(sources);
    let mut queued = tx.prepare(QUEUE_FAILURES_SQL).map_err(observation_error)?;
    let mut rows = queued
        .query(params![limit as i64 + 1])
        .map_err(observation_error)?;
    let mut failures = 0;
    while let Some(row) = rows.next().map_err(observation_error)? {
        if failures == limit {
            return Err(observation_failure(
                "failed queue inventory exceeds resources.max_sources",
            ));
        }
        failures += 1;
        for column in 0..3 {
            check_observation_cell(row, column, cell_limit)?;
        }
        let id: String = row.get(0).map_err(observation_error)?;
        let path: String = row.get(1).map_err(observation_error)?;
        let error: Option<String> = row.get(2).map_err(observation_error)?;
        issues.push((
            format!("persisted-queue:{id}"),
            persisted_issue(
                WorkIdentity::new("ingestion", &path, None, None),
                MonitorState::Failed,
                format!(
                    "Ingestion failed: {}. Run --clear-failures to retry.",
                    error.as_deref().unwrap_or("failure detail unavailable")
                ),
            ),
        ));
    }
    Ok(SourceObservations {
        known,
        active,
        blocked,
        issues,
    })
}

/// Keep durable explanations separate from transient call observations; the
/// monitoring owner supplies observation age without pretending it is failure time.
fn persisted_issue(identity: WorkIdentity, state: MonitorState, message: String) -> MonitorIssue {
    MonitorIssue {
        identity,
        stage: "document status".into(),
        state,
        message,
        affected: 1,
        retry_in_ms: None,
        attempt: None,
        retry_limit: None,
        observed_since: None,
        elapsed_ms: 0,
    }
}

/// Inspect borrowed SQL cells before allocating display strings from persisted state.
fn check_observation_cell(
    row: &rusqlite::Row<'_>,
    column: usize,
    limit: usize,
) -> Result<(), ApiError> {
    if let rusqlite::types::ValueRef::Text(text) = row.get_ref(column).map_err(observation_error)?
        && text.len() > limit
    {
        return Err(observation_failure(
            "source observation exceeds resources.max_json_cell_bytes",
        ));
    }
    Ok(())
}

/// Preserve the failing SQL boundary while retaining the last measured dashboard state.
fn observation_error(source: rusqlite::Error) -> ApiError {
    ApiError::StorageOperation {
        message: format!("read source monitoring observations: {source}"),
    }
}

/// Resource refusal is explicit rather than a silently partial source inventory.
fn observation_failure(message: &str) -> ApiError {
    ApiError::StorageOperation {
        message: message.to_owned(),
    }
}

/// Preserve the last measurement and expose its failed refresh with source context.
fn report_measurement(storage: &StorageContext, stage: &str, outcome: Result<(), ApiError>) {
    let key = format!("monitor measurement:{stage}");
    match outcome {
        Ok(()) => storage.monitoring().clear_issue(&key),
        Err(source) => {
            tracing::error!(event = "monitor.measurement_failed", stage, error = %source,
                "monitoring retained its last measured ingestion state");
            storage.monitoring().set_issue(
                key,
                MonitorIssue {
                    identity: WorkIdentity::new("monitor", "corpus", None, None),
                    stage: stage.to_string(),
                    state: MonitorState::Unavailable,
                    message: source.to_string(),
                    affected: 1,
                    retry_in_ms: None,
                    attempt: None,
                    retry_limit: None,
                    observed_since: None,
                    elapsed_ms: 0,
                },
            );
        }
    }
}
