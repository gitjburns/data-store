//! Explicit destructive rebuild-all. The existing schema survives; normal
//! ingestion owns reconstruction after exclusive storage access is released.

use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    time::Instant,
};

use rusqlite::params;
use tracing::{error, info};

use crate::{error::ApiError, hot_plane, model::OperationType, operations, state::AppState};

const RESET_SQL: &str = include_str!("../sql/fabric/reset.sql");

/// Drain before writing the recovery marker: an admitted projection build can
/// hold SQLite's writer for minutes. Acceptance waits for that work, while the
/// gate rejects competing requests and health explains the wait.
pub(crate) fn reserve(state: &AppState) -> Result<String, ApiError> {
    // The HTTP request identity remains the parent while no durable Operation
    // exists yet; identify the corpus and destructive trigger during draining.
    let context =
        crate::util::LogContext::new("rebuild_acceptance", &crate::util::diagnostic_id("rebuild"));
    context.record(
        "source_paths",
        tracing::field::display(state.config.storage.corpus_root.display()),
    );
    context.record("trigger", "operator_rebuild_all");
    let _context = context.enter();
    state.maintenance().begin()?;
    let started = Instant::now();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        info!(
            event = "rebuild_all.drain_started",
            "waiting for admitted storage work to finish before accepting rebuild-all"
        );
        state.maintenance().drain()?;
        info!(
            event = "rebuild_all.drained",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "all storage leases released"
        );

        operations::insert_pending(
            &state.config.storage.index_root,
            OperationType::RebuildAll,
            "corpus",
            &state.config.storage.corpus_root.to_string_lossy(),
        )
    }));
    let result = match outcome {
        Ok(result) => result,
        Err(payload) => Err(ApiError::InternalIo {
            message: format!(
                "rebuild-all acceptance panicked: {}",
                crate::util::panic_payload_message(payload.as_ref())
            ),
        }),
    };
    match &result {
        Ok(operation_id) => info!(
            event = "rebuild_all.accepted",
            operation_id, "rebuild-all accepted; admission closed"
        ),
        Err(source) => {
            error!(event = "rebuild_all.accept_failed", %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "rebuild-all acceptance failed; no data cleared");
            state.maintenance().fail(source.to_string());
        }
    }
    result
}

/// Own the detached task through its durable terminal boundary, even on panic.
/// Failure leaves admission closed; restarting cannot bypass the retained marker.
pub(crate) fn run(state: &AppState, operation_id: &str) {
    let context = crate::util::LogContext::new("operation", operation_id);
    context.record(
        "source_paths",
        tracing::field::display(state.config.storage.corpus_root.display()),
    );
    context.record("trigger", "operator_rebuild_all");
    let _context = context.enter();
    let started = Instant::now();
    info!(
        event = "rebuild_all.task_started",
        operation_id, "rebuild-all task started"
    );
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        clear_storage(state, operation_id)?;
        state
            .maintenance()
            .complete(|| operations::mark_succeeded(&state.config.storage.index_root, operation_id))
    }));
    let result = match outcome {
        Ok(result) => result,
        Err(payload) => Err(ApiError::InternalIo {
            message: format!(
                "rebuild-all task panicked: {}",
                crate::util::panic_payload_message(payload.as_ref())
            ),
        }),
    };
    match result {
        Ok(()) => info!(
            event = "rebuild_all.completed",
            operation_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            committed = true,
            rebuilding_in_background = true,
            "storage cleared; automatic rebuilding resumed (corpus rebuild is not yet complete)"
        ),
        Err(source) => {
            if let Err(mark_error) = record_failure(state, operation_id, &source.to_string()) {
                error!(event = "rebuild_all.terminal_mark_failed", operation_id, %mark_error,
                    "failed to persist terminal error; inspect the retained Operation and service log");
            }
            state.maintenance().fail(source.to_string());
            error!(event = "rebuild_all.failed", operation_id, %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "rebuild-all failed; storage remains paused until explicit retry");
        }
    }
    info!(
        event = "rebuild_all.task_finished",
        operation_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "rebuild-all task finished"
    );
}

/// A storage fault can precede the pending Operation's start; preserve the existing
/// status-guarded transition contract when recording that terminal failure.
fn record_failure(state: &AppState, operation_id: &str, detail: &str) -> Result<(), ApiError> {
    let root = &state.config.storage.index_root;
    if let Some(operation) = operations::get(root, operation_id)?
        && operation.status == crate::model::OperationStatus::Pending
    {
        operations::mark_running(root, operation_id)?;
    }
    operations::mark_failed(root, operation_id, detail)
}

/// Clear rows before removing blobs, retaining the Operation that detects a
/// crash between these non-atomic boundaries. No storage user runs until resume.
fn clear_storage(state: &AppState, operation_id: &str) -> Result<(), ApiError> {
    operations::mark_running(&state.config.storage.index_root, operation_id)?;
    let root = &state.config.storage.index_root;
    let targets = deletion_targets(root, &state.config.storage.corpus_root)?;
    let mut connection = hot_plane::open_write(root)?;
    let tx = hot_plane::begin_write_transaction(&mut connection, "rebuild_all", "clear_rows")?;
    info!(
        event = "rebuild_all.rows_started",
        operation_id, "clearing application rows; preserving schema and current rebuild Operation"
    );
    let cleared = tx
        .execute_batch(RESET_SQL)
        .and_then(|()| {
            tx.execute(
                "DELETE FROM operations WHERE id != ?1",
                params![operation_id],
            )
            .map(|_| ())
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "rebuild-all {operation_id} failed clearing application rows: {source}"
            ),
        });
    if let Err(source) = cleared {
        return Err(hot_plane::abort_transaction(
            tx,
            "rebuild_all",
            "clear_rows",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, "rebuild_all", "clear_rows")?;
    drop(connection);
    info!(
        event = "rebuild_all.rows_cleared",
        operation_id, "empty application tables committed"
    );

    for path in targets {
        let boundary = Instant::now();
        info!(event = "rebuild_all.files_started", operation_id, path = %path.display(), "removing stored artifacts or staging");
        match fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "rebuild-all {operation_id} failed removing {}: {source}",
                        path.display()
                    ),
                });
            }
        }
        info!(event = "rebuild_all.files_cleared", operation_id, path = %path.display(),
            elapsed_ms = boundary.elapsed().as_millis() as u64, "stored directory removed or already absent");
    }

    info!(
        event = "rebuild_all.publish_started",
        operation_id, "resetting caches and registering loaded policies"
    );
    let identity = state.application_identity();
    crate::register_policy_versions(
        root,
        &identity.entity_match_policy_hash,
        &identity.annotator_naming_policy_hash,
    )?;
    state.clear_rebuild_state()?;
    info!(
        event = "rebuild_all.publish_completed",
        operation_id, "empty caches and current policy registry ready"
    );
    Ok(())
}

/// Restrict recursive deletion to owned child directories, never the corpus
/// or a symlink substituted for an owned root. Nested symlinks are not followed
/// by remove_dir_all. Missing directories are valid on first run or retry.
fn deletion_targets(index_root: &Path, corpus_root: &Path) -> Result<[PathBuf; 2], ApiError> {
    let fabric = fs::canonicalize(index_root.join("fabric")).map_err(|source| {
        ApiError::StorageOperation {
            message: format!("cannot resolve fabric directory for rebuild-all: {source}"),
        }
    })?;
    let corpus = fs::canonicalize(corpus_root).map_err(|source| ApiError::SourceResolution {
        message: format!("cannot resolve corpus directory for rebuild-all: {source}"),
    })?;
    if fabric.starts_with(&corpus) || corpus.starts_with(&fabric) {
        return Err(ApiError::BadRequest {
            message: "rebuild-all requires disjoint corpus and fabric directories".into(),
        });
    }
    let targets = [fabric.join("artifacts"), fabric.join("staging")];
    for path in &targets {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(ApiError::BadRequest {
                    message: format!(
                        "rebuild-all target {} must be a real directory",
                        path.display()
                    ),
                });
            }
            Err(source) => {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "cannot inspect rebuild-all target {}: {source}",
                        path.display()
                    ),
                });
            }
        }
    }
    Ok(targets)
}
