//! Small cross-module helpers with no domain or transport dependencies
//! (C1c seam cut). Currently hosts panic-payload rendering shared by the
//! transport shell, operation pipelines, and inference device setup, plus
//! the bounded-text helpers shared by diagnostics and persisted details.

use std::any::Any;

/// Upper bound on diagnostic text carried into API errors and logs.
pub(crate) const MAX_DIAGNOSTIC_CHARS: usize = 16_000;

/// Truncate diagnostic text so API errors remain readable.
pub(crate) fn truncate_diagnostic_text(value: &str) -> String {
    let mut truncated = value
        .trim()
        .chars()
        .take(MAX_DIAGNOSTIC_CHARS)
        .collect::<String>();
    if value.chars().count() > MAX_DIAGNOSTIC_CHARS {
        truncated.push_str("...");
    }
    truncated
}

/// Cap applied to failure/error detail text before it is persisted into
/// hot-plane detail columns (acquisition_records.failure_detail,
/// sync_queue.last_error), so a pathological connector or drain error can
/// never bloat records or logs.
pub(crate) const PERSISTED_DETAIL_MAX_CHARS: usize = 500;

/// Bound detail text to PERSISTED_DETAIL_MAX_CHARS before persisting or
/// logging it; truncation is marked explicitly so a capped detail is never
/// mistaken for the complete message.
pub(crate) fn truncate_persisted_detail(detail: &str) -> String {
    if detail.chars().count() <= PERSISTED_DETAIL_MAX_CHARS {
        return detail.to_owned();
    }
    let mut bounded: String = detail.chars().take(PERSISTED_DETAIL_MAX_CHARS).collect();
    bounded.push_str(" [truncated]");
    bounded
}

/// Extract a bounded, readable message from a joined thread's panic payload.
///
/// Shared across thread-join boundaries so panic diagnostics stay consistent
/// across spawned-work owners. Returns a bounded diagnostic string safe for
/// logs.
pub(crate) fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    let message = if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    };

    truncate_diagnostic_text(&message)
}
