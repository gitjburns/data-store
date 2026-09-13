//! Shared diagnostic context, error formatting, and bounded text/backoff helpers
//! with no domain or transport dependencies.

use std::{
    any::Any,
    error::Error,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::limits::DiagnosticLimits;
use tracing::{Instrument, Span, field::Empty};

/// Process-local diagnostic correlation; canonical object IDs retain their
/// separate persistence contract. No clock/entropy failure may block logging.
static DIAGNOSTIC_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Identify a request/call within this process. Log timestamps and process
/// startup records distinguish separate runs if the OS later reuses a PID.
pub(crate) fn diagnostic_id(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        DIAGNOSTIC_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

/// Existing tracing context carried explicitly across async/blocking/thread
/// boundaries. Fields absent at this boundary remain absent rather than guessed.
#[derive(Clone, Debug)]
pub(crate) struct LogContext {
    span: Span,
}

impl LogContext {
    /// Open a child work context without emitting another log entry. ERROR-level
    /// spans keep correlation on warnings/errors when INFO events are filtered.
    pub(crate) fn new(kind: &'static str, id: &str) -> Self {
        let context = Self {
            span: tracing::error_span!(
                "work",
                work_kind = kind,
                process_id = std::process::id(),
                work_id = Empty,
                trigger = Empty,
                reason = Empty,
                stage = Empty,
                request_id = Empty,
                operation_id = Empty,
                query_id = Empty,
                source_id = Empty,
                parse_id = Empty,
                source_paths = Empty,
                annotation_id = Empty,
                annotation_progress = Empty,
                section_id = Empty,
                split_index = Empty,
                target_units = Empty,
                prior_invalid_outputs = Empty,
                dispatch_wait_ms = Empty,
                model_role = Empty,
                call_purpose = Empty,
                call_id = Empty,
                cycle_id = Empty,
                entry_id = Empty,
                snapshot_id = Empty,
                projection_id = Empty,
                source_system = Empty,
                route = Empty,
                request_path = Empty,
                method = Empty,
                target_object_type = Empty,
                target_object_id = Empty,
                process_role = Empty,
            ),
        };
        let field = match kind {
            "request" => "request_id",
            "operation" => "operation_id",
            "query" => "query_id",
            "source" => "source_id",
            "parse" => "parse_id",
            "annotation" => "annotation_id",
            "model_call" => "call_id",
            "sync_cycle" | "annotation_cycle" => "cycle_id",
            "queue_entry" => "entry_id",
            "snapshot" => "snapshot_id",
            "projection" => "projection_id",
            _ => "work_id",
        };
        context.record(field, id);
        context
    }

    /// Capture the current parent before handing work to another executor/thread.
    pub(crate) fn current() -> Self {
        Self {
            span: Span::current(),
        }
    }

    /// Add a fact once it is known at the owning boundary; formatting is deferred
    /// to tracing's field visitor and never requires a storage lookup here.
    pub(crate) fn record(&self, name: &'static str, value: impl tracing::Value) {
        self.span.record(name, value);
    }

    /// Enter synchronous work only. Never hold this guard across an await;
    /// instrument the future instead so tasks cannot leak context into each other.
    pub(crate) fn enter(&self) -> tracing::span::Entered<'_> {
        self.span.enter()
    }

    /// Attribute nested synchronous work while restoring its caller's context afterward.
    pub(crate) fn in_scope<F: FnOnce() -> R, R>(&self, work: F) -> R {
        self.span.in_scope(work)
    }

    /// Move context with a blocking/scoped-thread closure; the lease lasts only
    /// while that closure executes, including its terminal error handling.
    pub(crate) fn wrap<F: FnOnce() -> R, R>(self, work: F) -> impl FnOnce() -> R {
        move || self.in_scope(work)
    }

    /// Enter context on each future poll rather than keeping a thread-local
    /// span guard alive while an async task is suspended.
    pub(crate) fn instrument<F: Future>(&self, future: F) -> tracing::instrument::Instrumented<F> {
        future.instrument(self.span.clone())
    }
}

/// Give each model call its own identity under its parent query/source work.
pub(crate) fn model_call_context(role: &'static str, purpose: &str) -> LogContext {
    let context = LogContext::new("model_call", &diagnostic_id("call"));
    context.record("model_role", role);
    context.record("call_purpose", purpose);
    context
}

/// Preserve nested provider/OS causes that Display alone can omit. Bound even a
/// pathological cyclic error chain and mark truncation explicitly.
pub(crate) fn error_chain(error: &dyn Error, limits: &DiagnosticLimits) -> String {
    let mut detail = error.to_string();
    let mut next = error.source();
    for _ in 0..limits.error_chain_depth {
        let Some(cause) = next else {
            return truncate_diagnostic_text(&detail, limits);
        };
        detail.push_str("; caused by: ");
        detail.push_str(&cause.to_string());
        next = cause.source();
    }
    if next.is_some() {
        detail.push_str(" [cause chain truncated]");
    }
    truncate_diagnostic_text(&detail, limits)
}

/// Bound visible error details using the owning operation's configured character budget.
pub(crate) fn truncate_diagnostic_text(value: &str, limits: &DiagnosticLimits) -> String {
    let mut truncated = value
        .trim()
        .chars()
        .take(limits.max_error_chars)
        .collect::<String>();
    if value.chars().count() > limits.max_error_chars {
        truncated.push_str("...");
    }
    truncated
}

/// Bound persisted summaries and mark truncation; the original operation error
/// remains available at its authoritative boundary under the separate error budget.
pub(crate) fn truncate_persisted_detail(detail: &str, limits: &DiagnosticLimits) -> String {
    if detail.chars().count() <= limits.persisted_detail_chars {
        return detail.to_owned();
    }
    let mut bounded: String = detail.chars().take(limits.persisted_detail_chars).collect();
    bounded.push_str(" [truncated]");
    bounded
}

/// Extract a bounded, readable message from a joined thread's panic payload.
///
/// Shared across thread-join boundaries so panic diagnostics stay consistent
/// across spawned-work owners. Returns a bounded diagnostic string safe for
/// logs.
pub(crate) fn panic_payload_message(
    payload: &(dyn Any + Send),
    limits: &DiagnosticLimits,
) -> String {
    let message = if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    };

    truncate_diagnostic_text(&message, limits)
}

/// Diagnostic prefixes never decide identity; character boundaries also make
/// malformed non-ASCII identifiers safe to report before validation rejects them.
pub(crate) fn hash_prefix<'a>(hash: &'a str, limits: &DiagnosticLimits) -> &'a str {
    let end = hash
        .char_indices()
        .nth(limits.hash_prefix_chars)
        .map_or(hash.len(), |(byte, _)| byte);
    &hash[..end]
}
