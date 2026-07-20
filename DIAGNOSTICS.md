# Data Store Diagnostics Coding Standard

## Core Rule

Every meaningful lifecycle boundary and every error path must leave durable,
useful evidence in the service log.

The log must let an operator determine what started, what completed, what
failed, where it failed, what durable state committed, what active state
published, what the client was told, and what remains unknown after process or
transport failure.

Terminal output, CLI output, comments, SQLite rows, and inferred state are not
durable diagnostics. They may repeat facts, but the service log must contain
the authoritative evidence.

## Useful Logs

A useful log records at least one of these facts:

- a lifecycle boundary was reached;
- a state transition occurred;
- a durable storage boundary was reached;
- an active in-memory publish boundary was reached;
- an external process or model call started, succeeded, or failed;
- a spawned task completed, failed, panicked, or was cancelled;
- a failure occurred with local diagnostic context.

Avoid vague activity logs. A line such as `operation failed` is insufficient
unless it includes the operation, operation ID, stage, error, relevant safe
identifiers, and elapsed time when measurable.

## Boundary Rule

Before adding or changing code, identify the diagnostic boundaries the code
crosses. At each fallible or long-running boundary, log:

- start;
- success or meaningful checkpoint;
- normal error with local context;
- elapsed milliseconds when the boundary has measurable duration.

Subsystem boundaries that must not pass generic errors upward include:

- startup parent/child handoff;
- HTTP request and operation dispatch;
- request validation;
- source resolution;
- Docling conversion and other external process calls;
- unit splitting;
- dense embedding;
- ColBERT embedding and scoring;
- reranker scoring;
- storage operations;
- active cache publish;
- admin token file publication and cleanup;
- shutdown signaling.

Helper functions may return typed errors without logging when their caller owns
the diagnostic boundary. Boundary-owning functions must log or wrap failures
with the local facts known at that boundary.

## Required Lifecycle Coverage

Startup must log mode, bind address, config path when known, admin token file
publication status without the token, inference initialization, model-role
boundaries, smoke checks, storage/cache initialization, HTTP bind attempt and
success, readiness, and fatal startup errors.

Every operation must log accepted, validation failure when applicable, each
meaningful stage start and success/checkpoint, terminal result ready, terminal
error ready, operation task finish, and panic or cancellation when detectable.

Every storage transaction must log begin attempt, begin success or failure,
each persistence phase, each phase failure with local identifiers, commit
attempt, commit success or failure, rollback or abort when directly visible,
and post-commit publish start/success/failure when applicable.

Every model call and startup smoke check must log model role, call purpose,
start, success, normal error, elapsed milliseconds, compact input shape facts,
and configured limits. Keep model payloads out of logs.

Every external process call must log executable identity, purpose, start,
configured timeout, process ID when available, completion status or timeout,
elapsed milliseconds, and bounded stdout/stderr diagnostics on failure.

Every spawned task must have durable visibility for start or acceptance, normal
completion, normal error, panic when detectable, and cancellation or join
failure when detectable.

## Required Context

Include compact, local, safe facts that identify the boundary. Common fields
include:

| Boundary | Required context |
| --- | --- |
| Startup | phase, mode, bind address when known, config path when known, elapsed time |
| Operation | operation name, operation ID, stage, sequence when stream-related, elapsed time |
| Request validation | operation or route, rejected field, safe limit/value, status, error kind |
| Source resolution | requested source, relative source, resolved path on success, elapsed time |
| Docling/process | executable path, source reference, output directory, timeout, exit status, elapsed time, bounded failure diagnostics |
| Model call | model role, purpose, compact input shape, configured limits, elapsed time, error on failure |
| Storage | source path, version label, document ID when available, phase, counts, commit/publish state |
| Active publish | source path, version label, vector count, published timestamp, phase |
| Task | task purpose, operation ID when available, completion/error/panic/cancel state |
| Shutdown | request, confirmation, token-file cleanup result without token, service stop state |

Do not invent facts. If an outcome is unknown because the process or transport
failed, log the last known authoritative boundary and make the unknown portion
explicit.

## Forbidden Log Data

Never log:

- admin tokens;
- document contents or full markdown;
- full prompts or full model outputs;
- token dumps;
- vector values or embedding matrices;
- unbounded stdout or stderr;
- large request or response payloads;
- secrets from configuration or environment.

Use compact identifiers, counts, dimensions, hashes, paths, statuses, elapsed
times, and bounded diagnostics instead.

## Implementation Guidance

Prefer local, direct fixes:

- add a start, success, or error log at the boundary owner;
- add missing local context before an error leaves a subsystem boundary;
- log durable transaction boundaries before and after commit;
- mirror startup progress to the service log after file logging is initialized;
- add task completion, panic, and cancellation visibility for spawned work.

Do not introduce a new observability framework, operation ledger, queue, retry
layer, fallback path, or schema change without explicit design approval.

The goal is diagnostic clarity, not log volume. A small number of specific
lifecycle logs is better than many generic lines.
