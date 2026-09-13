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

Service logs record compact operational evidence, not complete external
request/response payloads. Selected annotator exchange fields belong in the separate
`logs/annotator.log` transcript described below. Broader external-payload
auditing remains deferred. Payload prohibitions apply regardless of log level.

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

## Log Levels

- INFO records startup/shutdown, queries and admin operations, actual ingestion
  and state changes, and meaningful stage completions with counts and elapsed time.
- DEBUG records routine scans, empty successful cycles, health/Operation polling,
  successful internal transaction mechanics, expected contention, individual
  diagnostic details, and task completion already covered by an operation outcome.
- WARN/ERROR retain failures and their specific source and boundary context.

The default INFO log must explain real work and failures without DEBUG enabled.
Cycle summaries use INFO when work occurred or problems were encountered, and
DEBUG when empty and successful. Aggregate repetitive non-error diagnostics at
the owning stage; retain individual details at DEBUG.

## Work Identity And Outcome

- Carry `LogContext` from the owning operation across task/thread boundaries.
  Enter synchronous scopes; instrument futures rather than holding span guards
  across awaits. Record each contextual fact once; use child contexts for stages.
- Reuse canonical IDs and include known source paths and the trigger. Request
  and call IDs are process-local diagnostics, not durable records or API handles.
- Distinguish response receipt, validation, staged writes, committed state, and
  publication. Report unknown outcomes explicitly; a rollback requested on drop
  is not an observed successful rollback.
- Label count scope: available versus examined, eligible versus exhausted, new
  failures versus unresolved failures, and observed attempts versus retry limits.
- Separate queue/lock waiting, endpoint round-trip, and persistence durations.
  Token counts come only from the provider or actual tokenizer; absent metadata
  stays absent, and character counts are never presented as tokens.
- Preserve the original error and its nested causes at the failed boundary.

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

Successful transaction mechanics use DEBUG. The owning operation records durable
outcomes for real work at INFO after commit, including post-commit publication
when applicable. Transaction failures and visible rollback/abort retain the
operation, phase, local identifiers, and source error at WARN/ERROR.

`sql.execution_timed_out` records operation, statement fingerprint, and configured
budget without SQL parameter values. Its budget includes open row iteration and
caller work between rows. Rollback cleanup has an independent budget;
`sql.drop_rollback.*` records completion, failure, or unconfirmed transaction state.

Every model call and startup smoke check must log model role, call purpose,
start, success, normal error, elapsed milliseconds, compact input shape facts,
and configured limits. Keep model payloads out of the service log.

`model_capacity.*` records configured and advertised HTTP capacity, model identity,
and metadata failures before readiness. Distinguish serving capacity from window
limits and existing application prefix handling; server-side truncation is disabled.

Every external process call must log executable identity, purpose, start,
configured timeout, process ID when available, completion status or timeout,
elapsed milliseconds, and bounded stdout/stderr diagnostics on failure.
`docling.sample.started`, `.spawned`, and `.completed`/`.failed` record optional
sampling PIDs, elapsed time, capture counts, timeout/truncation flags, and cleanup
outcome. Partial telemetry is not a complete sample; raw sample text is not logged.

Every spawned task must have durable visibility for start or acceptance, normal
completion, normal error, panic when detectable, and cancellation or join
failure when detectable.

Projection publication uses `projection_worker.*` lifecycle records with source,
parse, and applicable cohort/input identities. Archived embeddings are not yet
published; report publication only after commit. Preserve separate failure-audit,
retirement, cancellation, and join outcomes.

Query stages remain distinct: `query.channels.*` collects source/graph candidates,
`query.annotation.*` scans annotation representations, `query.fusion.*` records
grouped fusion, and `query.annotation_maxsim.*` scores annotation/source windows.
Retain candidate counts, invalid-input exclusions, source errors, and elapsed time.
Resource admission failures name the configured guard and retained artifact/work
identity; lowering a read budget must not be reported as historical corruption.

### Annotation Cancellation

`maintenance.annotation_cancellation_requested` records rebuild or shutdown
signalling. The existing `rebuild_all.drain_started` and
`annotator_http.call.started` events include `annotation_cancel_requested`
(boolean) and `annotation_cancel_reason` (`None` or `Some("rebuild")`,
`Some("shutdown")`, `Some("storage_paused")`, or
`Some("cancellation_owner_dropped")`). The drain event samples the gate after
cancellation is signalled; the call-start event samples the client's receiver
before HTTP polling and does not prove a request reached the server.

For in-flight requests, `annotator_http.call.cancelled` records `reason`,
`elapsed_ms`, and `remote_outcome="unknown"`. Worker `wave_cancelled` and
`cycle_cancelled` events under `annotation_worker` identify discarded results
and stopped storage work. Cancellation consumes no retry or failure accounting;
actual producer failures remain logged as `annotation_worker.discarded_failure`.
Transaction cancellation or rollback failure retains its own storage diagnostics.
Cancellation events depend on the work in progress; an idle worker need not
emit a request-cancelled event.

`rebuild_all.drained` reports elapsed milliseconds until all storage leases are
released; other admitted work may still delay this boundary after annotation
cancellation. `rebuild_all.completed` means storage clearing finished and
background ingestion resumed. Neither local HTTP cancellation nor these
rebuild events confirm that the remote server released inference resources.

The projection worker stops new batches at cancellation checkpoints. Its blocking
embedding calls finish or reach configured timeout/retry limits before the storage
lease is released; unpublished results are discarded.

## Required Context

Include compact, local, safe facts that identify the boundary. Common fields
include:

| Boundary | Required context |
| --- | --- |
| Startup | process/run identity, phase, mode, bind address when known, config path when known, elapsed time |
| Operation | request/work identity, operation ID when assigned, target, trigger, stage, elapsed time |
| Request validation | request ID, route/path, rejected field, safe limit/value, status, error kind |
| Source resolution | requested source, relative source, resolved path on success, elapsed time |
| Docling/process | executable path, source reference, output directory, timeout, exit status, elapsed time, bounded failure diagnostics |
| Model call | parent work and call IDs, role, purpose, input shape/limits, measured usage when supplied, finish reason when supplied, elapsed time, error on failure |
| Storage | source path, version label, document ID when available, phase, counts, commit/publish state |
| Active publish | source path, version label, vector count, published timestamp, phase |
| Task | task purpose, operation ID when available, completion/error/panic/cancel state |
| Shutdown | request, confirmation, token-file cleanup result without token, service stop state |

Do not invent facts. If an outcome is unknown because the process or transport
failed, log the last known authoritative boundary and make the unknown portion
explicit.

Enrich existing annotation-related service-log entries with
`annotation_progress="completed / total (percentage)"` for the associated
document's committed coverage of its measured excerpt/type plan. Use
`unavailable` without a measured document context, including startup and dry runs;
zero required work displays `0 / 0 (no required work)`. Do not add entries for
progress. Counts and percentage follow PROTOCOL.md; retries and output-item
counts are not completion, and 100% does not assert projection publication.

Projection health and DEBUG document observations use their own published,
pending, and failed counts with source/parse measurement times. Never substitute
annotation completion percentages for publication coverage.

## Forbidden Log Data

Except for annotator payloads in the dedicated transcript below, never log:

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

### Annotator transcript

`logs/annotator.log`, relative to the config directory, records annotator requests
without `response_format` or `stream`; temperature remains included. Responses
show only content, reasoning, completion tokens, reasoning tokens, prompt tokens,
and total tokens. Missing or malformed fields display as unavailable. This is a
selected-field transcript, not a complete external-payload archive.
Authentication credentials remain excluded. Both normal work and dry runs append
readable text blocks independently of the service-log level.

Annotator calls use `stream: false`. Buffer each call's REQUEST and RESPONSE;
append them with RESULT as one contiguous group when the outcome is known.
Carry one `call_id` through the group. Keep content and reasoning untruncated;
record success or the specific failure/cancellation reason in RESULT.
Do not emit chunk or generation-progress records. Each group is
written under one shared lock and flushed before releasing it; never hold the
file lock during model calls. Abrupt process termination can lose unfinished
groups; service-log call-start records remain. Report transcript
open/write failures in the service log without changing annotation outcomes.

Success means structural validation passed, not semantic verification or database
commit. Persistence remains recorded in the service log under the parent
annotation context. Cancellation leaves the remote outcome unknown.

Place `Progress: completed / total (percentage)` immediately before `END CALL`, using
committed document progress when the group is written. Keep that write before
database persistence: calls in one wave may repeat a count, and the final
transcript group may remain below 100%. Health and existing service-log commit
entries reflect subsequent commits. Use `Progress: unavailable` without a measured
document context; add no transcript entries for progress.

`annotation_stage.entity_decisions` records candidate, accepted, and rejected
counts after exact candidate accounting passes. Rejection is a successful model
decision; only accepted entities contribute to the stage's output-item count.
Rejected names and reasons remain in the transcript response.

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
