# Data Store Diagnostics Standard

This document is intentionally standalone. It is a coding standard and an
evaluation protocol, not an implementation plan.

## Intent

The service log must make service behavior diagnosable without guessing,
inspecting SQLite internals, relying on transient terminal output, or rerunning
expensive work.

For every important action, the log must let an operator determine:

- what started;
- what completed;
- what failed;
- where it failed;
- what durable state was committed or not committed;
- what active in-memory state was published or not published;
- what the client was told;
- what remains unknown when the process or transport failed.

This is not a request for high-volume logs. A small number of useful lifecycle
logs is better than many generic or repetitive logs.

## Definitions

**Diagnostic boundary** means a point where control crosses into work whose
failure or completion matters to service behavior. Examples include startup
phases, operation stages, model calls, external process calls, storage
transactions, cache publishes, operation stream delivery, spawned tasks, and
shutdown.

**Useful log** means a log entry that records a lifecycle boundary, state
transition, durable state boundary, client-delivery boundary, or failure with
local diagnostic facts.

**Durable evidence** means evidence written to
`logs/data-store.log`. Terminal-only output is not durable evidence.

**Terminal outcome** means a result or error that completes an operation or
startup path.

**Unknown outcome** means the service cannot prove whether work completed,
failed, or partially committed because the process, transport, or caller
disappeared before a terminal outcome was durably recorded.

**Local diagnostic context** means the facts available at the failing boundary:
operation name, operation ID, stage, source reference, version label, unit ID,
counts, elapsed time, status, error kind, configured path, model role, process
ID, exit status, or similar compact identifiers. Local context does not include
secrets, document contents, vector values, token dumps, or large raw payloads.

## Core Standard

Every meaningful lifecycle boundary and every error path must leave durable,
useful evidence in `logs/data-store.log`.

If a fact is important enough to print to an operator after file logging is
initialized, it is important enough to log durably.

If a failure crosses a subsystem boundary, the receiving or owning boundary
must add local diagnostic context before the error leaves that boundary.

If the service cannot know an outcome, the log must make the last known
authoritative boundary clear enough that the unknown part is narrow and
explicit.

## Useful Log Test

A log entry is useful only if it answers at least one of these questions:

- Which lifecycle boundary was reached?
- Which state transition occurred?
- Which durable state boundary was reached?
- Which active in-memory publish boundary was reached?
- Which external/model/process call started or completed?
- Which client-delivery boundary succeeded or failed?
- Which task completed, panicked, or was cancelled?
- Which failure happened, at what stage, with what local facts?

A log entry fails the useful log test if it is only a vague activity statement,
duplicates nearby facts without adding diagnostic value, or records data that
cannot help diagnose a real failure.

## Hard Rules

### R1. Startup Progress Must Be Durable

Every startup status or progress line emitted after file logging is initialized
must also be written to `logs/data-store.log`.

This includes progress sent to a background parent process. Parent-terminal
handoff output is not a substitute for service-log evidence.

Startup must log:

- startup mode and bind address;
- admin token file publication status without logging the token;
- inference initialization start, each model role boundary, each smoke-check
  boundary, success, and normal error;
- storage/cache initialization start, success, and normal error;
- HTTP bind attempt, bind success, readiness, and startup fatal failure;
- last startup boundary known to the parent if the background child exits during
  startup.

### R2. Operations Must Have Complete Lifecycle Logs

Every operation must log:

- accepted;
- request validation failure when applicable;
- every meaningful stage start;
- every meaningful stage success or checkpoint;
- terminal result ready;
- terminal error ready;
- terminal result/error delivery success or failure;
- operation task finish;
- operation task panic or cancellation when detectable.

Operation logs must include operation name and operation ID whenever an
operation ID exists.

### R3. Fallible Subsystem Boundaries Must Add Context

When code calls into a subsystem that can fail, the boundary owner must log or
wrap the error with local context before returning it.

Subsystem boundaries include:

- startup parent/child handoff;
- HTTP request and operation dispatch;
- source resolution;
- Docling conversion;
- unit splitting;
- dense embedding;
- ColBERT embedding and scoring;
- reranker scoring;
- storage operations;
- active cache publish;
- admin token file publication and cleanup;
- shutdown signaling.

Propagating a generic error across one of these boundaries without adding local
context is a diagnostics failure.

### R4. Storage Transactions Must Prove Their Outcome

Every storage transaction must log:

- transaction begin attempt;
- transaction begin success or failure;
- each persistence phase start or checkpoint;
- each persistence phase failure with local identifiers;
- commit attempt;
- commit success or failure;
- rollback or abort when directly visible;
- post-commit publish start, success, or failure when applicable.

For ingest, logs must distinguish:

- immutable document version persistence;
- unit row persistence;
- FTS persistence;
- dense vector persistence;
- ColBERT vector persistence;
- SQLite transaction commit;
- active document version update;
- active dense cache publish.

The log must make it clear whether durable SQLite state committed before active
in-memory state was published.

### R5. Model Calls And Smoke Checks Must Be Logged

Every model call and startup smoke check must log:

- model role;
- call purpose;
- start;
- success;
- normal error;
- elapsed milliseconds;
- compact input shape facts such as candidate count, text count, token count,
  configured max tokens, vector dimension, or layer count.

Logs must not include prompt text, document text, token dumps, logits for large
candidate sets, full vectors, or other bulk model payloads.

If the process receives SIGKILL during a model call, the call cannot log a
failure. The service must still have durable evidence for the last model-call
start boundary before the kill.

### R6. External Processes Must Be Logged

Every external process call must log:

- executable identity;
- purpose;
- start;
- configured timeout;
- process ID when available;
- completion status or timeout;
- elapsed milliseconds;
- bounded stdout/stderr diagnostics on failure.

Logs must not include unbounded external-process output.

### R7. Spawned Tasks Must Not Disappear Silently

Every spawned task must have durable visibility for:

- task start or accepted boundary;
- normal completion;
- normal error;
- panic when detectable;
- cancellation or join failure when detectable.

Fire-and-forget tasks are allowed only if another owner logs completion,
failure, and panic/cancellation for the task.

### R8. Client Delivery Is A Boundary, Not The Outcome

Operation stream delivery must be logged separately from backend execution.

The log must distinguish:

- backend execution succeeded and result delivery succeeded;
- backend execution succeeded and result delivery failed;
- backend execution failed and error delivery succeeded;
- backend execution failed and error delivery failed;
- client stream closed before terminal delivery.

Client delivery failure must not be reported as backend execution failure.

### R9. Terminal-Only Diagnostics Are Forbidden

After file logging is initialized, diagnostic facts must not exist only in:

- terminal output;
- startup handoff output;
- CLI output;
- comments;
- SQLite rows;
- inferred state.

Those surfaces may repeat or render facts, but `logs/data-store.log` must
contain the durable evidence.

### R10. Generic Failure Logs Are Forbidden

Logs such as "failed", "operation failed", or "storage error" are insufficient
unless they include the local facts needed to identify the failing boundary.

At minimum, a failure log must include:

- event name;
- boundary or stage;
- error string;
- relevant identifier fields available at that boundary;
- elapsed time when the boundary has measurable duration.

### R11. Sensitive Or Noisy Data Must Not Be Logged

Logs must not include:

- admin tokens;
- full document contents;
- full markdown contents;
- full prompts;
- full model outputs;
- token dumps;
- vector values;
- raw embedding matrices;
- unbounded stdout/stderr;
- large request or response payloads;
- secrets from configuration or environment.

Use compact identifiers, counts, dimensions, hashes, paths, statuses, and
bounded diagnostics instead.

### R12. Error Context Must Be Local And Specific

Errors should be contextualized at the boundary where the local facts are known.

Helper functions may return typed errors without logging if their caller owns
the diagnostic boundary. Boundary functions must not return those errors
unchanged when doing so would lose context.

## Required Fields By Boundary Type

| Boundary type | Required fields |
| --- | --- |
| Startup | startup phase, mode, bind address when known, config path when known, elapsed time when measurable |
| Background parent/child | parent PID, child PID, last startup line/progress, child exit status |
| Operation | operation name, operation ID, stage, sequence when stream-related, elapsed milliseconds |
| Request validation | operation or route, rejected field, limit/value when safe, status, error kind |
| Source resolution | requested source, relative source, resolved path on success, elapsed milliseconds |
| Docling | executable path, source reference, output directory, timeout, exit status, elapsed milliseconds, bounded diagnostics on failure |
| Unit splitting | source reference, version label when available, document ID when available, unit count, elapsed milliseconds |
| Model call | model role, call purpose, compact input shape, configured limits, elapsed milliseconds, error on failure |
| Storage transaction | source path, version label, document ID when available, phase, counts, commit/publish state |
| Unit/vector persistence | source path, version label, document ID, unit ID, current count, total count, phase |
| Active publish | source path, version label, vector count, published timestamp, phase |
| Operation stream | operation name, operation ID, sequence, event type, stage, delivery success/failure |
| Spawned task | task purpose, operation ID when available, completion/error/panic/cancel state |
| Shutdown | requested, confirmed, token-file cleanup result without token, service stopped |
| CLI ambiguous outcome | operation, operation ID, last event, elapsed time, health/admission detail, explicit unknown outcome |

## Evaluation Protocol

The evaluator must use this protocol before claiming the service satisfies this
standard.

The evaluator must not patch while evaluating. First produce the failure list.
Patching happens only after the failed boundaries are identified.

### Step 1. Build A Boundary Inventory

Create an inventory of every boundary in these categories:

- startup phases;
- background parent/child handoff paths;
- public HTTP routes;
- operation-stream operations;
- request validation paths;
- source resolution paths;
- Docling/external process paths;
- unit splitting paths;
- dense model calls;
- ColBERT model calls;
- reranker model calls;
- startup smoke checks;
- storage transactions;
- active cache publish paths;
- document version admin paths;
- shutdown paths;
- spawned tasks;
- operation stream delivery paths;
- CLI ambiguous-outcome paths.

Every inventory item must include file and function names.

Missing an applicable boundary category is an automatic evaluation failure.

### Step 2. Fill A Coverage Table

For every inventory item, fill this table:

| Field | Required value |
| --- | --- |
| Inventory ID | Stable short ID assigned by evaluator |
| Boundary category | One category from Step 1 |
| File/function | File path and function name |
| Start log | Event name and file line, or FAIL |
| Success log | Event name and file line, or FAIL or N/A with reason |
| Error log | Event name and file line, or FAIL or N/A with reason |
| Required fields | PASS/FAIL with missing fields listed |
| Sensitive/noisy data absent | PASS/FAIL |
| Terminal-only facts | PASS/FAIL |
| Error context local | PASS/FAIL |
| Verdict | PASS/FAIL |

Line references must point to the actual code that logs, wraps, or returns the
diagnostic fact. A vague statement that "the caller logs it" is not sufficient
unless the caller is identified in the table.

### Step 3. Apply Pass/Fail Rules

An inventory item passes only when all applicable checks pass.

These are automatic failures:

- no start log for a fallible or long-running boundary;
- no success log for a boundary whose success matters;
- no error log or error context for a fallible boundary;
- terminal-only diagnostic facts after file logging is initialized;
- generic error propagation across a subsystem boundary without local context;
- spawned task with no completion/panic/cancellation visibility;
- storage transaction with no commit attempt and commit outcome logs;
- model call or smoke check with no durable start boundary;
- operation stream terminal delivery not distinguished from backend execution;
- any required field missing without a specific reason;
- any "not sure" or inferred answer;
- any secret, content, vector, token dump, or unbounded payload in logs.

`N/A` is allowed only when the boundary genuinely cannot have that outcome. The
evaluator must explain why.

### Step 4. Produce An Evaluation Result

The evaluation result must contain:

- inventory count by category;
- coverage table;
- failed item list ordered by severity;
- exact code references for each failure;
- recommended minimal fix for each failure;
- explicit statement of PASS or FAIL for the service.

The service passes only when every inventory item passes.

## Verification Requirements

Code inspection is required but not sufficient. Representative forced-failure
checks must prove the logs are useful.

At minimum, verification should cover:

- startup fatal config error;
- startup child process exit during inference initialization;
- startup model smoke-check normal error when forceable;
- source resolution failure;
- Docling executable unavailable;
- Docling timeout or nonzero exit;
- ingest storage write failure when forceable without unsafe runtime migration;
- active publish failure when forceable;
- client stream loss before terminal event;
- terminal result delivery failure after backend success when forceable;
- failed search request validation;
- failed rollback request;
- graceful shutdown.

For each scenario, the verifier must state:

- command or setup used;
- expected log file;
- expected event names;
- expected required fields;
- whether the CLI or terminal output matches the durable log.

If a failure cannot be forced safely, the verifier must say so and rely on code
inspection for that specific boundary.

## Evaluation Output Template

Use this template when evaluating the current codebase:

```text
Diagnostics Evaluation: PASS|FAIL

Log file:
logs/data-store.log

Inventory counts:
- startup phases:
- parent/child handoff paths:
- routes:
- operations:
- model calls:
- smoke checks:
- external processes:
- storage transactions:
- spawned tasks:
- stream delivery paths:
- shutdown paths:
- CLI ambiguous-outcome paths:

Failed items:
1. [ID] Severity: high|medium|low
   Boundary:
   Code:
   Failure:
   Required standard:
   Minimal fix:

Coverage table:
| ID | Category | File/function | Start | Success | Error | Fields | Safe data | Terminal-only | Local context | Verdict |
| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |
```

## Implementation Guidance

Prefer boring, local fixes:

- add a start/success/error log at the owning boundary;
- add missing local context to an existing error;
- log the durable transaction boundary before and after commit;
- mirror startup progress to the service log;
- add task join logging for spawned work.

Do not introduce a new observability framework, operation ledger, queueing
system, retry layer, fallback path, or schema change unless separately approved.

The goal is correct diagnostic hygiene, not architecture expansion.
