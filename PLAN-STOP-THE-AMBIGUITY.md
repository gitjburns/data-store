# Plan: Stop The Ambiguity

## Current State

The Data Store service does not provide the basic diagnostic information needed
to operate or debug it reliably.

The most recent failure showed the problem clearly:

```text
[server-1780561060822-2 #146] storage_publishing: publishing document version
Elapsed: 186564 ms
error: POST http://127.0.0.1:8091/v1/operations operation `ingest` stream ended before a terminal event
```

This output is operationally ambiguous. It does not tell the user whether the
operation succeeded, failed, crashed, timed out, continued in the background, or
committed partial durable state. The assistant then had to inspect logs and
SQLite directly to infer what happened. That is a failure of the system, not a
reasonable debugging workflow.

This has been a recurring project failure: implementation has repeatedly
advanced without sufficient user-visible feedback, durable service logs, and
operation lifecycle diagnostics. As a result, each real bug becomes harder to
diagnose than it should be.

## Why This Is A Serious Problem

The service performs long-running, expensive, multi-stage operations:

- source resolution
- Docling conversion
- unit splitting
- dense embedding
- ColBERT document embedding
- SQLite transaction writes
- active version publish
- search snapshot/cache updates
- streamed operation result delivery

If any of those stages fails or disconnects without an explicit terminal record,
the operator loses the authoritative outcome. That violates the core project
principles:

- accurate data that is not visible is hidden
- visible data that is not accurate is misleading
- raw operation facts must remain available
- summaries may supplement raw data, not replace it

Without reliable observability, debugging devolves into guessing, database
spelunking, and rerunning expensive operations. That is not acceptable for this
service.

## Objective

Implement a holistic operation observability and diagnostic system so that every
important service action has:

- a clear user-visible status path
- durable service-side lifecycle logs
- an explicit terminal outcome or explicit unknown-outcome explanation
- enough structured detail to diagnose failures without inspecting SQLite
  internals
- anti-regression rules so future development cannot add opaque behavior again

The goal is not merely to improve one CLI error message. The goal is to make
ambiguous operation outcomes structurally impossible or, when a transport/process
failure makes the outcome unknowable, to say so explicitly and provide the exact
last known authoritative facts.

## Required Design

### 1. Operation Lifecycle Must Be Explicit

Every accepted operation must produce a complete lifecycle trail.

Required lifecycle records:

- operation accepted
- stage started
- progress checkpoint
- operation result ready
- operation result delivered
- operation error ready
- operation error delivered
- terminal delivery failed
- stream closed before terminal event
- operation task finished

Required fields:

- operation name
- operation ID
- sequence number when applicable
- stage
- message
- elapsed milliseconds
- terminal type when applicable
- status and error kind when applicable
- whether the event was delivered to the client

The server must distinguish these cases:

- operation execution failed
- operation execution succeeded but result delivery failed
- operation execution failed and error delivery also failed
- client stream closed before terminal delivery
- process shutdown occurred during an active operation

### 2. Long-Running Stage Boundaries Must Be Logged

Operation stream events are not enough. They disappear when the client
disconnects. Long-running operations must also write durable service logs.

Ingest must log at least:

- request admitted
- source resolution started/completed/failed
- Docling conversion started/progress/completed/failed
- unit splitting completed
- dense embedding started/progress/completed/failed
- ColBERT embedding started/progress/completed/failed
- storage publishing started
- vector validation completed
- SQLite transaction started
- document metadata persisted
- unit/vector persistence checkpoints
- SQLite transaction committed
- active version publish started/completed/failed
- ingest result assembled
- terminal event delivered or delivery failed

Logs must include compact operational facts:

- requested source
- resolved source path
- version label after allocation
- unit count
- dense vector count
- ColBERT vector count
- major stage elapsed milliseconds
- failure status/error kind/message

Logs must not include:

- admin tokens
- full document contents
- vector values
- large raw payloads
- secrets

### 3. CLI Must Never Hide Unknown Outcomes

The CLI must distinguish:

- terminal result event received
- terminal error event received
- HTTP error before stream opened
- local request/stream timeout
- stream closed before terminal event
- server unreachable after stream loss
- server reachable but operation outcome unknown

For premature stream closure, the CLI must not print a generic one-line error.
It must show:

- operation name
- operation ID
- elapsed time
- last event type
- last sequence
- last stage
- last message
- that the outcome is unknown
- that success must not be inferred
- a recommended follow-up command

Example target output:

```text
operation `ingest` stream ended before a terminal result/error
outcome: unknown
operationId: server-1780561060822-2
last event: sequence=146 type=status stage=storage_publishing message="publishing document version"
elapsed: 186564 ms
server may have crashed, closed the stream, or continued without delivering the terminal event
next: run `health`; for ingest, run `versions` if authorized
```

### 4. CLI Must Probe After Ambiguous Stream Loss

After premature stream closure, the CLI must run a short health probe and print
one of:

- service reachable and ready
- service reachable but not ready
- service unreachable
- health probe failed with status/error detail

Health does not prove operation success. It only helps classify the failure.
The CLI must state that clearly.

### 5. Durable Operation Ledger Is Required

Logs are necessary but not sufficient. A complete fix requires a durable
operation ledger.

Add explicit operation persistence:

- `operations`
- `operation_events`

The ledger must persist:

- operation ID
- operation name
- accepted timestamp
- current stage
- terminal status
- terminal result/error summary
- event sequence
- event type
- event stage
- event message
- progress counts
- error status/kind/message
- timestamps

New API endpoints:

- `GET /v1/operations/{operationId}`
- `GET /v1/operations/{operationId}/events`

After stream loss, the CLI must query the operation ledger by operation ID and
print the authoritative latest state:

- terminal result if present
- terminal error if present
- latest known non-terminal stage if still running or unknown

This schema must be introduced through explicit setup/migration scripts. Do not
add runtime migrations.

### 6. Startup And Fatal Runtime Errors Must Not Leave Zombie Services

Startup readiness-critical failures must be fatal before HTTP bind:

- inference initialization failure
- storage/cache initialization failure
- admin token publication failure
- HTTP bind failure

On fatal startup:

- report the fatal reason to stdout/parent process
- log the fatal reason
- remove the current admin token file if it was published
- exit non-zero
- do not bind HTTP in a degraded readiness-critical state

Background startup must use spawn/exec, not fork-without-exec, so Metal/XPC is
initialized in a fresh process.

### 7. Process Exit And Crash Visibility

The service must make best effort to log:

- graceful shutdown requested
- graceful shutdown completed
- startup fatal exit
- operation task panic if caught
- operation task cancellation/drop if detectable

Rust panics in operation tasks must not vanish silently. If an operation task is
spawned, the join path or panic handling must preserve the fact that it failed.

## Anti-Regression Rules

These rules are mandatory for future Data Store development.

1. No new operation stage may be added without both a stream event and a durable
   service log.

2. No long-running operation may have an unlogged blocking phase.

3. No operation may end without a terminal result, terminal error, or explicit
   unknown-outcome record.

4. No client-facing command may collapse transport loss, server error, timeout,
   and unknown outcome into one generic message.

5. No readiness-critical startup failure may allow the service to bind HTTP.

6. No runtime path may require SQLite inspection to determine whether a user
   operation succeeded.

7. No future feature is complete until its diagnostic surface is complete.

8. Any new persistence-affecting workflow must log the transaction boundary and
   publish boundary separately.

9. Any new external process/model/API call must log start, completion, elapsed
   time, and failure kind.

10. Raw operation facts must be preserved. Derived summaries are allowed only as
    additions.

## Verification Requirements

Required build checks:

```bash
cd service/data-store && cargo fmt
cd service/data-store && cargo check
cd service/data-store && cargo check --features metal
```

Manual verification scenarios:

1. Start service in background from a release binary.
2. Confirm background startup uses spawn/exec and Metal initializes.
3. Confirm startup fatal errors exit before HTTP bind.
4. Run `health` and verify terminal result display.
5. Run successful ingest and verify terminal result display.
6. Confirm operation ledger records all ingest stages.
7. Confirm service logs contain all ingest stage boundaries.
8. Simulate or force stream loss and verify CLI prints outcome unknown plus last
   event details.
9. Confirm CLI health probe runs after stream loss.
10. Confirm operation lookup reports latest persisted operation state.
11. Confirm failed ingest reports terminal error with status/kind/message.
12. Confirm no database inspection is required to determine operation outcome.

## Implementation Notes

Work should proceed as one comprehensive observability fix, not as optional
tiers. It may still require multiple sessions, but each session should preserve
the single objective: eliminate ambiguous operation outcomes and prevent future
regression into opaque behavior.

Do not resume ColBERT or ingest debugging until the diagnostic surface is strong
enough to explain failures directly.

## Expected Effort

Estimated effort: 25k-40k tokens across multiple focused sessions.

The durable operation ledger and CLI follow-up behavior are the largest pieces.
The existing startup spawn/exec and fatal-cleanup work is already partially
implemented in the working tree and must be completed/verified as part of this
plan.
