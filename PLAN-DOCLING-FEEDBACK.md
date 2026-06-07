# Docling Feedback And Activity Inspection Plan

## Purpose

Docling can emit `100%` progress before the process has actually finished and
before any markdown artifact exists. During that post-100% interval, the data
store service currently gives the user no meaningful feedback. The ingest
operation appears stuck at:

```text
docling_converting: converting source document 100/100 (100%)
```

The live observed case was operation `server-1780811383172-1` ingesting:

```text
A_Brief_Tour_of_Human_Consciousness_-_VS_Ramachandran.pdf
```

Docling reported `100/100` after about six seconds, but more than twenty
minutes later:

- the Docling child process was still running;
- the Rust service was still waiting on `child.wait()`;
- the conversion output directory was still empty;
- macOS `sample` showed Python/PyTorch CPU tensor work in `libtorch_cpu.dylib`
  with OpenMP worker activity;
- no `docling.process.wait_completed` or `ingest.docling_conversion.completed`
  log had been emitted.

The goal is to give accurate, concise user feedback about what the Docling
process is doing during this period, without exposing raw diagnostic reports or
changing ingest semantics.

## Target Behavior

When Docling progress stalls, especially after Docling reports `100%`, the
operation stream should continue to emit useful status/progress messages.

Example user-facing message:

```text
Docling reported 100%, but the process is still running.
Current activity: PyTorch CPU tensor work.
Elapsed: 21m. Output artifacts: none yet. Timeout remaining: ~39m.
```

The service should distinguish:

- Docling has reported 100%, but the process is still running.
- Docling is doing CPU model/tensor work.
- Docling appears to be waiting in OpenMP synchronization.
- Docling appears to be doing file I/O.
- Docling is idle or blocked.
- The service cannot classify current activity.
- Output artifacts have or have not appeared.

Raw stack samples must not be printed as normal user-facing output.

## Scope

Initial implementation is macOS-specific because this service is currently
running locally on macOS and `sample` is available there.

Linux support is later scope. The design should keep the platform-specific
inspector isolated so a Linux implementation can be added without changing the
ingest pipeline contract.

Allowed implementation scope:

- `service/data-store/src/docling.rs`
- a new `service/data-store/src/docling_activity.rs` module if useful
- `service/data-store/src/http.rs` only if needed to forward richer Docling
  feedback through the existing operation stream
- `service/data-store/src/main.rs` only if needed to register a new module

Do not change:

- service config;
- SQLite schema;
- runtime migrations;
- API request or result payload contracts;
- ingest success/failure semantics;
- Docling command arguments unless separately approved.

## Design Principles

- User feedback must be accurate and must not imply completion when Docling has
  only emitted a progress percentage.
- Process inspection must be bounded and low cadence.
- Raw process-inspection output is diagnostic material, not UI content.
- The operation stream should receive short, classified summaries only.
- Durable logs should record inspection start, completion, classification, safe
  process facts, output artifact facts, and inspection failures.
- Inspection failure must not fail ingest by itself. It should produce an
  `unknown` activity classification and continue waiting for Docling or timeout.
- The existing Docling document timeout remains the authoritative timeout unless
  a separate post-100% timeout policy is explicitly approved.

## Proposed Implementation

### 1. Track Docling Progress State

Track the latest Docling progress facts inside `run_docling` or a helper owned
by `run_docling`:

- last progress message;
- last percentage;
- last progress timestamp;
- whether Docling has reported `100%`;
- process start timestamp;
- configured document timeout;
- process ID;
- output directory.

This state should be updated when stderr progress lines are parsed.

### 2. Add Periodic Wait Feedback

While waiting for `child.wait()`, emit periodic feedback when:

- Docling has reported `100%` and the process is still running; or
- no Docling progress has arrived for a configured internal threshold.

Initial internal thresholds can be constants, not config:

- post-100% first inspection delay: about 10 seconds;
- repeated inspection cadence: about 30 seconds;
- no-progress feedback threshold: about 60 seconds.

Do not emit on every loop iteration.

### 3. Add A macOS Activity Inspector

Create a small platform-specific inspector that can run on macOS:

```text
sample <pid> <duration>
```

The inspector should:

- run for a short bounded duration, such as 2-3 seconds;
- capture output through pipes when practical, or write to a service-owned
  diagnostic path if `sample` requires a file;
- parse only enough content to classify current activity;
- avoid logging or emitting the full report by default;
- fail gracefully when `sample` is unavailable or fails.

Classification examples:

- `pytorch_cpu_tensor_work` when the sample contains `libtorch_cpu.dylib`,
  `TensorIterator`, `at::native`, convolution, softmax, sigmoid, add, multiply,
  or similar tensor kernels.
- `openmp_synchronization` when the dominant frames are `libomp.dylib`,
  `__kmp`, OpenMP barriers, or worker waits.
- `file_io` when dominant frames include filesystem reads/writes, `read`,
  `write`, `open`, `fsync`, or related file APIs.
- `process_wait` when dominant frames show subprocess waiting.
- `idle_or_blocked` when the process appears alive but mostly waiting without
  useful work frames.
- `unknown` when no reliable classification can be made.

Classification should include a concise user-facing label, for example:

```text
PyTorch CPU tensor work
OpenMP worker synchronization
file I/O
waiting/blocking
unknown activity
```

### 4. Inspect Output Artifacts

At each feedback interval, inspect the Docling output directory:

- number of files;
- number of markdown files;
- total byte size;
- whether the expected markdown path exists;
- largest file name and size when safe and useful.

Do not read file contents.

### 5. Emit User-Facing Feedback

Use existing operation progress messages for feedback. A practical message
shape:

```text
Docling reported 100%; still running PyTorch CPU tensor work; elapsed 21m; timeout remaining 39m; output artifacts 0 files, 0B
```

If Docling has not reported 100% but progress is stale:

```text
Docling still running; no progress for 65s; current activity unknown; elapsed 7m; timeout remaining 53m
```

These messages should be progress events on the `docling_converting` stage with
no percentage, because they are not Docling completion percentages.

### 6. Durable Logs

Add durable logs for:

- wait feedback tick started;
- activity inspection started;
- activity inspection completed;
- activity classification and safe facts;
- activity inspection failure;
- artifact inspection result;
- feedback delivery result when available.

Logs must include safe facts:

- operation ID when available;
- source reference;
- process ID;
- elapsed milliseconds;
- timeout remaining;
- last progress percentage/message age;
- output artifact count/size;
- activity classification.

Logs must not include:

- document contents;
- raw sample reports as normal log payloads;
- unbounded stdout/stderr;
- secrets.

### 7. Preserve Ingest Semantics

The activity inspector is reporting-only:

- it must not kill Docling;
- it must not shorten the existing document timeout;
- it must not change success/failure classification;
- it must not prevent final conversion success;
- it must not fail ingest if inspection fails.

If the process exits, the pipeline continues exactly as it does now:

1. join output reader tasks;
2. check exit status;
3. locate markdown artifact;
4. read and normalize markdown;
5. move to unit splitting.

## Verification Plan

Required build checks:

```bash
cargo fmt --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml
cargo check --manifest-path service/data-store/Cargo.toml --features metal
```

Manual runtime checks require separate approval before starting or stopping
service processes.

Manual checks:

1. Run an ingest where Docling emits normal progress and completes quickly.
   - Existing progress output remains compact.
   - No unnecessary inspector noise appears.
2. Run or reproduce an ingest where Docling reports `100%` but keeps running.
   - The client receives periodic classified feedback.
   - The feedback includes elapsed time, timeout remaining, artifact count/size,
     and current activity classification.
   - The service log records inspection and artifact boundaries.
3. Confirm the existing document timeout still works.
   - Timeout failure remains explicit.
   - Inspector failure does not mask Docling timeout.
4. Confirm successful completion after post-100% waiting still ingests normally.
   - Unit splitting begins after markdown is found and read.
   - No ingest semantics change.

## Current Live Diagnostic Summary

For the current stuck-looking operation:

- Operation ID: `server-1780811383172-1`
- Docling PID: `45960`
- Source: `A_Brief_Tour_of_Human_Consciousness_-_VS_Ramachandran.pdf`
- Last visible client progress: `docling_converting 100/100`
- Current classification from manual macOS `sample`: PyTorch CPU tensor work
  with OpenMP worker activity.
- Output artifact state: no files, output directory size `0B`.
- Rust service state: waiting for Docling process exit.

This is not a Rust ingest freeze. It is a Docling process continuing work after
emitting misleading `100%` progress.

## Approval Reminder

Before implementing this plan, present the concrete code-edit plan, estimated
development effort, and confidence level, then wait for explicit approval.

Configuration changes, schema changes, runtime migrations, service start/stop
actions, and process termination require separate explicit approval.
