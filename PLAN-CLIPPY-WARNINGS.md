# Clippy Warning Cleanup Plan

## Purpose

Continue the `cargo clippy` warning cleanup after the completed mechanical pass.
The remaining warnings require API-shape or ownership/layout changes, so they
should be handled deliberately and reviewed as scoped implementation work.

This plan does not use `#[allow(...)]`, crate-level lint suppression, or any
similar mechanism. Each warning should be addressed by changing the code shape
that caused it.

## Current Status

Pass 1 is complete. The mechanical warnings were addressed and verified with:

- `cargo fmt`
- `cargo check`
- `cargo clippy`

Pass 2.1 is complete. The Docling process-context warnings were addressed and
verified with:

- `cargo fmt`
- `cargo check`
- `cargo clippy`

Pass 2.2 is complete. The storage ingest-context warnings were addressed and
verified with:

- `cargo fmt`
- `cargo check`
- `cargo clippy`

Pass 2.3 is complete. The unit splitting-context warnings were addressed and
verified with:

- `cargo fmt`
- `cargo check`
- `cargo clippy`

The remaining warnings are:

- `result_large_err` in `src/http.rs`
- `large_enum_variant` in `src/inference/reranker_backend.rs`

Some warnings appear more than once because shared modules are compiled through
multiple binaries. Fix the source warning once rather than chasing duplicate
diagnostics.

## Session Rules For Continuation

Before editing, re-run `cargo clippy` to confirm the current warning set. If the
warning set differs from this plan, pause and update the plan with the user
before editing.

Before each edit:

- Read the exact current file region with line numbers.
- Keep each patch focused on one conceptual change.
- Re-read the edited region after the patch.
- Check whether the edited code needs comments for intent, invariants, lifecycle,
  ownership, or error behavior.

After each approved Rust change:

- Run `cargo fmt`.
- Run `cargo check`.
- Run `cargo clippy`.

Do not add tests unless automated testing has been explicitly re-enabled.

## Pass 2: Parameter Aggregates

Pass 2 addresses `too_many_arguments` warnings by introducing small data structs
that name the logical context crossing each function boundary.

The goal is not to hide arguments from Clippy. The goal is to make each function
accept the concept it actually operates on, with names that preserve diagnostic
and domain intent.

### 2.1 Docling Process Context

Status: complete.

Warnings:

- `src/docling.rs`: `wait_for_docling_process`
- `src/docling.rs`: `emit_post_100_docling_feedback`
- `src/docling.rs`: `read_child_output`

Recommended change:

Introduce a borrowed process context for Docling wait/feedback functions. It
should group the stable lifecycle and diagnostic facts that are already passed
together:

- `config: &DoclingConfig`
- `output_dir: &Path`
- `source: &ResolvedSource`
- `process_id: u32`
- `started: Instant`

Likely name:

- `DoclingProcessContext<'a>`

Use this context in:

- `wait_for_docling_process`
- `timeout_docling_process` if it improves call consistency
- `emit_post_100_docling_feedback`
- `log_post_100_report` if the same facts are still repeated locally

Keep `child`, `progress_state`, `progress_sender`, and
`expected_markdown_path` separate unless a second focused context emerges. Those
values are not all stable process identity facts: they are operation handles or
feedback-specific state.

For child output reader threads, introduce an owned reader context because the
data moves into spawned thread closures:

- `label: &'static str`
- `process_id: u32`
- `source_requested: String`
- `relative_source: String`
- `output_dir: String`

Likely name:

- `DoclingChildOutputContext`

Use this context in `read_child_output` so each stdout/stderr thread receives
one owned diagnostic context plus its pipe reader and optional progress state.

Important constraints:

- Do not reduce diagnostic fields in logs.
- Do not change Docling timeout, polling, progress, or post-100 feedback
  behavior.
- Do not introduce async or new task orchestration.

Verification focus:

- `cargo clippy` should no longer report the three Docling
  `too_many_arguments` warnings.
- Existing log fields should remain present.

### 2.2 Storage Ingest Context

Status: complete.

Warnings:

- `src/storage.rs`: `Storage::ingest_document`
- `src/storage.rs`: `insert_document`

Recommended change:

Introduce a storage ingest input struct for the public storage boundary:

- `conversion: &DoclingConversionResult`
- `version_label: &str`
- `units: &[RetrievalUnit]`
- `vectors: Vec<UnitDenseVector>`
- `colbert_vectors: Vec<UnitColbertDocumentVector>`
- `dense: &DenseModelConfig`
- `colbert: &ColbertModelConfig`

Likely name:

- `IngestDocumentInput<'a>`

Keep the progress callback as a separate parameter. It is behavior supplied by
the caller, not part of the document payload being persisted.

Call-site impact:

- Update `src/http.rs` where `storage.ingest_document(...)` is called.
- Avoid changing the operation stream event contract or storage progress
  messages.

For `insert_document`, introduce a metadata struct for the immutable
document-version row:

- `document_id: &str`
- `source_sha256: &str`
- `markdown_sha256: &str`
- `diagnostics_json: &str`
- `units_ingested: usize`
- `timestamp_ms: u64`

Likely name:

- `DocumentMetadataInsert<'a>`

Keep `tx`, `conversion`, and `version_label` separate unless the new struct reads
more clearly with those fields included. The preferred starting point is to group
only the computed metadata values because they are the loosely coupled argument
cluster.

Important constraints:

- Do not change transaction boundaries.
- Do not change active-version publish behavior.
- Do not change progress checkpoints or durable log fields.
- Do not change SQLite schema or add migration behavior.

Verification focus:

- `cargo clippy` should no longer report the two storage
  `too_many_arguments` warnings.
- `cargo check` should confirm all caller updates.

### 2.3 Unit Splitting Context

Status: complete.

Warnings:

- `src/units.rs`: `split_long_text_by_words`
- `src/units.rs`: `push_unit`

Recommended change:

Introduce a unit building context for stable document-level and splitting
settings:

- `document_id: &str`
- `source_path: &str`
- `min_chars: usize`

Consider including `max_tokens` only where the function uses token caps. It may
belong in a separate split context rather than the unit-push context.

Likely names:

- `UnitBuildContext<'a>`
- `LongTextSplitContext<'a>` if `max_tokens`, heading path, and page numbers need
  a separate grouping for `split_long_text_by_words`

Introduce a draft struct for the unit payload passed into `push_unit`:

- `heading_path: Vec<String>`
- `page_numbers: Vec<u32>`
- `content: String`
- `token_count: usize`

Likely name:

- `UnitDraft`

Expected shape:

- `push_unit(units: &mut Vec<RetrievalUnit>, context: &UnitBuildContext<'_>, draft: UnitDraft)`
- `split_long_text_by_words(...)` should take the stable context and only the
  values that vary per long text block.

Important constraints:

- Preserve deterministic unit sequence assignment from `units.len()`.
- Preserve token counting and minimum-character filtering.
- Preserve heading path and page number behavior.
- Avoid cloning more than the current implementation requires.

Verification focus:

- `cargo clippy` should no longer report the two unit
  `too_many_arguments` warnings.
- Review generated code after `cargo fmt` because match guards and builder calls
  in this file can become hard to scan.

## Pass 3: Ownership And Layout

Pass 3 addresses warnings where the root cause is object size or result layout.
These should be approved separately from Pass 2 because they change ownership
shape.

### 3.1 OperationFailure Error Size

Warnings:

- `src/http.rs`: `execute_operation`
- `src/http.rs`: `emit_terminal_result`

Current shape:

`OperationFailure` stores an `ApiError` directly. Clippy reports large
`Result<(), OperationFailure>` error variants at functions returning that type.

Recommended change:

Box the large error field inside `OperationFailure`:

```rust
struct OperationFailure {
    stage: &'static str,
    error: Box<ApiError>,
}
```

Update `OperationFailure::new` to accept `ApiError` and box it internally:

```rust
fn new(stage: &'static str, error: ApiError) -> Self {
    Self {
        stage,
        error: Box::new(error),
    }
}
```

Then update call sites that consume `failure.error` if needed. Prefer borrowing
or dereferencing the boxed error at the use site rather than changing the wider
operation error flow.

Why this approach:

- The operation error contract remains explicit.
- The stage remains cheap and direct.
- Boxing only the large field reduces the result error size without boxing the
  whole `OperationFailure`.

Important constraints:

- Do not change terminal error event shape.
- Do not change operation lifecycle logs.
- Do not change the distinction between backend execution failure and terminal
  result delivery failure.

Verification focus:

- `cargo clippy` should no longer report `result_large_err` in `src/http.rs`.
- Review all uses of `failure.error` for preserved error kind, status, and
  display behavior.

### 3.2 RerankerBackend Enum Size

Warning:

- `src/inference/reranker_backend.rs`: `RerankerBackend`

Current shape:

`RerankerBackend::Local(RerankerRuntime)` is much larger than
`RerankerBackend::Http(HttpRerankerClient)`, so the enum takes the size of the
largest variant.

Recommended change:

Box only the local reranker runtime:

```rust
pub enum RerankerBackend {
    Local(Box<RerankerRuntime>),
    Http(HttpRerankerClient),
}
```

Update construction in `src/inference/mod.rs`:

```rust
RerankerBackend::Local(Box::new(RerankerRuntime::load_with_progress(...)?))
```

Existing method matches can usually keep the same names because method calls on
`Box<RerankerRuntime>` dereference automatically. Confirm all match arms in
`src/inference/reranker_backend.rs`.

Why this approach:

- The enum remains a concrete, explicit backend selector.
- The no-fallback invariant remains visible in the type.
- Only the large variant gets indirection.
- No trait object or dynamic dispatch is introduced.

Important constraints:

- Do not introduce fallback behavior between local and HTTP rerankers.
- Do not change reranker health details, mode labels, or local model gate
  behavior.
- Do not change HTTP reranker scoring or diagnostics.

Verification focus:

- `cargo clippy` should no longer report `large_enum_variant`.
- Watch for duplicate reporting through `src/bin/../inference/reranker_backend.rs`;
  it should disappear once the source enum is fixed.

## Suggested Implementation Order

1. Re-run `cargo clippy` and confirm the warning set.
2. Pause for review before Pass 3 if the user has not already approved the
   ownership/layout changes.
3. Implement Pass 3.1 for `OperationFailure`.
4. Run `cargo fmt`, `cargo check`, and `cargo clippy`.
5. Implement Pass 3.2 for `RerankerBackend`.
6. Run `cargo fmt`, `cargo check`, and `cargo clippy`.

## Completion Criteria

The cleanup is complete when:

- `cargo fmt` passes.
- `cargo check` passes.
- `cargo clippy` passes with no warnings.
- No lint suppressions were added.
- No runtime migration, config, protocol, diagnostics, or behavior changes were
  introduced outside the approved warning cleanup.
