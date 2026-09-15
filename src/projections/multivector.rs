//! C6e multi-vector builder at the ColBERT grain (PLAN-grains Section 2):
//! ColBERT document-token matrices as `multi_vector` projections persisted
//! PER WINDOW (`colbert_windows`), where a window is a run of consecutive fine
//! chunks packed under `indexing.colbert_max_tokens`. Parse-scoped and
//! rebuildable from the parse's `chunk_projections` rows through local
//! length-routed embedding calls or remote batched token embeddings, using
//! the typed fabric matrix codec (spec §22).
//!
//! Construction. Chunks are read in `chunk_index` order (the only
//! reading-order authority) through the shared `lexical::read_parse_chunks`
//! reader on the caller's transaction. A run grows while the exact joined
//! canonical text fits the EFFECTIVE cap — `indexing.colbert_max_tokens` less
//! the backend's document format overhead (`ColbertBackend::
//! document_format_overhead`), so that the formatted document the model sees
//! never exceeds `document_max_tokens` and is never truncated; runs cross
//! section boundaries and never split a chunk (the build fails up front when
//! `indexing.fine_max_tokens` exceeds the effective cap less the minimum fill,
//! and a chunk that does not fit fails the build). At document end a run below
//! `indexing.min_fill_ratio` (of the configured cap) merges into its
//! predecessor when the combined text fits.
//!
//! Each window records its member `chunk_ids` in order and `fragments` as the
//! concatenation of its chunks' fragments (Unicode scalar offsets, never
//! bytes). The embedded model input is `chunk::model_input(section path of
//! the first chunk, canonical text)`; the prefix is never part of any range.
//! The ColBERT document limit is hard, so a window whose prefixed input would
//! exceed the cap is embedded without the prefix (logged at DEBUG).
//!
//! Persisted `token_count` is the matrix row count the model returned (the
//! codec derives the blob length from it); the cap is enforced on the
//! measured model input before embedding.
//!
//! The caller holds one model-call permit across a local build and passes no
//! permit for HTTP. This builder validates that admission without acquiring a
//! gate itself. Remote requests remain inside the caller's write transaction;
//! projection rows and their envelope still commit or roll back together.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::sqlite::{Connection, Transaction};
use rusqlite::{OptionalExtension, params};
use tokenizers::Tokenizer;
use tracing::{debug, error, info};

use crate::error::ApiError;
use crate::ids::new_retrieval_projection_id;
use crate::inference::{ColbertBackend, ColbertDocumentEmbedding};
use crate::limits::IndexingLimits;
use crate::model::{ProducerType, Provenance, ProvenanceInputRef, ProvenanceObjectType};
use crate::primitives::codec::{
    UnitColbertDocumentVector, decode_colbert_document_vector_blob, encode_colbert_matrix_blob,
};
use crate::primitives::utc_now;
use crate::primitives::validate::{StoredColbertDocumentVector, validate_colbert_document_vector};
use crate::projections::envelope::{self, NewProjection, ProjectionType};
use crate::state::ModelCallPermit;

use super::StoredChunk;
use super::chunk::{self, Fragment};

/// Stable producer name recorded in the projection's Provenance (spec §20).
/// Naming the ColBERT document embedder here — rather than reading the model
/// name off the runtime, which exposes no such accessor — keeps a rebuild's
/// producer identity answerable from the row without coupling to internal
/// runtime fields.
const COLBERT_DOCUMENT_PRODUCER: &str = "colbert_document_embedder";

/// Producer version, bumped when the embedding contract changes in a way that
/// must invalidate prior multi-vector rows on rebuild. Version 2 is the
/// ColBERT grain: one matrix per window of fine chunks instead of per unit.
const COLBERT_DOCUMENT_PRODUCER_VERSION: &str = "2";

/// Delete every prior window row for a parse. Rebuild deletes FIRST, then
/// re-inserts (see `build_multivectors`): the `UNIQUE(parse_id, window_index)`
/// index (schema.sql) turns a stale surviving row into an INSERT conflict, so
/// the delete-first ordering is what makes a rebuild idempotent rather than a
/// constraint failure.
const DELETE_PARSE_WINDOWS_SQL: &str = "
DELETE FROM colbert_windows WHERE parse_id = ?1";

/// Insert one ColBERT window row. `matrix_blob` is the row-major little-endian
/// f32 encoding of `token_count * dimension` values; token_count and dimension
/// are stored alongside so the decoder can validate the matrix shape without a
/// separate shape record (schema.sql; mirrors
/// `decode_colbert_document_vector_blob`). `chunk_ids_json` and
/// `fragments_json` are canonical JSON (§16.2).
const INSERT_WINDOW_SQL: &str = "
INSERT INTO colbert_windows (
  id, projection_id, source_id, parse_id, window_index,
  chunk_ids_json, fragments_json, token_count, dimension, matrix_blob, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// Query-time read of one persisted window by (parse, window id). Scoped to
/// the single active parse (§14) and keyed on the primary key so each probe
/// returns at most one row; membership, the shape columns, and the row-major
/// matrix blob are read so the loader can decode without a separate shape
/// record. SQL checks admission before copying a potentially oversized blob
/// into Rust; a NULL blob is a resource refusal, not a missing matrix.
const SELECT_WINDOW_SQL: &str = "
SELECT chunk_ids_json, fragments_json, token_count, dimension,
 CASE WHEN token_count <= ?3 AND token_count * dimension <= ?4
 AND length(matrix_blob) <= ?4 * 4 THEN matrix_blob END
FROM colbert_windows
WHERE parse_id = ?1 AND id = ?2";

/// Membership-only read of one parse's windows in reading order: the window
/// id and its member chunk ids, never the matrix, so chunk-to-window
/// resolution moves no matrix bytes.
const SELECT_PARSE_WINDOW_CHUNKS_SQL: &str = "
SELECT id, chunk_ids_json
FROM colbert_windows
WHERE parse_id = ?1
ORDER BY window_index";

/// Build the ColBERT window projection for one parse.
///
/// Opens a `MultiVector` envelope (`building`), reads the parse's chunks in
/// `chunk_index` order, packs them into windows under
/// `indexing.colbert_max_tokens`, embeds each window's model input through the
/// selected backend, validates each matrix, encodes it, and inserts one
/// `colbert_windows` row per window; on full success completes the envelope
/// `fresh`. Any failure marks the envelope `failed` and returns the error, so
/// the envelope's freshness always reflects the outcome.
///
/// Local calls require the caller's permit for the whole build and retain the
/// length threshold between singular and batched inference. HTTP requires no
/// permit and batches all windows, avoiding one remote request per long window.
///
/// Rebuild idempotence: prior rows for the parse are deleted BEFORE inserting
/// (see `DELETE_PARSE_WINDOWS_SQL`), so `UNIQUE(parse_id, window_index)` never
/// trips on a re-run. The whole call runs on the caller's transaction, so the
/// delete, the inserts, and the envelope lifecycle commit or roll back as one
/// unit.
// The caller owns transaction, target identity, model permit, producer identity,
// validation dimension, and observer; the builder must not reconstruct them.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_multivectors(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    colbert_runtime: &ColbertBackend,
    expected_dimension: usize,
    gate: Option<&ModelCallPermit>,
    model_identity: &str,
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<String, ApiError> {
    // Enforce caller-side admission before touching rows: local inference must
    // hold the accelerator permit, while HTTP must never hold it over a request.
    if gate.is_some() != colbert_runtime.uses_local_model_gate() {
        return Err(ApiError::StorageOperation {
            message: format!(
                "ColBERT {:?} multi-vector build received incompatible model-call gate admission",
                colbert_runtime.backend_kind(),
            ),
        });
    }
    let started_at = Instant::now();

    // The runtime tokenizer may truncate at the model's document limit. This
    // independent CPU-only copy counts the full text and never silently clips
    // it, so packing decisions and the cap check see every token.
    let mut counter = colbert_runtime.tokenizer().clone();
    counter
        .with_truncation(None)
        .map_err(|source| ApiError::InferenceInit {
            message: format!(
                "disable ColBERT window tokenizer truncation for {parse_id}: {source}"
            ),
        })?;
    counter.with_padding(None);

    // The budget is derived from the selected backend's format overhead, so the
    // cap packing enforces is the one the model's truncation actually leaves.
    let budget = window_budget(
        &tx.limits().indexing,
        colbert_runtime.document_format_overhead(),
    )?;

    // The chunk rows are read on the caller's transaction so the windows
    // describe exactly the chunk set this build's owner is publishing.
    let chunks = owned_chunks(
        super::lexical::read_parse_chunks(tx, parse_id)?,
        source_id,
        parse_id,
    )?;
    let windows = pack_windows(&chunks, parse_id, &counter, &budget)?;
    if let Some(monitor) = monitor {
        monitor.stage(
            "ColBERT window embeddings",
            Some(windows.len() as u64),
            "windows embedded",
        );
    }

    info!(
        event = "multivector_build.started",
        backend = ?colbert_runtime.backend_kind(),
        source_id,
        parse_id,
        chunk_count = chunks.len(),
        window_count = windows.len(),
        expected_dimension,
        "multi-vector projection build started"
    );

    let unit_ids = chunk::ordered_unit_ids(windows.iter().flat_map(|window| &window.fragments));
    let projection_id = envelope::insert_building(
        tx,
        &NewProjection {
            source_id: source_id.to_string(),
            parse_id: parse_id.to_string(),
            projection_type: ProjectionType::MultiVector,
            producer: colbert_producer_provenance(&unit_ids, model_identity),
            input_unit_ids: Some(unit_ids),
            input_annotation_ids: None,
            index_name: None,
            index_partition: None,
        },
    )?;

    // Any failure past this point marks the envelope failed on the same
    // transaction, so the freshness state never claims success the payload rows
    // did not achieve. `build_rows` performs the delete-first rebuild and the
    // per-window embed/validate/encode/insert loop.
    match build_rows(
        tx,
        source_id,
        parse_id,
        &projection_id,
        colbert_runtime,
        expected_dimension,
        &windows,
        monitor,
    ) {
        Ok(totals) => {
            // Payload lives entirely in the hot-plane table, so no archived
            // payload URI is recorded at completion (envelope contract).
            envelope::complete_fresh(tx, &projection_id, None)?;
            info!(
                event = "multivector_build.completed",
                backend = ?colbert_runtime.backend_kind(),
                // The enclosing owner reports durability after its commit.
                persistence = "pending_commit",
                source_id,
                parse_id,
                projection_id = %projection_id,
                chunk_count = chunks.len(),
                window_count = windows.len(),
                expected_dimension,
                total_tokens = totals.total_tokens,
                // Per-path routing counts (hybrid, 2026-07-18) so stage time in
                // this log is attributable to the batched vs singular path when
                // benchmarking; the failed arm omits them (routing may not have
                // completed at that boundary).
                batched_window_count = totals.batched_windows,
                singular_window_count = totals.singular_windows,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "multi-vector projection build completed"
            );
            Ok(projection_id)
        }
        Err(source) => {
            // A secondary marker failure must not erase the original model or
            // storage failure from the diagnostic record.
            envelope::mark_failed(tx, &projection_id, &source.to_string()).inspect_err(
                |mark_error| {
                    error!(
                        event = "multivector_build.failure_marker_failed",
                        source_id,
                        parse_id,
                        projection_id = %projection_id,
                        error = %source,
                        mark_error = %mark_error,
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        "multi-vector build and failure marker both failed"
                    );
                },
            )?;
            error!(
                event = "multivector_build.failed",
                backend = ?colbert_runtime.backend_kind(),
                source_id,
                parse_id,
                projection_id = %projection_id,
                window_count = windows.len(),
                expected_dimension,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                error = %source,
                "multi-vector projection build failed"
            );
            Err(source)
        }
    }
}

/// Require that every chunk row belongs to the expected source and parse and
/// that `chunk_index` is exactly the row's position (0..n contiguous), so
/// slice positions can stand in for reading order during packing.
fn owned_chunks(
    chunks: Vec<StoredChunk>,
    source_id: &str,
    parse_id: &str,
) -> Result<Vec<StoredChunk>, ApiError> {
    for (position, chunk) in chunks.iter().enumerate() {
        if chunk.source_id != source_id || chunk.parse_id != parse_id {
            return Err(failure(format!(
                "chunk {} belongs to source {} parse {}, not source {source_id} parse {parse_id}",
                chunk.id, chunk.source_id, chunk.parse_id
            )));
        }
        if chunk.chunk_index != position {
            return Err(failure(format!(
                "chunk {} of parse {parse_id} has chunk_index {} at position {position}; chunk order is not contiguous",
                chunk.id, chunk.chunk_index
            )));
        }
    }
    Ok(chunks)
}

/// The run being packed: the chunk positions it spans (`start..end`), its
/// canonical text, and the token count measured on exactly that text — every
/// path that changes `text` measures the new text whole before storing it.
struct OpenRun {
    start: usize,
    end: usize,
    text: String,
    tokens: usize,
}

/// One window ready to embed: its fresh row id (also the label the model call
/// reports under), reading-order position, membership, the text the model
/// sees, and that text's measured token count (at most the cap).
struct WindowInput {
    id: String,
    window_index: usize,
    chunk_ids: Vec<String>,
    fragments: Vec<Fragment>,
    model_input: String,
    token_count: usize,
}

/// Token bounds one build packs under, derived once from `indexing` and the
/// selected backend's document format overhead.
///
/// Invariant: `max_tokens` is `indexing.colbert_max_tokens - format_overhead`,
/// so a window whose bare model input measures at most `max_tokens` formats
/// (document prompt + marker) to at most the configured cap, which equals the
/// backend's `document_max_tokens`. A persisted window's formatted document
/// therefore never exceeds `document_max_tokens`, is never truncated, and the
/// stored matrix covers the whole window. `min_tokens` is the Section 2
/// minimum fill of the CONFIGURED cap, unchanged by the overhead.
struct WindowBudget {
    configured_max_tokens: usize,
    format_overhead: usize,
    max_tokens: usize,
    min_tokens: usize,
    fine_max_tokens: usize,
}

/// Derive the build's `WindowBudget` and reject a configuration under which
/// packing could not honor it: a fine chunk may measure up to
/// `indexing.fine_max_tokens`, and packing needs every chunk to fit the
/// effective cap with the minimum fill to spare, so
/// `fine_max_tokens > max_tokens - min_tokens` fails the build here rather than
/// mid-pack (startup validation checks the same relation against the configured
/// cap and cannot see the backend's overhead).
fn window_budget(
    indexing: &IndexingLimits,
    format_overhead: usize,
) -> Result<WindowBudget, ApiError> {
    let configured_max_tokens = indexing.colbert_max_tokens as usize;
    let max_tokens = configured_max_tokens
        .checked_sub(format_overhead)
        .filter(|effective| *effective > 0)
        .ok_or_else(|| {
            failure(format!(
                "indexing.colbert_max_tokens={configured_max_tokens} leaves no room for the ColBERT document format overhead of {format_overhead} tokens"
            ))
        })?;
    // The minimum is computed once, floored, so every run is held to one
    // integer bound.
    let min_tokens = (configured_max_tokens as f64 * indexing.min_fill_ratio).floor() as usize;
    let fine_max_tokens = indexing.fine_max_tokens as usize;
    let headroom = max_tokens.saturating_sub(min_tokens);
    if fine_max_tokens > headroom {
        return Err(failure(format!(
            "indexing.fine_max_tokens={fine_max_tokens} exceeds the ColBERT window headroom of {headroom} tokens: indexing.colbert_max_tokens={configured_max_tokens} less the document format overhead of {format_overhead} tokens gives an effective cap of {max_tokens}, less the minimum fill of {min_tokens} tokens"
        )));
    }
    Ok(WindowBudget {
        configured_max_tokens,
        format_overhead,
        max_tokens,
        min_tokens,
        fine_max_tokens,
    })
}

/// Pack chunks into windows under the budget's effective cap with the
/// Section 2 minimum-fill rule, never splitting a chunk (the same structure as
/// `section_dense::build_windows`, whose private member and window types keep
/// it from being shared):
///   - a chunk joins the open run while the EXACT joined canonical text fits
///     the effective cap (tokenization is not additive across joins, so every
///     candidate is measured whole);
///   - when it would overflow, the run closes and the chunk opens the next
///     run — with no split available, a run below the minimum closes short
///     rather than borrowing part of the next chunk;
///   - a chunk that alone exceeds the effective cap fails the build naming
///     both keys and the overhead, since `indexing.fine_max_tokens` below the
///     effective cap (checked by `window_budget`) is what guarantees every
///     chunk fits;
///   - at document end a final run below the minimum merges into the
///     preceding run when the combined text fits the effective cap; otherwise
///     it stays as the one permitted sub-minimum run.
///
/// Runs never consult section boundaries.
fn pack_windows(
    chunks: &[StoredChunk],
    parse_id: &str,
    tokenizer: &Tokenizer,
    budget: &WindowBudget,
) -> Result<Vec<WindowInput>, ApiError> {
    let max_tokens = budget.max_tokens;
    let min_tokens = budget.min_tokens;
    let mut runs: Vec<OpenRun> = Vec::new();
    let mut open: Option<OpenRun> = None;
    for (position, member) in chunks.iter().enumerate() {
        if let Some(mut run) = open.take() {
            let candidate = chunk::join_text(&run.text, &member.targeting_text);
            let tokens = count_tokens(tokenizer, parse_id, &candidate)?;
            if tokens <= max_tokens {
                run.end = position + 1;
                run.text = candidate;
                run.tokens = tokens;
                open = Some(run);
                continue;
            }
            // The chunk would overflow: the run closes as is, even below the
            // minimum, because a chunk is never split to top it up.
            runs.push(run);
        }
        let tokens = count_tokens(tokenizer, parse_id, &member.targeting_text)?;
        if tokens > max_tokens {
            return Err(failure(format!(
                "chunk {} of parse {parse_id} measures {tokens} tokens alone, above the effective ColBERT cap of {max_tokens} (indexing.colbert_max_tokens={} less the document format overhead of {} tokens); indexing.fine_max_tokens={} must keep every fine chunk within the effective cap",
                member.id,
                budget.configured_max_tokens,
                budget.format_overhead,
                budget.fine_max_tokens
            )));
        }
        open = Some(OpenRun {
            start: position,
            end: position + 1,
            // The run's text is extended in place as chunks join; the chunk
            // row keeps its own text.
            text: member.targeting_text.clone(),
            tokens,
        });
    }
    if let Some(run) = open.take() {
        // Document end: a final sub-minimum run merges backward when the
        // combined text fits the cap; otherwise it is the one permitted
        // sub-minimum run.
        let merge_target = if run.tokens < min_tokens {
            runs.last_mut()
        } else {
            None
        };
        match merge_target {
            Some(previous) => {
                let candidate = chunk::join_text(&previous.text, &run.text);
                let tokens = count_tokens(tokenizer, parse_id, &candidate)?;
                if tokens <= max_tokens {
                    previous.end = run.end;
                    previous.text = candidate;
                    previous.tokens = tokens;
                } else {
                    runs.push(run);
                }
            }
            None => runs.push(run),
        }
    }
    runs.into_iter()
        .enumerate()
        .map(|(index, run)| window_from_run(chunks, parse_id, tokenizer, max_tokens, index, run))
        .collect()
}

/// Materialize one run as a window input: chunk ids in order, fragments
/// concatenated, and the model input prefixed with the first chunk's section
/// path. Prefix-drop rule: the ColBERT document limit is hard, so when the
/// prefixed input measures above the effective cap (`max_tokens`, the
/// configured cap less the format overhead, per `WindowBudget`) the window is
/// embedded from its canonical text alone (already measured within the
/// effective cap) and the drop is logged at DEBUG; the prefix is never part of
/// any range, so nothing persisted changes.
fn window_from_run(
    chunks: &[StoredChunk],
    parse_id: &str,
    tokenizer: &Tokenizer,
    max_tokens: usize,
    index: usize,
    run: OpenRun,
) -> Result<WindowInput, ApiError> {
    let span = chunks.get(run.start..run.end).ok_or_else(|| {
        failure(format!(
            "ColBERT window {index} of {parse_id} spans chunks the parse does not have"
        ))
    })?;
    let first = span.first().ok_or_else(|| {
        failure(format!(
            "ColBERT window {index} of {parse_id} has no chunks"
        ))
    })?;
    let id = new_retrieval_projection_id()?;
    // The window is the persisted artifact; the chunk rows stay borrowed, so
    // their ids and fragments are copied into it.
    let fragments: Vec<Fragment> = span
        .iter()
        .flat_map(|member| member.fragments.iter().cloned())
        .collect();
    let chunk_ids: Vec<String> = span.iter().map(|member| member.id.clone()).collect();
    let prefixed = chunk::model_input(&first.section_path, &run.text);
    let prefixed_tokens = count_tokens(tokenizer, parse_id, &prefixed)?;
    let (model_input, token_count) = if prefixed_tokens <= max_tokens {
        (prefixed, prefixed_tokens)
    } else {
        debug!(
            event = "multivector_build.prefix_dropped",
            parse_id,
            window_id = %id,
            window_index = index,
            prefixed_tokens,
            canonical_tokens = run.tokens,
            max_tokens,
            "section-path prefix dropped from ColBERT window input to respect the document limit"
        );
        (run.text, run.tokens)
    };
    Ok(WindowInput {
        id,
        window_index: index,
        chunk_ids,
        fragments,
        model_input,
        token_count,
    })
}

/// Measure the exact text, including special tokens, with the build's
/// untruncated, unpadded counter; packing decisions and the cap check must
/// describe the same bytes, and tokenization is not additive across joins.
fn count_tokens(tokenizer: &Tokenizer, parse_id: &str, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoding| encoding.len())
        .map_err(|source| ApiError::InferenceInit {
            message: format!("ColBERT window tokenization failed for parse {parse_id}: {source}"),
        })
}

/// Delete prior rows, then embed/validate/encode/insert one matrix per window,
/// length-routing local windows while batching all HTTP windows, then
/// returning totals for the completion log (safe aggregates — never matrix
/// values, which are forbidden in logs). Split out so `build_multivectors` can
/// wrap the whole payload write in a single success/failure envelope
/// transition.
// Explicit transaction, source/parse/envelope targets, backend, validation
// dimension, prepared windows, and observer preserve the outer builder's ownership.
#[allow(clippy::too_many_arguments)]
fn build_rows(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    colbert_runtime: &ColbertBackend,
    expected_dimension: usize,
    windows: &[WindowInput],
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<BuildRowsTotals, ApiError> {
    // Delete-first rebuild: clear the parse's prior rows before inserting so the
    // UNIQUE(parse_id, window_index) index cannot conflict on a re-run.
    tx.execute(DELETE_PARSE_WINDOWS_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to delete prior ColBERT window rows for parse {parse_id}: {source}"
            ),
        })?;

    let now = utc_now()?;
    let mut total_tokens = 0usize;
    let mut completed_windows = 0u64;

    // Local batching cost depends on sequence length. The routing threshold is
    // compared against the window's measured model input, while inference adds
    // its own fixed prompt. Only packing changes; persistence stays identical.
    let mut batched_indices: Vec<usize> = Vec::new();
    let mut singular_indices: Vec<usize> = Vec::new();
    // The threshold measures local accelerator padding cost. Remote serving
    // owns that execution cost, so batch every HTTP window to avoid per-window RTTs.
    let local_routing = colbert_runtime.uses_local_model_gate();
    for (index, window) in windows.iter().enumerate() {
        if !local_routing || window.token_count <= colbert_runtime.local_batch_max_tokens() {
            batched_indices.push(index);
        } else {
            singular_indices.push(index);
        }
    }

    // Singular pool: each long window embeds through the singular path, which
    // self-logs one per-call model_call.* pair labelled with the window id.
    for &index in &singular_indices {
        let window = &windows[index];
        let call = colbert_runtime.monitor_call(monitor, "ColBERT window embedding");
        let result = colbert_runtime.embed_document(&window.id, &window.model_input, call.as_ref());
        if let Some(call) = call {
            call.finish_result(&result);
        }
        let embedding = result?;
        completed_windows += 1;
        if let Some(monitor) = monitor {
            monitor.progress(completed_windows, Some(windows.len() as u64));
        }
        let token_count = persist_window_matrix(
            tx,
            source_id,
            parse_id,
            projection_id,
            expected_dimension,
            &now,
            window,
            embedding,
        )?;
        total_tokens = total_tokens.saturating_add(token_count);
    }

    // Batched pool: sort by measured token count before packing, so each
    // configured batch groups similarly-long documents and its padding waste
    // is bounded by its own longest member (the embed paths' fixed
    // prompt-prefix delta preserves this ordering exactly). The sort is purely
    // a packing optimization; per-window validate/encode/INSERT is
    // path-independent and each row carries its own window_index, so storage
    // order is irrelevant.
    batched_indices.sort_by(|&left, &right| {
        windows[right]
            .token_count
            .cmp(&windows[left].token_count)
            .then_with(|| left.cmp(&right))
    });

    for batch_indices in batched_indices.chunks(colbert_runtime.document_batch_size()) {
        // One batch per slice, holding the caller's gate only for local
        // inference. embed_documents self-logs ONE model_call.* pair with
        // batch-level fields — no per-window label at that boundary.
        let batch: Vec<(&str, &str)> = batch_indices
            .iter()
            .map(|&index| {
                (
                    windows[index].id.as_str(),
                    windows[index].model_input.as_str(),
                )
            })
            .collect();
        let call = colbert_runtime.monitor_call(monitor, "ColBERT window embedding");
        let result = colbert_runtime.embed_documents(&batch, call.as_ref());
        if let Some(call) = call {
            call.finish_result(&result);
        }
        let embeddings = result?;
        completed_windows += batch.len() as u64;
        if let Some(monitor) = monitor {
            monitor.progress(completed_windows, Some(windows.len() as u64));
        }
        if embeddings.len() != batch.len() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "ColBERT batched embedding returned {} matrices for {} windows of parse {parse_id}",
                    embeddings.len(),
                    batch.len()
                ),
            });
        }

        // One iteration per returned entry, in input (batch) order: the
        // backend preserves input ordering, and `persist_window_matrix` also
        // checks the returned label against the window it is paired with.
        for (&index, embedding) in batch_indices.iter().zip(embeddings) {
            let token_count = persist_window_matrix(
                tx,
                source_id,
                parse_id,
                projection_id,
                expected_dimension,
                &now,
                &windows[index],
                embedding,
            )?;
            total_tokens = total_tokens.saturating_add(token_count);
        }
    }

    Ok(BuildRowsTotals {
        total_tokens,
        batched_windows: batched_indices.len(),
        singular_windows: singular_indices.len(),
    })
}

/// Validate, encode, and INSERT one window's embedded matrix, returning its
/// matrix row count for the build total. Shared by both routing paths so
/// persistence stays byte-identical regardless of which path produced the
/// matrix. The returned label must be the window's id: a mismatch means the
/// batched path's ordering contract broke, and the row must not be written.
// Every argument is a distinct row column, the transaction, or the pairing
// being checked; bundling them would hide which column each value binds to.
#[allow(clippy::too_many_arguments)]
fn persist_window_matrix(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    expected_dimension: usize,
    now: &str,
    window: &WindowInput,
    embedding: ColbertDocumentEmbedding,
) -> Result<usize, ApiError> {
    if embedding.unit_id != window.id {
        return Err(failure(format!(
            "ColBERT embedding labelled {} was returned for window {} of parse {parse_id}",
            embedding.unit_id, window.id
        )));
    }
    super::annotation_io::admitted_value_count(
        embedding.token_count,
        embedding.dimension,
        &tx.limits().resources,
    )?;
    // Validate BEFORE persisting: reject zero-token, wrong-dimension,
    // wrong-value-count, or non-finite matrices so only well-formed matrices
    // reach storage and MaxSim scoring (C7c) can trust the shape columns.
    let stored: StoredColbertDocumentVector = validate_colbert_document_vector(
        UnitColbertDocumentVector {
            unit_id: embedding.unit_id,
            token_count: embedding.token_count,
            dimension: embedding.dimension,
            vector: embedding.vector,
        },
        expected_dimension,
    )?;

    // Encode the validated row-major matrix to the little-endian f32 blob;
    // token_count (matrix rows) and dimension go to their own columns so the
    // decoder can re-derive the expected byte length and reject any drift.
    let matrix_blob = encode_colbert_matrix_blob(&stored.vector);
    let chunk_ids_json = canonical_json_string(&window.chunk_ids, "window chunk ids")?;
    let fragments_json = canonical_json_string(&window.fragments, "window fragments")?;

    // The row id is the validated label: it equals `window.id` (checked
    // above), so the persisted identity is the one the model call reported.
    tx.execute(
        INSERT_WINDOW_SQL,
        params![
            stored.unit_id,
            projection_id,
            source_id,
            parse_id,
            window.window_index as i64,
            chunk_ids_json,
            fragments_json,
            stored.token_count as i64,
            stored.dimension as i64,
            matrix_blob,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert ColBERT window {} of parse {parse_id}: {source}",
            window.id
        ),
    })?;

    Ok(stored.token_count)
}

/// Render a model shape as a canonical JSON string (§16.2 deterministic
/// bytes), the same encoding the chunker uses for its JSON columns, so a
/// window's membership columns and the chunk rows they reference agree.
fn canonical_json_string<T: serde::Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}

/// Safe aggregate totals `build_rows` hands back for the completion log: the
/// summed matrix row count across the parse plus how many windows each
/// routing path embedded, so stage time in the build log is attributable per
/// path when benchmarking the hybrid.
struct BuildRowsTotals {
    total_tokens: usize,
    batched_windows: usize,
    singular_windows: usize,
}

/// Assemble §20 provenance for the ColBERT document embedder over the units
/// whose text the windows embed. `producerType` is Model; the input refs are
/// the ContentUnits reached through the windows' fragments, preserving lineage
/// from the matrices back to their units.
fn colbert_producer_provenance(unit_ids: &[String], model_identity: &str) -> Provenance {
    let input_refs = unit_ids
        .iter()
        .map(|unit_id| ProvenanceInputRef {
            object_type: ProvenanceObjectType::ContentUnit,
            id: unit_id.clone(),
            text_range: None,
        })
        .collect::<Vec<_>>();

    Provenance {
        producer_type: ProducerType::Model,
        producer_name: COLBERT_DOCUMENT_PRODUCER.to_string(),
        producer_version: Some(COLBERT_DOCUMENT_PRODUCER_VERSION.to_string()),
        // Capacity and tokenizer/provider identity survive snapshot and restore.
        config_hash: Some(model_identity.to_owned()),
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs),
    }
}

/// Storage failure carrying a builder- or loader-specific message.
fn failure(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}

// ---------------------------------------------------------------------------
// Query-time loader (C7c).
//
// The build-time writer above PERSISTS one ColBERT matrix per window. The
// loader below is its query-time twin: it DECODES a bounded set of those
// persisted matrices back into `ColbertWindowEmbedding`s so C7c's MaxSim
// stage can re-score the fused candidate pool. It never re-embeds documents
// (§38: no search-time recomputation of persisted document vectors) — it reads
// what the writer stored. Reads ride the caller's DP1 read transaction/
// connection (parse-scoped, §14 active-parse invariant); the loader opens no
// connection and no transaction of its own.
// ---------------------------------------------------------------------------

/// One persisted ColBERT window decoded for scoring: its id, membership, and
/// the row-major `token_count x dimension` matrix.
#[derive(Debug, Clone)]
pub(crate) struct ColbertWindowEmbedding {
    pub(crate) window_id: String,
    /// Member chunk ids in chunk order.
    pub(crate) chunk_ids: Vec<String>,
    /// The concatenation of the member chunks' fragments, scalar offsets.
    pub(crate) fragments: Vec<Fragment>,
    pub(crate) token_count: usize,
    pub(crate) dimension: usize,
    pub(crate) vector: Vec<f32>,
}

/// Load persisted ColBERT windows for a bounded candidate set, keyed by window
/// id, preserving the caller's requested order.
///
/// The read is scoped to the single active `parse_id` (§14) and one row is
/// returned per requested window that exists there. `window_ids` is the pool
/// already capped by the caller — the loader does NOT read the whole parse,
/// because matrix movement is the cost the candidate cap bounds. A requested
/// window with no row is silently skipped; the returned vector is ordered to
/// match `window_ids` so downstream scoring order is deterministic and
/// independent of SQLite row order.
///
/// `expected_dimension` is the runtime ColBERT projection dimension; the
/// decoder re-derives the expected byte length from the stored
/// `(token_count, dimension)` columns and rejects any drift. A decode failure
/// maps to `StorageOperation` carrying the offending window id.
pub(crate) fn load_colbert_windows(
    conn: &Connection,
    parse_id: &str,
    window_ids: &[String],
    expected_dimension: usize,
) -> Result<Vec<ColbertWindowEmbedding>, ApiError> {
    if window_ids.is_empty() {
        return Ok(Vec::new());
    }

    // One prepared statement reused across the bounded candidate set keeps the
    // read to one primary-key probe per window; the pool is already capped, so
    // this is bounded work.
    let mut statement =
        conn.prepare(SELECT_WINDOW_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare ColBERT window loader query for parse {parse_id}: {source}"
                ),
            })?;
    let resources = &conn.limits().resources;

    let mut embeddings = Vec::with_capacity(window_ids.len());
    for window_id in window_ids {
        let row = statement
            .query_row(
                params![
                    parse_id,
                    window_id,
                    resources.max_embedding_rows,
                    resources.max_embedding_values
                ],
                |row| {
                    Ok(WindowRow {
                        chunk_ids_json: row.get(0)?,
                        fragments_json: row.get(1)?,
                        token_count: row.get::<_, i64>(2)?,
                        dimension: row.get::<_, i64>(3)?,
                        matrix_blob: row.get::<_, Option<Vec<u8>>>(4)?,
                    })
                },
            )
            .optional()
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to read ColBERT window {window_id} of parse {parse_id}: {source}"
                ),
            })?;

        // A missing row is a candidate that simply has no stored window in
        // this parse — skip it rather than fail the whole query. Callers that
        // need to know which candidates were scored compare returned ids.
        let Some(row) = row else { continue };

        let token_count = row.token_count.max(0) as usize;
        let row_dimension = row.dimension.max(0) as usize;
        super::annotation_io::admitted_value_count(token_count, row_dimension, resources)?;
        let matrix_blob = row.matrix_blob.ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "resource limit: ColBERT matrix for window {window_id} exceeds configured shape or byte budget"
            ),
        })?;
        // Decode + validate the stored blob; the codec re-derives the expected
        // byte length from (token_count, dimension) and rejects drift. Decode
        // errors are Strings — re-wrap with the window's identity.
        let vector = decode_colbert_document_vector_blob(
            window_id,
            &matrix_blob,
            token_count,
            row_dimension,
            expected_dimension,
        )
        .map_err(|message| ApiError::StorageOperation {
            message: format!(
                "failed to decode persisted matrix for ColBERT window {window_id} of parse {parse_id}: {message}"
            ),
        })?;

        embeddings.push(ColbertWindowEmbedding {
            window_id: window_id.clone(),
            chunk_ids: decode_json_column(&row.chunk_ids_json, window_id, "chunk ids")?,
            fragments: decode_json_column(&row.fragments_json, window_id, "fragments")?,
            token_count,
            dimension: expected_dimension,
            vector,
        });
    }

    Ok(embeddings)
}

/// Resolve fine chunks to the ColBERT windows containing them: one
/// membership-only read of the parse's windows (ids and chunk ids, no matrix),
/// filtered in Rust to the requested chunks. Each chunk belongs to exactly one
/// window by construction; a requested chunk with no window is absent from
/// the map.
pub(crate) fn windows_for_chunks(
    conn: &Connection,
    parse_id: &str,
    chunk_ids: &[String],
) -> Result<BTreeMap<String, String>, ApiError> {
    let mut resolved = BTreeMap::new();
    if chunk_ids.is_empty() {
        return Ok(resolved);
    }
    let mut statement = conn
        .prepare(SELECT_PARSE_WINDOW_CHUNKS_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare ColBERT window membership query for parse {parse_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to query ColBERT window membership for parse {parse_id}: {source}"
            ),
        })?;
    for row in rows {
        let (window_id, chunk_ids_json) = row.map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read ColBERT window membership row for parse {parse_id}: {source}"
            ),
        })?;
        let members: Vec<String> = decode_json_column(&chunk_ids_json, &window_id, "chunk ids")?;
        for member in members {
            if chunk_ids.contains(&member) {
                resolved.insert(member, window_id.clone());
            }
        }
    }
    Ok(resolved)
}

/// Decode one canonical JSON column of a window row, naming the window and
/// the column on failure so corruption is attributable.
fn decode_json_column<T: serde::de::DeserializeOwned>(
    json: &str,
    window_id: &str,
    what: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
        message: format!("{what} of ColBERT window {window_id} are unparseable: {source}"),
    })
}

/// One persisted window row, as read by the query-time loader: membership
/// columns, the shape columns, and the row-major matrix blob. Shape lives in
/// its own columns so the decoder can re-derive the expected byte length
/// without a separate record (mirrors the writer's INSERT contract).
struct WindowRow {
    chunk_ids_json: String,
    fragments_json: String,
    token_count: i64,
    dimension: i64,
    matrix_blob: Option<Vec<u8>>,
}
