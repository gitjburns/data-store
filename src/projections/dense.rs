//! C6c dense builder: per-chunk dense vectors as `dense_vector` projections
//! (`chunk_dense_vectors`), keyed to active parses with an explicit
//! rebuild-from-canonical path and the typed little-endian f32 codec (spec
//! §22, §8.3). Reads chunks back through `super::StoredChunk`.
//!
//! Granularity (approved design fact 2): one `chunk_dense_vectors` row per
//! `chunk_projections` row — the dense channel shares chunk targeting
//! granularity with the lexical channel, so a chunk's `targeting_text` is the
//! exact passage embedded.
//!
//! Atomicity: the build path takes the CALLER's `&Transaction` and drives the
//! whole per-parse build (envelope open → per-chunk insert → envelope
//! complete/fail) on it, so the projection metadata row, its lifecycle events,
//! and every `chunk_dense_vectors` row commit or roll back as one unit (mirror
//! of `super::envelope` and `crate::annotations::store`). The read path takes a
//! `&Connection`; one bounded SELECT needs no transaction.
//!
//! Model-call gate (approved design fact 9): dense embedding is now a
//! config-selected backend (`DenseEmbeddingBackend`, Local | Http). The gate
//! discipline is backend-aware and CALLER-SIDE. For the LOCAL backend, each
//! embed is a live accelerator call that MUST run UNDER the caller-side
//! model-call gate (`AppState::acquire_model_call_gate`); for the HTTP backend,
//! embedding is network I/O and MUST NOT hold the exclusive gate across the
//! request. This builder is pure — it does not own `AppState` — so the CALLER
//! decides whether to acquire the gate (via `uses_local_model_gate()`) and, for
//! the local path, passes ONE `ModelCallPermit` by reference as structural proof
//! the gate is held for the whole per-parse batch. Holding it once per parse
//! (not once per chunk) is the correct granularity for the local path:
//! re-acquiring per chunk would thrash the exclusive gate and interleave
//! unrelated model work between a single parse's chunks. The `_gate` parameter is
//! `Option<&ModelCallPermit>`: `Some` on the local path (permit held), `None` on
//! the HTTP path (no gate is acquired anywhere across the network round-trips).
//!
//! Writer lock across HTTP I/O (known, accepted): the model-call gate above is
//! NOT the only lock in play. Because the build path takes the CALLER's
//! `&Transaction` (see Atomicity), the scheduler's IMMEDIATE `projection_build`
//! transaction — and with it the hot-plane WRITER LOCK — is alive across the
//! whole build, INCLUDING the concurrent HTTP fan-out in
//! `embed_windows_concurrently`. This is a pre-existing property of the
//! take-the-caller's-tx contract (the former serial HTTP loop held it too, and
//! it long predates concurrency), accepted for now because the HTTP dense build
//! is seconds-long and the annotation worker resolves the contention via its
//! ruling-B pre-paid deferral. The banked structural fix (plan Option A) is to
//! embed BEFORE opening the transaction and take the lock only for the commit —
//! the pattern the annotation worker's producer waves already follow.

// Implemented by the C6c dense package. The build/read paths and the load-path
// row type are consumed by C6c-2 (the active dense cache, which loads through
// `visit_dense_vectors_for_parse`), the C6 integration agent (which wires
// `build_dense_vectors` into the projection worker and holds the model gate for
// the batch), and the C7 retrieval pipeline. None are wired yet, so the
// module-level allow names those pending consumers.
#![allow(dead_code)]

use std::time::Instant;

use rusqlite::{Connection, Transaction, params};
use tracing::{error, info};

use crate::error::ApiError;
use crate::inference::{DenseEmbeddingBackend, HttpDenseClient};
use crate::model::{ProducerType, Provenance, ProvenanceInputRef, ProvenanceObjectType};
use crate::primitives::codec::{decode_vector_blob, encode_vector_blob};
use crate::primitives::utc_now;
use crate::primitives::validate::validate_vector;
use crate::projections::StoredChunk;
use crate::projections::envelope::{self, NewProjection, ProjectionType};
use crate::state::ModelCallPermit;

/// Dense producer name recorded in the projection's Provenance (spec §20/§22).
/// Stable across rebuilds so a dense projection's producer identity is
/// answerable from its envelope. The embedding MODEL identity is self-logged by
/// the selected backend's `model_call.*` events (local per-passage embed or
/// HTTP batched request); this names the BUILDER.
const DENSE_PRODUCER_NAME: &str = "fabric-dense";

/// Dense builder version (spec §20 producerVersion). Bumped when the dense
/// build contract changes in a way that must invalidate prior dense vectors, so
/// a version change is a visible rebuild trigger.
const DENSE_PRODUCER_VERSION: &str = "1";

/// The tracing model_role/call_purpose recorded when the caller acquires the
/// gate for the batch. Kept here so the builder's per-parse boundary logs carry
/// the same identifiers the gate's `model_gate.*` events use.
const DENSE_MODEL_ROLE: &str = "dense";
const DENSE_CALL_PURPOSE: &str = "passage_embedding";

/// Number of chunk passages sent per batched HTTP embeddings request. Applies to
/// the HTTP dense backend ONLY; the local path stays batch-1 (CPa revert — the
/// 8B forward already saturates this GPU at batch 1, so right-padding a batch was
/// a net regression). Engineering fact, not an operator config knob: 32 is an
/// initial value pending the HTTP-backend re-benchmark measurements — mirror of
/// the `COLBERT_BATCH_ROUTE_MAX_TOKENS` engineering-fact style. Revisit against
/// that re-benchmark's throughput/latency data.
const DENSE_HTTP_BATCH_SIZE: usize = 32;

/// Number of `DENSE_HTTP_BATCH_SIZE` windows dispatched concurrently on scoped
/// OS threads for the HTTP dense backend ONLY (the local path stays batch-1,
/// gate-serialized — concurrency is impossible and wrong there). Engineering
/// fact, not an operator config knob: this initial value is sized against
/// provider rate limits; a failed window fails the source's whole projection
/// build, so this bound is deliberately conservative — revisit with observed
/// 429 rates. Each in-flight thread holds exactly one HTTP request; SQLite
/// persistence stays serial on the scheduler thread (see `build_all_chunks`).
const DENSE_HTTP_CONCURRENT_REQUESTS: usize = 8;

/// Ordered SELECT of a parse's chunks. Ordering by `id` keeps the per-parse
/// build and its logs deterministic across runs; the dense channel does not
/// depend on chunk order for correctness (each chunk embeds independently), but
/// a total order makes the batch reproducible.
const SELECT_PARSE_CHUNKS_SQL: &str = "
SELECT
  id, projection_id, source_id, parse_id, input_unit_ids_json,
  targeting_text, token_count, chunker_name, chunker_version,
  chunker_config_hash
FROM chunk_projections
WHERE parse_id = ?1
ORDER BY id";

/// Delete every dense vector of a parse ahead of a rebuild (idempotent
/// rebuild). Runs inside the caller's transaction so a rebuild that fails
/// mid-way rolls the delete back with it — the prior dense plane is never left
/// half-erased.
const DELETE_PARSE_DENSE_SQL: &str = "
DELETE FROM chunk_dense_vectors WHERE parse_id = ?1";

/// Insert one dense vector row. `dimension` and `norm` are stored alongside the
/// little-endian f32 `vector_blob` so the reader validates length and reuses
/// the precomputed L2 norm without rescanning the blob (spec §22, mirror of the
/// `StoredDenseVector` contract).
const INSERT_DENSE_SQL: &str = "
INSERT INTO chunk_dense_vectors (
  chunk_id, source_id, parse_id, dimension, norm, vector_blob, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// Stable row order permits reproducible scans. Reject oversized blobs in SQL
/// before SQLite copies them into Rust; the visitor reports the offending row.
const SELECT_PARSE_DENSE_SQL: &str = "
SELECT chunk_id, parse_id, dimension, norm,
       CASE WHEN length(vector_blob) = ?2 THEN vector_blob END
FROM chunk_dense_vectors
WHERE parse_id = ?1
ORDER BY chunk_id";

/// One validated dense vector loaded from the hot plane — the row shape C6c-2's
/// cache and the C7 retrieval pipeline consume. The `vector` is a row-major
/// f32 passage embedding of length `dimension`; `norm` is the precomputed L2
/// norm the query-time cosine scorer reuses. Every field is
/// codec+validator-checked before this struct is constructed (see
/// `visit_dense_vectors_for_parse`), so a consumer may rely on: `vector.len() ==
/// dimension`, all values finite, and `norm` finite and strictly positive —
/// the cache need NOT re-validate.
#[derive(Debug, Clone)]
pub(crate) struct StoredDenseVectorRow {
    pub(crate) chunk_id: String,
    pub(crate) parse_id: String,
    pub(crate) dimension: usize,
    pub(crate) norm: f32,
    pub(crate) vector: Vec<f32>,
}

/// The result of one per-parse dense build: what got written, for the caller's
/// status logging and the integration worker's cycle totals. Carries no vector
/// values (forbidden in logs).
#[derive(Debug, Clone, Copy)]
pub(crate) struct DenseBuildOutcome {
    pub(crate) chunk_count: usize,
    pub(crate) dimension: usize,
    /// How many chunks were embedded through the HTTP batched path vs one at a
    /// time (local singular, or an HTTP window of size 1). Mirrors the CPd
    /// multi-vector completion log's per-path attribution so build time is
    /// attributable to the batched vs singular path when benchmarking the HTTP
    /// backend. On the local backend `batched_chunk_count` is always 0.
    pub(crate) batched_chunk_count: usize,
    pub(crate) singular_chunk_count: usize,
}

/// Build the per-parse dense plane: open a `dense_vector` envelope, embed each
/// chunk's `targeting_text` as a passage through the config-selected dense
/// backend, validate and persist each vector, then complete the envelope — all
/// on the caller's transaction.
///
/// Backend routing (approved design). The LOCAL backend embeds one chunk at a
/// time (batch-1, CPa revert) under the caller-held model gate. The HTTP backend
/// packs chunks into `DENSE_HTTP_BATCH_SIZE`-sized windows and embeds up to
/// `DENSE_HTTP_CONCURRENT_REQUESTS` windows CONCURRENTLY on scoped OS threads
/// (holding NO gate), then persists every vector serially in chunk order on this
/// scheduler thread. Persistence is byte-identical across backends: every chunk
/// still flows through the same validate → encode → INSERT helper in chunk order,
/// so the stored plane's shape does not depend on which path produced a vector.
///
/// `_gate` is `Some` on the local path — the caller's batch-scoped
/// `ModelCallPermit`, taken by reference purely as structural proof the
/// model-call gate is held for the whole batch (see the module gate note), so the
/// local path cannot run without the caller holding a live permit acquired ONCE
/// per parse. It is `None` on the HTTP path, where no gate is acquired anywhere
/// across the network round-trips.
///
/// Rebuild ordering (idempotence): this runs AFTER the parse's chunks exist
/// (C6b built `chunk_projections`), and it deletes the parse's prior
/// `chunk_dense_vectors` before inserting, so a rebuild is a full replacement,
/// not an accumulation. The delete and inserts share the caller's transaction,
/// so a failed rebuild rolls back to the prior dense plane intact.
///
/// On any failure after the envelope is opened, the envelope is marked `failed`
/// on the SAME transaction and the failure is returned; the caller's rollback
/// then discards both the failed-envelope row and any partial inserts, but the
/// `projection.failed` event's audit trail... is the caller's to preserve by
/// committing the failure marker in a separate transaction if it wants the
/// event durable. This builder keeps the atomic unit whole and surfaces the
/// error; failure-event durability policy belongs to the integration caller.
pub(crate) fn build_dense_vectors(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    dense_backend: &DenseEmbeddingBackend,
    expected_dimension: usize,
    _gate: Option<&ModelCallPermit>,
) -> Result<DenseBuildOutcome, ApiError> {
    let started = Instant::now();

    // Read the parse's chunks first: the input set determines the envelope's
    // producer input refs and the number of embeddings, so it is gathered
    // before the envelope is opened.
    let chunks = read_parse_chunks(tx, parse_id)?;

    info!(
        event = "dense_build.started",
        source_id,
        parse_id,
        model_role = DENSE_MODEL_ROLE,
        call_purpose = DENSE_CALL_PURPOSE,
        chunk_count = chunks.len(),
        expected_dimension,
        "per-parse dense build started"
    );

    let projection_id =
        envelope::insert_building(tx, &new_projection(source_id, parse_id, &chunks))?;

    // From here the envelope exists; any failure marks it `failed` on the same
    // transaction before returning, so the lifecycle never stalls in `building`.
    match build_all_chunks(
        tx,
        source_id,
        parse_id,
        dense_backend,
        expected_dimension,
        &chunks,
    ) {
        Ok(counts) => {
            // Dense payloads live entirely in the `chunk_dense_vectors` hot
            // table, so there is no archived payload to reference: complete
            // fresh with payload_uri = None.
            envelope::complete_fresh(tx, &projection_id, None)?;
            let outcome = DenseBuildOutcome {
                chunk_count: chunks.len(),
                dimension: expected_dimension,
                batched_chunk_count: counts.batched,
                singular_chunk_count: counts.singular,
            };
            info!(
                event = "dense_build.completed",
                // The enclosing owner reports durability after its commit.
                persistence = "pending_commit",
                source_id,
                parse_id,
                projection_id = %projection_id,
                backend = dense_backend.backend_kind(),
                chunk_count = outcome.chunk_count,
                dimension = outcome.dimension,
                // Per-path routing counts so build time in this log is
                // attributable to the batched (HTTP window) vs singular (local
                // batch-1, or HTTP window of 1) path when benchmarking.
                batched_chunk_count = outcome.batched_chunk_count,
                singular_chunk_count = outcome.singular_chunk_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "per-parse dense build completed"
            );
            Ok(outcome)
        }
        Err(build_error) => {
            // Mark the envelope failed on the same transaction. If the marker
            // itself fails, surface THAT error (it indicates a deeper storage
            // fault) but keep the original build error in the log.
            let detail = build_error.to_string();
            if let Err(mark_error) = envelope::mark_failed(tx, &projection_id, &detail) {
                error!(
                    event = "dense_build.failed",
                    source_id,
                    parse_id,
                    projection_id = %projection_id,
                    error = %build_error,
                    mark_error = %mark_error,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "per-parse dense build failed and the envelope-failed marker also failed"
                );
                return Err(mark_error);
            }
            error!(
                event = "dense_build.failed",
                source_id,
                parse_id,
                projection_id = %projection_id,
                error = %build_error,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "per-parse dense build failed"
            );
            Err(build_error)
        }
    }
}

/// Per-path chunk counts `build_all_chunks` returns for the completion log:
/// how many chunks each embed path produced. Safe aggregates only (no vectors).
struct DenseBuildCounts {
    batched: usize,
    singular: usize,
}

/// Embed, validate, and persist every chunk of the parse, backend-routed. Split
/// out from `build_dense_vectors` so the envelope's failure marking has a single
/// error path to catch (the `?` operators here all funnel into that caller's
/// `Err` arm). Deletes the parse's prior dense rows first for idempotent rebuild.
///
/// Routing (approved design):
/// - LOCAL: one embed per chunk in chunk order. Deliberately batch-1 — the CPa
///   batched forward was measured 2026-07-18 at only ~4% better raw kernel
///   throughput on this hardware (the 8B forward already saturates the GPU at
///   batch 1) while right-padding inflated useful work ~1.7x, a net regression,
///   so CPa was reverted to this singular loop. Every chunk counts as singular.
/// - HTTP: chunks packed into `DENSE_HTTP_BATCH_SIZE`-sized windows; windows are
///   dispatched up to `DENSE_HTTP_CONCURRENT_REQUESTS` at a time on scoped OS
///   threads (see `embed_windows_concurrently`), each window's returned vectors
///   aligning 1:1 with its chunks in order. A window of size >1 counts its chunks
///   as batched, a trailing window of size 1 as singular (it is one round-trip
///   carrying one text).
///
/// Concurrency boundary (HTTP path): ONLY the HTTP calls fan out. The results are
/// collected per window, all threads join, and THEN every vector is persisted on
/// this (scheduler) thread through `persist_chunk_vector` in strict chunk order —
/// so all SQLite writes stay serial on the caller's `&Transaction` (rusqlite
/// `Transaction` is not `Sync`, and the atomicity contract requires one writer),
/// and the stored plane is byte-identical to the local path's. The first window
/// error propagates after the scope joins; because persistence happens only after
/// a fully successful fan-out, a failed build never persists partial results.
///
/// Persistence is byte-identical across both paths: every produced vector flows
/// through the shared `persist_chunk_vector` helper in chunk order, so the stored
/// plane's shape is independent of which backend or path produced a vector.
fn build_all_chunks(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    dense_backend: &DenseEmbeddingBackend,
    expected_dimension: usize,
    chunks: &[StoredChunk],
) -> Result<DenseBuildCounts, ApiError> {
    // Idempotent rebuild: clear the parse's prior dense plane before inserting.
    // Shares the caller's transaction, so a mid-build failure rolls this delete
    // back alongside the partial inserts.
    tx.execute(DELETE_PARSE_DENSE_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to clear prior dense vectors for parse {parse_id}: {source}"),
        })?;

    let now = utc_now()?;
    let mut counts = DenseBuildCounts {
        batched: 0,
        singular: 0,
    };

    match dense_backend {
        // LOCAL path: batch-1 under the caller-held gate (see module note). The
        // runtime self-logs its own `model_call.*` events per chunk; no vector
        // values are logged here (forbidden).
        DenseEmbeddingBackend::Local(runtime) => {
            for chunk in chunks {
                let raw_vector = runtime.embed_passage_vector(&chunk.targeting_text)?;
                persist_chunk_vector(
                    tx,
                    source_id,
                    parse_id,
                    expected_dimension,
                    &now,
                    chunk,
                    raw_vector,
                )?;
                counts.singular += 1;
            }
        }
        // HTTP path: no gate is held across the network round-trips. Pack chunks
        // into DENSE_HTTP_BATCH_SIZE windows and embed the windows CONCURRENTLY
        // (up to DENSE_HTTP_CONCURRENT_REQUESTS in flight), then persist every
        // vector serially in chunk order below. Fanning out ONLY the HTTP calls
        // keeps all SQLite writes on this thread's `tx`; the fan-out returns
        // per-window vectors in window order, so window order == chunk order.
        DenseEmbeddingBackend::Http(client) => {
            let windows: Vec<&[StoredChunk]> = chunks.chunks(DENSE_HTTP_BATCH_SIZE).collect();
            let texts: Vec<&str> = chunks
                .iter()
                .map(|chunk| chunk.targeting_text.as_str())
                .collect();
            let text_windows: Vec<&[&str]> = texts.chunks(DENSE_HTTP_BATCH_SIZE).collect();
            let window_vectors = embed_windows_concurrently(client, &text_windows, parse_id)?;

            // All windows embedded and length-checked; now persist serially in
            // chunk order on the caller's transaction. Every window's vectors
            // persist in chunk order so the stored plane matches the local path.
            for (window, vectors) in windows.iter().zip(window_vectors) {
                if window.len() > 1 {
                    counts.batched += window.len();
                } else {
                    counts.singular += window.len();
                }
                for (chunk, raw_vector) in window.iter().zip(vectors) {
                    persist_chunk_vector(
                        tx,
                        source_id,
                        parse_id,
                        expected_dimension,
                        &now,
                        chunk,
                        raw_vector,
                    )?;
                }
            }
        }
    }

    Ok(counts)
}

/// Embed a parse's `DENSE_HTTP_BATCH_SIZE` windows concurrently and return each
/// window's validated vectors in WINDOW ORDER (== chunk order), so the caller can
/// persist them serially without reordering.
///
/// Concurrency model: windows are processed in successive WAVES of up to
/// `DENSE_HTTP_CONCURRENT_REQUESTS`. Each wave opens one `std::thread::scope` and
/// spawns one scoped thread per window in the wave; every thread borrows the SAME
/// `&HttpDenseClient` (its methods take `&self` and it is `Sync` — it holds a
/// `reqwest::blocking::Client`, which is `Send + Sync`, plus immutable
/// String/PathBuf/Option fields), so NO `Arc` or clone is needed. Only the HTTP
/// call runs on the scoped thread; no SQLite handle crosses the boundary.
///
/// Error semantics preserve the serial contract exactly: the scope joins all
/// threads before this returns, then the FIRST window error (by window index) is
/// propagated and NOTHING is persisted for this build — matching the old serial
/// loop, where the first failing window aborted the build before any later window
/// persisted. A per-window defensive length check (vectors == chunks) is applied
/// here so a protocol regression cannot misalign vectors to chunk ids downstream.
///
/// HELD LOCKS (be honest): no `ModelCallPermit` is held here (HTTP path), but the
/// caller's enclosing write transaction — and therefore the hot-plane WRITER LOCK
/// — IS alive across this entire fan-out (see the module-header "Writer lock
/// across HTTP I/O" note). Pre-existing take-the-caller's-tx property, accepted
/// because the HTTP build is seconds-long and worker contention resolves via the
/// ruling-B pre-paid deferral; the banked fix (plan Option A) moves embedding
/// before the transaction opens.
fn embed_windows_concurrently(
    client: &HttpDenseClient,
    windows: &[&[&str]],
    parse_id: &str,
) -> Result<Vec<Vec<Vec<f32>>>, ApiError> {
    let mut results: Vec<Vec<Vec<f32>>> = Vec::with_capacity(windows.len());

    // Wave-bounded fan-out: at most DENSE_HTTP_CONCURRENT_REQUESTS HTTP calls are
    // in flight per scope. A conservative bound (see the constant) sized against
    // provider rate limits. An open scope holds only Rust borrows locally — but
    // the caller's write transaction (and the hot-plane writer lock) remains
    // held around this whole function; see the HELD LOCKS note above.
    for wave in windows.chunks(DENSE_HTTP_CONCURRENT_REQUESTS) {
        // Per-window outcomes collected in window order. Each scoped thread only
        // performs the pure HTTP call; the length check and error selection happen
        // on this thread after join so the abort/propagate order is deterministic.
        let wave_outcomes: Vec<Result<Vec<Vec<f32>>, ApiError>> = std::thread::scope(|scope| {
            let handles: Vec<_> = wave
                .iter()
                .map(|window| {
                    // Shared `&client` crosses the scope boundary by reference
                    // (Sync); the borrow lives only for this scope.
                    scope.spawn(
                        crate::util::LogContext::current()
                            .wrap(move || client.embed_passage_vectors(window)),
                    )
                })
                .collect();
            // Join every thread in the wave before leaving the scope: an in-flight
            // call always completes (bounded by the client's HTTP timeout); a
            // panicked embed thread is surfaced as a build error rather than
            // silently dropped. `scope` guarantees all spawned threads have joined
            // when it returns, but the explicit joins let a panic map to an error.
            handles
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(result) => result,
                    Err(payload) => Err(ApiError::StorageOperation {
                        message: format!(
                            "HTTP dense embed thread panicked for parse {parse_id}: {}",
                            crate::util::panic_payload_message(payload.as_ref())
                        ),
                    }),
                })
                .collect()
        });

        // First-error-wins in window order, mirroring the old serial loop: the
        // earliest failing window aborts the whole build before any vector is
        // persisted (persistence happens only in the caller, after this returns).
        for (window, outcome) in wave.iter().zip(wave_outcomes) {
            let vectors = outcome?;
            // Defensive 1:1 length check (the client guarantees it): a protocol
            // regression must not silently misalign vectors to chunk ids.
            if vectors.len() != window.len() {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "HTTP dense backend returned {} vectors for {} inputs of parse {parse_id}",
                        vectors.len(),
                        window.len()
                    ),
                });
            }
            results.push(vectors);
        }
    }

    Ok(results)
}

/// Embed additional retrieval representations with the same bounded HTTP
/// batching as passages. The caller owns local accelerator admission and every
/// persistence boundary; these worker threads perform network calls only.
pub(crate) fn embed_texts(
    backend: &DenseEmbeddingBackend,
    texts: &[&str],
    parse_id: &str,
) -> Result<Vec<Vec<f32>>, ApiError> {
    match backend {
        DenseEmbeddingBackend::Local(runtime) => texts
            .iter()
            .map(|text| runtime.embed_complete_passage_vector(text))
            .collect(),
        DenseEmbeddingBackend::Http(client) => {
            let windows: Vec<&[&str]> = texts.chunks(DENSE_HTTP_BATCH_SIZE).collect();
            Ok(embed_windows_concurrently(client, &windows, parse_id)?
                .into_iter()
                .flatten()
                .collect())
        }
    }
}

/// Validate one chunk's embedded vector and INSERT its row. Shared by both embed
/// paths so persistence stays byte-identical regardless of which backend/path
/// produced the vector.
///
/// Validate BEFORE persisting: dimension, finiteness, and a finite nonzero norm
/// are checked here, yielding the precomputed L2 norm stored so the reader (and
/// the C6c-2 cache) can reuse it without rescanning the blob. A bad vector fails
/// the whole build rather than persisting an unusable row; failure is attributed
/// to the chunk id.
fn persist_chunk_vector(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    expected_dimension: usize,
    now: &str,
    chunk: &StoredChunk,
    raw_vector: Vec<f32>,
) -> Result<(), ApiError> {
    let validated =
        validate_vector(chunk.id.clone(), raw_vector, expected_dimension).map_err(|message| {
            ApiError::StorageOperation {
                message: format!("dense vector validation failed: {message}"),
            }
        })?;

    // Encode/decode boundary: persist through the shared little-endian f32 codec
    // so the blob layout matches what `visit_dense_vectors_for_parse` decodes;
    // blob encoding is never hand-rolled here.
    let blob = encode_vector_blob(&validated.vector);

    tx.execute(
        INSERT_DENSE_SQL,
        params![
            chunk.id,
            source_id,
            parse_id,
            expected_dimension as i64,
            validated.norm,
            blob,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert dense vector for chunk {} of parse {parse_id}: {source}",
            chunk.id
        ),
    })?;

    Ok(())
}

/// Scan persisted passage vectors in the caller's database snapshot, retaining
/// only one decoded row. SQLite and the operating system may cache file pages;
/// query correctness never depends on keeping the vector plane in heap memory.
/// Callers must discard accumulated scores if any later row fails validation.
pub(crate) fn visit_dense_vectors_for_parse(
    conn: &Connection,
    parse_id: &str,
    expected_dimension: usize,
    mut visit: impl FnMut(&StoredDenseVectorRow) -> Result<(), ApiError>,
) -> Result<usize, ApiError> {
    let expected_bytes = expected_dimension
        .checked_mul(std::mem::size_of::<f32>())
        .and_then(|bytes| i64::try_from(bytes).ok())
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!("dense dimension byte length overflow for parse {parse_id}"),
        })?;
    let mut statement =
        conn.prepare(SELECT_PARSE_DENSE_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare dense-vector load query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id, expected_bytes], |row| {
            Ok(DenseVectorRawRow {
                chunk_id: row.get(0)?,
                parse_id: row.get(1)?,
                dimension: row.get::<_, i64>(2)?,
                norm: row.get(3)?,
                vector_blob: row.get(4)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query dense vectors for parse {parse_id}: {source}"),
        })?;

    let mut row_count = 0;
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read dense-vector row for parse {parse_id}: {source}"),
        })?;
        let vector = dense_row_from_raw(row, expected_dimension)?;
        visit(&vector)?;
        row_count += 1;
    }
    Ok(row_count)
}

/// One `chunk_dense_vectors` row as read from SQLite, before its blob is
/// decoded+validated and its stored dimension is re-typed.
struct DenseVectorRawRow {
    chunk_id: String,
    parse_id: String,
    dimension: i64,
    norm: f32,
    vector_blob: Option<Vec<u8>>,
}

/// Decode+validate one raw dense row into a `StoredDenseVectorRow`. The stored
/// `dimension` is compared against the caller's `expected_dimension` and the
/// blob is decoded through the shared codec (which re-validates length,
/// finiteness, and nonzero norm), so any drift between the stored plane and the
/// current model dimension — or any corruption — fails loudly here.
fn dense_row_from_raw(
    row: DenseVectorRawRow,
    expected_dimension: usize,
) -> Result<StoredDenseVectorRow, ApiError> {
    let stored_dimension =
        usize::try_from(row.dimension).map_err(|_| ApiError::StorageOperation {
            message: format!(
                "dense vector {} has a negative stored dimension {}",
                row.chunk_id, row.dimension
            ),
        })?;

    // decode_vector_blob checks stored-vs-expected dimension, byte length, and
    // re-runs validate_vector (finite + nonzero norm) on the decoded values, so
    // the returned vector already satisfies the StoredDenseVectorRow guarantees.
    let blob = row.vector_blob.ok_or_else(|| ApiError::StorageOperation {
        message: format!(
            "dense vector {} blob length differs from expected dimension {expected_dimension}",
            row.chunk_id
        ),
    })?;
    let vector = decode_vector_blob(&row.chunk_id, &blob, stored_dimension, expected_dimension)
        .map_err(|message| ApiError::StorageOperation {
            message: format!("failed to decode stored dense vector: {message}"),
        })?;

    // A valid vector with a corrupt stored norm would still change cosine
    // ordering. Keep the persisted denominator consistent with its payload.
    let computed_norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if !row.norm.is_finite()
        || row.norm <= 0.0
        || (computed_norm - row.norm).abs() > computed_norm * 1e-5
    {
        return Err(ApiError::StorageOperation {
            message: format!(
                "dense vector {} has an inconsistent stored norm",
                row.chunk_id
            ),
        });
    }
    Ok(StoredDenseVectorRow {
        chunk_id: row.chunk_id,
        parse_id: row.parse_id,
        dimension: expected_dimension,
        norm: row.norm,
        vector,
    })
}

/// Read a parse's chunks in a stable order for the build. Reads
/// `chunk_projections` directly (rather than through a C6b reader, which does
/// not yet exist) and re-types each row into `super::StoredChunk`, decoding the
/// `input_unit_ids_json` array. `token_count` is nullable in the schema and
/// stays `Option`.
fn read_parse_chunks(tx: &Transaction<'_>, parse_id: &str) -> Result<Vec<StoredChunk>, ApiError> {
    let mut statement =
        tx.prepare(SELECT_PARSE_CHUNKS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to prepare chunk query for parse {parse_id}: {source}"),
            })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok(ChunkRawRow {
                id: row.get(0)?,
                projection_id: row.get(1)?,
                source_id: row.get(2)?,
                parse_id: row.get(3)?,
                input_unit_ids_json: row.get(4)?,
                targeting_text: row.get(5)?,
                token_count: row.get::<_, Option<i64>>(6)?,
                chunker_name: row.get(7)?,
                chunker_version: row.get(8)?,
                chunker_config_hash: row.get(9)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query chunks for parse {parse_id}: {source}"),
        })?;

    let mut chunks = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read chunk row for parse {parse_id}: {source}"),
        })?;
        chunks.push(chunk_from_raw(row)?);
    }
    Ok(chunks)
}

/// One `chunk_projections` row as read from SQLite, before its
/// `input_unit_ids_json` is decoded and its nullable `token_count` is re-typed.
struct ChunkRawRow {
    id: String,
    projection_id: String,
    source_id: String,
    parse_id: String,
    input_unit_ids_json: String,
    targeting_text: String,
    token_count: Option<i64>,
    chunker_name: String,
    chunker_version: String,
    chunker_config_hash: String,
}

/// Re-type one persisted chunk row into `super::StoredChunk`, decoding the
/// `input_unit_ids_json` array. A stored value that no longer parses is a
/// corruption surfaced with the chunk's identity, never silently dropped.
fn chunk_from_raw(row: ChunkRawRow) -> Result<StoredChunk, ApiError> {
    let input_unit_ids: Vec<String> =
        serde_json::from_str(&row.input_unit_ids_json).map_err(|source| {
            ApiError::StorageOperation {
                message: format!(
                    "persisted input unit ids of chunk {} are unparseable: {source}",
                    row.id
                ),
            }
        })?;
    // token_count is a nonnegative count; a negative stored value is corruption.
    let token_count = row
        .token_count
        .map(|value| {
            u64::try_from(value).map_err(|_| ApiError::StorageOperation {
                message: format!("chunk {} has a negative stored token_count {value}", row.id),
            })
        })
        .transpose()?;

    Ok(StoredChunk {
        id: row.id,
        projection_id: row.projection_id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        input_unit_ids,
        targeting_text: row.targeting_text,
        token_count,
        chunker_name: row.chunker_name,
        chunker_version: row.chunker_version,
        chunker_config_hash: row.chunker_config_hash,
    })
}

/// Assemble the `NewProjection` request for a parse's dense envelope: a
/// `DenseVector` projection carrying the dense-builder producer identity and,
/// as input refs, the chunk projections it embeds (spec §20/§22). Chunk ids are
/// the honest inputs — the dense plane is derived from `chunk_projections`, not
/// directly from content units.
fn new_projection(source_id: &str, parse_id: &str, chunks: &[StoredChunk]) -> NewProjection {
    let input_refs = chunks
        .iter()
        .map(|chunk| ProvenanceInputRef {
            object_type: ProvenanceObjectType::RetrievalProjection,
            id: chunk.projection_id.clone(),
            text_range: None,
        })
        .collect::<Vec<_>>();

    let producer = Provenance {
        producer_type: ProducerType::Model,
        producer_name: DENSE_PRODUCER_NAME.to_string(),
        producer_version: Some(DENSE_PRODUCER_VERSION.to_string()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(input_refs),
    };

    NewProjection {
        source_id: source_id.to_string(),
        parse_id: parse_id.to_string(),
        projection_type: ProjectionType::DenseVector,
        input_unit_ids: None,
        input_annotation_ids: None,
        producer,
        index_name: None,
        index_partition: None,
    }
}
