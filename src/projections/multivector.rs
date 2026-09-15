//! C6e multi-vector builder: ColBERT document-token matrices as `multi_vector`
//! projections persisted PER CONTENT UNIT (`unit_multivector_projections`,
//! D4), parse-scoped and rebuildable from canonical state through
//! local length-routed embedding calls or remote batched token embeddings,
//! using the typed fabric matrix codec (spec §22).
//!
//! PER-UNIT rationale (D4, resolved 2026-07-11). ColBERT matrices are keyed to
//! canonical ContentUnits, one row per (parse, unit) — NOT per chunk. The MVP
//! keeps ColBERT as a `multi_vector` projection; the payload key is the unit id, so
//! MaxSim scoring (C7c) and hot cleanup scan the parse's units directly. Chunks
//! (C6b) are a separate projection plane and never the multi-vector key.
//!
//! Derived from canonical state (D-fact 5). Input text comes from the parse's
//! canonical ContentUnits in reading order — never parser output — so a rebuild
//! is a pure function of committed canonical evidence.
//!
//! The caller holds one model-call permit across a local build and passes no
//! permit for HTTP. This builder validates that admission without acquiring a
//! gate itself. Remote requests remain inside the caller's write transaction;
//! projection rows and their envelope still commit or roll back together.

use std::time::Instant;

use crate::sqlite::{Connection, Transaction};
use rusqlite::{OptionalExtension, params};
use tokenizers::Tokenizer;
use tracing::{error, info};

use crate::error::ApiError;
use crate::ids::new_retrieval_projection_id;
use crate::inference::{ColbertBackend, ColbertDocumentEmbedding};
use crate::model::{
    ContentType, ProducerType, Provenance, ProvenanceInputRef, ProvenanceObjectType,
};
use crate::primitives::codec::{
    UnitColbertDocumentVector, decode_colbert_document_vector_blob, encode_colbert_matrix_blob,
};
use crate::primitives::utc_now;
use crate::primitives::validate::{StoredColbertDocumentVector, validate_colbert_document_vector};
use crate::projections::envelope::{self, NewProjection, ProjectionType};
use crate::state::ModelCallPermit;

/// Stable producer name recorded in the projection's Provenance (spec §20).
/// Naming the ColBERT document embedder here — rather than reading the model
/// name off the runtime, which exposes no such accessor — keeps a rebuild's
/// producer identity answerable from the row without coupling to internal
/// runtime fields.
const COLBERT_DOCUMENT_PRODUCER: &str = "colbert_document_embedder";

/// Producer version, bumped when the embedding contract changes in a way that
/// must invalidate prior multi-vector rows on rebuild.
const COLBERT_DOCUMENT_PRODUCER_VERSION: &str = "1";

/// Ordered SELECT of a parse's content units in reading order. The multi-vector
/// builder uses configured batch sizes, except that local units above the
/// configured batching token threshold embed singularly. It
/// persists one row per unit, and reading order is preserved only so build/rebuild
/// logs and any downstream iteration are deterministic; unlike the annotation
/// producers, ColBERT matrices are per-unit and order-independent for storage.
/// `sequence_index` is nullable; NULLs sort last and a tiebreak on `id` keeps
/// the order total across runs. No deleted-row filter: content_units are
/// hard-deleted by hot cleanup (§31.2), never soft-deleted.
const SELECT_PARSE_UNITS_SQL: &str = "
SELECT id, content_type, body_json
FROM content_units
WHERE parse_id = ?1
ORDER BY sequence_index IS NULL, sequence_index, id";

/// Delete every prior multi-vector row for a parse. Rebuild deletes FIRST, then
/// re-inserts (see `build_multivectors`): the `UNIQUE(parse_id, unit_id)` index
/// (schema.sql) turns a stale surviving row into an INSERT conflict, so the
/// delete-first ordering is what makes a rebuild idempotent rather than a
/// constraint failure.
const DELETE_PARSE_MULTIVECTORS_SQL: &str = "
DELETE FROM unit_multivector_projections WHERE parse_id = ?1";

/// Query-time read of one persisted multi-vector row by (parse, unit). Scoped
/// to the single active parse (§14) and keyed on the `UNIQUE(parse_id, unit_id)`
/// index so each probe returns at most one row; the shape columns and the
/// row-major matrix blob are read so the loader can decode without a separate
/// shape record. SQL checks admission before copying a potentially oversized blob
/// into Rust; a NULL result is a resource refusal, not a missing matrix.
const SELECT_UNIT_MULTIVECTOR_SQL: &str = "
SELECT token_count, dimension,
 CASE WHEN token_count <= ?3 AND token_count * dimension <= ?4
 AND length(matrix_blob) <= ?4 * 4 THEN matrix_blob END
FROM unit_multivector_projections
WHERE parse_id = ?1 AND unit_id = ?2";

/// Insert one per-unit ColBERT matrix row. `matrix_blob` is the row-major
/// little-endian f32 encoding of `token_count * dimension` values; token_count
/// and dimension are stored alongside so the decoder can validate the matrix
/// shape without a separate shape record (schema.sql; mirrors
/// `decode_colbert_document_vector_blob`).
const INSERT_MULTIVECTOR_SQL: &str = "
INSERT INTO unit_multivector_projections (
  id, projection_id, source_id, parse_id, unit_id,
  token_count, dimension, matrix_blob, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";

/// Build the per-unit ColBERT multi-vector projection for one parse.
///
/// Opens a `MultiVector` envelope (`building`), reads the parse's ordered
/// ContentUnits, embeds each unit's evidence text through the selected backend,
/// validates each matrix, encodes it, and inserts one `unit_multivector_projections` row
/// per unit; on full success completes the envelope `fresh`. Any failure marks
/// the envelope `failed` and returns the error, so the envelope's freshness
/// always reflects the outcome.
///
/// Local calls require the caller's permit for the whole build and retain the
/// length threshold between singular and batched inference. HTTP requires no
/// permit and batches all units, avoiding one remote request per long unit.
///
/// Rebuild idempotence: prior rows for the parse are deleted BEFORE inserting
/// (see `DELETE_PARSE_MULTIVECTORS_SQL`), so `UNIQUE(parse_id, unit_id)` never
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
    let units = read_parse_units(tx, parse_id)?;
    let embeddable: Vec<UnitText> = units
        .into_iter()
        .filter_map(|unit| {
            evidence_text(&unit).map(|text| UnitText {
                unit_id: unit.id,
                text,
            })
        })
        .filter(|unit| !unit.text.trim().is_empty())
        .collect();
    if let Some(monitor) = monitor {
        monitor.stage(
            "ColBERT content embeddings",
            Some(embeddable.len() as u64),
            "units embedded",
        );
    }

    info!(
        event = "multivector_build.started",
        backend = ?colbert_runtime.backend_kind(),
        source_id,
        parse_id,
        unit_count = embeddable.len(),
        expected_dimension,
        "multi-vector projection build started"
    );

    let projection_id = envelope::insert_building(
        tx,
        &NewProjection {
            source_id: source_id.to_string(),
            parse_id: parse_id.to_string(),
            projection_type: ProjectionType::MultiVector,
            input_unit_ids: Some(embeddable.iter().map(|unit| unit.unit_id.clone()).collect()),
            input_annotation_ids: None,
            producer: colbert_producer_provenance(&embeddable, model_identity),
            index_name: None,
            index_partition: None,
        },
    )?;

    // Any failure past this point marks the envelope failed on the same
    // transaction, so the freshness state never claims success the payload rows
    // did not achieve. `build_rows` performs the delete-first rebuild and the
    // per-unit embed/validate/encode/insert loop.
    match build_rows(
        tx,
        source_id,
        parse_id,
        &projection_id,
        colbert_runtime,
        expected_dimension,
        &embeddable,
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
                unit_count = embeddable.len(),
                expected_dimension,
                total_tokens = totals.total_tokens,
                // Per-path routing counts (hybrid, 2026-07-18) so stage time in
                // this log is attributable to the batched vs singular path when
                // benchmarking; the failed arm omits them (routing may not have
                // completed at that boundary).
                batched_unit_count = totals.batched_units,
                singular_unit_count = totals.singular_units,
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
                unit_count = embeddable.len(),
                expected_dimension,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                error = %source,
                "multi-vector projection build failed"
            );
            Err(source)
        }
    }
}

/// Delete prior rows, then embed/validate/encode/insert one matrix per unit,
/// length-routing local units while batching all HTTP units, then returning totals for the
/// completion log (safe aggregates — never matrix values, which are forbidden
/// in logs). Split out so `build_multivectors` can wrap the whole payload write
/// in a single success/failure envelope transition.
// Explicit transaction, source/parse/envelope targets, backend, validation
// dimension, prepared units, and observer preserve the outer builder's ownership.
#[allow(clippy::too_many_arguments)]
fn build_rows(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    colbert_runtime: &ColbertBackend,
    expected_dimension: usize,
    units: &[UnitText],
    monitor: Option<&crate::monitoring::WorkHandle>,
) -> Result<BuildRowsTotals, ApiError> {
    // Delete-first rebuild: clear the parse's prior rows before inserting so the
    // UNIQUE(parse_id, unit_id) index cannot conflict on a re-run.
    tx.execute(DELETE_PARSE_MULTIVECTORS_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to delete prior multi-vector rows for parse {parse_id}: {source}"
            ),
        })?;

    let now = utc_now()?;
    let mut total_tokens = 0usize;
    let mut completed_units = 0u64;

    // Local batching cost depends on sequence length. The configured routing
    // threshold measures complete raw text, while inference adds its own prompt.
    // Only packing changes; canonical unit ownership and persistence stay identical.
    let mut counter = colbert_runtime.tokenizer().clone();
    counter
        .with_truncation(None)
        .map_err(|source| ApiError::InferenceInit {
            message: format!("disable ColBERT routing truncation for {parse_id}: {source}"),
        })?;
    counter.with_padding(None);
    let tokenizer = &counter;
    let mut token_lengths: Vec<usize> = Vec::with_capacity(units.len());
    for unit in units {
        token_lengths.push(count_document_tokens(tokenizer, &unit.unit_id, &unit.text)?);
    }
    let mut batched_indices: Vec<usize> = Vec::new();
    let mut singular_indices: Vec<usize> = Vec::new();
    // The threshold measures local accelerator padding cost. Remote serving
    // owns that execution cost, so batch every HTTP unit to avoid per-unit RTTs.
    let local_routing = colbert_runtime.uses_local_model_gate();
    for (index, &token_length) in token_lengths.iter().enumerate() {
        if !local_routing || token_length <= colbert_runtime.local_batch_max_tokens() {
            batched_indices.push(index);
        } else {
            singular_indices.push(index);
        }
    }

    // Singular pool: each long unit embeds through the singular path, which
    // self-logs one per-unit model_call.* pair — per-unit unit_id attribution
    // returns for exactly the units this path serves.
    for &index in &singular_indices {
        let unit = &units[index];
        let call = colbert_runtime.monitor_call(monitor, "content unit embedding");
        let result = colbert_runtime.embed_document(&unit.unit_id, &unit.text, call.as_ref());
        if let Some(call) = call {
            call.finish_result(&result);
        }
        let embedding = result?;
        completed_units += 1;
        if let Some(monitor) = monitor {
            monitor.progress(completed_units, Some(units.len() as u64));
        }
        let token_count = persist_unit_matrix(
            tx,
            source_id,
            parse_id,
            projection_id,
            expected_dimension,
            &now,
            embedding,
        )?;
        total_tokens = total_tokens.saturating_add(token_count);
    }

    // Batched pool: sort by the routing token counts before packing, so each
    // configured batch groups similarly-long
    // documents and its padding waste is bounded by its own longest member
    // (the counts are already in hand from routing, retiring the earlier
    // byte-length proxy sort; the embed paths' fixed prompt-prefix delta
    // preserves this ordering exactly). The sort is purely a packing
    // optimization; per-unit validate/encode/INSERT is path-independent and
    // each row still carries its own unit id. Reading-order storage is
    // irrelevant (see SELECT comment), so reordering here is safe.
    batched_indices.sort_by(|&left, &right| {
        token_lengths[right]
            .cmp(&token_lengths[left])
            .then_with(|| left.cmp(&right))
    });

    for window in batched_indices.chunks(colbert_runtime.document_batch_size()) {
        // One batch per window, holding the caller's gate only for local
        // inference. embed_documents self-logs ONE
        // model_call.* pair with batch-level fields (text_count, summed chars,
        // summed true token count) — no per-unit unit_id at this boundary.
        let batch: Vec<(&str, &str)> = window
            .iter()
            .map(|&index| (units[index].unit_id.as_str(), units[index].text.as_str()))
            .collect();
        let call = colbert_runtime.monitor_call(monitor, "content unit embedding");
        let result = colbert_runtime.embed_documents(&batch, call.as_ref());
        if let Some(call) = call {
            call.finish_result(&result);
        }
        let embeddings = result?;
        completed_units += batch.len() as u64;
        if let Some(monitor) = monitor {
            monitor.progress(completed_units, Some(units.len() as u64));
        }
        if embeddings.len() != batch.len() {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "ColBERT batched embedding returned {} matrices for {} units of parse {parse_id}",
                    embeddings.len(),
                    batch.len()
                ),
            });
        }

        // One iteration per returned entry, in input (window) order. The batched
        // matrix is each document's true-length rows, so `persist_unit_matrix`
        // writes the same LE-f32 bytes the singular path would have written.
        for embedding in embeddings {
            let token_count = persist_unit_matrix(
                tx,
                source_id,
                parse_id,
                projection_id,
                expected_dimension,
                &now,
                embedding,
            )?;
            total_tokens = total_tokens.saturating_add(token_count);
        }
    }

    Ok(BuildRowsTotals {
        total_tokens,
        batched_units: batched_indices.len(),
        singular_units: singular_indices.len(),
    })
}

/// Validate, encode, and INSERT one unit's embedded matrix, returning its true
/// token count for the build total. Shared by both routing paths so persistence
/// stays byte-identical regardless of which path produced the matrix: the
/// batched path extracts each document's true-length rows before this point, so
/// both paths hand over the same per-unit shape.
fn persist_unit_matrix(
    tx: &Transaction<'_>,
    source_id: &str,
    parse_id: &str,
    projection_id: &str,
    expected_dimension: usize,
    now: &str,
    embedding: ColbertDocumentEmbedding,
) -> Result<usize, ApiError> {
    super::annotation_io::admitted_value_count(
        embedding.token_count,
        embedding.dimension,
        &tx.limits().resources,
    )?;
    // Validate BEFORE persisting: reject zero-token, wrong-dimension,
    // wrong-value-count, or non-finite matrices so only well-formed matrices
    // reach storage and MaxSim scoring (C7c) can trust the shape columns. Each
    // unit's own token_count is validated here, unaffected by the batch-level
    // summed count in the batched model-call log.
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
    // token_count and dimension go to their own columns so the decoder can
    // re-derive the expected byte length and reject any drift.
    let matrix_blob = encode_colbert_matrix_blob(&stored.vector);
    let row_id = new_retrieval_projection_id()?;

    tx.execute(
        INSERT_MULTIVECTOR_SQL,
        params![
            row_id,
            projection_id,
            source_id,
            parse_id,
            stored.unit_id,
            stored.token_count as i64,
            stored.dimension as i64,
            matrix_blob,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert multi-vector row for unit {} of parse {parse_id}: {source}",
            stored.unit_id
        ),
    })?;

    Ok(stored.token_count)
}

/// Count one unit's ColBERT document tokens over the RAW unit text for hybrid
/// routing, with the same tokenizer call as the C6b chunk builder
/// (`encode(text, true)`, full encoding length). The embed paths tokenize the
/// prompt-PREFIXED text (colbert.rs `format_document`), so the embedded
/// sequence is a fixed few tokens longer than this count — see the routing
/// comment in `build_rows` for the packing contract. Count complete raw text
/// independently of inference truncation. Failures carry the unit id — narrower attribution than the
/// batched embed path's window-local index — and use the same error variant
/// the ColBERT runtime raises for its own tokenization failures.
fn count_document_tokens(
    tokenizer: &Tokenizer,
    unit_id: &str,
    text: &str,
) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoding| encoding.len())
        .map_err(|source| ApiError::InferenceInit {
            message: format!("ColBERT routing tokenization failed for unit {unit_id}: {source}"),
        })
}

/// Safe aggregate totals `build_rows` hands back for the completion log: the
/// summed true token count across the parse plus how many units each routing
/// path embedded, so stage time in the build log is attributable per path when
/// benchmarking the hybrid.
struct BuildRowsTotals {
    total_tokens: usize,
    batched_units: usize,
    singular_units: usize,
}

/// Assemble §20 provenance for the ColBERT document embedder over the units it
/// embeds. `producerType` is Model; the input refs are the ContentUnits whose
/// text is embedded, preserving lineage from the matrices back to their units.
fn colbert_producer_provenance(units: &[UnitText], model_identity: &str) -> Provenance {
    let input_refs = units
        .iter()
        .map(|unit| ProvenanceInputRef {
            object_type: ProvenanceObjectType::ContentUnit,
            id: unit.unit_id.clone(),
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

/// One unit's id and its evidence text, ready to embed.
struct UnitText {
    unit_id: String,
    text: String,
}

/// The subset of `content_units` this builder reads: id, type, and body (for
/// text extraction). Reading-order columns drive only the ORDER BY.
struct UnitRow {
    id: String,
    content_type: String,
    body_json: String,
}

/// One persisted unit re-typed into the fields the builder embeds. `content_type`
/// re-types through the model enum (a value outside the schema CHECK set fails
/// loudly) and `body_json` parses to JSON so `evidence_text` can select its text
/// field. Only the embedding-relevant fields are carried.
struct EmbedUnit {
    id: String,
    content_type: ContentType,
    body: serde_json::Value,
}

/// Read one parse's content units in reading order (bounded by the parse's unit
/// count). Mirrors the annotation producer's ordered read; failures surface as
/// `StorageOperation` with the parse id.
fn read_parse_units(conn: &Connection, parse_id: &str) -> Result<Vec<EmbedUnit>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_PARSE_UNITS_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare parse-units query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok(UnitRow {
                id: row.get(0)?,
                content_type: row.get(1)?,
                body_json: row.get(2)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query units for parse {parse_id}: {source}"),
        })?;

    let mut units = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read unit row for parse {parse_id}: {source}"),
        })?;
        units.push(embed_unit(row)?);
    }
    Ok(units)
}

/// Re-type one persisted unit row into `EmbedUnit`. `content_type` re-types
/// through the model enum and `body_json` parses back to JSON; a corrupt value
/// fails loudly here with the unit's identity rather than being silently mis-read.
fn embed_unit(row: UnitRow) -> Result<EmbedUnit, ApiError> {
    let content_type: ContentType = serde_json::from_value(serde_json::Value::String(
        row.content_type.clone(),
    ))
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "persisted content unit {} type {:?} is not a known variant: {source}",
            row.id, row.content_type
        ),
    })?;
    let body: serde_json::Value =
        serde_json::from_str(&row.body_json).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "persisted body of content unit {} is unparseable: {source}",
                row.id
            ),
        })?;

    Ok(EmbedUnit {
        id: row.id,
        content_type,
        body,
    })
}

/// Extract the evidence-bearing text a unit contributes to ColBERT embedding,
/// returning `None` for the container/structural types that carry no direct
/// text. No normalization or metadata is mixed in beyond selecting the body's
/// text-bearing field.
///
/// One of five synchronized readers of the SPEC-epub §2.1 evidence-bearing
/// types (`text` for text_block, caption, table_cell; `code` for code_block;
/// nothing else, no normalized-text fallback). Keep field selection aligned
/// with `assembly::evidence::evidence_text`,
/// `projections::chunk::extract_targeting_text`,
/// `annotations::producer::evidence_text`, and `projections::view`'s
/// `render_document`. Query passages use the assembly extractor, so these
/// readers must agree on canonical text when a content type changes.
fn evidence_text(unit: &EmbedUnit) -> Option<String> {
    match unit.content_type {
        ContentType::TextBlock | ContentType::Caption | ContentType::TableCell => unit
            .body
            .get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::CodeBlock => unit
            .body
            .get("code")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        ContentType::Document
        | ContentType::Page
        | ContentType::TextSection
        | ContentType::List
        | ContentType::ListItem
        | ContentType::Aside
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure => None,
    }
}

// ---------------------------------------------------------------------------
// Query-time loader (C7c).
//
// The build-time writer above PERSISTS one ColBERT matrix per (parse, unit).
// The loader below is its query-time twin: it DECODES a bounded set of those
// persisted matrices back into `ColbertDocumentEmbedding`s so C7c's MaxSim
// stage can re-score the fused candidate pool. It never re-embeds documents
// (§38: no search-time recomputation of persisted document vectors) — it reads
// what the writer stored. Reads ride the caller's DP1 read transaction/
// connection (parse-scoped, §14 active-parse invariant); the loader opens no
// connection and no transaction of its own.
// ---------------------------------------------------------------------------

/// Load persisted ColBERT matrices for a bounded candidate set, keyed by
/// `unit_id`, and preserving the caller's requested order.
///
/// `unit_multivector_projections` is keyed `UNIQUE(parse_id, unit_id)`, so the
/// read is scoped to the single active `parse_id` (§14) and one row is returned
/// per requested unit that has a stored matrix. `unit_ids` is the fused pool
/// slice already capped at `colbert_candidate_pool_size` by the caller — the
/// loader does NOT read the whole parse, because matrix movement is the cost
/// the post-MVP rescope was designed to bound. A requested unit with no stored
/// matrix is silently skipped (not every fused candidate carries a multi-vector
/// projection); the returned vector is ordered to match `unit_ids` so downstream
/// scoring order is deterministic and independent of SQLite row order.
///
/// `expected_dimension` is the runtime ColBERT projection dimension; the decoder
/// re-derives the expected byte length from the stored `(token_count, dimension)`
/// columns and rejects any drift. A decode failure maps to `StorageOperation`
/// carrying the offending `unit_id` (never a generic message).
pub(crate) fn load_multivectors_for_units(
    conn: &Connection,
    parse_id: &str,
    unit_ids: &[String],
    expected_dimension: usize,
) -> Result<Vec<ColbertDocumentEmbedding>, ApiError> {
    if unit_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Read each requested unit's row on the caller's connection. A single
    // prepared statement reused across the bounded candidate set keeps the read
    // to one parse-scoped index probe per unit against the UNIQUE(parse_id,
    // unit_id) index; the pool is already capped, so this is bounded work.
    let mut statement = conn
        .prepare(SELECT_UNIT_MULTIVECTOR_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare multi-vector loader query for parse {parse_id}: {source}"
            ),
        })?;

    let mut embeddings = Vec::with_capacity(unit_ids.len());
    for unit_id in unit_ids {
        let row = statement
            .query_row(params![parse_id, unit_id, conn.limits().resources.max_embedding_rows, conn.limits().resources.max_embedding_values], |row| {
                Ok(MultivectorRow {
                    token_count: row.get::<_, i64>(0)?,
                    dimension: row.get::<_, i64>(1)?,
                    matrix_blob: row.get::<_, Option<Vec<u8>>>(2)?,
                })
            })
            .optional()
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to read multi-vector row for unit {unit_id} of parse {parse_id}: {source}"
                ),
            })?;

        // A missing row is a fused candidate that simply has no stored ColBERT
        // matrix — skip it rather than fail the whole query. Callers that need
        // to know which candidates were scored compare returned unit ids.
        let Some(row) = row else { continue };

        let token_count = row.token_count.max(0) as usize;
        let row_dimension = row.dimension.max(0) as usize;
        super::annotation_io::admitted_value_count(
            token_count,
            row_dimension,
            &conn.limits().resources,
        )?;
        let matrix_blob = row.matrix_blob.ok_or_else(|| ApiError::StorageOperation {
            message: format!("resource limit: ColBERT matrix for {unit_id} exceeds configured shape or byte budget"),
        })?;
        // Decode + validate the stored blob; the codec re-derives the expected
        // byte length from (token_count, dimension) and rejects drift. Decode
        // errors are Strings — re-wrap with the unit's identity (never generic).
        let vector = decode_colbert_document_vector_blob(
            unit_id,
            &matrix_blob,
            token_count,
            row_dimension,
            expected_dimension,
        )
        .map_err(|message| ApiError::StorageOperation {
            message: format!(
                "failed to decode persisted multi-vector for unit {unit_id} of parse {parse_id}: {message}"
            ),
        })?;

        embeddings.push(ColbertDocumentEmbedding {
            unit_id: unit_id.clone(),
            token_count,
            dimension: expected_dimension,
            vector,
        });
    }

    Ok(embeddings)
}

/// One persisted multi-vector row, as read by the query-time loader: the shape
/// columns plus the row-major matrix blob. Shape lives in its own columns so the
/// decoder can re-derive the expected byte length without a separate record
/// (mirrors the writer's INSERT contract).
struct MultivectorRow {
    token_count: i64,
    dimension: i64,
    matrix_blob: Option<Vec<u8>>,
}
