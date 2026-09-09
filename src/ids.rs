//! Canonical ID assignment (spec §16.4): typed-prefix time-ordered IDs for
//! core records and parse-scoped deterministic IDs for ContentUnits and
//! UnitRelationships. IDs are assigned by the core system only.
//!
//! Two ID families live here:
//!
//! - Typed-prefix generated IDs (`src_`, `loc_`, `parse_`, `acq_`, `ann_`,
//!   `proj_`, `qer_`, `snap_`, `evt_`, `syncq_`, `op_`):
//!   prefix + zero-padded epoch-millisecond timestamp + random hex suffix.
//!   These IDs are unique, immutable, and opaque — consumers must never parse
//!   them or derive meaning from their internal structure. The only externally
//!   guaranteed property is that lexicographic order within one prefix matches
//!   assignment-time order.
//! - Parse-scoped deterministic IDs for ContentUnit and UnitRelationship:
//!   `<parseId>:<discriminator>:<zero-padded sequence>`. These are a pure
//!   function of parser output plus the assignment rule, so canonical bundles
//!   are bit-reproducible and unitId-ascending ordering is stable.
//!
//! Rebuilds re-import IDs from stored artifacts; IDs are never re-derived.
//! Request diagnostic IDs are separate process-local handles, not canonical records.

use crate::error::ApiError;
use crate::primitives::current_time_ms;

/// Digits in the zero-padded epoch-millisecond component of generated IDs.
/// 14 digits hold every timestamp until year 5138, so lexicographic order of
/// the time component equals chronological order for the system's lifetime.
const TIME_COMPONENT_DIGITS: usize = 14;

/// Random bytes appended (as lowercase hex) after the time component to make
/// generated IDs unique even when many are minted in the same millisecond.
const RANDOM_SUFFIX_BYTES: usize = 10;

/// Digits in the zero-padded sequence component of parse-scoped deterministic
/// IDs. This width is part of the persisted canonical-bundle contract: it must
/// never change, or previously persisted bundles would no longer be
/// bit-reproducible. Six digits keep lexicographic order equal to sequence
/// order for up to 1,000,000 units per parse; beyond that the formatting
/// widens naturally (IDs stay unique and deterministic, but purely
/// lexicographic comparison across the boundary would misorder).
const SEQUENCE_DIGITS: usize = 6;

/// Mint a new SourceObject ID (`src_` prefix, time-ordered, opaque).
pub(crate) fn new_source_object_id() -> Result<String, ApiError> {
    new_prefixed_id("src_")
}

/// Mint a new ParseRun ID (`parse_` prefix, time-ordered, opaque).
pub(crate) fn new_parse_run_id() -> Result<String, ApiError> {
    new_prefixed_id("parse_")
}

/// Mint a new AcquisitionRecord ID (`acq_` prefix, time-ordered, opaque).
pub(crate) fn new_acquisition_record_id() -> Result<String, ApiError> {
    new_prefixed_id("acq_")
}

/// Mint a new QueryExecutionRecord ID (`qer_` prefix, time-ordered, opaque).
/// The `qer_` mint is currently reused as the per-query correlation handle in
/// the `/query` pipeline (`http.rs`); it becomes the QueryExecutionRecord row
/// id when the deferred QER audit tier lands.
pub(crate) fn new_query_execution_record_id() -> Result<String, ApiError> {
    new_prefixed_id("qer_")
}

/// Mint a new ForensicSnapshot ID (`snap_` prefix, time-ordered, opaque).
pub(crate) fn new_forensic_snapshot_id() -> Result<String, ApiError> {
    new_prefixed_id("snap_")
}

/// Mint a new SystemEvent ID (`evt_` prefix, time-ordered, opaque).
pub(crate) fn new_system_event_id() -> Result<String, ApiError> {
    new_prefixed_id("evt_")
}

/// Mint a new SourceLocation ID (`loc_` prefix, time-ordered, opaque).
pub(crate) fn new_source_location_id() -> Result<String, ApiError> {
    new_prefixed_id("loc_")
}

/// Mint a new SemanticAnnotation ID (`ann_` prefix, time-ordered, opaque).
/// Annotations are rebuilt within their parse lifecycle rather than being
/// bit-reproducible bundle records, so they take generated IDs, not the
/// parse-scoped deterministic scheme reserved for units and relationships.
pub(crate) fn new_annotation_id() -> Result<String, ApiError> {
    new_prefixed_id("ann_")
}

/// Mint a new RetrievalProjection ID (`proj_` prefix, time-ordered, opaque).
/// Like annotations, projections are rebuilt within their parse lifecycle
/// rather than being bit-reproducible bundle records, so they take generated
/// IDs, not the parse-scoped deterministic scheme reserved for units and
/// relationships.
pub(crate) fn new_retrieval_projection_id() -> Result<String, ApiError> {
    new_prefixed_id("proj_")
}

/// Mint a new sync-queue entry ID (`syncq_` prefix, time-ordered, opaque).
/// Queue entries are operational rows, not spec §16.4 records, but they share
/// the generated-ID format so every core-assigned identifier reads alike.
pub(crate) fn new_sync_queue_entry_id() -> Result<String, ApiError> {
    new_prefixed_id("syncq_")
}

/// Mint a new Operation ID (`op_` prefix, time-ordered, opaque). The §34.6
/// Operation record's handle: returned to the caller of an async admin route
/// and polled through `GET /operations/{operationId}`. Minted by the
/// operations store when it inserts a pending row (`operations::insert_pending`).
pub(crate) fn new_operation_id() -> Result<String, ApiError> {
    new_prefixed_id("op_")
}

/// Correlate HTTP diagnostics within the process without introducing a durable
/// record or a new request failure when clock/entropy services are unavailable.
pub(crate) fn new_request_id() -> String {
    crate::util::diagnostic_id("req")
}

/// Derive the deterministic ContentUnit ID for a unit at `sequence_index`
/// within the parse identified by `parse_id`: `<parseId>:unit:<seq>` with the
/// sequence zero-padded to [`SEQUENCE_DIGITS`]. Same inputs always yield the
/// same ID, so canonical bundles are bit-reproducible from parser output.
pub(crate) fn content_unit_id(parse_id: &str, sequence_index: u64) -> String {
    format!(
        "{parse_id}:unit:{sequence_index:0width$}",
        width = SEQUENCE_DIGITS
    )
}

/// Derive the deterministic UnitRelationship ID for a relationship at
/// `sequence_index` within the parse identified by `parse_id`:
/// `<parseId>:rel:<seq>` with the sequence zero-padded to
/// [`SEQUENCE_DIGITS`]. Same inputs always yield the same ID.
pub(crate) fn unit_relationship_id(parse_id: &str, sequence_index: u64) -> String {
    format!(
        "{parse_id}:rel:{sequence_index:0width$}",
        width = SEQUENCE_DIGITS
    )
}

/// Build one generated ID: prefix + fixed-width epoch-millisecond timestamp +
/// random hex suffix. Clock failure and OS-randomness failure both surface as
/// explicit errors; ID minting never panics and never falls back to a weaker
/// uniqueness source.
fn new_prefixed_id(prefix: &str) -> Result<String, ApiError> {
    let minted_at_ms = current_time_ms()?;
    let suffix = random_suffix_hex()?;

    Ok(format!(
        "{prefix}{minted_at_ms:0width$}{suffix}",
        width = TIME_COMPONENT_DIGITS
    ))
}

/// Produce [`RANDOM_SUFFIX_BYTES`] bytes of OS randomness as lowercase hex.
/// A randomness failure is an explicit error so callers keep normal error
/// flow instead of panicking inside ID assignment.
fn random_suffix_hex() -> Result<String, ApiError> {
    let mut suffix_bytes = [0u8; RANDOM_SUFFIX_BYTES];
    getrandom::fill(&mut suffix_bytes).map_err(|source| ApiError::InternalIo {
        message: format!("OS randomness unavailable for ID generation: {source}"),
    })?;

    let mut encoded = String::with_capacity(RANDOM_SUFFIX_BYTES * 2);
    for value in suffix_bytes {
        encoded.push_str(&format!("{value:02x}"));
    }

    Ok(encoded)
}
