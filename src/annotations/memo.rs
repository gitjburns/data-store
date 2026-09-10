//! CAc: §21.2 memoization cache — content-keyed reuse of producer results,
//! recorded honestly through the Provenance memoization fields (§21.3).
//!
//! Cache identity covers source-unit hashes, exact fragment ranges/text hashes,
//! annotation type, and the producer's ordered stage contracts. One memo entry
//! reuses a complete chain; intermediate requests are not persisted separately.
//!
//! CA2 KEY SPLIT (user-ruled 2026-07-19). Two keys derive from ONE shared
//! `KeyMaterial` (unit hashes × source slices × annotation type):
//!   - the CONTENT KEY (`content_key_hash`) also includes target unit IDs, so
//!     each source location gets its own completion and provenance; producer
//!     identity is excluded, keeping model changes scoped to unfinished work;
//!   - the MEMO KEY (`memoization_key_hash`) hashes that material PLUS the
//!     producer identity hash and stays the CACHE key here — cross-identity
//!     reuse must remain impossible (memoization honesty).
//!
//! Fragment keys intentionally differ from legacy whole-group keys. Existing
//! records remain intact, but cannot stand in for a fragment's precise coverage.
//!
//! One cache ROW caches one producer INVOCATION's full output — an array of
//! produced items — because a single invocation can yield many annotations
//! (e.g. every entity in a section). `original_annotation_id` anchors the row
//! to item 1's minted annotation id; each item additionally carries its own
//! originating annotation id so a reuse can name the exact `memoizedFrom`
//! target per re-minted row (§21.3 honesty).
//!
//! Lifetime note (schema §21.2): unlike `semantic_annotations` rows, a memo
//! row DELIBERATELY survives parse archival and hot cleanup — reuse ACROSS
//! parses is the cache's entire purpose — so it is never removed when a parse
//! is superseded.

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::annotations::producer::{Invocation, ProducerKind};
use crate::config::AnnotatorModelConfig;
use crate::error::ApiError;
use crate::model::SemanticAnnotationType;
use crate::primitives::utc_now;

/// Read one content unit's text hash (falling back to its body hash) by id.
/// The §21.2 key hashes `textHash`-or-`bodyHash` of each target's content, in
/// target order; `text_hash` is nullable in the schema, so the COALESCE
/// mirrors the spec's "textHash-or-bodyHash of target content" fallback.
const SELECT_UNIT_CONTENT_HASH_SQL: &str =
    "SELECT COALESCE(text_hash, body_hash) FROM content_units WHERE id = ?1";

/// Look up one cached invocation output by its memoization key hash. Only the
/// cached item array is read: the row-level confidence and anchor annotation
/// id are inspection conveniences mirroring item 1, and the array is the
/// authoritative per-item record.
const SELECT_MEMO_ENTRY_SQL: &str = "
SELECT body_json
FROM annotation_memo
WHERE memoization_key_hash = ?1";

/// Insert one memo row caching a full producer invocation output. Plain
/// INSERT (not upsert): a duplicate key is impossible under the single-thread
/// worker (see `record`), so a conflict is a genuine invariant breach to
/// surface, never to absorb.
const INSERT_MEMO_ROW_SQL: &str = "
INSERT INTO annotation_memo (
  memoization_key_hash, annotation_type, producer_identity_hash,
  body_json, confidence, original_annotation_id, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// One item within a cached invocation output: a single produced annotation's
/// body and confidence, plus the id of the annotation it was originally
/// minted as (the per-item `memoizedFrom` target when the item is reused).
#[derive(Debug, Clone)]
pub(crate) struct MemoItem {
    pub(crate) body: serde_json::Value,
    pub(crate) confidence: Option<f64>,
    pub(crate) original_annotation_id: String,
}

/// One cached producer invocation output: the full ordered item array a memo
/// hit re-mints as fresh annotations without running the model.
#[derive(Debug, Clone)]
pub(crate) struct MemoEntry {
    pub(crate) items: Vec<MemoItem>,
}

/// The persisted shape of one cached item inside the `body_json` array. Each
/// item keeps its OWN confidence and originating annotation id so a memo hit
/// re-mints every row with faithful per-item confidence and an exact per-item
/// `memoizedFrom` target (§21.3 honesty) — the single row-level
/// confidence/anchor columns cannot carry per-item fidelity for multi-item
/// invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PersistedMemoItem {
    body: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    confidence: Option<f64>,
    original_annotation_id: String,
}

/// Shared coverage identity. Unit hashes alone cannot distinguish two fragments
/// of the same unit, so both satisfaction and memo reuse include the exact slices.
struct KeyMaterial {
    // Location affects completion, not reusable model output for identical input.
    target_unit_ids: Vec<String>,
    target_content_hashes: Vec<String>,
    source_slices: Vec<crate::model::provenance::ProvenanceTextRange>,
    annotation_type: String,
}

/// Read the shared key material for one planned invocation. Reads each target's
/// content hash by unit id (via a named-constant SQL query) in target order,
/// then resolves the annotation type wire name. Consumed by both key derivations.
fn key_material(
    conn: &Connection,
    kind: ProducerKind,
    invocation: &Invocation,
) -> Result<KeyMaterial, ApiError> {
    let mut target_content_hashes = Vec::with_capacity(invocation.targets.len());
    for target in &invocation.targets {
        target_content_hashes.push(unit_content_hash(conn, &target.unit_id)?);
    }
    let annotation_type = annotation_type_wire_name(kind.annotation_type())?;
    Ok(KeyMaterial {
        target_unit_ids: invocation
            .targets
            .iter()
            .map(|target| target.unit_id.clone())
            .collect(),
        target_content_hashes,
        source_slices: invocation
            .targets
            .iter()
            .map(|target| target.text_range())
            .collect(),
        annotation_type,
    })
}

/// Identify completion at this canonical source location, independently of the
/// producer. Equal text in another unit may reuse its memo but still needs its
/// own annotation rows; otherwise graph evidence loses the second location.
pub(crate) fn content_key_hash(
    conn: &Connection,
    kind: ProducerKind,
    invocation: &Invocation,
) -> Result<String, ApiError> {
    let material = key_material(conn, kind, invocation)?;
    content_key_hash_from_material(&material)
}

/// Add producer identity to the shared fragment coverage key. A model, prompt,
/// schema, or generation-contract change cannot reuse another producer's output.
pub(crate) fn memoization_key_hash(
    conn: &Connection,
    kind: ProducerKind,
    config: &AnnotatorModelConfig,
    invocation: &Invocation,
) -> Result<String, ApiError> {
    let material = key_material(conn, kind, invocation)?;
    let producer_identity_hash = kind.identity_hash(config)?;

    // Repeated identical text at different offsets remains separate coverage.
    // Legacy whole-group keys cannot satisfy or reuse a new fragment's output.
    let key_document = json!({
        "targetContentHashes": material.target_content_hashes,
        "annotationType": material.annotation_type,
        "producerIdentityHash": producer_identity_hash,
        "sourceSlices": material.source_slices,
    });
    crate::canonical::canonical_sha256_hex(&key_document)
}

/// Hash the shared content material into the CA2 content key. Separated from
/// `content_key_hash` so a caller already holding `KeyMaterial` (never today,
/// but kept symmetric with the memo path) does not re-read the unit hashes.
fn content_key_hash_from_material(material: &KeyMaterial) -> Result<String, ApiError> {
    let key_document = json!({
        "targetUnitIds": material.target_unit_ids,
        "targetContentHashes": material.target_content_hashes,
        "annotationType": material.annotation_type,
        "sourceSlices": material.source_slices,
    });
    crate::canonical::canonical_sha256_hex(&key_document)
}

/// Read one memo entry by key, returning `None` when the key has never been
/// cached. The stored `body_json` is the canonical JSON ARRAY of the cached
/// items; it re-parses back through serde here, so a corrupt payload fails
/// loudly with the key's identity rather than being misread.
pub(crate) fn lookup(conn: &Connection, key: &str) -> Result<Option<MemoEntry>, ApiError> {
    let row: Option<String> = conn
        .query_row(SELECT_MEMO_ENTRY_SQL, params![key], |row| row.get(0))
        .optional()
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to look up memo entry for key {key}: {source}"),
        })?;

    let Some(body_json) = row else {
        return Ok(None);
    };
    Ok(Some(memo_entry_from_row(key, &body_json)?))
}

/// Cache one producer invocation's full output on the caller's transaction, so
/// the cache write commits atomically with the annotation rows it caches (the
/// truth and its cache can never disagree). `body_json` stores the canonical
/// JSON ARRAY of the items; `original_annotation_id` stores item 1's id as the
/// row-level anchor, matching what a hit re-mints against. A duplicate key
/// surfaces as a `StorageOperation` error: the single worker thread computes
/// the key, checks presence, and records within one cycle, so a caller can
/// never legitimately record a key twice.
pub(crate) fn record(
    tx: &Transaction<'_>,
    key: &str,
    annotation_type: SemanticAnnotationType,
    producer_identity_hash: &str,
    items: &[MemoItem],
) -> Result<(), ApiError> {
    // The cache row caches one INVOCATION's full output: the body column is
    // the ordered array of persisted items, each carrying its own body,
    // confidence, and originating annotation id so re-mints stay per-item
    // faithful.
    let persisted = items
        .iter()
        .map(|item| PersistedMemoItem {
            body: item.body.clone(),
            confidence: item.confidence,
            original_annotation_id: item.original_annotation_id.clone(),
        })
        .collect::<Vec<_>>();
    let body_json =
        canonical_json_string_of(&persisted, &format!("memo item array for key {key}"))?;
    let annotation_type_wire = annotation_type_wire_name(annotation_type)?;

    // The row-level confidence/anchor columns mirror item 1 for inspection
    // only; the persisted array is what lookup re-reads.
    let Some(anchor) = items.first() else {
        return Err(ApiError::StorageOperation {
            message: format!("refusing to record an empty memo entry for key {key}"),
        });
    };
    let confidence = anchor.confidence;
    let now = utc_now()?;

    tx.execute(
        INSERT_MEMO_ROW_SQL,
        params![
            key,
            annotation_type_wire,
            producer_identity_hash,
            body_json,
            confidence,
            anchor.original_annotation_id,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to record memo entry for key {key}: {source}"),
    })?;
    Ok(())
}

/// Read one target unit's content hash (textHash, falling back to bodyHash).
/// A missing unit id is an invariant breach — the invocation plan was built
/// from these very units — so it surfaces loudly rather than defaulting.
fn unit_content_hash(conn: &Connection, unit_id: &str) -> Result<String, ApiError> {
    conn.query_row(SELECT_UNIT_CONTENT_HASH_SQL, params![unit_id], |row| {
        row.get::<_, String>(0)
    })
    .optional()
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read content hash for target unit {unit_id}: {source}"),
    })?
    .ok_or_else(|| ApiError::StorageOperation {
        message: format!(
            "target unit {unit_id} has no content_units row; the invocation plan referenced a \
             unit that no longer exists"
        ),
    })
}

/// Re-type one persisted memo row into a `MemoEntry`. The stored `body_json`
/// is the canonical JSON array of persisted items, each with its own body,
/// confidence, and originating annotation id — per-item fidelity survives the
/// cache round-trip, so re-minted rows never borrow item 1's confidence or
/// `memoizedFrom` target.
fn memo_entry_from_row(key: &str, body_json: &str) -> Result<MemoEntry, ApiError> {
    let persisted: Vec<PersistedMemoItem> =
        serde_json::from_str(body_json).map_err(|source| ApiError::StorageOperation {
            message: format!("persisted memo item array for key {key} is unparseable: {source}"),
        })?;

    let items = persisted
        .into_iter()
        .map(|item| MemoItem {
            body: item.body,
            confidence: item.confidence,
            original_annotation_id: item.original_annotation_id,
        })
        .collect();
    Ok(MemoEntry { items })
}

/// Render one annotation type through its serde wire name so the cached
/// `annotation_type` column and the key document can never drift from the Rust
/// enum (same pattern as the store's `enum_wire_name`).
fn annotation_type_wire_name(annotation_type: SemanticAnnotationType) -> Result<String, ApiError> {
    match serde_json::to_value(annotation_type) {
        Ok(serde_json::Value::String(name)) => Ok(name),
        // Unreachable for a plain renamed enum; kept explicit so a future
        // representation change fails loudly instead of persisting garbage.
        other => Err(ApiError::InternalIo {
            message: format!("annotation type did not serialize to a string: {other:?}"),
        }),
    }
}

/// Render any shape as a canonical JSON string for a `*_json` column
/// (deterministic bytes per spec §16.2, same policy as the annotation store).
fn canonical_json_string_of<T: Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the error
    // arm keeps the panic-free Result policy instead of unwrapping.
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}
