//! CAa: hot-plane persistence and freshness lifecycle for SemanticAnnotation
//! rows (spec §21), with `annotation.*` events appended atomically on the
//! caller's connection.
//!
//! Atomicity invariant: every mutating function takes the CALLER's
//! `&rusqlite::Transaction` and appends its event on that same transaction
//! (via `crate::events::append_event`), so the row change and the audit event
//! commit or roll back together — the event trail can never claim a lifecycle
//! transition that did not durably happen. Read functions take a
//! `&rusqlite::Connection`; one bounded SELECT needs no transaction.
//!
//! Freshness lifecycle (spec §21): an annotation is inserted `building` with
//! no body, then transitions `building → fresh` on success, `building →
//! failed` on producer failure, or (once fresh) `fresh → stale` when its
//! inputs change. Every UPDATE is status-guarded and asserts it hit exactly
//! one row, so a transition whose precondition vanished fails loudly rather
//! than silently no-opping.

use rusqlite::{Connection, Transaction, params};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::ids::new_annotation_id;
use crate::model::{
    AnnotationFreshnessStatus, Provenance, SemanticAnnotation, SemanticAnnotationType,
    SystemEventType,
};
use crate::primitives::utc_now;
use crate::util::truncate_persisted_detail;

/// Insert one annotation in the `building` state: body_json starts NULL (no
/// body until the producer completes), and freshness_status is the literal
/// 'building', matching the schema CHECK set. The remaining *_json columns are
/// canonical JSON of their model shapes.
const INSERT_BUILDING_SQL: &str = "
INSERT INTO semantic_annotations (
  id, source_id, parse_id, target_unit_ids_json, annotation_type,
  body_json, provenance_json, confidence, freshness_status,
  memoization_key_hash, content_key_hash, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, NULL, 'building', ?7, ?8, ?9)";

/// Status-guarded `building → fresh` transition: writes the completed body,
/// confidence, and final provenance, and only matches a row still `building`.
/// The guard is what makes double-completion or completion of an
/// already-failed row a loud zero-row failure instead of a silent overwrite.
///
/// CA2 memo-key RE-STAMP (user-ruled 2026-07-19). This UPDATE also re-stamps
/// `memoization_key_hash` (?5) to the key of the producer that ACTUALLY ran.
/// Under content-scoped satisfaction a reopened `failed`/orphaned row may have
/// been minted under a DIFFERENT producer identity than the one now completing
/// it (a model switch reopens the prior model's failed rows for the current
/// model to retry); the memo key must therefore key the memo CACHE row on the
/// completing identity, not the stale minting identity. `content_key_hash` is
/// left untouched: by construction the target content is unchanged, so the
/// content key is identity-invariant. Completion is the earliest point the
/// running identity is known (the reopen transition cannot know which producer
/// will run), so the re-stamp lives here, not at `retry_failed`.
const COMPLETE_FRESH_SQL: &str = "
UPDATE semantic_annotations
SET body_json = ?2, confidence = ?3, provenance_json = ?4,
    memoization_key_hash = ?5,
    freshness_status = 'fresh'
WHERE id = ?1 AND freshness_status = 'building'";

/// Status-guarded `building → failed` transition. No body or error column is
/// written: a failed annotation has no body, and the failure detail lives in
/// the annotation.failed event and the service log, not on the row.
const MARK_FAILED_SQL: &str = "
UPDATE semantic_annotations
SET freshness_status = 'failed'
WHERE id = ?1 AND freshness_status = 'building'";

/// Status-guarded `fresh → stale` transition. Only a currently-fresh
/// annotation can go stale; building/failed rows are not eligible, so the
/// guard rejects them.
const MARK_STALE_SQL: &str = "
UPDATE semantic_annotations
SET freshness_status = 'stale'
WHERE id = ?1 AND freshness_status = 'fresh'";

/// Insert one annotation directly in the `fresh` state: body, confidence, and
/// final provenance are all known up front (a memo hit re-mints a cached
/// item, or a multi-item invocation completes items 2..N alongside item 1).
/// freshness_status is the literal 'fresh'; the remaining *_json columns are
/// canonical JSON of their model shapes.
const INSERT_FRESH_SQL: &str = "
INSERT INTO semantic_annotations (
  id, source_id, parse_id, target_unit_ids_json, annotation_type,
  body_json, provenance_json, confidence, freshness_status,
  memoization_key_hash, content_key_hash, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'fresh', ?9, ?10, ?11)";

/// Status-guarded `failed → building` transition reopening one failed row for
/// a fresh build attempt. The guard makes reopening a non-failed row a loud
/// zero-row failure; it clears no body because a failed row never had one.
const RETRY_FAILED_SQL: &str = "
UPDATE semantic_annotations
SET freshness_status = 'building'
WHERE id = ?1 AND freshness_status = 'failed'";

/// Read every fresh, non-deleted annotation whose parse is the source's
/// CURRENT active parse. The subselect on source_objects.active_parse_id is
/// what enforces spec §21 rule 1 at the query — an annotation is unreadable
/// unless its parse is active — so a superseded parse's annotations simply do
/// not appear even though their rows still exist pending hot cleanup.
const SELECT_FRESH_FOR_ACTIVE_PARSE_SQL: &str = "
SELECT
  id, source_id, parse_id, target_unit_ids_json, annotation_type,
  body_json, provenance_json, confidence, freshness_status,
  created_at, deleted_at
FROM semantic_annotations
WHERE source_id = ?1
  AND parse_id = (SELECT active_parse_id FROM source_objects WHERE id = ?1)
  AND freshness_status = 'fresh'
  AND deleted_at IS NULL";

/// Only fresh records satisfy coverage; stale, failed, and building rows do not.
const SELECT_CONTENT_KEYS_FOR_PARSE_SQL: &str = "
SELECT content_key_hash FROM semantic_annotations
WHERE parse_id = ?1 AND deleted_at IS NULL AND freshness_status = 'fresh'";

/// Read every reopenable row of a parse: `failed` and `building` rows whose
/// CONTENT key has NO `fresh` sibling. A content key with a fresh row is
/// satisfied and never reopened (the NOT-IN subselect). `building` rows
/// qualify because the single worker thread completes every build inside the
/// cycle that opened it — a building row still visible at DISCOVERY time is
/// by construction a crash orphan (the process died between the build_open
/// and build_complete transactions), not in-flight work. Ordered so callers
/// can pick the lexicographically first row per key deterministically.
///
/// CA2 (user-ruled 2026-07-19): reopen matching is content-scoped, so a
/// content key satisfied by ANY producer identity's fresh row is not reopened,
/// and a failed row minted under a prior identity is reopened for the CURRENT
/// identity to retry (its memo key is re-stamped at completion).
const SELECT_REOPENABLE_ROWS_FOR_PARSE_SQL: &str = "
SELECT content_key_hash, id, freshness_status
FROM semantic_annotations
WHERE parse_id = ?1 AND deleted_at IS NULL
  AND freshness_status IN ('failed', 'building')
  AND content_key_hash NOT IN (
    SELECT content_key_hash FROM semantic_annotations
    WHERE parse_id = ?1 AND deleted_at IS NULL
      AND freshness_status = 'fresh'
  )
ORDER BY content_key_hash, id";

/// SystemEvent object_type for semantic_annotations rows.
const OBJECT_TYPE_SEMANTIC_ANNOTATION: &str = "semantic_annotation";

/// The inputs needed to open one annotation build: everything known before
/// the producer runs. The planned producer identity is carried as a full
/// Provenance because it is known up front (spec §21 rule 3).
/// `memoization_key_hash` is the §21.2 identity-scoped memo key (keys the memo
/// CACHE lookup/row); `content_key_hash` is the CA2 content-scoped key
/// (annotation type × ordered target content hashes, WITHOUT producer identity)
/// that keys SATISFACTION and reopenable classification. Both are denormalized
/// into their own indexed columns. (CA2 ruling, user-approved 2026-07-19.)
#[derive(Debug, Clone)]
pub(crate) struct NewAnnotation {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) target_unit_ids: Vec<String>,
    pub(crate) annotation_type: SemanticAnnotationType,
    pub(crate) provenance: Provenance,
    pub(crate) memoization_key_hash: String,
    pub(crate) content_key_hash: String,
}

/// Insert a `building` annotation and append its `annotation.requested` event
/// on the caller's transaction, returning the freshly minted `ann_` id. The
/// row carries the planned producer's provenance and no body yet; the body
/// arrives at `complete_fresh`. Row write and event are one atomic unit (see
/// the module atomicity invariant).
pub(crate) fn insert_building(
    tx: &Transaction<'_>,
    request: &NewAnnotation,
) -> Result<String, ApiError> {
    let id = new_annotation_id()?;
    let annotation_type = enum_wire_name(&request.annotation_type, "annotation type")?;
    let target_unit_ids_json = canonical_json_string_of(
        &request.target_unit_ids,
        &format!("target unit ids for annotation {id}"),
    )?;
    let provenance_json = canonical_json_string_of(
        &request.provenance,
        &format!("provenance for annotation {id}"),
    )?;
    let now = utc_now()?;

    tx.execute(
        INSERT_BUILDING_SQL,
        params![
            id,
            request.source_id,
            request.parse_id,
            target_unit_ids_json,
            annotation_type,
            provenance_json,
            request.memoization_key_hash,
            request.content_key_hash,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert building annotation {id} for parse {}: {source}",
            request.parse_id
        ),
    })?;

    // Payload names the request identity so the audit trail records what was
    // asked for even before any body exists.
    let payload = Map::from_iter([
        entry("sourceId", &request.source_id),
        entry("parseId", &request.parse_id),
        entry("annotationType", &annotation_type),
    ]);
    let event = new_system_event(
        SystemEventType::AnnotationRequested,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        &id,
        Some(payload),
    )?;
    append_event(tx, &event)?;

    Ok(id)
}

/// Insert one annotation directly in the `fresh` state and append BOTH
/// `annotation.requested` and `annotation.completed` on the caller's
/// transaction, returning the freshly minted `ann_` id. Used when body,
/// confidence, and final provenance are all known up front — a memo hit
/// re-minting a cached item, or the extra items (2..N) of a multi-item
/// invocation whose first item completes a `building` row. Emitting both
/// events is honest: this annotation truly was requested AND completed within
/// this one transaction, so the audit trail records both facts rather than
/// inventing a phantom `building` phase that never durably existed. Row writes
/// and events are one atomic unit (see the module atomicity invariant).
pub(crate) fn insert_fresh(
    tx: &Transaction<'_>,
    request: &NewAnnotation,
    body: &Value,
    confidence: Option<f64>,
    final_provenance: &Provenance,
) -> Result<String, ApiError> {
    let id = new_annotation_id()?;
    let annotation_type = enum_wire_name(&request.annotation_type, "annotation type")?;
    let target_unit_ids_json = canonical_json_string_of(
        &request.target_unit_ids,
        &format!("target unit ids for annotation {id}"),
    )?;
    let body_json = canonical_json_string_of(body, &format!("body for annotation {id}"))?;
    let provenance_json = canonical_json_string_of(
        final_provenance,
        &format!("final provenance for annotation {id}"),
    )?;
    let now = utc_now()?;

    tx.execute(
        INSERT_FRESH_SQL,
        params![
            id,
            request.source_id,
            request.parse_id,
            target_unit_ids_json,
            annotation_type,
            body_json,
            provenance_json,
            confidence,
            request.memoization_key_hash,
            request.content_key_hash,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert fresh annotation {id} for parse {}: {source}",
            request.parse_id
        ),
    })?;

    // Both lifecycle facts are true for a directly-fresh row: it was requested
    // and completed in the same transaction, so both events are appended.
    let requested_payload = Map::from_iter([
        entry("sourceId", &request.source_id),
        entry("parseId", &request.parse_id),
        entry("annotationType", &annotation_type),
    ]);
    let requested = new_system_event(
        SystemEventType::AnnotationRequested,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        &id,
        Some(requested_payload),
    )?;
    append_event(tx, &requested)?;

    let completed_payload = Map::from_iter([entry("annotationId", &id)]);
    let completed = new_system_event(
        SystemEventType::AnnotationCompleted,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        &id,
        Some(completed_payload),
    )?;
    append_event(tx, &completed)?;

    Ok(id)
}

/// Transition `building → fresh`: write the completed body, confidence, and
/// final provenance (which may now record memoized reuse, §21.2), then append
/// `annotation.completed`. The UPDATE is status-guarded and asserted to hit
/// exactly one row, so completing a missing or non-building row is a loud
/// failure, never a silent overwrite. Row write and event are one atomic unit.
///
/// `memoization_key_hash` is the key of the producer that ACTUALLY completed
/// this row and is RE-STAMPED onto the row (CA2, user-ruled 2026-07-19): a
/// content-scoped reopen may have adopted a row minted under a different
/// producer identity, so the memo CACHE key must key on the completing
/// identity. `content_key_hash` is unchanged by construction (same content).
pub(crate) fn complete_fresh(
    tx: &Transaction<'_>,
    annotation_id: &str,
    body: &Value,
    confidence: Option<f64>,
    final_provenance: &Provenance,
    memoization_key_hash: &str,
) -> Result<(), ApiError> {
    let body_json =
        canonical_json_string_of(body, &format!("body for annotation {annotation_id}"))?;
    let provenance_json = canonical_json_string_of(
        final_provenance,
        &format!("final provenance for annotation {annotation_id}"),
    )?;

    let updated = tx
        .execute(
            COMPLETE_FRESH_SQL,
            params![
                annotation_id,
                body_json,
                confidence,
                provenance_json,
                memoization_key_hash
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to complete annotation {annotation_id}: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("annotation {annotation_id} building → fresh"),
    )?;

    let payload = Map::from_iter([entry("annotationId", annotation_id)]);
    let event = new_system_event(
        SystemEventType::AnnotationCompleted,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        annotation_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `building → failed` and append `annotation.failed` carrying the
/// bounded failure detail. The semantic_annotations row itself has no error
/// column by design: the event payload and the service log own the failure
/// detail, so the durable record survives independently of the row (which hot
/// cleanup may later remove). Row write and event are one atomic unit.
pub(crate) fn mark_failed(
    tx: &Transaction<'_>,
    annotation_id: &str,
    bounded_detail: &str,
) -> Result<(), ApiError> {
    let detail = truncate_persisted_detail(bounded_detail);

    let updated = tx
        .execute(MARK_FAILED_SQL, params![annotation_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark annotation {annotation_id} failed: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("annotation {annotation_id} building → failed"),
    )?;

    let mut payload = Map::from_iter([entry("annotationId", annotation_id)]);
    payload.insert("detail".to_owned(), Value::String(detail));
    let event = new_system_event(
        SystemEventType::AnnotationFailed,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        annotation_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `failed → building`, reopening one failed annotation for a fresh
/// build attempt, and re-emit `annotation.requested` (the reopened row is
/// being requested again). The UPDATE is status-guarded and asserted to hit
/// exactly one row, so reopening a non-failed row is a loud failure.
///
/// Retry rationale (§35, §13.5): LLM/network failures are NON-deterministic,
/// so the worker's cycle-level retry of a failed annotation does not violate
/// the §13.5 no-blind-retry rule, which governs DETERMINISTIC failures on
/// identical input. The worker owns independent per-run execution and malformed-
/// output allowances and their timers. Reopening spends neither budget; the
/// caller accounts for the observed producer result. Every reopen leaves this
/// event and a log line.
pub(crate) fn retry_failed(tx: &Transaction<'_>, annotation_id: &str) -> Result<(), ApiError> {
    let updated = tx
        .execute(RETRY_FAILED_SQL, params![annotation_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to reopen annotation {annotation_id} for retry: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("annotation {annotation_id} failed → building"),
    )?;

    // The reopened row is being requested again; the audit trail records the
    // new request so a later completion/failure is attributable to this retry.
    let payload = Map::from_iter([entry("annotationId", annotation_id)]);
    let event = new_system_event(
        SystemEventType::AnnotationRequested,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        annotation_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `fresh → stale` and append `annotation.stale`, making a
/// post-activation staling visible truth rather than silent absence (spec §21
/// rule 3). Row write and event are one atomic unit.
// Post-MVP consumer: no active caller marks a row stale yet, so the allow names
// that future consumer. CA2 (user-ruled 2026-07-19) changed the producer-
// identity-change story that this comment previously described: satisfaction is
// now content-scoped, so a changed producer identity does NOT rebuild
// already-satisfied content — an unchanged content key stays fresh under its
// original identity and no new row is minted (the frontier alone is annotated).
// Only CHANGED content (a new content key) mints a new row; the superseded
// parse's rows still become unreachable and are removed by C9-era hot cleanup.
#[allow(dead_code)]
pub(crate) fn mark_stale(tx: &Transaction<'_>, annotation_id: &str) -> Result<(), ApiError> {
    let updated = tx
        .execute(MARK_STALE_SQL, params![annotation_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark annotation {annotation_id} stale: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("annotation {annotation_id} fresh → stale"),
    )?;

    let payload = Map::from_iter([entry("annotationId", annotation_id)]);
    let event = new_system_event(
        SystemEventType::AnnotationStale,
        OBJECT_TYPE_SEMANTIC_ANNOTATION,
        annotation_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Read every fresh, non-deleted annotation of a source's CURRENT active
/// parse (spec §21 rule 1: annotations are unreadable unless their parse is
/// active — enforced in the query's active_parse_id subselect, not by the
/// caller). Rows of a superseded parse never surface even while their rows
/// linger pending hot cleanup.
// Consumed by C6 projection builders.
#[allow(dead_code)]
pub(crate) fn fresh_for_active_parse(
    conn: &Connection,
    source_id: &str,
) -> Result<Vec<SemanticAnnotation>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_FRESH_FOR_ACTIVE_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare fresh-annotation query for source {source_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![source_id], |row| {
            Ok(AnnotationRow {
                id: row.get(0)?,
                source_id: row.get(1)?,
                parse_id: row.get(2)?,
                target_unit_ids_json: row.get(3)?,
                annotation_type: row.get(4)?,
                body_json: row.get(5)?,
                provenance_json: row.get(6)?,
                confidence: row.get(7)?,
                freshness_status: row.get(8)?,
                created_at: row.get(9)?,
                deleted_at: row.get(10)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query fresh annotations for source {source_id}: {source}"),
        })?;

    let mut annotations = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read annotation row for source {source_id}: {source}"),
        })?;
        annotations.push(annotation_from_row(row)?);
    }
    Ok(annotations)
}

/// Read completed coverage for discovery and projection admission. Completion
/// depends on the current excerpt plan, not on unrelated legacy failed rows.
pub(crate) fn fresh_content_key_hashes_for_parse(
    conn: &Connection,
    parse_id: &str,
) -> Result<std::collections::HashSet<String>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_CONTENT_KEYS_FOR_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare content-key query for parse {parse_id}: {source}"),
        })?;
    let rows = statement
        .query_map(params![parse_id], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query content keys for parse {parse_id}: {source}"),
        })?;

    let mut hashes = std::collections::HashSet::new();
    for row in rows {
        let hash = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read content-key row for parse {parse_id}: {source}"),
        })?;
        hashes.insert(hash);
    }
    Ok(hashes)
}

/// How a reopenable row re-enters the build path: a `failed` row is flipped
/// back to building via `retry_failed`; a crash-orphaned `building` row is
/// adopted as-is (it already carries the in-flight status the build path
/// needs — flipping it would fabricate a transition that never happened).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReopenableStatus {
    Failed,
    OrphanedBuilding,
}

/// One reopenable row chosen for a key: the row the discovery worker reuses
/// as the build's visible `building` row instead of inserting a new one.
#[derive(Debug, Clone)]
pub(crate) struct ReopenableRow {
    pub(crate) annotation_id: String,
    pub(crate) status: ReopenableStatus,
}

/// Read one reopenable row per unsatisfied key of a parse, as a map from
/// CONTENT key hash to the chosen row. A content key appears here only when it
/// has NO fresh row: its rows are `failed` (a prior producer call failed) or
/// crash-orphaned `building` (the process died mid-build — see the SQL
/// comment for why a discovery-time building row can never be live work).
/// The first row per key in (key, id) order is chosen, so the pick is
/// deterministic across runs; discovery reopens exactly one row per key. CA2
/// (user-ruled 2026-07-19): keying reopen on the content key means a row minted
/// under a prior producer identity is reopened for the current identity, and a
/// content key already fresh under ANY identity is left satisfied.
pub(crate) fn reopenable_rows_for_parse(
    conn: &Connection,
    parse_id: &str,
) -> Result<std::collections::HashMap<String, ReopenableRow>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_REOPENABLE_ROWS_FOR_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare reopenable-rows query for parse {parse_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![parse_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query reopenable rows for parse {parse_id}: {source}"),
        })?;

    let mut reopenable = std::collections::HashMap::new();
    for row in rows {
        let (key, id, status) = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read reopenable row for parse {parse_id}: {source}"),
        })?;
        let status = match status.as_str() {
            "failed" => ReopenableStatus::Failed,
            "building" => ReopenableStatus::OrphanedBuilding,
            // Unreachable: the SQL selects exactly these two statuses; kept
            // explicit so a query edit cannot silently misclassify rows.
            other => {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "reopenable row {id} for parse {parse_id} has unexpected status {other}"
                    ),
                });
            }
        };
        // First row per key wins under the (key, id) ORDER BY.
        reopenable.entry(key).or_insert(ReopenableRow {
            annotation_id: id,
            status,
        });
    }
    Ok(reopenable)
}

/// One semantic_annotations row as read from SQLite, before its *_json and
/// wire-string columns are re-typed back into the model shape.
struct AnnotationRow {
    id: String,
    source_id: String,
    parse_id: String,
    target_unit_ids_json: String,
    annotation_type: String,
    // Only ever Some for the fresh rows the readers select, but modeled as
    // Option to match the nullable body_json column honestly.
    body_json: Option<String>,
    provenance_json: String,
    confidence: Option<f64>,
    freshness_status: String,
    created_at: String,
    deleted_at: Option<String>,
}

/// Re-type one persisted row into a `SemanticAnnotation`. The *_json columns
/// parse back through serde and the annotation_type/freshness_status wire
/// strings re-type through their model enums, so a value outside the schema
/// CHECK set (or a corrupt payload) fails loudly here rather than being
/// misread. This reader only runs over fresh rows, whose body_json is never
/// NULL by the §21 envelope; a NULL body on a fresh row is a corruption and
/// is surfaced as an error rather than defaulted.
fn annotation_from_row(row: AnnotationRow) -> Result<SemanticAnnotation, ApiError> {
    let annotation_type: SemanticAnnotationType =
        wire_value(&row.annotation_type, &format!("annotation {} type", row.id))?;
    let freshness_status: AnnotationFreshnessStatus = wire_value(
        &row.freshness_status,
        &format!("annotation {} freshness", row.id),
    )?;
    let target_unit_ids: Vec<String> = parse_json_column(
        &row.target_unit_ids_json,
        &format!("target unit ids of annotation {}", row.id),
    )?;
    let provenance: Provenance = parse_json_column(
        &row.provenance_json,
        &format!("provenance of annotation {}", row.id),
    )?;
    let Some(body_json) = row.body_json else {
        return Err(ApiError::StorageOperation {
            message: format!(
                "fresh annotation {} has a NULL body_json; the §21 envelope requires \
                 a body once fresh",
                row.id
            ),
        });
    };
    let body: Value = parse_json_column(&body_json, &format!("body of annotation {}", row.id))?;

    Ok(SemanticAnnotation {
        id: row.id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        target_unit_ids,
        annotation_type,
        body,
        provenance,
        confidence: row.confidence,
        freshness_status,
        created_at: row.created_at,
        deleted_at: row.deleted_at,
    })
}

/// Render one closed model enum through its serde wire name so persisted
/// column values can never drift from the Rust enum or the schema CHECK set
/// (same pattern as `crate::acquisition::enum_wire_name`).
fn enum_wire_name<T: Serialize>(value: &T, what: &'static str) -> Result<String, ApiError> {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => Ok(name),
        // Unreachable for plain renamed enums; kept explicit so a future
        // representation change fails loudly instead of persisting garbage.
        other => Err(ApiError::InternalIo {
            message: format!("{what} did not serialize to a string: {other:?}"),
        }),
    }
}

/// Re-type one persisted wire string through its model enum. The schema CHECK
/// constraints make an unknown value unreachable through this code path, but
/// re-typing keeps the read half symmetric with `enum_wire_name` so a corrupt
/// value fails loudly instead of being string-compared into a wrong branch.
fn wire_value<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> Result<T, ApiError> {
    serde_json::from_value(Value::String(text.to_owned())).map_err(|source| {
        ApiError::StorageOperation {
            message: format!("persisted {what} value {text:?} is not a known variant: {source}"),
        }
    })
}

/// Parse one persisted *_json column back into its model shape. A stored
/// value that no longer matches the model is a corruption surfaced with the
/// column's identity, never silently dropped.
fn parse_json_column<T: serde::de::DeserializeOwned>(
    json: &str,
    what: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
        message: format!("persisted {what} is unparseable: {source}"),
    })
}

/// Render any model shape as a canonical JSON string for a *_json column
/// (deterministic bytes per spec §16.2, same policy as the event appender and
/// `crate::acquisition`).
fn canonical_json_string_of<T: Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the error
    // arm keeps the panic-free Result policy instead of unwrapping.
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}

/// Enforce that a status-guarded UPDATE hit exactly one row. Zero rows means
/// the guarded state vanished between read and write inside the caller's
/// transaction — an invariant breach to surface, never a silent no-op (mirror
/// of `crate::activation::expect_single_row`).
fn expect_single_row(updated: usize, what: &str) -> Result<(), ApiError> {
    if updated == 1 {
        return Ok(());
    }
    Err(ApiError::StorageOperation {
        message: format!("{what} updated {updated} rows; the status guard did not match"),
    })
}
