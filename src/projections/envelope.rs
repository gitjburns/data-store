//! C6 shared envelope persistence: hot-plane lifecycle for RetrievalProjection
//! metadata rows (spec §22, the `retrieval_projections` table), with
//! `projection.*` events appended atomically on the caller's connection. All
//! six C6 builders persist their envelopes through this one module so
//! envelope discipline cannot diverge per builder.
//!
//! Atomicity invariant (mirror of `crate::annotations::store`): every mutating
//! function takes the CALLER's `&rusqlite::Transaction` and appends its event
//! on that same transaction (via `crate::events::append_event`), so the row
//! change and the audit event commit or roll back together — the event trail
//! can never claim a lifecycle transition that did not durably happen. Read
//! functions take a `&rusqlite::Connection`; one bounded SELECT needs no
//! transaction.
//!
//! Freshness lifecycle (spec §22): an envelope is inserted `building`, then
//! transitions `building → fresh` on build success (recording the archived
//! payload URI), `building → failed` on builder failure, `fresh → stale` when
//! its inputs change, or `fresh → superseded` when its parse is superseded.
//! Every UPDATE is status-guarded and asserts it hit exactly one row, so a
//! transition whose precondition vanished fails loudly rather than silently
//! no-opping.

// Consumed by C6a–C6f builders and C7 read paths; remove this allow as they
// wire it.
#![allow(dead_code)]

use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::ids::new_retrieval_projection_id;
use crate::model::{Provenance, SystemEventType};
use crate::primitives::utc_now;
use crate::util::truncate_persisted_detail;

/// Insert one envelope in the `building` state: payload_uri starts NULL (no
/// archived payload until the builder completes) and the validity window is
/// unset. The *_json columns are canonical JSON of their model shapes;
/// freshness_status is the literal 'building', matching the spec §22 status
/// set enforced by the Rust enum on read-back.
const INSERT_BUILDING_SQL: &str = "
INSERT INTO retrieval_projections (
  id, source_id, parse_id, projection_type, input_unit_ids_json,
  input_annotation_ids_json, producer_json, index_name, index_partition,
  payload_uri, freshness_status, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, 'building', ?10)";

/// Status-guarded `building → fresh` transition: records the archived payload
/// URI (nullable — a projection whose payload lives entirely in its hot-plane
/// payload table archives at snapshot time instead) and only matches a row
/// still `building`. The guard makes double-completion or completion of an
/// already-failed row a loud zero-row failure instead of a silent overwrite.
const COMPLETE_FRESH_SQL: &str = "
UPDATE retrieval_projections
SET payload_uri = ?2, freshness_status = 'fresh'
WHERE id = ?1 AND freshness_status = 'building'";

/// Status-guarded `building → failed` transition. No error column exists by
/// design: the failure detail lives in the projection.failed event and the
/// service log, so the durable record survives independently of the row.
const MARK_FAILED_SQL: &str = "
UPDATE retrieval_projections
SET freshness_status = 'failed'
WHERE id = ?1 AND freshness_status = 'building'";

/// Status-guarded `fresh → stale` transition. Only a currently-fresh envelope
/// can go stale; building/failed rows are not eligible, so the guard rejects
/// them.
const MARK_STALE_SQL: &str = "
UPDATE retrieval_projections
SET freshness_status = 'stale'
WHERE id = ?1 AND freshness_status = 'fresh'";

/// Status-guarded `fresh → superseded` transition, taken when the envelope's
/// parse is superseded at cutover (spec §22, §31.2). valid_to records when the
/// envelope stopped being production truth.
const MARK_SUPERSEDED_SQL: &str = "
UPDATE retrieval_projections
SET freshness_status = 'superseded', valid_to = ?2
WHERE id = ?1 AND freshness_status = 'fresh'";

/// Read every fresh, non-deleted envelope whose parse is the source's CURRENT
/// active parse. The subselect on source_objects.active_parse_id enforces
/// active-parse scoping at the query (mirror of the §21 rule-1 enforcement in
/// `annotations::store::fresh_for_active_parse`): a superseded parse's
/// envelopes never surface even while their rows linger pending hot cleanup.
const SELECT_FRESH_FOR_ACTIVE_PARSE_SQL: &str = "
SELECT
  id, source_id, parse_id, projection_type, input_unit_ids_json,
  input_annotation_ids_json, producer_json, index_name, index_partition,
  payload_uri, freshness_status, created_at, valid_from, valid_to, deleted_at
FROM retrieval_projections
WHERE source_id = ?1
  AND parse_id = (SELECT active_parse_id FROM source_objects WHERE id = ?1)
  AND freshness_status = 'fresh'
  AND deleted_at IS NULL";

/// Read the projection_type of every FRESH, non-deleted envelope of ONE parse,
/// keyed by parse_id directly (NOT active-parse-scoped like
/// `SELECT_FRESH_FOR_ACTIVE_PARSE_SQL`). The activation prerequisite check runs
/// this against the CANDIDATE parse BEFORE the active-parse pointer swaps to it,
/// so it must not filter on `source_objects.active_parse_id` — the candidate is
/// not yet active. Only projection_type is selected: the prerequisite check
/// asks which content-derived types are fresh, not their full envelopes.
const SELECT_FRESH_TYPES_FOR_PARSE_SQL: &str = "
SELECT projection_type
FROM retrieval_projections
WHERE parse_id = ?1
  AND freshness_status = 'fresh'
  AND deleted_at IS NULL";

/// Hard-delete every retrieval_projections row of one parse for ONE projection
/// type. Rebuild idempotence for the builders whose envelope module exposes no
/// wholesale replace: the integration wiring calls this immediately BEFORE
/// invoking a builder so a re-run replaces rather than accumulates envelopes
/// (see `delete_for_parse`). Type-scoped so deleting one channel's stale
/// envelopes never touches another channel's rows for the same parse.
const DELETE_FOR_PARSE_SQL: &str = "
DELETE FROM retrieval_projections
WHERE parse_id = ?1 AND projection_type = ?2";

/// SystemEvent object_type for retrieval_projections rows.
const OBJECT_TYPE_RETRIEVAL_PROJECTION: &str = "retrieval_projection";

/// Spec §22 `RetrievalProjectionType`, snake_case wire names. The full closed
/// spec set is mirrored (matching the model convention of complete spec
/// enums), though the MVP builders emit only six of the ten: learned-sparse
/// and temporal channels are deferred post-MVP and reranker features have no
/// C6 producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectionType {
    LexicalDocument,
    LearnedSparseVector,
    DenseVector,
    MultiVector,
    Chunk,
    Summary,
    GraphProjection,
    TemporalProjection,
    RerankerFeature,
    DerivedView,
}

/// Spec §22 `freshnessStatus`: the closed five-value projection lifecycle.
/// The `retrieval_projections` DDL carries no CHECK for this column (C2e
/// heritage), so this enum is the single enforcement point — every write path
/// uses status literals guarded here and every read re-types through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectionFreshnessStatus {
    Fresh,
    Stale,
    Building,
    Failed,
    Superseded,
}

/// The inputs needed to open one projection build: everything known before
/// the builder runs. source_id/parse_id are required Strings (not Options,
/// unlike the spec-optional columns) because every C6 projection is
/// parse-scoped by the approved cluster design; the planned producer identity
/// is carried as a full Provenance because it is known before invocation.
/// input_annotation_ids is set only by the annotation-derived builders (C6d
/// summary, C6f graph) — spec §22 inputAnnotationIds.
#[derive(Debug, Clone)]
pub(crate) struct NewProjection {
    pub(crate) source_id: String,
    pub(crate) parse_id: String,
    pub(crate) projection_type: ProjectionType,
    pub(crate) input_unit_ids: Option<Vec<String>>,
    pub(crate) input_annotation_ids: Option<Vec<String>>,
    pub(crate) producer: Provenance,
    pub(crate) index_name: Option<String>,
    pub(crate) index_partition: Option<String>,
}

/// One retrieval_projections row re-typed into its model shape, column for
/// column (spec §22 RetrievalProjection envelope). source_id/parse_id are
/// Options here, honestly matching the nullable columns, even though every
/// C6-written row populates both.
#[derive(Debug, Clone)]
pub(crate) struct ProjectionEnvelope {
    pub(crate) id: String,
    pub(crate) source_id: Option<String>,
    pub(crate) parse_id: Option<String>,
    pub(crate) projection_type: ProjectionType,
    pub(crate) input_unit_ids: Option<Vec<String>>,
    pub(crate) input_annotation_ids: Option<Vec<String>>,
    pub(crate) producer: Provenance,
    pub(crate) index_name: Option<String>,
    pub(crate) index_partition: Option<String>,
    pub(crate) payload_uri: Option<String>,
    pub(crate) freshness_status: ProjectionFreshnessStatus,
    pub(crate) created_at: String,
    pub(crate) valid_from: Option<String>,
    pub(crate) valid_to: Option<String>,
    pub(crate) deleted_at: Option<String>,
}

/// Insert a `building` envelope and append its `projection.requested` event on
/// the caller's transaction, returning the freshly minted `proj_` id. The row
/// carries the planned producer's provenance and no payload URI yet; the URI
/// arrives at `complete_fresh`. Row write and event are one atomic unit (see
/// the module atomicity invariant).
pub(crate) fn insert_building(
    tx: &Transaction<'_>,
    request: &NewProjection,
) -> Result<String, ApiError> {
    let id = new_retrieval_projection_id()?;
    let projection_type = enum_wire_name(&request.projection_type, "projection type")?;
    let input_unit_ids_json = request
        .input_unit_ids
        .as_ref()
        .map(|ids| canonical_json_string_of(ids, &format!("input unit ids for projection {id}")))
        .transpose()?;
    let input_annotation_ids_json = request
        .input_annotation_ids
        .as_ref()
        .map(|ids| {
            canonical_json_string_of(ids, &format!("input annotation ids for projection {id}"))
        })
        .transpose()?;
    let producer_json = canonical_json_string_of(
        &request.producer,
        &format!("producer provenance for projection {id}"),
    )?;
    let now = utc_now()?;

    tx.execute(
        INSERT_BUILDING_SQL,
        params![
            id,
            request.source_id,
            request.parse_id,
            projection_type,
            input_unit_ids_json,
            input_annotation_ids_json,
            producer_json,
            request.index_name,
            request.index_partition,
            now,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert building projection {id} for parse {}: {source}",
            request.parse_id
        ),
    })?;

    // Payload names the request identity so the audit trail records what was
    // asked for even before any payload exists.
    let payload = Map::from_iter([
        entry("sourceId", &request.source_id),
        entry("parseId", &request.parse_id),
        entry("projectionType", &projection_type),
    ]);
    let event = new_system_event(
        SystemEventType::ProjectionRequested,
        OBJECT_TYPE_RETRIEVAL_PROJECTION,
        &id,
        Some(payload),
    )?;
    append_event(tx, &event)?;

    Ok(id)
}

/// Transition `building → fresh`: record the archived payload URI (None for
/// projections whose payload lives entirely in a hot-plane payload table),
/// then append `projection.completed`. The UPDATE is status-guarded and
/// asserted to hit exactly one row, so completing a missing or non-building
/// row is a loud failure, never a silent overwrite. Row write and event are
/// one atomic unit.
pub(crate) fn complete_fresh(
    tx: &Transaction<'_>,
    projection_id: &str,
    payload_uri: Option<&str>,
) -> Result<(), ApiError> {
    let updated = tx
        .execute(COMPLETE_FRESH_SQL, params![projection_id, payload_uri])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to complete projection {projection_id}: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("projection {projection_id} building → fresh"),
    )?;

    let payload = Map::from_iter([entry("projectionId", projection_id)]);
    let event = new_system_event(
        SystemEventType::ProjectionCompleted,
        OBJECT_TYPE_RETRIEVAL_PROJECTION,
        projection_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `building → failed` and append `projection.failed` carrying the
/// bounded failure detail. The retrieval_projections row itself has no error
/// column by design: the event payload and the service log own the failure
/// detail, so the durable record survives independently of the row (which hot
/// cleanup may later remove). Row write and event are one atomic unit.
pub(crate) fn mark_failed(
    tx: &Transaction<'_>,
    projection_id: &str,
    bounded_detail: &str,
) -> Result<(), ApiError> {
    let detail = truncate_persisted_detail(bounded_detail);

    let updated = tx
        .execute(MARK_FAILED_SQL, params![projection_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark projection {projection_id} failed: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("projection {projection_id} building → failed"),
    )?;

    let mut payload = Map::from_iter([entry("projectionId", projection_id)]);
    payload.insert("detail".to_owned(), Value::String(detail));
    let event = new_system_event(
        SystemEventType::ProjectionFailed,
        OBJECT_TYPE_RETRIEVAL_PROJECTION,
        projection_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `fresh → stale` and append `projection.stale`, making an
/// input-driven staling visible truth rather than silent absence. Row write
/// and event are one atomic unit.
pub(crate) fn mark_stale(tx: &Transaction<'_>, projection_id: &str) -> Result<(), ApiError> {
    let updated = tx
        .execute(MARK_STALE_SQL, params![projection_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark projection {projection_id} stale: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("projection {projection_id} fresh → stale"),
    )?;

    let payload = Map::from_iter([entry("projectionId", projection_id)]);
    let event = new_system_event(
        SystemEventType::ProjectionStale,
        OBJECT_TYPE_RETRIEVAL_PROJECTION,
        projection_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Transition `fresh → superseded` (parse superseded at cutover, §31.2),
/// stamping valid_to with the supersession time, then append
/// `projection.superseded`. The UPDATE is status-guarded and asserted to hit
/// exactly one row. Row write and event are one atomic unit (see the module
/// atomicity invariant).
///
/// The `projection.superseded` event is the recorded ADDITIVE extension of the
/// spec §33 vocabulary this module named as needed (same precedent as the CA
/// `annotation.*` additions); the `SystemEventType::ProjectionSuperseded`
/// variant it mints was landed alongside this emission, so the supersession is
/// now visible truth on the projection's own audit trail rather than only
/// inferable from the owning cutover's `parse.*` events.
pub(crate) fn mark_superseded(tx: &Transaction<'_>, projection_id: &str) -> Result<(), ApiError> {
    let now = utc_now()?;
    let updated = tx
        .execute(MARK_SUPERSEDED_SQL, params![projection_id, now])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to mark projection {projection_id} superseded: {source}"),
        })?;
    expect_single_row(
        updated,
        &format!("projection {projection_id} fresh → superseded"),
    )?;

    let payload = Map::from_iter([entry("projectionId", projection_id)]);
    let event = new_system_event(
        SystemEventType::ProjectionSuperseded,
        OBJECT_TYPE_RETRIEVAL_PROJECTION,
        projection_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Delete this parse's envelopes of ONE projection type on the caller's
/// transaction, returning the number of rows removed. This is the wholesale
/// rebuild-idempotence operation the builders whose payload delete is separate
/// from their envelope (the `DerivedView` builder deletes no prior envelope, and
/// every builder re-inserts a fresh `building` envelope) rely on the integration
/// wiring to run FIRST: the wiring calls this per projection type immediately
/// before invoking the matching builder, so a re-run replaces rather than
/// accumulates envelopes.
///
/// No event is appended: a delete of a rebuildable projection envelope is hot
/// cleanup, not an audited lifecycle transition (spec §22 events cover
/// requested/completed/failed/stale, not the pre-rebuild sweep). The row change
/// rides the caller's transaction, so it commits or rolls back with the rebuild
/// it precedes. Type-scoped so one channel's sweep never removes another
/// channel's envelopes for the same parse.
pub(crate) fn delete_for_parse(
    tx: &Transaction<'_>,
    parse_id: &str,
    projection_type: ProjectionType,
) -> Result<usize, ApiError> {
    let type_name = enum_wire_name(&projection_type, "projection type")?;
    tx.execute(DELETE_FOR_PARSE_SQL, params![parse_id, type_name])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to delete {type_name} projections for parse {parse_id}: {source}"
            ),
        })
}

/// Read the set of projection types that are FRESH for one parse, keyed by
/// parse_id directly (see `SELECT_FRESH_TYPES_FOR_PARSE_SQL`). The activation
/// prerequisite check calls this on the CANDIDATE parse before its pointer swap,
/// so it deliberately does NOT scope on the source's active parse. Each
/// persisted `projection_type` string is re-typed through the enum so a value
/// outside the spec §22 set fails loudly rather than being silently ignored by
/// the caller's membership test.
pub(crate) fn fresh_types_for_parse(
    conn: &Connection,
    parse_id: &str,
) -> Result<std::collections::HashSet<ProjectionType>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_FRESH_TYPES_FOR_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare fresh-projection-type query for parse {parse_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![parse_id], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to query fresh projection types for parse {parse_id}: {source}"
            ),
        })?;

    let mut types = std::collections::HashSet::new();
    for row in rows {
        let type_name = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read projection type row for parse {parse_id}: {source}"),
        })?;
        let projection_type: ProjectionType =
            wire_value(&type_name, &format!("projection type for parse {parse_id}"))?;
        types.insert(projection_type);
    }
    Ok(types)
}

/// Require a unique fresh representation so two dense indexes cannot satisfy
/// each other's activation requirement. SQLite IS also matches the unnamed
/// passage index's NULL identity without treating it as a wildcard.
pub(crate) fn has_fresh_index_for_parse(
    conn: &Connection,
    parse_id: &str,
    projection_type: ProjectionType,
    index_name: Option<&str>,
) -> Result<bool, ApiError> {
    const SQL: &str = "SELECT count(*) FROM retrieval_projections
        WHERE parse_id = ?1 AND projection_type = ?2 AND index_name IS ?3
        AND freshness_status = 'fresh' AND deleted_at IS NULL";
    let type_name = enum_wire_name(&projection_type, "projection type")?;
    let count: i64 = conn
        .query_row(SQL, params![parse_id, type_name, index_name], |row| {
            row.get(0)
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to check {type_name} index {index_name:?} for parse {parse_id}: {source}"
            ),
        })?;
    if count > 1 {
        return Err(ApiError::StorageOperation {
            message: format!(
                "parse {parse_id} has {count} fresh {type_name} indexes named {index_name:?}; expected exactly one"
            ),
        });
    }
    Ok(count == 1)
}

/// Read every fresh, non-deleted envelope of a source's CURRENT active parse
/// (active-parse scoping enforced in the query's active_parse_id subselect,
/// not by the caller — mirror of `annotations::store::fresh_for_active_parse`).
/// Envelopes of a superseded parse never surface even while their rows linger
/// pending hot cleanup.
pub(crate) fn fresh_for_active_parse(
    conn: &Connection,
    source_id: &str,
) -> Result<Vec<ProjectionEnvelope>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_FRESH_FOR_ACTIVE_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare fresh-projection query for source {source_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![source_id], |row| {
            Ok(EnvelopeRow {
                id: row.get(0)?,
                source_id: row.get(1)?,
                parse_id: row.get(2)?,
                projection_type: row.get(3)?,
                input_unit_ids_json: row.get(4)?,
                input_annotation_ids_json: row.get(5)?,
                producer_json: row.get(6)?,
                index_name: row.get(7)?,
                index_partition: row.get(8)?,
                payload_uri: row.get(9)?,
                freshness_status: row.get(10)?,
                created_at: row.get(11)?,
                valid_from: row.get(12)?,
                valid_to: row.get(13)?,
                deleted_at: row.get(14)?,
            })
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query fresh projections for source {source_id}: {source}"),
        })?;

    let mut envelopes = Vec::new();
    for row in rows {
        let row = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read projection row for source {source_id}: {source}"),
        })?;
        envelopes.push(envelope_from_row(row)?);
    }
    Ok(envelopes)
}

/// One retrieval_projections row as read from SQLite, before its *_json and
/// wire-string columns are re-typed back into the envelope shape.
struct EnvelopeRow {
    id: String,
    source_id: Option<String>,
    parse_id: Option<String>,
    projection_type: String,
    input_unit_ids_json: Option<String>,
    input_annotation_ids_json: Option<String>,
    producer_json: String,
    index_name: Option<String>,
    index_partition: Option<String>,
    payload_uri: Option<String>,
    freshness_status: String,
    created_at: String,
    valid_from: Option<String>,
    valid_to: Option<String>,
    deleted_at: Option<String>,
}

/// Re-type one persisted row into a `ProjectionEnvelope`. The *_json columns
/// parse back through serde and the projection_type/freshness_status wire
/// strings re-type through their enums, so a value outside the spec §22 sets
/// (or a corrupt payload) fails loudly here rather than being misread — the
/// only enforcement point for freshness_status, which carries no schema CHECK.
fn envelope_from_row(row: EnvelopeRow) -> Result<ProjectionEnvelope, ApiError> {
    let projection_type: ProjectionType =
        wire_value(&row.projection_type, &format!("projection {} type", row.id))?;
    let freshness_status: ProjectionFreshnessStatus = wire_value(
        &row.freshness_status,
        &format!("projection {} freshness", row.id),
    )?;
    let input_unit_ids: Option<Vec<String>> = row
        .input_unit_ids_json
        .as_deref()
        .map(|json| parse_json_column(json, &format!("input unit ids of projection {}", row.id)))
        .transpose()?;
    let input_annotation_ids: Option<Vec<String>> = row
        .input_annotation_ids_json
        .as_deref()
        .map(|json| {
            parse_json_column(
                json,
                &format!("input annotation ids of projection {}", row.id),
            )
        })
        .transpose()?;
    let producer: Provenance = parse_json_column(
        &row.producer_json,
        &format!("producer provenance of projection {}", row.id),
    )?;

    Ok(ProjectionEnvelope {
        id: row.id,
        source_id: row.source_id,
        parse_id: row.parse_id,
        projection_type,
        input_unit_ids,
        input_annotation_ids,
        producer,
        index_name: row.index_name,
        index_partition: row.index_partition,
        payload_uri: row.payload_uri,
        freshness_status,
        created_at: row.created_at,
        valid_from: row.valid_from,
        valid_to: row.valid_to,
        deleted_at: row.deleted_at,
    })
}

/// Render one closed enum through its serde wire name so persisted column
/// values can never drift from the Rust enum (same pattern as
/// `crate::annotations::store::enum_wire_name`, private there by convention).
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

/// Re-type one persisted wire string through its enum, keeping the read half
/// symmetric with `enum_wire_name` so a corrupt value fails loudly instead of
/// being string-compared into a wrong branch.
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
/// `crate::annotations::store`).
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
/// of `crate::annotations::store::expect_single_row`).
fn expect_single_row(updated: usize, what: &str) -> Result<(), ApiError> {
    if updated == 1 {
        return Ok(());
    }
    Err(ApiError::StorageOperation {
        message: format!("{what} updated {updated} rows; the status guard did not match"),
    })
}
