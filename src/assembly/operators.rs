//! Parse-scoped canonical relationship reads for selected evidence.
//! The caller owns the SQLite snapshot and assembly diagnostic boundary.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, Row, params};

use crate::assembly::model::{MAX_PASSAGE_UNITS, MAX_QUERY_RESULTS};

use crate::error::ApiError;
use crate::model::provenance::Provenance;
use crate::model::relationship::{UnitRelationship, UnitRelationshipType};

/// Ordered SELECT of every column of `unit_relationships`, so a matched edge can
/// be rebuilt into a full `UnitRelationship` for `EvidencePack.relationships`.
/// The column order is fixed here and consumed positionally by `row_to_edge`.
const EDGE_COLUMNS: &str = "id, source_id, parse_id, from_unit_id, to_unit_id, \
     relationship_type, relationship_role, sequence_index, confidence, \
     provenance_json, created_at";

/// Read canonical links within the selected evidence set without adding units.
/// Endpoints and parse are constrained in SQL; the caller owns the read snapshot.
pub(crate) fn selected_relationships(
    conn: &Connection,
    selected: &[(&str, &str)],
) -> Result<Vec<UnitRelationship>, ApiError> {
    let mut by_parse: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for &(parse_id, unit_id) in selected {
        by_parse.entry(parse_id).or_default().push(unit_id);
    }
    let limit = MAX_QUERY_RESULTS * MAX_PASSAGE_UNITS;
    let sql = format!(
        "SELECT {EDGE_COLUMNS} FROM unit_relationships \
         WHERE parse_id = ?1 AND from_unit_id = ?2 \
         AND to_unit_id IN (SELECT value FROM json_each(?3)) \
         ORDER BY id LIMIT ?4"
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare selected evidence relationship read: {source}"),
        })?;
    let mut edges = Vec::new();
    let mut seen = BTreeSet::new();
    for (parse_id, unit_ids) in by_parse {
        let targets = serde_json::to_string(&unit_ids).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to encode selected relationship targets for parse {parse_id}: {source}"
            ),
        })?;
        for unit_id in unit_ids {
            // One extra row makes overflow observable. Never truncate raw links
            // and return success, because consumers would treat them as complete.
            let remaining = limit.saturating_sub(edges.len());
            let rows = statement.query_map(
                params![parse_id, unit_id, targets, (remaining + 1) as i64],
                row_to_edge,
            ).map_err(|source| ApiError::StorageOperation {
                message: format!("failed to read selected relationships for unit {unit_id} of parse {parse_id}: {source}"),
            })?;
            for row in rows {
                let edge = row.map_err(|source| ApiError::StorageOperation {
                    message: format!("failed to decode selected relationship for unit {unit_id} of parse {parse_id}: {source}"),
                })?;
                if seen.insert(edge.id.clone()) {
                    if edges.len() == limit {
                        return Err(ApiError::StorageOperation {
                            message: format!(
                                "raw evidence relationship safety limit {limit} exceeded at unit {unit_id} of parse {parse_id}"
                            ),
                        });
                    }
                    edges.push(edge);
                }
            }
        }
    }
    Ok(edges)
}

/// Rebuild a `UnitRelationship` from one `unit_relationships` row, in the fixed
/// `EDGE_COLUMNS` order. `provenance_json`, when present, is deserialized into a
/// `Provenance` (§20); a NULL column stays `None`. A malformed provenance blob
/// surfaces as a rusqlite conversion error carrying the row, propagated by the
/// caller with the unit/parse identity.
fn row_to_edge(row: &Row<'_>) -> rusqlite::Result<UnitRelationship> {
    let relationship_type_wire: String = row.get(5)?;
    // Re-type the persisted wire string through the model enum; an out-of-set
    // value fails loudly at this row rather than being silently mis-read.
    let relationship_type: UnitRelationshipType = serde_json::from_value(
        serde_json::Value::String(relationship_type_wire.clone()),
    )
    .map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(source))
    })?;

    let provenance: Option<Provenance> = match row.get::<_, Option<String>>(9)? {
        Some(json) => Some(serde_json::from_str(&json).map_err(|source| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Text,
                Box::new(source),
            )
        })?),
        None => None,
    };

    Ok(UnitRelationship {
        id: row.get(0)?,
        source_id: row.get(1)?,
        parse_id: row.get(2)?,
        from_unit_id: row.get(3)?,
        to_unit_id: row.get(4)?,
        relationship_type,
        relationship_role: row.get(6)?,
        sequence_index: row.get(7)?,
        confidence: row.get(8)?,
        provenance,
        created_at: row.get(10)?,
        // A traversed edge is a live canonical row; assembly never reads soft-
        // deleted edges (the hot plane hard-deletes on archive), so `deleted_at`
        // is not a column here and is left `None`.
        deleted_at: None,
    })
}
