//! Canonical logical section resolution shared by indexing and passage assembly.

use std::collections::BTreeSet;

use rusqlite::{Connection, params};
use serde_json::Value;

use crate::assembly::model::MAX_PASSAGE_UNITS;
use crate::error::ApiError;

const MAX_SECTION_BODY_BYTES: usize = 1_048_576;
const PARENT_SQL: &str = "
SELECT DISTINCT p.id, p.content_type,
       CASE WHEN length(CAST(p.body_json AS BLOB)) <= ?3 THEN p.body_json END
FROM unit_relationships r JOIN content_units p
  ON p.parse_id = r.parse_id AND p.id = r.from_unit_id
WHERE r.parse_id = ?1 AND r.to_unit_id = ?2
  AND r.relationship_type IN ('logically_contains', 'contains')
  AND p.content_type != 'page'
LIMIT 2";

/// Resolve the nearest logical section without treating physical page containment
/// as ancestry. Ambiguous parents, cycles, and excessive depth are explicit errors.
pub(crate) fn read_section(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
) -> Result<(Option<String>, Vec<String>), ApiError> {
    let mut current = unit_id.to_owned();
    let mut visited = BTreeSet::from([current.clone()]);
    let mut statement = conn.prepare(PARENT_SQL).map_err(|source| {
        failure(format!(
            "prepare logical ancestry of {unit_id} in {parse_id}: {source}"
        ))
    })?;
    for _ in 0..MAX_PASSAGE_UNITS {
        let parents = statement
            .query_map(params![parse_id, current, MAX_SECTION_BODY_BYTES], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|source| {
                failure(format!(
                    "read logical parent of {current} in {parse_id}: {source}"
                ))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| {
                failure(format!(
                    "decode logical parent of {current} in {parse_id}: {source}"
                ))
            })?;
        if parents.len() > 1 {
            return Err(failure(format!(
                "ambiguous logical parent for {current} in {parse_id}"
            )));
        }
        let Some((id, content_type, body)) = parents.into_iter().next() else {
            return Ok((None, Vec::new()));
        };
        if !visited.insert(id.clone()) {
            return Err(failure(format!(
                "logical containment cycle at {id} in {parse_id}"
            )));
        }
        let body = body.ok_or_else(|| {
            failure(format!(
                "section ancestor {id} in {parse_id} exceeds {MAX_SECTION_BODY_BYTES} body bytes"
            ))
        })?;
        let body: Value = serde_json::from_str(&body).map_err(|source| {
            failure(format!(
                "body of section ancestor {id} in {parse_id}: {source}"
            ))
        })?;
        if content_type == "text_section" {
            let path = section_path(&body, parse_id, &id)?;
            return Ok((Some(id), path));
        }
        current = id;
    }
    Err(failure(format!(
        "logical section ancestry of {unit_id} exceeds {MAX_PASSAGE_UNITS} hops in {parse_id}"
    )))
}

/// Canonical optional paths may be absent or null. Both use the recorded heading;
/// malformed non-null paths fail identically for live and archived canonical rows.
pub(crate) fn section_path(
    body: &Value,
    parse_id: &str,
    section_id: &str,
) -> Result<Vec<String>, ApiError> {
    if let Some(path) = body.get("sectionPath").filter(|path| !path.is_null()) {
        return serde_json::from_value(path.clone()).map_err(|source| {
            failure(format!(
                "section path of {section_id} in {parse_id}: {source}"
            ))
        });
    }
    Ok(body
        .get("headingText")
        .and_then(Value::as_str)
        .map(|heading| vec![heading.to_owned()])
        .unwrap_or_default())
}

/// Preserve graph and SQL identity at the shared section-resolution boundary.
fn failure(message: String) -> ApiError {
    ApiError::StorageOperation { message }
}
