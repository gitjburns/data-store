//! The seven §25 graph operators (`include_anchor`, `include_parent_container`,
//! `include_heading_path`, `include_caption_pair`, `include_explicit_references`,
//! `include_continuation_chain`, `include_text_neighbors`) as parse-scoped reads
//! over `unit_relationships`. Owned by package C8a.
//!
//! DECOUPLING (R11): every operator is a SYNCHRONOUS pure graph read taking
//! `conn: &rusqlite::Connection` (the caller-owned query read transaction, DP1)
//! plus explicit inputs; no async/http/tokio, and no operator opens its own
//! connection or begins its own transaction.
//!
//! PARSE-SCOPING (§14): every SQL predicate includes `parse_id = ?`. A query in
//! scope over several sources fuses hits from several active parses, so an
//! operator must never traverse an edge outside the anchor's own parse; the two
//! §25 indexes (`idx_unit_relationships_parse_from` / `_parse_to`) key exactly
//! on `(parse_id, unit_id)` in each direction.
//!
//! DIAGNOSTICS: these operators are internal reads whose caller (C8b's
//! `build_evidence_pack`) owns the assembly diagnostic boundary. They emit no
//! start/success logs of their own but attach local context (unit_id, parse_id,
//! and where relevant the relationship type) to every error they surface
//! (DIAGNOSTICS-ONBOARDING.md: helpers may return typed errors without logging
//! when the caller owns the boundary).

use rusqlite::{Connection, Row};

use crate::error::ApiError;
use crate::model::provenance::Provenance;
use crate::model::relationship::{UnitRelationship, UnitRelationshipType};

/// Hard cap on how many hops a chain/path walk (`include_heading_path`,
/// `include_continuation_chain`) follows before stopping. Canonical structure is
/// acyclic by construction, but a corrupt graph must not spin: the walk also
/// carries a visited-set, and this cap is the belt-and-braces upper bound. It is
/// generous relative to any real heading depth or continuation run.
const MAX_WALK_HOPS: usize = 256;

/// What one operator contributed: the unit ids it added (in traversal order,
/// before the caller's global dedupe/ordering) and the edges it actually
/// traversed. C8b surfaces `traversed_edges` in `EvidencePack.relationships`
/// when the request set `includeRelationships` — these are ONLY the edges the
/// operators walked, not every edge of the touched units.
pub(crate) struct OperatorOutput {
    /// Unit ids this operator added, excluding the anchor unless the operator
    /// is `include_anchor`.
    pub(crate) added_unit_ids: Vec<String>,
    /// The structural edges this operator traversed to reach the added units.
    pub(crate) traversed_edges: Vec<UnitRelationship>,
}

/// The containment relationship types, in the direction parent → child (the
/// `from_unit_id` is the container). `include_parent_container` and
/// `include_heading_path` walk these UPWARD, so they read rows where the anchor
/// is the `to_unit_id`. Verified against `pdf_worker::build_relationships`:
/// containment edges are pushed `from = parent, to = child`.
const CONTAINMENT_TYPES: [UnitRelationshipType; 3] = [
    UnitRelationshipType::Contains,
    UnitRelationshipType::PhysicallyContains,
    UnitRelationshipType::LogicallyContains,
];

/// Ordered SELECT of every column of `unit_relationships`, so a matched edge can
/// be rebuilt into a full `UnitRelationship` for `EvidencePack.relationships`.
/// The column order is fixed here and consumed positionally by `row_to_edge`.
const EDGE_COLUMNS: &str = "id, source_id, parse_id, from_unit_id, to_unit_id, \
     relationship_type, relationship_role, sequence_index, confidence, \
     provenance_json, created_at";

/// `include_anchor` (§25): include the anchor unit itself. Traverses no edges.
/// The only operator that adds the anchor to the pack; every other operator
/// adds NEIGHBORS, so the caller relies on this to seat the anchor.
pub(crate) fn include_anchor(anchor_unit_id: &str) -> OperatorOutput {
    OperatorOutput {
        added_unit_ids: vec![anchor_unit_id.to_string()],
        traversed_edges: Vec::new(),
    }
}

/// `include_parent_container` (§25): add the anchor's immediate parent
/// container(s). Reads containment edges INTO the anchor (`to_unit_id = anchor`)
/// and returns each `from_unit_id` (the container). Normally one parent, but the
/// operator returns all matched containers rather than assuming a single edge.
pub(crate) fn include_parent_container(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let edges = incoming_edges_of_types(conn, parse_id, anchor_unit_id, &CONTAINMENT_TYPES)?;
    let added_unit_ids = edges.iter().map(|e| e.from_unit_id.clone()).collect();
    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges: edges,
    })
}

/// `include_heading_path` (§25): add the full chain of containers above the
/// anchor, up to the parse root. Walks containment edges UPWARD (`to_unit_id`
/// = current) hop by hop, taking one parent per hop, bounded by a visited-set
/// (cycle guard) and `MAX_WALK_HOPS`. Multiple parents at a hop would be a
/// malformed containment tree; the walk follows the first by row order and
/// records that edge, so the path stays a single ancestor chain.
pub(crate) fn include_heading_path(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let mut added_unit_ids = Vec::new();
    let mut traversed_edges = Vec::new();
    let mut visited = vec![anchor_unit_id.to_string()];
    let mut current = anchor_unit_id.to_string();

    for _ in 0..MAX_WALK_HOPS {
        let mut edges = incoming_edges_of_types(conn, parse_id, &current, &CONTAINMENT_TYPES)?;
        // Take the first container edge as this hop's parent; a well-formed
        // containment tree has exactly one. Stop when the current unit has no
        // container (the parse root).
        let Some(edge) = edges.drain(..).next() else {
            break;
        };
        let parent = edge.from_unit_id.clone();
        // Cycle guard: a corrupt graph that loops back to a visited ancestor
        // stops the walk rather than spinning to the hop cap.
        if visited.iter().any(|seen| seen == &parent) {
            break;
        }
        visited.push(parent.clone());
        added_unit_ids.push(parent.clone());
        traversed_edges.push(edge);
        current = parent;
    }

    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges,
    })
}

/// `include_caption_pair` (§25): complete a figure/table/caption anchor with its
/// pair. Reads edges OUT of the anchor of both caption types (`from_unit_id`
/// = anchor): a figure/table anchor carries `has_caption` → its caption; a
/// caption anchor carries `caption_of` → its target. Verified against
/// `pdf_worker`: it pushes both directions per pair (`from = caption caption_of
/// target`, `from = target has_caption caption`), so keying on the anchor as
/// `from_unit_id` finds the pair whichever end the anchor is.
pub(crate) fn include_caption_pair(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let edges = outgoing_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[
            UnitRelationshipType::HasCaption,
            UnitRelationshipType::CaptionOf,
        ],
    )?;
    let added_unit_ids = edges.iter().map(|e| e.to_unit_id.clone()).collect();
    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges: edges,
    })
}

/// `include_explicit_references` (§25): add units the anchor explicitly
/// references. Reads `references` edges OUT of the anchor (`from_unit_id`
/// = anchor). Implemented in full even though no v1 rule invokes it and NO
/// parser emits `references`: the policy externalizes when it applies (§25), so
/// the operator must exist for a later reference-bearing parser to activate it
/// with no code change. Returns an empty output until such edges exist.
pub(crate) fn include_explicit_references(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let edges = outgoing_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[UnitRelationshipType::References],
    )?;
    let added_unit_ids = edges.iter().map(|e| e.to_unit_id.clone()).collect();
    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges: edges,
    })
}

/// `include_continuation_chain` (§25): follow the anchor's `continues_on` chain.
/// Walks `continues_on` edges OUT of the current unit (`from_unit_id` = current)
/// hop by hop, bounded by a visited-set and `MAX_WALK_HOPS`. INERT under the MVP
/// corpus — no parser emits `continues_on`, so this returns an empty output and
/// the R14 check warns; it activates unchanged once a parser emits the edge.
pub(crate) fn include_continuation_chain(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let mut added_unit_ids = Vec::new();
    let mut traversed_edges = Vec::new();
    let mut visited = vec![anchor_unit_id.to_string()];
    let mut current = anchor_unit_id.to_string();

    for _ in 0..MAX_WALK_HOPS {
        let mut edges = outgoing_edges_of_types(
            conn,
            parse_id,
            &current,
            &[UnitRelationshipType::ContinuesOn],
        )?;
        let Some(edge) = edges.drain(..).next() else {
            break;
        };
        let next = edge.to_unit_id.clone();
        // Cycle guard against a corrupt continuation loop.
        if visited.iter().any(|seen| seen == &next) {
            break;
        }
        visited.push(next.clone());
        added_unit_ids.push(next.clone());
        traversed_edges.push(edge);
        current = next;
    }

    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges,
    })
}

/// `include_text_neighbors` (§25): add the anchor's ±1 reading-order neighbors.
/// AUTHORITATIVE MECHANISM: the `precedes` edge (parser-emitted `from = earlier,
/// to = later`), read in BOTH directions — the previous neighbor is the
/// `from_unit_id` of a `precedes` edge INTO the anchor, the next neighbor is the
/// `to_unit_id` of a `precedes` edge OUT of the anchor. `follows` is the
/// symmetric inverse and is read too so a future parser emitting `follows`
/// instead of `precedes` still yields neighbors; `content_units.sequence_index`
/// is deliberately NOT used — the relationship graph is the canonical structure
/// (§19) and `sequence_index` is only a convenience copy.
pub(crate) fn include_text_neighbors(
    conn: &Connection,
    parse_id: &str,
    anchor_unit_id: &str,
) -> Result<OperatorOutput, ApiError> {
    let mut added_unit_ids = Vec::new();
    let mut traversed_edges = Vec::new();

    // Previous neighbor: `precedes` INTO the anchor, or `follows` OUT of it.
    let prev_precedes = incoming_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[UnitRelationshipType::Precedes],
    )?;
    for edge in prev_precedes {
        added_unit_ids.push(edge.from_unit_id.clone());
        traversed_edges.push(edge);
    }
    let prev_follows = outgoing_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[UnitRelationshipType::Follows],
    )?;
    for edge in prev_follows {
        added_unit_ids.push(edge.to_unit_id.clone());
        traversed_edges.push(edge);
    }

    // Next neighbor: `precedes` OUT of the anchor, or `follows` INTO it.
    let next_precedes = outgoing_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[UnitRelationshipType::Precedes],
    )?;
    for edge in next_precedes {
        added_unit_ids.push(edge.to_unit_id.clone());
        traversed_edges.push(edge);
    }
    let next_follows = incoming_edges_of_types(
        conn,
        parse_id,
        anchor_unit_id,
        &[UnitRelationshipType::Follows],
    )?;
    for edge in next_follows {
        added_unit_ids.push(edge.from_unit_id.clone());
        traversed_edges.push(edge);
    }

    Ok(OperatorOutput {
        added_unit_ids,
        traversed_edges,
    })
}

/// Read every edge OUT of `unit_id` (`from_unit_id = unit_id`) whose type is one
/// of `types`, parse-scoped (§14) on the `(parse_id, from_unit_id)` index. The
/// type filter matches the persisted `relationship_type` column against
/// `UnitRelationshipType::wire_name()` — never a hand-written wire string — so a
/// renamed variant is a compile error, not a silent miss.
fn outgoing_edges_of_types(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
    types: &[UnitRelationshipType],
) -> Result<Vec<UnitRelationship>, ApiError> {
    edges_in_direction(conn, parse_id, unit_id, types, EdgeDirection::Outgoing)
}

/// Read every edge INTO `unit_id` (`to_unit_id = unit_id`) whose type is one of
/// `types`, parse-scoped (§14) on the `(parse_id, to_unit_id)` index. See
/// `outgoing_edges_of_types` for the wire-name discipline.
fn incoming_edges_of_types(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
    types: &[UnitRelationshipType],
) -> Result<Vec<UnitRelationship>, ApiError> {
    edges_in_direction(conn, parse_id, unit_id, types, EdgeDirection::Incoming)
}

/// Which end of the edge the anchor unit sits on for a directional read.
enum EdgeDirection {
    /// Anchor is `from_unit_id`; the read walks OUT to `to_unit_id`.
    Outgoing,
    /// Anchor is `to_unit_id`; the read walks IN from `from_unit_id`.
    Incoming,
}

/// Shared parse-scoped edge read in one direction, filtered to `types`. Empty
/// `types` short-circuits to no rows (an empty `IN ()` is not valid SQL and
/// would otherwise be a silent full scan). The relationship-type placeholders
/// are bound from `wire_name()`; the query is fully parameterized (no string
/// interpolation of ids). BOUNDED by the caller's edge set — canonical
/// per-unit degree is small — and returns rows in the table's natural order,
/// which the caller re-sorts deterministically (R13).
fn edges_in_direction(
    conn: &Connection,
    parse_id: &str,
    unit_id: &str,
    types: &[UnitRelationshipType],
    direction: EdgeDirection,
) -> Result<Vec<UnitRelationship>, ApiError> {
    if types.is_empty() {
        return Ok(Vec::new());
    }

    let anchor_column = match direction {
        EdgeDirection::Outgoing => "from_unit_id",
        EdgeDirection::Incoming => "to_unit_id",
    };
    // One `?` placeholder per relationship type; positions 1..=2 are
    // parse_id/unit_id, the rest are the wire names.
    let type_placeholders = std::iter::repeat_n("?", types.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {EDGE_COLUMNS} \
         FROM unit_relationships \
         WHERE parse_id = ?1 AND {anchor_column} = ?2 \
         AND relationship_type IN ({type_placeholders})"
    );

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare edge read for unit {unit_id} of parse {parse_id}: {source}"
            ),
        })?;

    // Bind parse_id, unit_id, then each type's wire name in placeholder order.
    let mut bindings: Vec<&dyn rusqlite::ToSql> = vec![&parse_id, &unit_id];
    let wire_names: Vec<&'static str> = types.iter().map(|t| t.wire_name()).collect();
    for wire in &wire_names {
        bindings.push(wire);
    }

    let edges = stmt
        .query_map(bindings.as_slice(), row_to_edge)
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read edges for unit {unit_id} of parse {parse_id}: {source}"
            ),
        })?;

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
