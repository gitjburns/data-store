//! CA2-P4: bounded, read-only vocabulary inspection over `semantic_annotations`.
//!
//! WHY THIS SURFACE EXISTS (CA2 ruling 8, PLAN-CANONICAL-FABRIC.md CA2 Current
//! Status entry): the annotation mitigation rulesets (the operator-editable
//! entity-match and annotator-naming policy documents, D3 amendment) are
//! corpus-dependent — authorable ONLY from the entity names and relation
//! predicates the models actually produced. Nothing else exposes that: the
//! service log carries bounded facts only (never model output), and raw sqlite
//! is not an operator surface. This module is that surface. It feeds the
//! operator-inspect-and-adjust ruleset loop (dry-run → inspect → author
//! policies → normal start); the self-learning ruleset tier is post-MVP (§5).
//!
//! POSTURE — this surface EXISTS to reveal anomalies, so it does NOT fail on
//! imperfect data. Unlike the projection builders (`projections/graph.rs`,
//! `projections/view.rs`), which fail loudly on a malformed annotation body
//! because a corrupt row must not silently corrupt a derived index, aggregation
//! here COUNTS malformed rows into a group-less bucket and reports the count.
//! Inspection must work on exactly the imperfect, never-cleaned data an operator
//! needs to see. Empty-marker rows (body EXACTLY `[]`) are the worker's
//! by-design "no result" convention and are SKIPPED-and-counted, mirroring the
//! CPd2 consumer skip convention (`crate::projections::graph::accumulate_mentions`
//! / `derive_edges` and `crate::projections::view::build_summary`; the
//! must-stay-in-step banner lives at `crate::annotations::worker::complete_build`).
//!
//! ACCESS SAFETY (D1): the caller opens a read-only connection at the handler
//! boundary (`crate::hot_plane::open_read`) and passes it in — this mirrors how
//! the existing public read routes structure their reads (the read function
//! opens `open_read`, the handler runs it inside `spawn_blocking`). Every read
//! here is bounded by the row-read cap below, and every response is bounded by
//! the per-response group cap, with truncation reported explicitly.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::Value;

use crate::error::ApiError;
use crate::model::SemanticAnnotationType;
use crate::projections::graph::normalize_entity_name;

/// Hard cap on rows READ from `semantic_annotations` per request. A bounded scan
/// keeps one inspection request from touching an unbounded row set (D1 bounded
/// read discipline). When the scan hits this cap the response is flagged
/// truncated so the operator knows the vocabulary is a partial view. Chosen
/// generously (the vocabulary of a real corpus is far smaller than its annotation
/// row count, but a single normalized name can have many raw-form rows) while
/// still bounding a single request's work.
const MAX_ROWS_READ: usize = 200_000;

/// Hard cap on GROUPS returned in one response (distinct normalized names for
/// entity, distinct predicates for relation). Groups past this cap are dropped
/// and the response is flagged truncated. Bounds the response size independently
/// of `MAX_ROWS_READ`: a pathological corpus could stay under the row cap yet
/// still produce an unwieldy group list.
const MAX_GROUPS: usize = 50_000;

/// Bucket label for rows whose provenance carries no `modelName`. Model
/// attribution is Option on `Provenance` (`model_name`), so a row without it is
/// still counted honestly under this explicit sentinel rather than dropped.
const UNKNOWN_MODEL: &str = "(unknown)";

/// Read-only scope for a vocabulary request. ACTIVE mirrors the active-parse
/// subselect discipline in `crate::annotations::store` (only annotations of each
/// source's CURRENT active parse, §21 rule 1) — the same set query-time
/// consumers see. ALL reads every fresh, non-deleted annotation row regardless
/// of active status, which is what makes dry-run output on never-activated
/// parses (CA2-P5) inspectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VocabularyScope {
    Active,
    All,
}

/// ACTIVE-scope entity/relation row read: fresh, non-deleted annotations of the
/// requested type whose parse is its source's CURRENT active parse. The
/// active-parse subselect is the same §21-rule-1 discipline
/// `SELECT_FRESH_FOR_ACTIVE_PARSE_SQL` uses in `store.rs`; correlating it to
/// `source_id` scopes each row to its own source's active parse. Only the three
/// columns aggregation needs are read, ordered by id for a deterministic
/// truncation frontier when the row cap is hit. The `LIMIT` is `MAX_ROWS_READ + 1`
/// so reading one extra row proves truncation without a second COUNT query.
const SELECT_ACTIVE_SQL: &str = "
SELECT source_id, body_json, provenance_json
FROM semantic_annotations sa
WHERE sa.annotation_type = ?1
  AND sa.freshness_status = 'fresh'
  AND sa.deleted_at IS NULL
  AND sa.parse_id = (
    SELECT active_parse_id FROM source_objects WHERE id = sa.source_id
  )
ORDER BY sa.id
LIMIT ?2";

/// ALL-scope entity/relation row read: every fresh, non-deleted annotation of
/// the requested type regardless of active status, so dry-run output on
/// never-activated parses is inspectable. Same three columns, same deterministic
/// id ordering, same `MAX_ROWS_READ + 1` limit as the active variant.
const SELECT_ALL_SQL: &str = "
SELECT source_id, body_json, provenance_json
FROM semantic_annotations
WHERE annotation_type = ?1
  AND freshness_status = 'fresh'
  AND deleted_at IS NULL
ORDER BY id
LIMIT ?2";

/// The entity vocabulary for one request: the per-normalized-name groups (sorted
/// by normalized name ascending so spelling variants sit adjacent), plus the
/// bounded-scan bookkeeping the response reports.
#[derive(Debug)]
pub(crate) struct EntityVocabulary {
    pub(crate) groups: Vec<EntityGroup>,
    /// Rows whose body was EXACTLY `[]` (the worker's empty-marker convention):
    /// SKIPPED here and counted, never grouped (see the module banner).
    pub(crate) skipped_markers: usize,
    /// Rows whose body was neither a valid entity body nor the empty marker:
    /// counted here, never grouped, never fatal (inspection reveals anomalies).
    pub(crate) malformed_rows: usize,
    /// True when the row scan hit `MAX_ROWS_READ` OR the group set was clipped to
    /// `MAX_GROUPS`; the served vocabulary is then a partial view.
    pub(crate) truncated: bool,
    /// Total rows READ (bounded by `MAX_ROWS_READ`), for the handler-boundary log.
    pub(crate) rows_read: usize,
}

/// One entity vocabulary group: all raw forms that normalize to one name.
#[derive(Debug)]
pub(crate) struct EntityGroup {
    /// The normalized node identity (`normalize_entity_name`), the single
    /// normalization source of truth shared with the graph channel.
    pub(crate) normalized_name: String,
    /// The distinct raw `name` strings (pre-normalization) that folded into this
    /// group, each with its occurrence count, sorted by raw form. Contrasting
    /// the raw forms against the shared normalized name shows exactly what
    /// normalization did and did not fold.
    pub(crate) raw_forms: Vec<RawFormCount>,
    /// The distinct `entityType` values seen across this group's rows, sorted.
    /// entityType is metadata, not identity (D9): one normalized name may appear
    /// with several types and stays ONE group.
    pub(crate) entity_types: Vec<String>,
    /// Count of DISTINCT source_ids contributing to this group.
    pub(crate) source_count: usize,
    /// Per-model occurrence counts (provenance `modelName`; `(unknown)` when
    /// absent), sorted by model name. Reveals model-boundary vocabulary drift
    /// (the accepted mixed-model consequence, CA2 ruling 1).
    pub(crate) model_counts: Vec<ModelCount>,
    /// Total occurrences folded into this group across all raw forms.
    pub(crate) total_count: usize,
}

/// The relation vocabulary for one request: the per-predicate groups (sorted by
/// predicate), plus the same bounded-scan bookkeeping the entity path reports.
#[derive(Debug)]
pub(crate) struct RelationVocabulary {
    pub(crate) groups: Vec<RelationGroup>,
    pub(crate) skipped_markers: usize,
    pub(crate) malformed_rows: usize,
    pub(crate) truncated: bool,
    pub(crate) rows_read: usize,
}

/// One relation vocabulary group: one distinct predicate and its attribution.
#[derive(Debug)]
pub(crate) struct RelationGroup {
    /// The relation body's `predicate` string verbatim (relations are grouped by
    /// predicate; subject/object are not part of the predicate vocabulary).
    pub(crate) predicate: String,
    /// Total occurrences of this predicate.
    pub(crate) total_count: usize,
    /// Count of DISTINCT source_ids contributing this predicate.
    pub(crate) source_count: usize,
    /// Per-model occurrence counts (provenance `modelName`; `(unknown)` when
    /// absent), sorted by model name.
    pub(crate) model_counts: Vec<ModelCount>,
}

/// One raw form and its occurrence count within an entity group.
#[derive(Debug)]
pub(crate) struct RawFormCount {
    pub(crate) raw_form: String,
    pub(crate) count: usize,
}

/// One model name and its occurrence count within a group.
#[derive(Debug)]
pub(crate) struct ModelCount {
    pub(crate) model_name: String,
    pub(crate) count: usize,
}

/// One `semantic_annotations` row as read for aggregation: only the body and
/// provenance the aggregators fold. The row's `source_id` rides a parallel vector
/// (see `read_rows_with_source`), and the deterministic scan/truncation frontier
/// is fixed by the SQL `ORDER BY id`, not by any field here. `body_json` is NEVER
/// NULL on a fresh row (the §21 envelope, enforced by `store::annotation_from_row`),
/// but is modeled as read-optional so a corrupt NULL is counted as malformed
/// rather than panicking.
struct VocabularyRow {
    body_json: Option<String>,
    provenance_json: String,
}

/// Which SQL to run for a scope. Split out so both aggregators share one scan.
fn scope_sql(scope: VocabularyScope) -> &'static str {
    match scope {
        VocabularyScope::Active => SELECT_ACTIVE_SQL,
        VocabularyScope::All => SELECT_ALL_SQL,
    }
}

/// Extract the provenance `modelName` for a row, or the `(unknown)` sentinel when
/// absent. Provenance carries far more than the model name, so this parses only
/// the one field via a minimal shape rather than the full `Provenance` — a
/// per-row full parse is unnecessary and would couple aggregation to unrelated
/// provenance fields. A provenance blob that will not parse at all is treated as
/// unknown-model (the row is still counted; inspection tolerates imperfect data).
fn model_name_of(provenance_json: &str) -> String {
    // Minimal projection of the one field aggregation reads; `modelName` is the
    // camelCase wire name of `Provenance.model_name`.
    #[derive(serde::Deserialize)]
    struct ModelNameOnly {
        #[serde(rename = "modelName")]
        model_name: Option<String>,
    }
    match serde_json::from_str::<ModelNameOnly>(provenance_json) {
        Ok(parsed) => parsed
            .model_name
            .unwrap_or_else(|| UNKNOWN_MODEL.to_owned()),
        Err(_) => UNKNOWN_MODEL.to_owned(),
    }
}

/// True when a body is EXACTLY the empty JSON array `[]` — the worker's
/// by-design empty-marker convention (`complete_build`). Same test the graph
/// consumers use (`body.as_array().is_some_and(Vec::is_empty)`); kept in step
/// with those sites and the worker's must-stay-in-step banner.
fn is_empty_marker(body: &Value) -> bool {
    body.as_array().is_some_and(Vec::is_empty)
}

/// Mutable per-normalized-name accumulator for the entity aggregation. Counts are
/// held in maps for O(1) folding and rendered into the sorted response vectors at
/// flush. Model/type/source counting share the same shape across both scopes.
struct EntityAccumulator {
    raw_forms: BTreeMap<String, usize>,
    entity_types: std::collections::BTreeSet<String>,
    source_ids: std::collections::HashSet<String>,
    model_counts: BTreeMap<String, usize>,
    total_count: usize,
}

/// Aggregate the entity vocabulary for a scope. Groups fresh entity annotations
/// by NORMALIZED name (the shared `normalize_entity_name`), folding each row's
/// raw `name`, `entityType`, source_id, and provenance model into the group.
/// Empty-marker rows are skipped-and-counted; malformed rows (not the marker, and
/// missing a string `name`) are counted into `malformed_rows` and never grouped.
/// Groups are sorted by normalized name ascending so spelling variants sit
/// adjacent, then clipped to `MAX_GROUPS` (setting `truncated`).
pub(crate) fn entity_vocabulary(
    conn: &Connection,
    scope: VocabularyScope,
) -> Result<EntityVocabulary, ApiError> {
    let type_wire = wire_name(SemanticAnnotationType::Entity)?;
    // Aggregation reads source_id per row, so include it via a dedicated scan
    // that carries source_id alongside body/provenance.
    let (rows, source_id_map, rows_truncated, rows_read) =
        read_rows_with_source(conn, scope, &type_wire)?;

    let mut accumulators: BTreeMap<String, EntityAccumulator> = BTreeMap::new();
    let mut skipped_markers: usize = 0;
    let mut malformed_rows: usize = 0;

    for (index, row) in rows.iter().enumerate() {
        let Some(body) = parse_body(row) else {
            // NULL or unparseable body: an anomaly to reveal, not to fail on.
            malformed_rows += 1;
            continue;
        };
        if is_empty_marker(&body) {
            skipped_markers += 1;
            continue;
        }
        // A well-formed entity body is `{ "name": <str>, "entityType": <str> }`.
        // Anything else (missing/non-string name) is malformed here — never
        // fatal, unlike the graph builder which fails loudly on the same shape.
        let Some(raw_name) = body.get("name").and_then(Value::as_str) else {
            malformed_rows += 1;
            continue;
        };
        let normalized = normalize_entity_name(raw_name);
        let source_id = &source_id_map[index];
        let model = model_name_of(&row.provenance_json);

        let accumulator = accumulators
            .entry(normalized)
            .or_insert_with(|| EntityAccumulator {
                raw_forms: BTreeMap::new(),
                entity_types: std::collections::BTreeSet::new(),
                source_ids: std::collections::HashSet::new(),
                model_counts: BTreeMap::new(),
                total_count: 0,
            });
        *accumulator
            .raw_forms
            .entry(raw_name.to_owned())
            .or_insert(0) += 1;
        if let Some(entity_type) = body.get("entityType").and_then(Value::as_str) {
            accumulator.entity_types.insert(entity_type.to_owned());
        }
        accumulator.source_ids.insert(source_id.clone());
        *accumulator.model_counts.entry(model).or_insert(0) += 1;
        accumulator.total_count += 1;
    }

    // BTreeMap iteration is already normalized-name ascending (variants adjacent).
    let group_total = accumulators.len();
    let groups_truncated = group_total > MAX_GROUPS;
    let groups: Vec<EntityGroup> = accumulators
        .into_iter()
        .take(MAX_GROUPS)
        .map(|(normalized_name, accumulator)| EntityGroup {
            normalized_name,
            raw_forms: accumulator
                .raw_forms
                .into_iter()
                .map(|(raw_form, count)| RawFormCount { raw_form, count })
                .collect(),
            entity_types: accumulator.entity_types.into_iter().collect(),
            source_count: accumulator.source_ids.len(),
            model_counts: accumulator
                .model_counts
                .into_iter()
                .map(|(model_name, count)| ModelCount { model_name, count })
                .collect(),
            total_count: accumulator.total_count,
        })
        .collect();

    Ok(EntityVocabulary {
        groups,
        skipped_markers,
        malformed_rows,
        truncated: rows_truncated || groups_truncated,
        rows_read,
    })
}

/// Mutable per-predicate accumulator for the relation aggregation.
struct RelationAccumulator {
    source_ids: std::collections::HashSet<String>,
    model_counts: BTreeMap<String, usize>,
    total_count: usize,
}

/// Aggregate the relation vocabulary for a scope. Groups fresh relation
/// annotations by the body's `predicate` string, folding source_id and
/// provenance model into each group. Empty markers skipped-and-counted;
/// malformed rows (missing a string `predicate`) counted, never grouped. Groups
/// sorted by predicate, clipped to `MAX_GROUPS`.
pub(crate) fn relation_vocabulary(
    conn: &Connection,
    scope: VocabularyScope,
) -> Result<RelationVocabulary, ApiError> {
    let type_wire = wire_name(SemanticAnnotationType::Relation)?;
    let (rows, source_id_map, rows_truncated, rows_read) =
        read_rows_with_source(conn, scope, &type_wire)?;

    let mut accumulators: BTreeMap<String, RelationAccumulator> = BTreeMap::new();
    let mut skipped_markers: usize = 0;
    let mut malformed_rows: usize = 0;

    for (index, row) in rows.iter().enumerate() {
        let Some(body) = parse_body(row) else {
            malformed_rows += 1;
            continue;
        };
        if is_empty_marker(&body) {
            skipped_markers += 1;
            continue;
        }
        let Some(predicate) = body.get("predicate").and_then(Value::as_str) else {
            malformed_rows += 1;
            continue;
        };
        let source_id = &source_id_map[index];
        let model = model_name_of(&row.provenance_json);

        let accumulator =
            accumulators
                .entry(predicate.to_owned())
                .or_insert_with(|| RelationAccumulator {
                    source_ids: std::collections::HashSet::new(),
                    model_counts: BTreeMap::new(),
                    total_count: 0,
                });
        accumulator.source_ids.insert(source_id.clone());
        *accumulator.model_counts.entry(model).or_insert(0) += 1;
        accumulator.total_count += 1;
    }

    let group_total = accumulators.len();
    let groups_truncated = group_total > MAX_GROUPS;
    let groups: Vec<RelationGroup> = accumulators
        .into_iter()
        .take(MAX_GROUPS)
        .map(|(predicate, accumulator)| RelationGroup {
            predicate,
            total_count: accumulator.total_count,
            source_count: accumulator.source_ids.len(),
            model_counts: accumulator
                .model_counts
                .into_iter()
                .map(|(model_name, count)| ModelCount { model_name, count })
                .collect(),
        })
        .collect();

    Ok(RelationVocabulary {
        groups,
        skipped_markers,
        malformed_rows,
        truncated: rows_truncated || groups_truncated,
        rows_read,
    })
}

/// Run the bounded scan and return the rows, a parallel source_id vector (index
/// i in the row list ↔ index i here), the row-cap truncation flag, and the rows
/// read count. source_id is read per row for the distinct-source count but is not
/// needed as a struct field on `VocabularyRow` elsewhere, so it rides a parallel
/// vector built from the same scan (one query, no second pass).
fn read_rows_with_source(
    conn: &Connection,
    scope: VocabularyScope,
    type_wire: &str,
) -> Result<(Vec<VocabularyRow>, Vec<String>, bool, usize), ApiError> {
    let sql = scope_sql(scope);
    let mut statement = conn
        .prepare(sql)
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to prepare vocabulary scan for {type_wire}: {source}"),
        })?;
    let scan_limit = MAX_ROWS_READ as i64 + 1;
    let mapped = statement
        .query_map(rusqlite::params![type_wire, scan_limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                VocabularyRow {
                    body_json: row.get(1)?,
                    provenance_json: row.get(2)?,
                },
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query vocabulary rows for {type_wire}: {source}"),
        })?;

    let mut rows = Vec::new();
    let mut source_ids = Vec::new();
    for entry in mapped {
        let (source_id, row) = entry.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read vocabulary row for {type_wire}: {source}"),
        })?;
        rows.push(row);
        source_ids.push(source_id);
    }

    let truncated = rows.len() > MAX_ROWS_READ;
    if truncated {
        rows.truncate(MAX_ROWS_READ);
        source_ids.truncate(MAX_ROWS_READ);
    }
    let rows_read = rows.len();
    Ok((rows, source_ids, truncated, rows_read))
}

/// Parse one row's `body_json` into a JSON value, or `None` if the column is NULL
/// or unparseable. A `None` is counted as malformed by the caller — never fatal.
fn parse_body(row: &VocabularyRow) -> Option<Value> {
    let json = row.body_json.as_deref()?;
    serde_json::from_str(json).ok()
}

/// Render a closed `SemanticAnnotationType` through its serde wire name (mirror
/// of `store::enum_wire_name`), so the SQL filter can never drift from the Rust
/// enum or the schema CHECK set.
fn wire_name(annotation_type: SemanticAnnotationType) -> Result<String, ApiError> {
    match serde_json::to_value(annotation_type) {
        Ok(Value::String(name)) => Ok(name),
        other => Err(ApiError::InternalIo {
            message: format!("annotation type did not serialize to a string: {other:?}"),
        }),
    }
}
