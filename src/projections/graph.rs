//! C6f graph projection builder: entity-mention (`graph_entity_mentions`) and
//! entity-edge (`graph_entity_edges`) projections over CA entity/relation
//! annotations, per the D9 resolution — entity node identity is the normalized
//! name, traversal is semantic-only, and the canonical relationship source of
//! truth stays UnitRelationship storage (spec §22, §32).
//!
//! D9 RESOLUTION (2026-07-14) — the binding constraints this module honors,
//! quoted so the code cannot drift from the ruling:
//!   - "Entry = lexical match of query text against stored entity-annotation
//!     names at candidate generation, no LLM in query path." This builder only
//!     WRITES the `normalized_name` lookup surface; the query-time entry is
//!     C7b's, and it MUST reuse `normalize_entity_name` (below) so the query's
//!     match key is byte-identical to the stored key.
//!   - "Edges = SEMANTIC-ONLY traversal (matched entities' target units,
//!     relation annotations touching those entities, far-end entities' target
//!     units); structural UnitRelationships are NEVER walked at query time."
//!     So this builder reads ONLY CA entity/relation annotations, never
//!     `unit_relationships`; the read functions expose exactly the three
//!     semantic hops and nothing structural.
//!   - "ENTITY NODE IDENTITY IS THE NORMALIZED ENTITY NAME (entityType is node
//!     metadata, not identity)." Mentions are keyed and accumulated by
//!     normalized name; `entity_type` rides along as metadata and is never part
//!     of identity or of the edge key.
//!   - "Timing = query-time over indexed hot-plane lookups, no materialized
//!     closures, hop budget = 1 relational hop." This builder materializes NO
//!     closure — only per-annotation mention rows and per-annotation edge rows;
//!     `one_hop_edges` walks exactly one relational hop for C7b.
//!   - "Ordering tiers … within tiers by entity-name match strength,
//!     deterministic tiebreak by unitId ascending." TIER ORDERING IS C7b'S JOB
//!     (fusion is rank-only; within-channel order is the entire score). This
//!     module provides the lookups C7b ranks over — it does not rank.
//!
//! Design facts (approved, spec §3 C6f / §4 D8/D9):
//!   (4) Graph projection per D9: entity mentions keyed by NORMALIZED NAME;
//!       directional relation edges (subject→object) with relation_type =
//!       predicate; one-hop, semantic-only. entityType is metadata, NOT
//!       identity.
//!   (5) Rebuilt from CA entity/relation annotations (read via
//!       `annotations::store::fresh_for_active_parse`), never from Docling.
//!   (7) Envelope via `super::envelope` only (projection_type GraphProjection);
//!       this module hand-writes NO `retrieval_projections` SQL.
//!   (8) PURE functions taking (tx/conn, identifiers); touches NO
//!       scheduler/worker/main. Builds POST-ACTIVATION (integration wires it on
//!       the annotation-worker hook) because it reads FRESH annotations —
//!       running pre-activation would read zero annotations (spec §21 rule 1).

use std::collections::BTreeMap;
use std::time::Instant;

use rusqlite::{Connection, Transaction, params};
use serde_json::Value;
use tracing::{error, info};
use unicode_normalization::UnicodeNormalization;

use super::envelope::{self, NewProjection, ProjectionType};
use crate::error::ApiError;
use crate::ids::new_retrieval_projection_id;
use crate::model::{
    ProducerType, Provenance, ProvenanceInputRef, ProvenanceObjectType, SemanticAnnotation,
    SemanticAnnotationType,
};

/// Stable producer name recorded in the graph projection's Provenance (§20).
/// The graph builder runs no model of its own — it MATERIALIZES CA
/// entity/relation annotations into the mention/edge lookup surface — so its
/// provenance is a deterministic `System` materializer, distinct from the
/// model producer that authored the annotations.
const GRAPH_PRODUCER_NAME: &str = "fabric_graph_materializer";

/// Producer version, bumped when the materialization shape (mention/edge row
/// derivation or the normalization scheme) changes in a way that must
/// invalidate prior payloads.
const PRODUCER_VERSION: &str = "1";

/// Delete every prior `graph_entity_mentions` row for a parse. Rebuild
/// idempotence (design fact per brief): a rebuild REPLACES rather than
/// accumulates, so both payload tables are cleared for the parse before the
/// fresh rows are inserted, in a fixed order (mentions then edges) so a rebuild
/// is deterministic. These payload tables are NOT envelope-owned, so clearing
/// them here does not violate the envelope single-source rule (which governs
/// only `retrieval_projections`).
const DELETE_MENTIONS_FOR_PARSE_SQL: &str = "
DELETE FROM graph_entity_mentions WHERE parse_id = ?1";

/// Delete every prior `graph_entity_edges` row for a parse (see the mentions
/// delete for the rebuild-idempotence rationale and the fixed delete order).
const DELETE_EDGES_FOR_PARSE_SQL: &str = "
DELETE FROM graph_entity_edges WHERE parse_id = ?1";

/// Insert one `graph_entity_mentions` row: one row per (parse, normalized
/// entity name), with `unit_ids_json` the canonical JSON string array of the
/// deduplicated, deterministically ordered ContentUnit IDs the entity's
/// annotations target, and `entity_type` the body's entityType (metadata, not
/// identity — D9).
const INSERT_MENTION_SQL: &str = "
INSERT INTO graph_entity_mentions (
  id, projection_id, source_id, parse_id, normalized_name, entity_type,
  unit_ids_json, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";

/// Insert one `graph_entity_edges` row: one row per CA relation annotation,
/// directional (from_normalized_name = normalized subject, to_normalized_name =
/// normalized object), relation_type = the relation's predicate, and
/// `target_unit_ids_json` the canonical JSON string array of the relation
/// annotation's target ContentUnit IDs (the edge's supporting units).
const INSERT_EDGE_SQL: &str = "
INSERT INTO graph_entity_edges (
  id, projection_id, source_id, parse_id, from_normalized_name,
  to_normalized_name, relation_type, target_unit_ids_json, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";

/// Direct mentions of one normalized entity name within a parse: the row's
/// `unit_ids_json`, decoded by the caller. One row per (parse, normalized_name)
/// by construction, so this reads at most one row. Uses the
/// `idx_graph_entity_mentions_parse_name` index (D9 entry lookup).
const SELECT_MENTIONS_FOR_NAME_SQL: &str = "
SELECT unit_ids_json FROM graph_entity_mentions
WHERE parse_id = ?1 AND normalized_name = ?2";

/// One-hop edges touching a normalized entity name in EITHER direction within a
/// parse (D9 edges, one relational hop). The two-arm query is a union of the
/// `idx_graph_entity_edges_parse_from` and `idx_graph_entity_edges_parse_to`
/// index lookups; each arm yields the far-end normalized name and the edge's
/// supporting units, so C7b can reach "relation annotations touching the
/// matched entity → far-end entities". Ordered deterministically so repeated
/// reads return a stable sequence.
const SELECT_ONE_HOP_EDGES_SQL: &str = "
SELECT to_normalized_name AS far_name, relation_type, target_unit_ids_json,
       'out' AS direction
FROM graph_entity_edges
WHERE parse_id = ?1 AND from_normalized_name = ?2
UNION ALL
SELECT from_normalized_name AS far_name, relation_type, target_unit_ids_json,
       'in' AS direction
FROM graph_entity_edges
WHERE parse_id = ?1 AND to_normalized_name = ?2
ORDER BY far_name, relation_type, target_unit_ids_json, direction";

/// Every stored normalized entity name for a parse, ascending (D9-amendment
/// fuzzy scan surface, CA2-P2 2026-07-19). One row per (parse, normalized_name)
/// by construction, so this enumerates each stored name exactly once. `ORDER BY
/// normalized_name` gives a DETERMINISTIC scan order so the fuzzy match classes
/// C7b runs over this set are order-stable across reads. Uses the
/// `idx_graph_entity_mentions_parse_name` index (the same index the exact
/// entry probes). Names only — no unit ids — because the fuzzy classes select
/// WHICH stored names a query matches; the matched names are then resolved to
/// units through the existing `mentions_for_name` / `one_hop_edges` lookups.
const SELECT_ENTITY_NAMES_FOR_PARSE_SQL: &str = "
SELECT normalized_name FROM graph_entity_mentions
WHERE parse_id = ?1
ORDER BY normalized_name";

/// Normalize a raw entity name to its node identity (D9: entity node identity
/// IS the normalized name). The scheme is deterministic and is the SINGLE
/// SOURCE OF TRUTH for entity-name identity: C7b's query-time entry MUST call
/// this exact function so the query's match key is byte-identical to the stored
/// `normalized_name`, else two spellings of one entity would map to different
/// nodes at build vs. query time (PRINCIPLES.md single-source rule).
///
/// Steps, applied in this fixed order (order matters — see justification):
///   1. Trim leading/trailing ASCII+Unicode whitespace, then collapse every
///      internal run of whitespace to a single ASCII space. Whitespace-only
///      differences ("Acme  Corp" vs "Acme Corp") must not fork the node.
///   2. NFC-normalize (Unicode Normalization Form C) via the SAME
///      `unicode_normalization` crate `crate::canonical` uses. NFC is applied
///      so the node identity agrees with the canonical serialization form (a
///      name stored anywhere as canonical JSON is already NFC), and so
///      combining-mark vs. precomposed spellings of one name collapse together.
///      `canonical.rs` applies NFC internally inside `canonical_json_bytes_of`
///      and exposes no standalone NFC fn, so this applies `nfc()` directly.
///   3. Unicode-aware lowercase via `str::to_lowercase` (NOT ASCII-only
///      `to_ascii_lowercase`). Case fold is chosen over ASCII-lowercase because
///      entity names are frequently non-ASCII (accented Latin, Greek, Cyrillic)
///      and only Unicode-aware lowercasing folds e.g. 'İ'→'i̇', 'Σ'→'σ';
///      ASCII-lowercase would leave those uppercased, forking the node.
///
/// Why NFC BEFORE lowercase: `to_lowercase` can re-introduce
/// non-NFC sequences for some code points, so a final NFC pass is applied AFTER
/// lowercasing to guarantee the output is NFC regardless of case-mapping
/// expansion. The result is therefore idempotent: normalizing an
/// already-normalized name returns it unchanged.
pub(crate) fn normalize_entity_name(raw: &str) -> String {
    // Step 1: trim + collapse internal whitespace runs to a single ASCII space.
    let whitespace_collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    // Step 2: NFC before case folding.
    let nfc: String = whitespace_collapsed.nfc().collect();
    // Step 3: Unicode-aware lowercase, then a final NFC pass because case
    // mapping can emit non-NFC sequences (see the doc note on ordering).
    lowered_then_nfc(&nfc)
}

/// Apply Unicode-aware lowercasing and re-NFC the result, so the returned
/// string is guaranteed NFC even when a case mapping expanded a code point into
/// a non-NFC sequence. Split out only to keep `normalize_entity_name` readable.
fn lowered_then_nfc(nfc: &str) -> String {
    let lowered = nfc.to_lowercase();
    lowered.nfc().collect()
}

/// One entity mention accumulated across a parse's entity annotations, keyed by
/// normalized name. `unit_ids` accumulates (deduplicated, then deterministically
/// ordered on flush) the target ContentUnit IDs of every entity annotation that
/// normalizes to this name; `entity_type` is the first-seen entityType metadata
/// for the name (D9: entityType is metadata, not identity, so a name that
/// appears with two entityTypes is still ONE node — the first-seen type is kept
/// and the divergence is logged, never used to split the node).
struct MentionAccumulator {
    unit_ids: std::collections::BTreeSet<String>,
    entity_type: Option<String>,
}

/// Build the graph projection for a parse: open a `GraphProjection` envelope,
/// materialize the parse's FRESH CA entity annotations into
/// `graph_entity_mentions` rows (keyed by normalized name) and its FRESH CA
/// relation annotations into directional `graph_entity_edges` rows, then
/// complete the envelope fresh. On any failure after the envelope opens, the
/// envelope is marked failed so the failed build is visible truth (spec §22),
/// never a dangling `building` row.
///
/// Lifecycle phase (reported for integration): MUST run POST-activation. It
/// reads fresh annotations through `annotations::store::fresh_for_active_parse`,
/// whose active-parse subselect returns rows only once the parse is the
/// source's active parse; running it pre-activation would read zero annotations
/// and materialize an empty graph. Recommendation: wire it on the
/// annotation-worker completion hook, after the entity/relation annotations are
/// fresh.
///
/// Payload archival decision (reported): the graph payload is NOT re-archived
/// to the artifact store. The mention/edge rows ARE the payload and live in
/// their hot-plane tables; the source names/relations live durably on the CA
/// annotations linked via `input_annotation_ids`. So the envelope completes
/// with `payload_uri = None` (the same "payload lives elsewhere" shape
/// `envelope::complete_fresh` documents for a None URI).
///
/// REBUILD idempotence: this builder DOES delete prior `graph_entity_mentions`
/// and `graph_entity_edges` rows for the parse (fixed order: mentions then
/// edges) before inserting, because those payload tables are NOT envelope-owned
/// and clearing them is this builder's responsibility. It does NOT delete prior
/// `GraphProjection` envelopes — that is an envelope operation `super::envelope`
/// exposes none for (reported as missing); integration MUST delete-for-parse
/// the envelope before invoking so a rebuild replaces rather than accumulates.
pub(crate) fn build_graph_projection(
    tx: &Transaction<'_>,
    conn: &Connection,
    source_id: &str,
    parse_id: &str,
) -> Result<String, ApiError> {
    let started = Instant::now();
    info!(
        event = "graph.build_started",
        source_id, parse_id, "graph projection build starting"
    );

    // Read fresh annotations for the source's active parse; freshness AND
    // active-parse scoping are enforced inside the store query (design fact 5),
    // so this builder trusts the returned set. Filter to the requested parse
    // defensively so a caller passing a non-active parse_id materializes an
    // empty graph rather than another parse's annotations.
    let annotations: Vec<SemanticAnnotation> =
        crate::annotations::store::fresh_for_active_parse(conn, source_id)?
            .into_iter()
            .filter(|annotation| annotation.parse_id == parse_id)
            .collect();

    // Partition into the two semantic types this builder consumes; all other
    // annotation types are irrelevant to the graph channel and are ignored.
    let entity_annotations: Vec<&SemanticAnnotation> = annotations
        .iter()
        .filter(|a| a.annotation_type == SemanticAnnotationType::Entity)
        .collect();
    let relation_annotations: Vec<&SemanticAnnotation> = annotations
        .iter()
        .filter(|a| a.annotation_type == SemanticAnnotationType::Relation)
        .collect();

    // Accumulate mentions and derive edge rows OUTSIDE the envelope, so a
    // corrupt annotation body fails the build before any envelope row exists.
    // Each helper also reports how many empty-marker rows (body EXACTLY `[]`) it
    // skipped; the counts are surfaced in the success log below.
    let AccumulatedMentions {
        mentions,
        skipped_markers: skipped_entity_markers,
    } = accumulate_mentions(&entity_annotations)?;
    let DerivedEdges {
        edges,
        skipped_markers: skipped_relation_markers,
    } = derive_edges(&relation_annotations)?;

    // input_annotation_ids = every entity+relation annotation consumed, so the
    // envelope's lineage names exactly the annotations this graph was built
    // from (spec §22 inputAnnotationIds). Deterministic order (sorted) so
    // repeated builds over the same set produce identical envelope payloads.
    // DELIBERATE: skipped empty-marker rows REMAIN in this lineage — the builder
    // consumed them (it read and classified each), and the skip is made visible
    // via the logged `skipped_entity_markers`/`skipped_relation_markers` counts,
    // not by omitting them from lineage.
    let mut input_annotation_ids: Vec<String> = entity_annotations
        .iter()
        .chain(relation_annotations.iter())
        .map(|a| a.id.clone())
        .collect();
    input_annotation_ids.sort();
    input_annotation_ids.dedup();

    let producer = graph_producer(&input_annotation_ids);
    let request = NewProjection {
        source_id: source_id.to_owned(),
        parse_id: parse_id.to_owned(),
        projection_type: ProjectionType::GraphProjection,
        // A graph projection is annotation-derived: no input UNIT ids; its
        // lineage is the source annotations (spec §22 inputAnnotationIds).
        input_unit_ids: None,
        input_annotation_ids: Some(input_annotation_ids.clone()),
        producer,
        index_name: None,
        index_partition: None,
    };

    let projection_id = envelope::insert_building(tx, &request)?;

    // From here on, any failure marks the envelope failed (visible truth, spec
    // §22) and returns the original error unmasked.
    let mention_rows = mentions.len();
    let edge_rows = edges.len();
    if let Err(build_error) =
        persist_payload(tx, &projection_id, source_id, parse_id, &mentions, &edges)
    {
        record_build_failure(tx, &projection_id, source_id, parse_id, &build_error);
        return Err(build_error);
    }

    // The graph payload lives in its hot-plane tables / on the source
    // annotations, so the envelope completes with payload_uri = None.
    if let Err(complete_error) = envelope::complete_fresh(tx, &projection_id, None) {
        record_build_failure(tx, &projection_id, source_id, parse_id, &complete_error);
        return Err(complete_error);
    }

    info!(
        event = "graph.build_succeeded",
        source_id,
        parse_id,
        projection_id = %projection_id,
        entity_annotation_count = entity_annotations.len(),
        relation_annotation_count = relation_annotations.len(),
        skipped_entity_markers,
        skipped_relation_markers,
        mention_rows,
        edge_rows,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "graph projection build succeeded"
    );
    Ok(projection_id)
}

/// One derived edge row awaiting insertion: the normalized endpoint pair, the
/// predicate as relation_type, and the relation annotation's target units. Held
/// as an intermediate so all bodies are validated before the envelope opens.
struct DerivedEdge {
    from_normalized_name: String,
    to_normalized_name: String,
    relation_type: String,
    target_unit_ids: Vec<String>,
}

/// The accumulated mentions plus the count of empty-marker entity annotations
/// skipped while accumulating. The count is threaded out (rather than logged
/// inside the helper) so the build-success log carries it alongside the other
/// per-build facts; see `build_graph_projection`.
struct AccumulatedMentions {
    mentions: BTreeMap<String, MentionAccumulator>,
    skipped_markers: usize,
}

/// Accumulate entity annotations into per-normalized-name mentions. For each
/// entity annotation: read its `{ "name", "entityType" }` body, normalize the
/// name to its node identity, and fold the annotation's target_unit_ids into
/// that name's unit set (a BTreeSet gives dedup + deterministic order in one
/// step, so the mention row's unit_ids_json is stable across rebuilds). The
/// entityType is metadata: the first-seen value is kept per name; a name later
/// seen with a DIFFERENT entityType still maps to the SAME node (D9 identity
/// rule) and the divergence is logged (no contents), never used to fork.
///
/// EMPTY-MARKER SKIP (visible, counted — distinct from malformed-body failure):
/// the annotation worker records an EMPTY producer result as a fresh annotation
/// whose body is EXACTLY `[]` (an empty JSON array), so the freshness key stays
/// satisfied and the work is not rediscovered every cycle. That by-design marker
/// carries no entity, so it is skipped here and the skip is COUNTED (surfaced in
/// the build-success log), never fed to `entity_name`. This is NOT a silent drop
/// of a malformed body: any OTHER body missing a string `name` still fails
/// loudly via `entity_name`. Only the exact `[]` shape is skipped.
/// MUST STAY IN STEP with the worker's empty-marker write site
/// (`crate::annotations::worker::complete_build`) and the two sibling consumer
/// skips (`derive_edges` here, `crate::projections::view::build_summary`); the
/// worker's must-stay-in-step banner names all three.
fn accumulate_mentions(
    entity_annotations: &[&SemanticAnnotation],
) -> Result<AccumulatedMentions, ApiError> {
    // BTreeMap keyed by normalized name → deterministic mention-row emission
    // order (name ascending) independent of annotation read order.
    let mut mentions: BTreeMap<String, MentionAccumulator> = BTreeMap::new();
    let mut skipped_markers: usize = 0;

    for annotation in entity_annotations {
        // Skip the empty-marker row (body is EXACTLY `[]`) before extraction; it
        // encodes "no entities found", not a corrupt entity (see doc above).
        if annotation.body.as_array().is_some_and(Vec::is_empty) {
            skipped_markers += 1;
            continue;
        }
        let raw_name = entity_name(annotation)?;
        let entity_type = entity_type(annotation);
        let normalized = normalize_entity_name(&raw_name);

        let accumulator = mentions.entry(normalized).or_insert(MentionAccumulator {
            unit_ids: std::collections::BTreeSet::new(),
            entity_type: entity_type.clone(),
        });
        // First-seen entityType wins; a later divergence is metadata noise, not
        // an identity fork (D9). Logged without contents so the divergence is
        // observable without leaking annotation bodies.
        if accumulator.entity_type != entity_type {
            info!(
                event = "graph.entity_type_divergence",
                annotation_id = %annotation.id,
                "entity name seen with a differing entityType; keeping first-seen \
                 (name identity is normalized name only, entityType is metadata)"
            );
        }
        for unit_id in &annotation.target_unit_ids {
            accumulator.unit_ids.insert(unit_id.clone());
        }
    }

    Ok(AccumulatedMentions {
        mentions,
        skipped_markers,
    })
}

/// The derived edge rows plus the count of empty-marker relation annotations
/// skipped while deriving. The count is threaded out for the build-success log
/// (see `build_graph_projection`), mirroring `AccumulatedMentions`.
struct DerivedEdges {
    edges: Vec<DerivedEdge>,
    skipped_markers: usize,
}

/// Derive one directional edge per relation annotation. For each relation
/// annotation: read its `{ "subject", "predicate", "object" }` body, normalize
/// subject and object to their node identities, and emit an edge
/// (subject→object) with relation_type = predicate and the relation
/// annotation's target_unit_ids as supporting units. Directional semantics are
/// preserved (subject is the from-node, object is the to-node): C7b's two-arm
/// `one_hop_edges` reader restores both-direction traversal, so this builder
/// stores each edge once in its natural direction rather than duplicating it.
///
/// EMPTY-MARKER SKIP (visible, counted — distinct from malformed-body failure):
/// an empty producer result is recorded by the worker as a fresh annotation
/// whose body is EXACTLY `[]` (see `accumulate_mentions` for the full rationale).
/// That by-design marker carries no relation, so it is skipped here and the skip
/// is COUNTED, never fed to `relation_triple`. Any OTHER body missing a string
/// `subject`/`predicate`/`object` still fails loudly via `relation_triple`; only
/// the exact `[]` shape is skipped.
/// MUST STAY IN STEP with the worker's empty-marker write site
/// (`crate::annotations::worker::complete_build`) and the two sibling consumer
/// skips (`accumulate_mentions` here, `crate::projections::view::build_summary`);
/// the worker's must-stay-in-step banner names all three.
fn derive_edges(relation_annotations: &[&SemanticAnnotation]) -> Result<DerivedEdges, ApiError> {
    let mut edges = Vec::with_capacity(relation_annotations.len());
    let mut skipped_markers: usize = 0;
    for annotation in relation_annotations {
        // Skip the empty-marker row (body is EXACTLY `[]`) before extraction; it
        // encodes "no relations found", not a corrupt relation (see doc above).
        if annotation.body.as_array().is_some_and(Vec::is_empty) {
            skipped_markers += 1;
            continue;
        }
        let (subject, predicate, object) = relation_triple(annotation)?;
        edges.push(DerivedEdge {
            from_normalized_name: normalize_entity_name(&subject),
            to_normalized_name: normalize_entity_name(&object),
            relation_type: predicate,
            target_unit_ids: annotation.target_unit_ids.clone(),
        });
    }
    Ok(DerivedEdges {
        edges,
        skipped_markers,
    })
}

/// Clear the parse's prior payload rows (mentions then edges, fixed order) and
/// insert the freshly derived mention and edge rows. Runs on the caller's
/// transaction so the whole payload swap commits or rolls back with the
/// envelope lifecycle (atomicity mirror of `super::envelope`). Every mention
/// row gets a fresh `proj_` id; each carries the envelope's projection_id so a
/// row maps back to its envelope.
fn persist_payload(
    tx: &Transaction<'_>,
    projection_id: &str,
    source_id: &str,
    parse_id: &str,
    mentions: &BTreeMap<String, MentionAccumulator>,
    edges: &[DerivedEdge],
) -> Result<(), ApiError> {
    // Rebuild idempotence: delete prior rows for the parse before inserting, in
    // the fixed mentions-then-edges order.
    tx.execute(DELETE_MENTIONS_FOR_PARSE_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to delete prior graph mentions for parse {parse_id}: {source}"
            ),
        })?;
    tx.execute(DELETE_EDGES_FOR_PARSE_SQL, params![parse_id])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to delete prior graph edges for parse {parse_id}: {source}"),
        })?;

    let now = crate::primitives::utc_now()?;

    for (normalized_name, accumulator) in mentions {
        let id = new_retrieval_projection_id()?;
        // Deterministic array from the BTreeSet (dedup already applied on
        // insert); canonical JSON so the stored bytes are stable per §16.2.
        let unit_ids: Vec<&String> = accumulator.unit_ids.iter().collect();
        let unit_ids_json = canonical_json_string_of(
            &unit_ids,
            &format!("unit ids for graph mention {normalized_name} of parse {parse_id}"),
        )?;
        tx.execute(
            INSERT_MENTION_SQL,
            params![
                id,
                projection_id,
                source_id,
                parse_id,
                normalized_name,
                accumulator.entity_type,
                unit_ids_json,
                now,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to insert graph mention for parse {parse_id}: {source}"),
        })?;
    }

    for edge in edges {
        let id = new_retrieval_projection_id()?;
        let target_unit_ids_json = canonical_json_string_of(
            &edge.target_unit_ids,
            &format!(
                "target unit ids for graph edge {}→{} of parse {parse_id}",
                edge.from_normalized_name, edge.to_normalized_name
            ),
        )?;
        tx.execute(
            INSERT_EDGE_SQL,
            params![
                id,
                projection_id,
                source_id,
                parse_id,
                edge.from_normalized_name,
                edge.to_normalized_name,
                edge.relation_type,
                target_unit_ids_json,
                now,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to insert graph edge for parse {parse_id}: {source}"),
        })?;
    }

    Ok(())
}

/// The direct-mention units of one normalized entity name within a parse (D9
/// entry): the decoded `unit_ids` of the single `graph_entity_mentions` row for
/// (parse, normalized_name), or an empty vec when the name has no mention row.
///
/// C7b MUST normalize its query-derived name with `normalize_entity_name`
/// BEFORE calling this so its lookup key matches the stored key. Returning
/// (normalized_name, unit_ids) lets C7b intersect the unit sets ACROSS multiple
/// matched entities to compute D9 ordering tier 1 (units connected to MORE THAN
/// ONE matched entity); the tier RANKING itself is C7b's job, not this
/// module's.
fn read_mention_units(
    conn: &Connection,
    parse_id: &str,
    normalized_name: &str,
) -> Result<Vec<String>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_MENTIONS_FOR_NAME_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare graph mention query for parse {parse_id}: {source}"
            ),
        })?;
    let mut rows = statement
        .query(params![parse_id, normalized_name])
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query graph mention for parse {parse_id}: {source}"),
        })?;

    // At most one row per (parse, normalized_name) by construction; read the
    // first if present, else an empty unit set (an unmentioned name).
    if let Some(row) = rows.next().map_err(|source| ApiError::StorageOperation {
        message: format!("failed to read graph mention row for parse {parse_id}: {source}"),
    })? {
        let unit_ids_json: String = row.get(0).map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to read graph mention unit ids for parse {parse_id}: {source}"
            ),
        })?;
        parse_json_column(
            &unit_ids_json,
            &format!("unit ids of graph mention {normalized_name} in parse {parse_id}"),
        )
    } else {
        Ok(Vec::new())
    }
}

/// The direct-mention units of a normalized entity name (public C7b entry
/// point; see `read_mention_units` for the D9 semantics and the caller's
/// normalize-first obligation). Returned as (normalized_name, unit_ids) so C7b
/// can intersect across matched entities for tier-1 ordering.
pub(crate) fn mentions_for_name(
    conn: &Connection,
    parse_id: &str,
    normalized_name: &str,
) -> Result<(String, Vec<String>), ApiError> {
    let unit_ids = read_mention_units(conn, parse_id, normalized_name)?;
    Ok((normalized_name.to_owned(), unit_ids))
}

/// One far-end reach of a one-hop traversal from a matched entity (D9 edges):
/// the far-end entity's normalized name, the edge's relation_type, the edge's
/// supporting units, and the direction the edge was traversed (`Out` = the
/// matched name was the subject/from-node; `In` = it was the object/to-node).
/// C7b reads the far-end's own mention units via `mentions_for_name` to reach
/// "far-end entities' target units"; this struct supplies the far name and the
/// edge's own supporting units (the middle hop).
pub(crate) struct OneHopEdge {
    pub(crate) far_normalized_name: String,
    pub(crate) relation_type: String,
    pub(crate) target_unit_ids: Vec<String>,
    pub(crate) direction: EdgeDirection,
}

/// Which end of the stored directional edge the matched entity sat on. The edge
/// is stored once in its natural subject→object direction; this records whether
/// the one-hop reader matched the from-node (`Out`) or the to-node (`In`), so
/// C7b keeps directional semantics without the builder duplicating edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeDirection {
    Out,
    In,
}

/// One-hop edges touching a normalized entity name in BOTH directions within a
/// parse (D9 edges, one relational hop; hop budget = 1). Returns, for every
/// edge where the name is the from- OR the to-node, the far-end normalized name
/// plus the edge's relation_type and supporting units. This is the "relation
/// annotations touching the matched entity → far-end entities" hop of D9's
/// traversal; C7b follows each far name back through `mentions_for_name` to
/// reach the far entities' units. NO materialized closure is walked (D9): this
/// is a single indexed two-arm lookup per call.
///
/// C7b MUST pass a name already run through `normalize_entity_name`.
pub(crate) fn one_hop_edges(
    conn: &Connection,
    parse_id: &str,
    normalized_name: &str,
) -> Result<Vec<OneHopEdge>, ApiError> {
    let mut statement =
        conn.prepare(SELECT_ONE_HOP_EDGES_SQL)
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to prepare graph one-hop query for parse {parse_id}: {source}"
                ),
            })?;
    let rows = statement
        .query_map(params![parse_id, normalized_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query graph one-hop edges for parse {parse_id}: {source}"),
        })?;

    let mut edges = Vec::new();
    for row in rows {
        let (far_name, relation_type, target_unit_ids_json, direction) =
            row.map_err(|source| ApiError::StorageOperation {
                message: format!("failed to read graph edge row for parse {parse_id}: {source}"),
            })?;
        let target_unit_ids = parse_json_column(
            &target_unit_ids_json,
            &format!("target unit ids of a graph edge in parse {parse_id}"),
        )?;
        let direction = match direction.as_str() {
            "out" => EdgeDirection::Out,
            "in" => EdgeDirection::In,
            // Unreachable: the SQL emits exactly these two literals; kept
            // explicit so a query edit cannot silently misclassify direction.
            other => {
                return Err(ApiError::StorageOperation {
                    message: format!(
                        "graph edge in parse {parse_id} has unexpected direction {other}"
                    ),
                });
            }
        };
        edges.push(OneHopEdge {
            far_normalized_name: far_name,
            relation_type,
            target_unit_ids,
            direction,
        });
    }
    Ok(edges)
}

/// Enumerate every stored NORMALIZED entity name for a parse, ascending (D9
/// amendment, CA2-P2 2026-07-19): the scan surface the graph channel's fuzzy
/// match classes (acronym, token-prefix) run over. Returns the names already in
/// their node-identity form (they were stored via `normalize_entity_name`), so
/// C7b compares its already-normalized query tokens/n-grams against them
/// directly — no re-normalization of the stored side is needed or performed.
///
/// Deterministic order (ascending) so the fuzzy scan and the candidate cap C7b
/// applies over it are order-stable across repeated reads. Per-parse name counts
/// are bounded (entity vocabulary per source is small), so enumerating the whole
/// set and matching in Rust is the intended shape — this is NOT a per-name
/// indexed probe like `mentions_for_name`. Reads only the passed parse (scope is
/// enforced by the caller iterating only in-scope parses, §6/§38).
pub(crate) fn entity_names_for_parse(
    conn: &Connection,
    parse_id: &str,
) -> Result<Vec<String>, ApiError> {
    let mut statement = conn
        .prepare(SELECT_ENTITY_NAMES_FOR_PARSE_SQL)
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to prepare graph entity-name enumeration for parse {parse_id}: {source}"
            ),
        })?;
    let rows = statement
        .query_map(params![parse_id], |row| row.get::<_, String>(0))
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to query graph entity names for parse {parse_id}: {source}"),
        })?;

    let mut names = Vec::new();
    for row in rows {
        let name = row.map_err(|source| ApiError::StorageOperation {
            message: format!("failed to read graph entity-name row for parse {parse_id}: {source}"),
        })?;
        names.push(name);
    }
    Ok(names)
}

/// Extract the `name` string from an entity annotation's `{ "name",
/// "entityType" }` body. A missing or non-string `name` is a corrupt entity
/// annotation surfaced with the annotation id — the materialization reflects
/// its inputs honestly and fails loudly on a malformed body. The one body shape
/// NOT reaching here is the by-design empty-marker (`[]`), which `accumulate_
/// mentions` skips and counts BEFORE calling this; that visible, counted skip is
/// not a malformed body and is not this function's concern.
fn entity_name(annotation: &SemanticAnnotation) -> Result<String, ApiError> {
    annotation
        .body
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ApiError::StorageOperation {
            message: format!(
                "entity annotation {} has no string `name` body field",
                annotation.id
            ),
        })
}

/// Extract the optional `entityType` string from an entity annotation body.
/// entityType is METADATA (D9), so an absent or non-string value is not an
/// error — it simply yields `None` (the mention row's entity_type column is
/// nullable). A whitespace-only value is treated as absent.
fn entity_type(annotation: &SemanticAnnotation) -> Option<String> {
    annotation
        .body
        .get("entityType")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
}

/// Extract the (subject, predicate, object) triple from a relation annotation's
/// `{ "subject", "predicate", "object" }` body. Any missing or non-string
/// field is a corrupt relation annotation surfaced with the annotation id: an
/// edge with no endpoint or no predicate is meaningless, so it fails the build
/// loudly rather than being silently dropped. The by-design empty-marker (`[]`)
/// body never reaches here — `derive_edges` skips and counts it BEFORE calling
/// this; that visible, counted skip is not a malformed body.
fn relation_triple(annotation: &SemanticAnnotation) -> Result<(String, String, String), ApiError> {
    let field = |name: &str| -> Result<String, ApiError> {
        annotation
            .body
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| ApiError::StorageOperation {
                message: format!(
                    "relation annotation {} has no string `{name}` body field",
                    annotation.id
                ),
            })
    };
    Ok((field("subject")?, field("predicate")?, field("object")?))
}

/// Assemble the graph producer Provenance (§20): a `System` materializer naming
/// the source entity/relation annotations as its inputs. Distinct from the
/// model producer that authored the annotations — this producer only
/// materializes/links, so it carries no model or prompt fields.
fn graph_producer(input_annotation_ids: &[String]) -> Provenance {
    Provenance {
        producer_type: ProducerType::System,
        producer_name: GRAPH_PRODUCER_NAME.to_owned(),
        producer_version: Some(PRODUCER_VERSION.to_owned()),
        config_hash: None,
        model_name: None,
        model_version: None,
        prompt_hash: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: Some(
            input_annotation_ids
                .iter()
                .map(|id| ProvenanceInputRef {
                    object_type: ProvenanceObjectType::SemanticAnnotation,
                    id: id.clone(),
                })
                .collect(),
        ),
    }
}

/// Mark an already-opened envelope failed on a post-open build failure, so the
/// failed build is visible truth (spec §22) rather than a dangling `building`
/// row. A secondary failure marking-failed is logged (the row may already be
/// gone) but never masks the original error, which the caller still returns.
fn record_build_failure(
    tx: &Transaction<'_>,
    projection_id: &str,
    source_id: &str,
    parse_id: &str,
    cause: &ApiError,
) {
    error!(
        event = "graph.build_failed",
        source_id,
        parse_id,
        projection_id,
        error = %cause,
        "graph projection build failed after envelope opened; marking failed"
    );
    if let Err(mark_error) = envelope::mark_failed(tx, projection_id, &cause.to_string()) {
        error!(
            event = "graph.mark_failed_failed",
            projection_id,
            error = %mark_error,
            "failed to mark graph projection failed after a build failure"
        );
    }
}

/// Render any model shape as a canonical JSON string for a *_json column
/// (deterministic bytes per spec §16.2, same policy as `super::envelope` and
/// `crate::annotations::store`).
fn canonical_json_string_of<T: serde::Serialize>(
    value: &T,
    what: &str,
) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the error
    // arm keeps the panic-free Result policy instead of unwrapping.
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}

/// Parse one persisted *_json column back into its model shape. A stored value
/// that no longer matches the model is a corruption surfaced with the column's
/// identity, never silently dropped (mirror of the `super::envelope` reader).
fn parse_json_column<T: serde::de::DeserializeOwned>(
    json: &str,
    what: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(json).map_err(|source| ApiError::StorageOperation {
        message: format!("persisted {what} is unparseable: {source}"),
    })
}
