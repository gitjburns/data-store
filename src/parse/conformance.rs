//! Conformance measurement (spec §12.5): measures what a validated parse
//! actually contains — type counts, locator coverage, caption pairing,
//! table decomposition — producing the ConformanceReport the §13.3
//! activation dominance rule compares dimension by dimension. Measurement is
//! pure: no IO, no clock, no storage. The importer (C4b) validates the
//! candidates first, supplies the timestamp, and persists the report;
//! conformance never gates anything here — it only measures truthfully.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::ApiError;
use crate::model::{ConformanceReport, ContentType, UnitRelationshipType};
use crate::parse::bundle::{CandidateContentUnit, CandidateUnitRelationship};

/// Relationship types that express containment descent (spec §19: page
/// contains block, table contains row, ...). Used by the table-decomposition
/// walk: a table counts as decomposed when a `table_cell` is reachable from
/// it through these edges (table → row → cell, or table → cell directly).
/// `appears_on`, caption edges, and ordering edges are placement/pairing
/// relations, not containment, and deliberately do not count as descent.
const CONTAINMENT_TYPES: [UnitRelationshipType; 3] = [
    UnitRelationshipType::Contains,
    UnitRelationshipType::PhysicallyContains,
    UnitRelationshipType::LogicallyContains,
];

/// Dimension keys of the `dimensions` map (spec §12.5). These are a stable
/// persisted contract: the §13.3 dominance rule compares reports of
/// different parses key by key, so renaming a key silently breaks dominance
/// comparisons against previously persisted reports. Every dimension is
/// oriented so that HIGHER is better; adding a dimension makes holds more
/// likely (one more axis a re-parse can regress on), so additions must be
/// deliberate.
const DIMENSION_LOCATOR_COVERAGE: &str = "locator_coverage";
const DIMENSION_CAPTION_PAIRING_RATE: &str = "caption_pairing_rate";
const DIMENSION_TABLE_DECOMPOSITION_RATE: &str = "table_decomposition_rate";
const DIMENSION_RELATIONSHIP_COVERAGE: &str = "relationship_coverage";

/// Measure the §12.5 conformance report over one parse's VALIDATED candidate
/// records (the §13.1 hard gates have already passed, so every local
/// reference resolves and every type is well-formed). `measured_at` is
/// supplied by the caller so this function stays pure and deterministic:
/// identical candidates always yield an identical report body and
/// `reportHash` for a given timestamp. The two optional rates are `None`
/// (and absent from `dimensions`) when their subject population is empty —
/// an unmeasurable rate is reported as unmeasured, never as 0 or 1; the
/// dominance rule (C5a) owns comparing reports whose dimension sets differ.
pub(crate) fn measure(
    parse_id: &str,
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
    measured_at: &str,
) -> Result<ConformanceReport, ApiError> {
    let mut unit_type_counts: BTreeMap<String, u64> = BTreeMap::new();
    for unit in units {
        *unit_type_counts
            .entry(unit.content_type.wire_name().to_owned())
            .or_insert(0) += 1;
    }

    let mut relationship_type_counts: BTreeMap<String, u64> = BTreeMap::new();
    for relationship in relationships {
        *relationship_type_counts
            .entry(relationship.relationship_type.wire_name().to_owned())
            .or_insert(0) += 1;
    }

    let locator_coverage = measure_locator_coverage(units);
    let caption_pairing_rate = measure_caption_pairing_rate(units, relationships);
    let table_decomposition_rate = measure_table_decomposition_rate(units, relationships);
    let relationship_coverage = measure_relationship_coverage(units, relationships);

    // The dimensions map is what §13.3 actually compares; the named report
    // fields above are the spec's fixed view of the same measurements.
    // Optional rates enter the map only when measurable.
    let mut dimensions: BTreeMap<String, f64> = BTreeMap::new();
    dimensions.insert(DIMENSION_LOCATOR_COVERAGE.to_owned(), locator_coverage);
    dimensions.insert(
        DIMENSION_RELATIONSHIP_COVERAGE.to_owned(),
        relationship_coverage,
    );
    if let Some(rate) = caption_pairing_rate {
        dimensions.insert(DIMENSION_CAPTION_PAIRING_RATE.to_owned(), rate);
    }
    if let Some(rate) = table_decomposition_rate {
        dimensions.insert(DIMENSION_TABLE_DECOMPOSITION_RATE.to_owned(), rate);
    }

    seal_report(ConformanceReport {
        parse_id: parse_id.to_owned(),
        unit_type_counts,
        relationship_type_counts,
        locator_coverage,
        caption_pairing_rate,
        table_decomposition_rate,
        dimensions,
        measured_at: measured_at.to_owned(),
        // Placeholder; seal_report computes the hash over the body without
        // this field and fills it in.
        report_hash: String::new(),
    })
}

/// Fraction of units carrying at least one locator (spec §12.5
/// `locatorCoverage`; §17: every evidence-bearing unit should be located).
/// An empty unit set vacuously satisfies "every unit is located", so it
/// measures 1.0 rather than dividing by zero.
fn measure_locator_coverage(units: &[CandidateContentUnit]) -> f64 {
    if units.is_empty() {
        return 1.0;
    }
    let located = units
        .iter()
        .filter(|unit| !unit.locators.is_empty())
        .count();
    located as f64 / units.len() as f64
}

/// Fraction of caption units paired to another unit through a `caption_of`
/// or `has_caption` edge, on either side of the edge (spec §12.5
/// `captionPairingRate`). `None` when the parse contains no captions: with
/// nothing to pair, the rate is unmeasurable, not perfect.
fn measure_caption_pairing_rate(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
) -> Option<f64> {
    let captions: BTreeSet<&str> = units
        .iter()
        .filter(|unit| unit.content_type == ContentType::Caption)
        .map(|unit| unit.local_id.as_str())
        .collect();
    if captions.is_empty() {
        return None;
    }

    // Either side of either caption edge counts: caption_of points caption →
    // subject, has_caption points subject → caption (spec §19), and a parser
    // may emit either or both directions.
    let mut caption_edge_endpoints: BTreeSet<&str> = BTreeSet::new();
    for relationship in relationships {
        if matches!(
            relationship.relationship_type,
            UnitRelationshipType::CaptionOf | UnitRelationshipType::HasCaption
        ) {
            caption_edge_endpoints.insert(relationship.from_local_id.as_str());
            caption_edge_endpoints.insert(relationship.to_local_id.as_str());
        }
    }

    let paired = captions
        .iter()
        .filter(|local_id| caption_edge_endpoints.contains(*local_id))
        .count();
    Some(paired as f64 / captions.len() as f64)
}

/// Fraction of table units with at least one `table_cell` descendant
/// reachable through containment edges (spec §12.5
/// `tableDecompositionRate`): a table without reachable cells was emitted as
/// an opaque object, not decomposed for table retrieval (§15.2). `None` when
/// the parse contains no tables.
fn measure_table_decomposition_rate(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
) -> Option<f64> {
    let tables: Vec<&str> = units
        .iter()
        .filter(|unit| unit.content_type == ContentType::Table)
        .map(|unit| unit.local_id.as_str())
        .collect();
    if tables.is_empty() {
        return None;
    }

    let content_types: BTreeMap<&str, ContentType> = units
        .iter()
        .map(|unit| (unit.local_id.as_str(), unit.content_type))
        .collect();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for relationship in relationships {
        if CONTAINMENT_TYPES.contains(&relationship.relationship_type) {
            children
                .entry(relationship.from_local_id.as_str())
                .or_default()
                .push(relationship.to_local_id.as_str());
        }
    }

    let decomposed = tables
        .iter()
        .filter(|table| has_table_cell_descendant(table, &children, &content_types))
        .count();
    Some(decomposed as f64 / tables.len() as f64)
}

/// Breadth-first containment walk from one table looking for a `table_cell`.
/// The visited set guards against cycles: validation guarantees resolvable
/// references, not an acyclic graph, so a pathological containment cycle
/// must terminate the walk instead of looping.
fn has_table_cell_descendant<'a>(
    root: &str,
    children: &BTreeMap<&'a str, Vec<&'a str>>,
    content_types: &BTreeMap<&'a str, ContentType>,
) -> bool {
    let mut visited: BTreeSet<&str> = BTreeSet::new();
    let mut queue: VecDeque<&str> = children
        .get(root)
        .map(|direct| direct.iter().copied().collect())
        .unwrap_or_default();
    while let Some(local_id) = queue.pop_front() {
        if !visited.insert(local_id) {
            continue;
        }
        if content_types.get(local_id) == Some(&ContentType::TableCell) {
            return true;
        }
        if let Some(next) = children.get(local_id) {
            queue.extend(next.iter().copied());
        }
    }
    false
}

/// Fraction of units participating in at least one relationship of any type,
/// as either endpoint. Cheap extra dimension justified as follows: the
/// canonical structure IS the relationship graph (spec §19), so a unit no
/// edge touches is structure the parser extracted but failed to integrate;
/// with identical input bytes (§13.3 compares re-parses only), a
/// better-connected parse dominates, all else equal. An empty unit set
/// measures vacuously 1.0.
fn measure_relationship_coverage(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
) -> f64 {
    if units.is_empty() {
        return 1.0;
    }
    let mut connected: BTreeSet<&str> = BTreeSet::new();
    for relationship in relationships {
        connected.insert(relationship.from_local_id.as_str());
        connected.insert(relationship.to_local_id.as_str());
    }
    let connected_units = units
        .iter()
        .filter(|unit| connected.contains(unit.local_id.as_str()))
        .count();
    connected_units as f64 / units.len() as f64
}

/// Compute and set `reportHash` (spec §12.5, hashing rules §16.2): the
/// canonical SHA-256 of the report body serialized WITHOUT its own
/// `reportHash` field — a record cannot contain its own hash, so the shared
/// self-hash helper removes the field before hashing (and fails loudly if a
/// model rename ever makes that field disappear) and the hash is filled in
/// after.
fn seal_report(mut report: ConformanceReport) -> Result<ConformanceReport, ApiError> {
    report.report_hash =
        crate::canonical::canonical_sha256_hex_without_field(&report, "reportHash")?;
    Ok(report)
}
