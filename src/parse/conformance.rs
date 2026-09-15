//! Conformance measurement (spec §12.5): measures what a validated parse
//! actually contains — type counts, locator coverage, caption pairing,
//! table and list decomposition, section kind coverage (SPEC-epub §2.6) —
//! producing the ConformanceReport the §13.3
//! activation dominance rule compares dimension by dimension. Measurement is
//! pure: no IO, no clock, no storage. The importer (C4b) validates the
//! candidates first, supplies the timestamp, and persists the report;
//! conformance never gates anything here — it only measures truthfully.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::Deserialize;

use crate::error::ApiError;
use crate::model::body::{SectionKind, TextSectionBody};
use crate::model::{ConformanceReport, ContentType, UnitRelationshipType};
use crate::parse::bundle::{CandidateContentUnit, CandidateUnitRelationship};

/// The single relationship type that expresses containment descent
/// (SPEC-epub §2.4: `contains` is the only containment edge). Used by the
/// decomposition walks: a table counts as decomposed when a `table_cell` is
/// reachable from it through these edges (table → row → cell), a list when a
/// `list_item` is. `appears_on`, caption edges, ordering edges, and
/// `references` are placement/pairing relations, not containment, and
/// deliberately do not count as descent.
const CONTAINMENT_TYPE: UnitRelationshipType = UnitRelationshipType::Contains;

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
const DIMENSION_LIST_DECOMPOSITION_RATE: &str = "list_decomposition_rate";
const DIMENSION_SECTION_KIND_COVERAGE: &str = "section_kind_coverage";
const DIMENSION_RELATIONSHIP_COVERAGE: &str = "relationship_coverage";

/// Measure the §12.5 conformance report over one parse's VALIDATED candidate
/// records (the §13.1 hard gates have already passed, so every local
/// reference resolves and every type is well-formed). `measured_at` is
/// supplied by the caller so this function stays pure and deterministic:
/// identical candidates always yield an identical report body and
/// `reportHash` for a given timestamp. The four optional rates are `None`
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
    let list_decomposition_rate = measure_list_decomposition_rate(units, relationships);
    let section_kind_coverage = measure_section_kind_coverage(units)?;
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
    if let Some(rate) = list_decomposition_rate {
        dimensions.insert(DIMENSION_LIST_DECOMPOSITION_RATE.to_owned(), rate);
    }
    if let Some(rate) = section_kind_coverage {
        dimensions.insert(DIMENSION_SECTION_KIND_COVERAGE.to_owned(), rate);
    }

    seal_report(ConformanceReport {
        parse_id: parse_id.to_owned(),
        unit_type_counts,
        relationship_type_counts,
        locator_coverage,
        caption_pairing_rate,
        table_decomposition_rate,
        list_decomposition_rate,
        section_kind_coverage,
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

/// Fraction of caption units with at least one `caption_of` edge (SPEC-epub
/// §2.6 `captionPairingRate`, defined over edges because bodies carry no unit
/// references). Only `caption_of` counts, and only on its `from` side: the edge
/// points caption → subject (SPEC-epub §2.4), so a caption appearing as its
/// target is malformed and does not count as paired. A subject-side
/// `has_caption` edge alone does not pair the caption. `None` when the parse
/// contains no captions: with nothing to pair, the rate is unmeasurable, not
/// perfect.
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

    let caption_of_sources: BTreeSet<&str> = relationships
        .iter()
        .filter(|relationship| relationship.relationship_type == UnitRelationshipType::CaptionOf)
        .map(|relationship| relationship.from_local_id.as_str())
        .collect();

    let paired = captions
        .iter()
        .filter(|local_id| caption_of_sources.contains(*local_id))
        .count();
    Some(paired as f64 / captions.len() as f64)
}

/// Fraction of table units with at least one `table_cell` descendant
/// reachable through `contains` edges (SPEC-epub §2.6
/// `tableDecompositionRate`): a table without reachable cells was emitted as
/// an opaque object, not decomposed for table retrieval (§15.2). `None` when
/// the parse contains no tables.
fn measure_table_decomposition_rate(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
) -> Option<f64> {
    measure_decomposition_rate(
        units,
        relationships,
        ContentType::Table,
        ContentType::TableCell,
    )
}

/// Fraction of list units with at least one `list_item` descendant reachable
/// through `contains` edges (SPEC-epub §2.6 `listDecompositionRate`), the
/// list mirror of the table rate. `None` when the parse contains no lists.
fn measure_list_decomposition_rate(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
) -> Option<f64> {
    measure_decomposition_rate(
        units,
        relationships,
        ContentType::List,
        ContentType::ListItem,
    )
}

/// Fraction of `subject`-typed units from which at least one `target`-typed
/// unit is reachable through `contains` edges, at any depth (table → row →
/// cell counts as well as table → cell directly). `None` when no subject
/// exists: an unmeasurable rate is unmeasured, never 0 or 1.
fn measure_decomposition_rate(
    units: &[CandidateContentUnit],
    relationships: &[CandidateUnitRelationship],
    subject: ContentType,
    target: ContentType,
) -> Option<f64> {
    let subjects: Vec<&str> = units
        .iter()
        .filter(|unit| unit.content_type == subject)
        .map(|unit| unit.local_id.as_str())
        .collect();
    if subjects.is_empty() {
        return None;
    }

    let content_types: BTreeMap<&str, ContentType> = units
        .iter()
        .map(|unit| (unit.local_id.as_str(), unit.content_type))
        .collect();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for relationship in relationships {
        if relationship.relationship_type == CONTAINMENT_TYPE {
            children
                .entry(relationship.from_local_id.as_str())
                .or_default()
                .push(relationship.to_local_id.as_str());
        }
    }

    let decomposed = subjects
        .iter()
        .filter(|root| has_descendant_of_type(root, target, &children, &content_types))
        .count();
    Some(decomposed as f64 / subjects.len() as f64)
}

/// Breadth-first containment walk from one root looking for a unit of type
/// `target`. The visited set guards against cycles: validation guarantees
/// resolvable references, not an acyclic graph, so a pathological containment
/// cycle must terminate the walk instead of looping.
fn has_descendant_of_type<'a>(
    root: &str,
    target: ContentType,
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
        if content_types.get(local_id) == Some(&target) {
            return true;
        }
        if let Some(next) = children.get(local_id) {
            queue.extend(next.iter().copied());
        }
    }
    false
}

/// Fraction of `text_section` units whose body `kind` is not `unknown`
/// (SPEC-epub §2.6 `sectionKindCoverage`): an unclassified section is
/// structure the worker extracted but could not name. `None` when the parse
/// contains no sections. Bodies are untyped JSON on the candidate record;
/// the §13.1 hard gates already proved every `text_section` body
/// deserializes as `TextSectionBody`, so a failure here is a broken caller
/// contract and is surfaced as an error rather than counted either way.
fn measure_section_kind_coverage(units: &[CandidateContentUnit]) -> Result<Option<f64>, ApiError> {
    let sections: Vec<&CandidateContentUnit> = units
        .iter()
        .filter(|unit| unit.content_type == ContentType::TextSection)
        .collect();
    if sections.is_empty() {
        return Ok(None);
    }

    let mut classified = 0usize;
    for unit in &sections {
        let body = TextSectionBody::deserialize(&unit.body).map_err(|error| ApiError::BadRequest {
            message: format!(
                "conformance: text_section body of unit \"{}\" failed validated deserialization: {error}",
                unit.local_id
            ),
        })?;
        if body.kind != SectionKind::Unknown {
            classified += 1;
        }
    }
    Ok(Some(classified as f64 / sections.len() as f64))
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
