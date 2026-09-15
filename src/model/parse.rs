//! ParseRun lifecycle records (spec §12), parser capability declarations
//! (spec §12.4), and measured conformance reports (spec §12.5).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::locator::Locator;
use crate::model::relationship::UnitRelationshipType;
use crate::model::unit::ContentType;

/// Spec §12. One parse attempt over a SourceObject. A source may accumulate
/// many ParseRuns over time, but at most one is `active` (§12 rule 1) and at
/// most one held candidate exists per source (§12 rule 4).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParseRun {
    pub(crate) id: String,
    pub(crate) source_id: String,

    pub(crate) parser_name: String,
    pub(crate) parser_version: String,
    pub(crate) parser_config_hash: String,
    pub(crate) capability_profile_hash: String,

    pub(crate) status: ParseRunStatus,

    /// A held parse is status `ready` with this reason set: retained,
    /// non-queryable, awaiting explicit disposition (§13.3–§13.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) held_reason: Option<ParseHeldReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) conformance_report: Option<ConformanceReport>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) activated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) archived_at: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) artifact_bundle_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) artifact_bundle_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parser_raw_output_uri: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warnings: Option<Vec<ParseWarning>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics: Option<ParseMetrics>,

    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

/// Spec §12 `status`. Lifecycle states of a ParseRun; only `active` is
/// query-visible (§14).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParseRunStatus {
    Building,
    Ready,
    Active,
    Archiving,
    Archived,
    Failed,
}

/// Spec §12 `heldReason`. The only defined hold cause: the dominance rule
/// (§13.3) found the re-parse worse on at least one conformance dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParseHeldReason {
    ConformanceRegression,
}

/// Spec §12. One non-fatal finding emitted during a parse, optionally pinned
/// to a source position.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParseWarning {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) severity: ParseWarningSeverity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) locator: Option<Locator>,
}

/// Spec §12 `ParseWarning.severity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParseWarningSeverity {
    Info,
    Warning,
    Error,
}

/// SPEC-epub §2.5 `ParseMetrics` (superseding spec §12). Counts summarizing
/// what the parse produced; all fields are optional because parsers only
/// report what they measure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParseMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unit_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relationship_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) page_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) section_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) list_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) aside_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) table_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) figure_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) code_block_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) annotation_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection_count: Option<u64>,
}

/// Spec §12.4. A parser's statically declared, versioned emission surface.
/// A parse that emits structure violating its own declaration fails
/// validation (§13.1); omitted-but-declared capability is measured by the
/// conformance report instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParserCapabilityProfile {
    pub(crate) parser_name: String,
    pub(crate) parser_version: String,
    pub(crate) parser_config_hash: String,

    pub(crate) emits_content_types: Vec<ContentType>,
    pub(crate) emits_relationship_types: Vec<UnitRelationshipType>,
    pub(crate) emits_locator_kinds: Vec<String>,
    // Body-field declarations keyed by content type name; BTreeMap keeps key
    // order deterministic for canonical serialization and hashing (§16.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) emits_body_fields: Option<BTreeMap<String, Vec<String>>>,

    pub(crate) profile_hash: String,
}

/// Spec §12.5. What the parse actually contains, measured by the core at
/// import. Always measured and always reported; it gates activation only via
/// the dominance rule over `dimensions` (§13.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConformanceReport {
    pub(crate) parse_id: String,

    // BTreeMap keeps key order deterministic for canonical serialization and
    // the `reportHash` (§16.2).
    pub(crate) unit_type_counts: BTreeMap<String, u64>,
    pub(crate) relationship_type_counts: BTreeMap<String, u64>,

    pub(crate) locator_coverage: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caption_pairing_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) table_decomposition_rate: Option<f64>,
    // SPEC-epub §2.6: present only when the parse has at least one `list` /
    // `text_section` respectively; absent means unmeasurable, not zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) list_decomposition_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) section_kind_coverage: Option<f64>,

    /// Extensible set of measured conformance metrics compared dimension by
    /// dimension by the activation dominance rule (§13.3).
    pub(crate) dimensions: BTreeMap<String, f64>,

    pub(crate) measured_at: String,
    pub(crate) report_hash: String,
}
