//! PDF parser worker (spec §12.1–§12.2, decision D6): runs the Docling
//! engine in `--to json` mode and maps DoclingDocument JSON into typed
//! candidate content units, relationships, and locators, staged as a parser
//! output bundle via `crate::parse::bundle`. Implemented by work package
//! C4c.
//!
//! Trust boundary (spec §12.1): this worker is an untrusted producer. It
//! writes staged bundles only — never canonical storage or hot indexes —
//! and everything it emits is a candidate claim the importer (C4b)
//! validates. All IDs here are parser-local: Docling `self_ref` strings
//! (`#/texts/12`) or names derived from them (`#/pages/3`,
//! `#/tables/0/cells/7`); canonical IDs are assigned at import.
//!
//! Failure discipline (recorded-outcome pattern): once a bundle workspace
//! exists, every fault on the Docling/mapping side of the §12.1 boundary —
//! tool failure, timeout, JSON parse failure, mapping failure — is recorded
//! as a promoted FAILED bundle and returned as `Ok(bundle_dir)` (§12.2:
//! failure bundles are preserved for diagnostics). `Err` is reserved for
//! staging-infrastructure faults (bundle create/append/finish), where no
//! trustworthy bundle can exist.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Instant,
};

use crate::parse::cleanup::{self, CleanupKind};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    canonical,
    config::DoclingConfig,
    docling::{ResolvedDoclingOptions, convert_source_to_document_json, resolve_docling_options},
    error::ApiError,
    model::{
        CaptionBody, CharRangeLocator, ContentType, CoordinateSystem, FigureBody, FigureType,
        Locator, PageBboxLocator, PageBody, ParseMetrics, ParseWarningSeverity,
        ParserCapabilityProfile, TableBody, TableCellBody, TableHeader, TableHeaderSpan,
        TextBlockBody, TextBlockRole, TextSectionBody, UnitRelationshipType,
    },
    parse::bundle::{
        BundleIdentity, BundleWriter, CandidateContentUnit, CandidateUnitRelationship,
        CandidateWarning, ParserExecutionStatus, ParserResult, parse_staging_root,
    },
    primitives::utc_now,
    source::ResolvedSource,
    util::truncate_persisted_detail,
};

/// Parser identity name recorded in every bundle this worker stages.
pub(crate) const PDF_PARSER_NAME: &str = "docling_pdf";

/// Version of this worker's mapping logic (not the Docling tool version,
/// which is an observed tool-identity fact). Bump when the mapping rules
/// change; a bump makes re-parses a new parse identity.
pub(crate) const PDF_PARSER_VERSION: &str = "2";

/// Docling output format this worker consumes (decision D6); folded into
/// the parser configuration hash because the output mode shapes the
/// candidate structure.
const PDF_PARSER_OUTPUT_FORMAT: &str = "json";

/// `schema_name` a DoclingDocument JSON artifact must declare; any other
/// value means the artifact is not the contract this mapping was built
/// against, and the parse fails loudly instead of guessing.
const DOCLING_DOCUMENT_SCHEMA_NAME: &str = "DoclingDocument";

/// Docling bbox origin this worker accepts for item provenance. Docling PDF
/// coordinates are PDF points with the origin at the page's BOTTOM-LEFT
/// corner — the native PDF convention §17 `pdf_points` carries.
const DOCLING_BOTTOMLEFT_ORIGIN: &str = "BOTTOMLEFT";

/// Run one PDF parse: convert the source with Docling `--to json`, map the
/// DoclingDocument into candidate units/relationships/warnings, and stage
/// the promoted parser output bundle, returning its directory.
///
/// Returns `Ok(bundle_dir)` for BOTH succeeded and failed parses (see the
/// module-level failure discipline); callers must read the bundle's
/// `ParserResult` status, not infer success from `Ok`.
pub(crate) fn run_pdf_parse(
    config: &DoclingConfig,
    document_timeout_seconds: u64,
    index_root: &Path,
    source: ResolvedSource,
    source_id: &str,
    source_hash: &str,
) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    let started_at = utc_now()?;
    // Effective options are resolved before any workspace exists so the
    // bundle identity carries the exact configuration hash (D3: these
    // Docling options are identity-bearing parser configuration). A config
    // fault here is an infrastructure error — no bundle exists yet.
    let options = resolve_docling_options(config, document_timeout_seconds)?;
    let parser_config_hash = pdf_parser_config_hash(&options)?;
    let profile = capability_profile(&parser_config_hash)?;

    info!(
        event = "parse.pdf_worker.started",
        parser_name = PDF_PARSER_NAME,
        parser_version = PDF_PARSER_VERSION,
        source_id,
        source_requested = %source.requested,
        relative_source = %source.relative_path.display(),
        "PDF parse worker starting"
    );

    let identity = BundleIdentity {
        parser_name: PDF_PARSER_NAME.to_string(),
        parser_version: PDF_PARSER_VERSION.to_string(),
        parser_config_hash,
        capability_profile_hash: profile.profile_hash,
        source_id: source_id.to_string(),
        source_hash: source_hash.to_string(),
    };
    let mut writer = BundleWriter::create(&parse_staging_root(index_root), identity)?;
    let raw_dir = writer.parser_raw_dir()?;

    // Docling writes its JSON artifact directly into the bundle's
    // parser_raw/ workspace: preserved raw output per §12.1 rule 6,
    // digested into the manifest at finish. No progress consumer exists at
    // this boundary: scheduler dispatch supplies none, and whether streamed
    // progress returns is the open D2 surface decision, hence `None`.
    let conversion_started = Instant::now();
    let conversion = match convert_source_to_document_json(
        config,
        document_timeout_seconds,
        index_root,
        source,
        None,
        Some(&raw_dir),
    ) {
        Ok(conversion) => conversion,
        Err(conversion_error) => {
            // Tool failure is a recorded parse outcome. The Docling
            // layer already logged full bounded diagnostics and embeds
            // bounded stdout/stderr in the error message; no captured
            // stream bytes exist on this path, so the bundle's log
            // files stay empty.
            return finish_failed(
                writer,
                source_id,
                &started_at,
                started,
                format!("Docling JSON conversion failed: {conversion_error}"),
                &[],
                &[],
            );
        }
    };
    info!(
        event = "parse.pdf_worker.conversion_completed",
        parser_name = PDF_PARSER_NAME,
        source_id,
        json_path = %conversion.json_path.display(),
        json_bytes = conversion.json_text.len(),
        conversion_elapsed_ms = conversion_started.elapsed().as_millis() as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Docling JSON conversion completed"
    );

    let document: DoclingDocument = match serde_json::from_str(&conversion.json_text) {
        Ok(document) => document,
        Err(parse_error) => {
            return finish_failed(
                writer,
                source_id,
                &started_at,
                started,
                format!("DoclingDocument JSON parse failed: {parse_error}"),
                conversion.stdout.as_bytes(),
                conversion.stderr.as_bytes(),
            );
        }
    };
    if document.schema_name != DOCLING_DOCUMENT_SCHEMA_NAME {
        return finish_failed(
            writer,
            source_id,
            &started_at,
            started,
            format!(
                "Docling artifact declares schema_name \"{}\", expected \"{DOCLING_DOCUMENT_SCHEMA_NAME}\"",
                document.schema_name
            ),
            conversion.stdout.as_bytes(),
            conversion.stderr.as_bytes(),
        );
    }

    let mut mapped = match map_document(&document) {
        Ok(mapped) => mapped,
        Err(mapping_error) => {
            return finish_failed(
                writer,
                source_id,
                &started_at,
                started,
                format!("DoclingDocument mapping failed: {}", mapping_error.detail),
                conversion.stdout.as_bytes(),
                conversion.stderr.as_bytes(),
            );
        }
    };

    // Cleanup precedes canonical IDs/hashes and model work. The raw Docling JSON
    // and pre-cleanup graph remain in parser_raw; warnings follow merged local IDs.
    let cleanup_report = cleanup::stage_cleanup(
        &raw_dir,
        &mut mapped.units,
        &mut mapped.relationships,
        CleanupKind::Pdf,
        source_id,
    )?;
    for warning in &mut mapped.warnings {
        warning.unit_local_id = warning
            .unit_local_id
            .as_deref()
            .and_then(|id| cleanup_report.remap_local_id(id))
            .map(str::to_string);
    }
    mapped.metrics.unit_count = Some(mapped.units.len() as u64);
    mapped.metrics.relationship_count = Some(mapped.relationships.len() as u64);

    // Streaming the mapped records is a staging-infrastructure boundary:
    // an append failure propagates as Err. A create/append fault leaves a
    // `.tmp` directory behind, which is inert by construction — its name
    // never matches a promoted bundle name (see the bundle.rs promotion
    // invariant).
    for unit in &mapped.units {
        writer.append_candidate_unit(unit)?;
    }
    for relationship in &mapped.relationships {
        writer.append_candidate_relationship(relationship)?;
    }
    for warning in &mapped.warnings {
        writer.append_warning(warning)?;
    }

    // Mapping counts by type: compact operator-facing shape facts only —
    // never document contents (diagnostics standard).
    let mut unit_type_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    for unit in &mapped.units {
        *unit_type_counts
            .entry(unit.content_type.wire_name())
            .or_insert(0) += 1;
    }
    info!(
        event = "parse.pdf_worker.mapping_completed",
        parser_name = PDF_PARSER_NAME,
        source_id,
        unit_count = mapped.units.len(),
        relationship_count = mapped.relationships.len(),
        warning_count = mapped.warnings.len(),
        unit_type_counts = ?unit_type_counts,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "DoclingDocument mapped to candidate records"
    );

    let parser_result = ParserResult {
        status: ParserExecutionStatus::Succeeded,
        error: None,
        started_at,
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        tool_identity: tool_identity_of(&document),
    };
    // finish logs promotion (or failure) with counts; worker adds nothing.
    writer.finish(
        &parser_result,
        &mapped.metrics,
        conversion.stdout.as_bytes(),
        conversion.stderr.as_bytes(),
    )
}

/// Resolve this worker's EFFECTIVE identity for a given Docling
/// configuration: the capability profile carrying parser name, version, and
/// the configuration hash `run_pdf_parse` would stamp into a bundle staged
/// under the same config. The scheduler's §13.5 no-blind-retry guard and the
/// importer validation both key on this identity, so it must come from the
/// same derivation the worker itself uses (`resolve_docling_options` +
/// `pdf_parser_config_hash`), never a re-implementation.
pub(crate) fn effective_capability_profile(
    config: &DoclingConfig,
    document_timeout_seconds: u64,
) -> Result<ParserCapabilityProfile, ApiError> {
    let options = resolve_docling_options(config, document_timeout_seconds)?;
    let parser_config_hash = pdf_parser_config_hash(&options)?;
    capability_profile(&parser_config_hash)
}

/// This worker's statically declared emission surface (spec §12.4): exactly
/// the content types, relationship types, and locator kinds the mapping in
/// this module can produce. `continues_on` is deliberately absent — Docling
/// represents a cross-page item as ONE item with multiple `prov` entries,
/// so the fact lives in multi-page locator sets, never in a relationship
/// between two units. `parser_config_hash` is an input because the profile
/// identifies one parser CONFIGURATION, not just the code (spec §12.4).
pub(crate) fn capability_profile(
    parser_config_hash: &str,
) -> Result<ParserCapabilityProfile, ApiError> {
    let mut profile = ParserCapabilityProfile {
        parser_name: PDF_PARSER_NAME.to_string(),
        parser_version: PDF_PARSER_VERSION.to_string(),
        parser_config_hash: parser_config_hash.to_string(),
        emits_content_types: vec![
            ContentType::Page,
            ContentType::TextSection,
            ContentType::TextBlock,
            ContentType::Table,
            ContentType::TableCell,
            ContentType::Figure,
            ContentType::Caption,
        ],
        emits_relationship_types: vec![
            UnitRelationshipType::Contains,
            UnitRelationshipType::PhysicallyContains,
            UnitRelationshipType::LogicallyContains,
            UnitRelationshipType::Precedes,
            UnitRelationshipType::AppearsOn,
            UnitRelationshipType::CaptionOf,
            UnitRelationshipType::HasCaption,
        ],
        emits_locator_kinds: vec!["page_bbox".to_string(), "char_range".to_string()],
        emits_body_fields: None,
        profile_hash: String::new(),
    };
    profile.profile_hash = parser_profile_hash_of(&profile)?;
    Ok(profile)
}

/// Canonical hash over a capability profile's fields excluding
/// `profile_hash` itself (the shared self-hash pattern in
/// `crate::canonical`); the model struct stays the single source of the
/// hashed shape.
fn parser_profile_hash_of(profile: &ParserCapabilityProfile) -> Result<String, ApiError> {
    canonical::canonical_sha256_hex_without_field(profile, canonical::PROFILE_HASH_JSON_KEY)
}

/// Canonical hash over the effective, identity-bearing Docling parser
/// configuration (D3 resolution: pdf_backend, ocr_mode, device,
/// num_threads, page_batch_size, document_timeout_seconds) plus the D6
/// output format. Changing any of these values yields a different
/// `parserConfigHash` and therefore a new parse identity.
fn pdf_parser_config_hash(options: &ResolvedDoclingOptions) -> Result<String, ApiError> {
    let value = serde_json::json!({
        "pdfBackend": options.pdf_backend,
        "ocrMode": options.ocr_mode,
        "device": options.device,
        "numThreads": options.num_threads,
        "pageBatchSize": options.page_batch_size,
        "documentTimeoutSeconds": options.document_timeout_seconds,
        "outputFormat": PDF_PARSER_OUTPUT_FORMAT,
        "cleanupVersion": cleanup::CLEANUP_VERSION,
    });
    canonical::canonical_sha256_hex(&value)
}

/// External-tool identity facts for the ParserResult. The DoclingDocument
/// schema name and version come free from the parsed artifact. The Docling
/// CLI version is deliberately omitted: conversion stderr does not reliably
/// carry it, and obtaining it would cost a second process invocation.
fn tool_identity_of(document: &DoclingDocument) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "docling_document_schema".to_string(),
            document.schema_name.clone(),
        ),
        (
            "docling_document_version".to_string(),
            document.version.clone(),
        ),
    ])
}

/// Stage a FAILED bundle for a parse whose tool or mapping stage faulted
/// (recorded-outcome pattern): the failure detail is bounded, the bundle is
/// promoted for diagnostics (§12.2), and the promoted directory is returned
/// as `Ok`. `Err` from here means the staging infrastructure itself failed.
fn finish_failed(
    writer: BundleWriter,
    source_id: &str,
    started_at: &str,
    started: Instant,
    detail: String,
    stdout_log: &[u8],
    stderr_log: &[u8],
) -> Result<PathBuf, ApiError> {
    let bounded_detail = truncate_persisted_detail(&detail);
    // A failed parse is an expected untrusted-input outcome (warn), recorded
    // durably in the staged failure bundle.
    warn!(
        event = "parse.pdf_worker.failed",
        parser_name = PDF_PARSER_NAME,
        source_id,
        error = %bounded_detail,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "PDF parse failed; staging failure bundle"
    );
    let parser_result = ParserResult {
        status: ParserExecutionStatus::Failed,
        error: Some(bounded_detail),
        started_at: started_at.to_string(),
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        // No tool facts on failure paths: the identity source (the parsed
        // DoclingDocument) may not exist.
        tool_identity: BTreeMap::new(),
    };
    writer.finish(
        &parser_result,
        &empty_parse_metrics(),
        stdout_log,
        stderr_log,
    )
}

/// Metrics record for a failed parse: parsers only report what they
/// measure (spec §12), and a failed execution measured nothing.
fn empty_parse_metrics() -> ParseMetrics {
    ParseMetrics {
        unit_count: None,
        relationship_count: None,
        page_count: None,
        table_count: None,
        figure_count: None,
        ocr_region_count: None,
        annotation_count: None,
        projection_count: None,
    }
}

// ---------------------------------------------------------------------------
// DoclingDocument shapes (external schema, tolerant deserialization)
// ---------------------------------------------------------------------------

/// Minimal typed view of a DoclingDocument JSON artifact (schema 1.10.x,
/// grounded against the D6 sample conversion from Docling 2.93.0).
///
/// Deliberate asymmetry with `crate::model`: these shapes carry NO
/// `deny_unknown_fields`, because the schema belongs to Docling, not this
/// service — unknown fields must be tolerated across Docling upgrades. Only
/// the fields this worker maps are declared; nothing is lost, because the
/// full raw artifact is preserved in the bundle's `parser_raw/` directory.
/// The declared fields stay REQUIRED (no `serde(default)` on them): their
/// absence means the mapping's assumptions no longer hold, which must fail
/// the parse loudly rather than silently emit nothing.
#[derive(Debug, Deserialize)]
struct DoclingDocument {
    schema_name: String,
    version: String,
    /// Reading-order content tree root; children reference `texts`,
    /// `tables`, `pictures`, and `groups` entries by `$ref`.
    body: DoclingNode,
    /// Furniture-layer tree root (page decoration). In the observed D6
    /// sample this root is EMPTY — headers/footers ride in `body.children`
    /// — but traversal still covers it defensively for documents that do
    /// populate it, with the visited set absorbing any overlap.
    furniture: DoclingNode,
    groups: Vec<DoclingNode>,
    texts: Vec<DoclingTextItem>,
    pictures: Vec<DoclingVisualItem>,
    tables: Vec<DoclingTableItem>,
    /// Keyed by page-number string; the numeric `page_no` field inside each
    /// entry is authoritative for ordering (string keys sort "10" < "2").
    pages: BTreeMap<String, DoclingPage>,
}

/// One `{"$ref": "#/texts/12"}` reference in the Docling content tree.
#[derive(Debug, Deserialize)]
struct DoclingRef {
    #[serde(rename = "$ref")]
    target: String,
}

/// Structural tree node (body root, furniture root, group). Groups are
/// pure containers in this mapping — never emitted as units.
#[derive(Debug, Deserialize)]
struct DoclingNode {
    self_ref: String,
    #[serde(default)]
    children: Vec<DoclingRef>,
}

/// One entry of the `texts` array; `label` discriminates the mapping.
#[derive(Debug, Deserialize)]
struct DoclingTextItem {
    self_ref: String,
    label: String,
    #[serde(default)]
    children: Vec<DoclingRef>,
    #[serde(default)]
    prov: Vec<DoclingProv>,
    text: String,
    /// Original (pre-sanitization) text; Docling leaves `text` empty when
    /// its sanitizer produces nothing (observed on formula items).
    #[serde(default)]
    orig: Option<String>,
    /// Heading level, present on `section_header` items.
    #[serde(default)]
    level: Option<u64>,
}

/// One entry of the `pictures` array.
#[derive(Debug, Deserialize)]
struct DoclingVisualItem {
    self_ref: String,
    #[serde(default)]
    children: Vec<DoclingRef>,
    #[serde(default)]
    prov: Vec<DoclingProv>,
    #[serde(default)]
    captions: Vec<DoclingRef>,
}

/// One entry of the `tables` array.
#[derive(Debug, Deserialize)]
struct DoclingTableItem {
    self_ref: String,
    #[serde(default)]
    children: Vec<DoclingRef>,
    #[serde(default)]
    prov: Vec<DoclingProv>,
    #[serde(default)]
    captions: Vec<DoclingRef>,
    data: DoclingTableData,
}

/// Tabular payload of one table item.
#[derive(Debug, Deserialize)]
struct DoclingTableData {
    num_rows: u64,
    num_cols: u64,
    table_cells: Vec<DoclingTableCell>,
}

/// One cell of a Docling table. Cell bboxes are TOPLEFT-origin fragments
/// without a page number, so they are not representable as §17 `page_bbox`
/// locators and are not declared here.
#[derive(Debug, Deserialize)]
struct DoclingTableCell {
    #[serde(default = "default_span")]
    row_span: u64,
    #[serde(default = "default_span")]
    col_span: u64,
    start_row_offset_idx: u64,
    start_col_offset_idx: u64,
    #[serde(default)]
    text: String,
    #[serde(default)]
    column_header: bool,
    #[serde(default)]
    row_header: bool,
}

/// Serde default for absent cell spans: an unspanned cell covers one
/// row/column.
fn default_span() -> u64 {
    1
}

/// One provenance entry: page number, bbox, and the char span of the
/// item's text this entry covers. A cross-page item carries multiple
/// entries.
#[derive(Debug, Deserialize)]
struct DoclingProv {
    page_no: u64,
    bbox: DoclingBbox,
    #[serde(default)]
    charspan: Option<[u64; 2]>,
}

/// Docling bbox object; `coord_origin` is `BOTTOMLEFT` for PDF sources.
#[derive(Debug, Deserialize)]
struct DoclingBbox {
    l: f64,
    t: f64,
    r: f64,
    b: f64,
    #[serde(default)]
    coord_origin: Option<String>,
}

/// One entry of the `pages` map.
#[derive(Debug, Deserialize)]
struct DoclingPage {
    size: DoclingPageSize,
    page_no: u64,
}

/// Physical page dimensions in PDF points.
#[derive(Debug, Deserialize)]
struct DoclingPageSize {
    width: f64,
    height: f64,
}

// ---------------------------------------------------------------------------
// Mapping
// ---------------------------------------------------------------------------

/// Mapping fault: the artifact parsed as JSON but violates an assumption
/// the candidate mapping depends on. Recorded as a failed parse, never
/// propagated as an infrastructure error.
struct MappingError {
    detail: String,
}

/// Everything one successful mapping produced, ready for streaming into
/// the bundle writer.
struct MappedDocument {
    units: Vec<CandidateContentUnit>,
    relationships: Vec<CandidateUnitRelationship>,
    warnings: Vec<CandidateWarning>,
    metrics: ParseMetrics,
}

/// One `$ref` resolved to its typed Docling item.
enum ResolvedItem<'a> {
    Text(&'a DoclingTextItem),
    Table(&'a DoclingTableItem),
    Picture(&'a DoclingVisualItem),
    Group(&'a DoclingNode),
}

/// One open section during traversal: heading level drives the nesting
/// (pop everything at the same or deeper level before opening a sibling).
struct SectionFrame {
    level: u64,
    local_id: String,
    heading_text: String,
}

/// Map one DoclingDocument to candidate units, relationships, warnings,
/// and metrics. Pure transformation: no I/O, independently reviewable.
fn map_document(document: &DoclingDocument) -> Result<MappedDocument, MappingError> {
    DocumentMapper {
        document,
        units: Vec::new(),
        unit_types: BTreeMap::new(),
        unit_pages: BTreeMap::new(),
        warnings: Vec::new(),
        caption_targets: BTreeMap::new(),
        page_local_ids: BTreeMap::new(),
        section_stack: Vec::new(),
        visited: BTreeSet::new(),
        warned_missing_pages: BTreeSet::new(),
    }
    .run()
}

/// Traversal state for one document mapping.
struct DocumentMapper<'a> {
    document: &'a DoclingDocument,
    units: Vec<CandidateContentUnit>,
    /// Content type per emitted local ID: containment-edge typing, caption
    /// membership checks, and the duplicate-ID guard.
    unit_types: BTreeMap<String, ContentType>,
    /// Page numbers each unit's prov covers (order-preserving, deduped),
    /// feeding the `appears_on` edges.
    unit_pages: BTreeMap<String, Vec<u64>>,
    warnings: Vec<CandidateWarning>,
    /// Caption self_ref -> local IDs of the items it captions (built from
    /// the explicit `captions` arrays on tables and pictures).
    caption_targets: BTreeMap<String, Vec<String>>,
    /// Page number -> page unit local ID.
    page_local_ids: BTreeMap<u64, String>,
    section_stack: Vec<SectionFrame>,
    /// Refs already visited; defensively tolerates an item listed under
    /// both tree roots (not observed in the D6 sample, whose furniture root
    /// is empty) and breaks reference cycles. Also the reconciliation
    /// baseline for the unreferenced-item warning after traversal.
    visited: BTreeSet<String>,
    /// Pages already warned about as missing, so a missing page warns once
    /// instead of once per unit on it.
    warned_missing_pages: BTreeSet<u64>,
}

impl<'a> DocumentMapper<'a> {
    /// Execute the full mapping: pages, caption index, body then furniture
    /// traversal, then the relationship graph and metrics.
    fn run(mut self) -> Result<MappedDocument, MappingError> {
        self.emit_pages()?;
        self.collect_caption_targets();

        let document = self.document;
        // Body first: its children are the document reading order. The
        // furniture root follows defensively: in the observed D6 sample the
        // furniture root is EMPTY (headers/footers ride in body.children),
        // but a document that does populate it must still be covered, and
        // the visited set keeps any overlap between the roots harmless.
        for child in &document.body.children {
            self.visit(&child.target, None)?;
        }
        for child in &document.furniture.children {
            self.visit(&child.target, None)?;
        }

        // Reconcile the typed arrays against the traversal: an item neither
        // tree references would otherwise drop SILENTLY — present in the raw
        // Docling artifact but absent from the candidate stream — so every
        // such self_ref is surfaced as a warning.
        let unreferenced: Vec<&str> = document
            .texts
            .iter()
            .map(|item| item.self_ref.as_str())
            .chain(document.tables.iter().map(|item| item.self_ref.as_str()))
            .chain(document.pictures.iter().map(|item| item.self_ref.as_str()))
            .chain(document.groups.iter().map(|node| node.self_ref.as_str()))
            .filter(|self_ref| !self.visited.contains(*self_ref))
            .collect();
        for self_ref in unreferenced {
            self.push_warning(
                "docling_unreferenced_item",
                format!(
                    "item {self_ref} is not referenced from the body or furniture trees \
                     and was not emitted"
                ),
                ParseWarningSeverity::Warning,
                None,
            );
        }

        let relationships = self.build_relationships();
        let metrics = self.measure_metrics(relationships.len() as u64);

        Ok(MappedDocument {
            units: self.units,
            relationships,
            warnings: self.warnings,
            metrics,
        })
    }

    /// Emit one `page` unit per pages-map entry, in physical page order.
    /// Pages have no prov, so they carry no locators; their local IDs are
    /// worker-derived (`#/pages/{page_no}`) because Docling pages have no
    /// self_ref.
    fn emit_pages(&mut self) -> Result<(), MappingError> {
        let mut pages: Vec<&DoclingPage> = self.document.pages.values().collect();
        pages.sort_by_key(|page| page.page_no);
        for page in pages {
            let local_id = format!("#/pages/{}", page.page_no);
            let body = PageBody {
                page_number: page.page_no,
                width: page.size.width,
                height: page.size.height,
                rotation: None,
                rendered_image_uri: None,
            };
            self.emit_unit(
                local_id.clone(),
                ContentType::Page,
                &body,
                None,
                Vec::new(),
                Vec::new(),
            )?;
            self.page_local_ids.insert(page.page_no, local_id);
        }
        Ok(())
    }

    /// Index the explicit caption edges Docling declares: each table's and
    /// picture's `captions` array names the caption text items pairing
    /// with it.
    fn collect_caption_targets(&mut self) {
        let document = self.document;
        for table in &document.tables {
            for caption in &table.captions {
                self.caption_targets
                    .entry(caption.target.clone())
                    .or_default()
                    .push(table.self_ref.clone());
            }
        }
        for picture in &document.pictures {
            for caption in &picture.captions {
                self.caption_targets
                    .entry(caption.target.clone())
                    .or_default()
                    .push(picture.self_ref.clone());
            }
        }
    }

    /// Visit one content-tree ref in reading order. `inherited_parent` is
    /// set when the item is nested under an emitted container (table,
    /// figure, or another emitted item) and overrides section/page
    /// parentage.
    fn visit(&mut self, target: &str, inherited_parent: Option<&str>) -> Result<(), MappingError> {
        // Visited guard: tolerates duplicate listing across the two tree
        // roots and breaks reference cycles a malformed artifact could
        // contain.
        if !self.visited.insert(target.to_string()) {
            return Ok(());
        }
        let Some(resolved) = self.resolve_ref(target) else {
            self.push_warning(
                "docling_unsupported_ref",
                format!("content tree references unsupported or missing item {target}"),
                ParseWarningSeverity::Warning,
                None,
            );
            return Ok(());
        };
        match resolved {
            // Groups (label `list` and any other grouping) are containers
            // only: no unit is emitted; children attach to the group's
            // parent context and keep their reading order.
            ResolvedItem::Group(group) => {
                for child in &group.children {
                    self.visit(&child.target, inherited_parent)?;
                }
                Ok(())
            }
            ResolvedItem::Text(item) => self.visit_text(item, inherited_parent),
            ResolvedItem::Table(item) => self.visit_table(item, inherited_parent),
            ResolvedItem::Picture(item) => self.visit_picture(item, inherited_parent),
        }
    }

    /// Resolve one `#/{array}/{index}` ref to its typed item. Lifetime is
    /// tied to the document, not `&self`, so callers can keep mutating the
    /// mapper while holding the resolved item.
    fn resolve_ref(&self, target: &str) -> Option<ResolvedItem<'a>> {
        let mut parts = target.strip_prefix("#/")?.split('/');
        let kind = parts.next()?;
        let index: usize = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        let document = self.document;
        match kind {
            "texts" => document.texts.get(index).map(ResolvedItem::Text),
            "tables" => document.tables.get(index).map(ResolvedItem::Table),
            "pictures" => document.pictures.get(index).map(ResolvedItem::Picture),
            "groups" => document.groups.get(index).map(ResolvedItem::Group),
            _ => None,
        }
    }

    /// Map one text item by label (grounded in the D6 sample's observed
    /// label set). Unknown labels are skipped with a warning — visible,
    /// never silent — and their children still attach to the surrounding
    /// context.
    fn visit_text(
        &mut self,
        item: &'a DoclingTextItem,
        inherited_parent: Option<&str>,
    ) -> Result<(), MappingError> {
        let emitted_id = match item.label.as_str() {
            "section_header" => Some(self.emit_section(item, inherited_parent)?),
            "text" => {
                Some(self.emit_text_block(item, TextBlockRole::Paragraph, inherited_parent)?)
            }
            "list_item" => {
                Some(self.emit_text_block(item, TextBlockRole::ListItem, inherited_parent)?)
            }
            "footnote" => {
                Some(self.emit_text_block(item, TextBlockRole::Footnote, inherited_parent)?)
            }
            "formula" => {
                Some(self.emit_text_block(item, TextBlockRole::Formula, inherited_parent)?)
            }
            // Furniture rows are emitted as filterable header/footer
            // blocks: downstream consumers exclude them by blockRole
            // instead of this worker silently dropping page evidence.
            "page_header" => {
                Some(self.emit_text_block(item, TextBlockRole::Header, inherited_parent)?)
            }
            "page_footer" => {
                Some(self.emit_text_block(item, TextBlockRole::Footer, inherited_parent)?)
            }
            "caption" => Some(self.emit_caption(item, inherited_parent)?),
            other => {
                self.push_warning(
                    "docling_unknown_text_label",
                    format!("text item {} has unmapped label \"{other}\"", item.self_ref),
                    ParseWarningSeverity::Warning,
                    Some(item.self_ref.clone()),
                );
                None
            }
        };
        // Children of an emitted item nest under it; children of a skipped
        // item fall through to the surrounding parent context so they are
        // not lost.
        let child_parent = emitted_id.as_deref().or(inherited_parent);
        for child in &item.children {
            self.visit(&child.target, child_parent)?;
        }
        Ok(())
    }

    /// Emit one `text_section` unit for a section header, deriving nesting
    /// from heading levels: a level-L header closes every open section at
    /// level >= L and opens inside the remaining stack top. A header reached
    /// through a container (`inherited_parent` set) is container-scoped: it
    /// emits under the container parent and leaves the global stack
    /// untouched, so it can neither close the enclosing trail nor capture
    /// later top-level siblings (D6 sample: unit #/texts/38 previously got
    /// the wrong parent this way).
    fn emit_section(
        &mut self,
        item: &'a DoclingTextItem,
        inherited_parent: Option<&str>,
    ) -> Result<String, MappingError> {
        let level = item.level.unwrap_or(1);
        if inherited_parent.is_none() {
            while self
                .section_stack
                .last()
                .is_some_and(|frame| frame.level >= level)
            {
                self.section_stack.pop();
            }
        }
        let heading_text = text_content(item);
        let parent = match inherited_parent {
            Some(parent) => Some(parent.to_string()),
            None => match self.section_stack.last() {
                Some(frame) => Some(frame.local_id.clone()),
                None => self.page_parent(&item.prov),
            },
        };
        // The section path is the full heading trail including this
        // heading, so a section is addressable without walking the graph.
        let mut section_path: Vec<String> = self
            .section_stack
            .iter()
            .map(|frame| frame.heading_text.clone())
            .collect();
        section_path.push(heading_text.clone());

        let body = TextSectionBody {
            heading_text: Some(heading_text.clone()),
            heading_level: Some(level),
            section_path: Some(section_path),
            normalized_text: None,
        };
        let locators = self.locators_from_prov(&item.prov, &item.self_ref);
        let local_id = self.emit_unit(
            item.self_ref.clone(),
            ContentType::TextSection,
            &body,
            parent,
            locators,
            prov_pages(&item.prov),
        )?;
        // Container-scoped sections (see the function comment) never join
        // the global trail.
        if inherited_parent.is_none() {
            self.section_stack.push(SectionFrame {
                level,
                local_id: local_id.clone(),
                heading_text,
            });
        }
        Ok(local_id)
    }

    /// Emit one `text_block` unit with the given role.
    fn emit_text_block(
        &mut self,
        item: &'a DoclingTextItem,
        role: TextBlockRole,
        inherited_parent: Option<&str>,
    ) -> Result<String, MappingError> {
        // Furniture rows are page decoration: they attach to their page,
        // never to the open section, so section reading flow stays clean.
        let parent = if matches!(role, TextBlockRole::Header | TextBlockRole::Footer) {
            self.page_parent(&item.prov)
        } else {
            self.body_parent(&item.prov, inherited_parent)
        };
        let body = TextBlockBody {
            text: text_content(item),
            normalized_text: None,
            block_role: Some(role),
            language: None,
        };
        let locators = self.locators_from_prov(&item.prov, &item.self_ref);
        self.emit_unit(
            item.self_ref.clone(),
            ContentType::TextBlock,
            &body,
            parent,
            locators,
            prov_pages(&item.prov),
        )
    }

    /// Emit one `caption` unit. Pairing lives ONLY in the caption_of/
    /// has_caption edges built later from `caption_targets`:
    /// `captionForUnitIds` is deliberately omitted (a spec-legal absent
    /// optional) because a body may not carry unit references — the importer
    /// never remaps refs inside bodies, so a populated field would persist
    /// parser-local self_refs into canonical state and poison the
    /// content-derived bodyHash (§16.1). Same policy as `headerRefs` on
    /// table cells.
    fn emit_caption(
        &mut self,
        item: &'a DoclingTextItem,
        inherited_parent: Option<&str>,
    ) -> Result<String, MappingError> {
        let body = CaptionBody {
            text: text_content(item),
            normalized_text: None,
            caption_for_unit_ids: None,
        };
        let parent = self.body_parent(&item.prov, inherited_parent);
        let locators = self.locators_from_prov(&item.prov, &item.self_ref);
        self.emit_unit(
            item.self_ref.clone(),
            ContentType::Caption,
            &body,
            parent,
            locators,
            prov_pages(&item.prov),
        )
    }

    /// Emit one `table` unit plus one `table_cell` unit per Docling cell,
    /// then visit the table's tree children (its caption items).
    fn visit_table(
        &mut self,
        item: &'a DoclingTableItem,
        inherited_parent: Option<&str>,
    ) -> Result<(), MappingError> {
        let parent = self.body_parent(&item.prov, inherited_parent);
        let caption = self.resolved_caption_text(&item.captions, &item.self_ref);

        // Header declarations come from the per-cell header flags; spans
        // are declared only when they exceed one cell, the implicit
        // default.
        let mut headers = Vec::new();
        for cell in &item.data.table_cells {
            if cell.column_header || cell.row_header {
                headers.push(TableHeader {
                    row_index: Some(cell.start_row_offset_idx),
                    column_index: Some(cell.start_col_offset_idx),
                    text: cell.text.clone(),
                    span: table_header_span(cell),
                });
            }
        }
        let body = TableBody {
            caption,
            row_count: item.data.num_rows,
            column_count: item.data.num_cols,
            headers: (!headers.is_empty()).then_some(headers),
            normalized_markdown: None,
            normalized_csv_uri: None,
            normalized_html_uri: None,
        };
        let locators = self.locators_from_prov(&item.prov, &item.self_ref);
        let table_id = self.emit_unit(
            item.self_ref.clone(),
            ContentType::Table,
            &body,
            parent,
            locators,
            prov_pages(&item.prov),
        )?;

        // Cells in Docling's table_cells order (row-major as produced).
        // Cell IDs are worker-derived because Docling cells have no
        // self_ref. MVP scope, documented: value/valueType stay unset (no
        // typed-value inference here) and headerRefs stays unset (header
        // association needs the importer's canonical IDs). Cells carry no
        // locators: Docling cell bboxes are TOPLEFT-origin and lack a page
        // number, so no declared locator kind represents them faithfully.
        for (cell_index, cell) in item.data.table_cells.iter().enumerate() {
            let cell_body = TableCellBody {
                row_index: cell.start_row_offset_idx,
                column_index: cell.start_col_offset_idx,
                row_span: (cell.row_span > 1).then_some(cell.row_span),
                column_span: (cell.col_span > 1).then_some(cell.col_span),
                text: (!cell.text.is_empty()).then(|| cell.text.clone()),
                normalized_text: None,
                value: None,
                value_type: None,
                header_refs: None,
            };
            self.emit_unit(
                format!("{}/cells/{cell_index}", item.self_ref),
                ContentType::TableCell,
                &cell_body,
                Some(table_id.clone()),
                Vec::new(),
                Vec::new(),
            )?;
        }

        for child in &item.children {
            self.visit(&child.target, Some(&table_id))?;
        }
        Ok(())
    }

    /// Emit one `figure` unit for a picture, then visit its tree children
    /// (caption items and any text Docling extracted inside the figure).
    fn visit_picture(
        &mut self,
        item: &'a DoclingVisualItem,
        inherited_parent: Option<&str>,
    ) -> Result<(), MappingError> {
        let parent = self.body_parent(&item.prov, inherited_parent);
        let body = FigureBody {
            // Placeholder image-export mode (the shared Docling arg set)
            // retains no image bytes, so there is no image URI to claim.
            image_uri: None,
            caption: self.resolved_caption_text(&item.captions, &item.self_ref),
            alt_text: None,
            // Docling's `picture` label carries no visual classification;
            // Unknown is the honest declared type.
            figure_type: Some(FigureType::Unknown),
            ocr_text: None,
        };
        let locators = self.locators_from_prov(&item.prov, &item.self_ref);
        let figure_id = self.emit_unit(
            item.self_ref.clone(),
            ContentType::Figure,
            &body,
            parent,
            locators,
            prov_pages(&item.prov),
        )?;
        for child in &item.children {
            self.visit(&child.target, Some(&figure_id))?;
        }
        Ok(())
    }

    /// Resolve the first declared caption ref of a table/picture to its
    /// text, for the convenience `caption` body field; unresolvable refs
    /// warn instead of failing (pairing edges are built independently).
    fn resolved_caption_text(
        &mut self,
        captions: &[DoclingRef],
        owner_ref: &str,
    ) -> Option<String> {
        let first = captions.first()?;
        match self.resolve_ref(&first.target) {
            Some(ResolvedItem::Text(caption_item)) => Some(text_content(caption_item)),
            _ => {
                self.push_warning(
                    "docling_caption_ref_unresolved",
                    format!(
                        "caption ref {} on {owner_ref} does not resolve to a text item",
                        first.target
                    ),
                    ParseWarningSeverity::Warning,
                    Some(owner_ref.to_string()),
                );
                None
            }
        }
    }

    /// Nearest containing unit for one body-layer item: explicit container
    /// (table/figure nesting), else the innermost open section, else the
    /// page of the item's first prov entry.
    fn body_parent(&mut self, prov: &[DoclingProv], inherited: Option<&str>) -> Option<String> {
        if let Some(parent) = inherited {
            return Some(parent.to_string());
        }
        if let Some(frame) = self.section_stack.last() {
            return Some(frame.local_id.clone());
        }
        self.page_parent(prov)
    }

    /// Page unit containing the item's first prov entry, when that page
    /// exists; a prov entry naming an unknown page warns once per page.
    fn page_parent(&mut self, prov: &[DoclingProv]) -> Option<String> {
        let first = prov.first()?;
        match self.page_local_ids.get(&first.page_no) {
            Some(local_id) => Some(local_id.clone()),
            None => {
                self.warn_missing_page(first.page_no);
                None
            }
        }
    }

    /// Warn once about a prov page number absent from the pages map.
    fn warn_missing_page(&mut self, page_no: u64) {
        if self.warned_missing_pages.insert(page_no) {
            self.push_warning(
                "docling_missing_page",
                format!("prov references page {page_no}, which is not in the pages map"),
                ParseWarningSeverity::Warning,
                None,
            );
        }
    }

    /// Build §17 locators from an item's prov entries: one `page_bbox` per
    /// entry (Docling gives PDF points with a BOTTOM-LEFT origin — the
    /// native convention `pdf_points` denotes) plus one `char_range` per
    /// non-degenerate charspan (Docling emits `[0, 0]` on non-textual
    /// items). A cross-page item yields one locator per prov entry: that
    /// multi-page locator set IS the cross-page fact (see
    /// [`capability_profile`] on the absence of `continues_on`).
    fn locators_from_prov(&mut self, prov: &[DoclingProv], owner_ref: &str) -> Vec<Locator> {
        let mut locators = Vec::new();
        for entry in prov {
            // Any origin other than BOTTOMLEFT would silently corrupt the
            // coordinate meaning, so it is skipped with a warning instead
            // of emitted; an absent origin is treated as the PDF default.
            let origin_ok = entry
                .bbox
                .coord_origin
                .as_deref()
                .map(|origin| origin == DOCLING_BOTTOMLEFT_ORIGIN)
                .unwrap_or(true);
            if origin_ok {
                locators.push(Locator::PageBbox(PageBboxLocator {
                    page_number: entry.page_no,
                    // The locator contract is [x0, y0, x1, y1] with
                    // x0 <= x1 and y0 <= y1. Under Docling's BOTTOM-LEFT
                    // origin, `b` (bottom) is the SMALLER y, so the packing
                    // is [l, b, r, t] — conventional PDF llx,lly,urx,ury.
                    bbox: [entry.bbox.l, entry.bbox.b, entry.bbox.r, entry.bbox.t],
                    coordinate_system: Some(CoordinateSystem::PdfPoints),
                }));
            } else {
                self.push_warning(
                    "docling_unexpected_coord_origin",
                    format!(
                        "prov bbox on {owner_ref} declares origin {:?}; page_bbox locator skipped",
                        entry.bbox.coord_origin
                    ),
                    ParseWarningSeverity::Warning,
                    Some(owner_ref.to_string()),
                );
            }
            if let Some([start, end]) = entry.charspan
                && end > start
            {
                locators.push(Locator::CharRange(CharRangeLocator { start, end }));
            }
        }
        locators
    }

    /// Append one candidate unit, assigning the next global reading-order
    /// sequence index, and index its type and pages. A duplicate local ID
    /// is a mapping bug (the visited set and derived-ID schemes should
    /// make it impossible), surfaced as a loud mapping failure.
    fn emit_unit<T: Serialize>(
        &mut self,
        local_id: String,
        content_type: ContentType,
        body: &T,
        parent_local_id: Option<String>,
        locators: Vec<Locator>,
        pages: Vec<u64>,
    ) -> Result<String, MappingError> {
        if self
            .unit_types
            .insert(local_id.clone(), content_type)
            .is_some()
        {
            return Err(MappingError {
                detail: format!("duplicate candidate unit local ID {local_id}"),
            });
        }
        let body = serde_json::to_value(body).map_err(|source| MappingError {
            detail: format!(
                "failed to serialize {} body for {local_id}: {source}",
                content_type.wire_name()
            ),
        })?;
        if !pages.is_empty() {
            self.unit_pages.insert(local_id.clone(), pages);
        }
        self.units.push(CandidateContentUnit {
            local_id: local_id.clone(),
            content_type,
            body,
            parent_local_id,
            sequence_index: self.units.len() as u64,
            locators,
        });
        Ok(local_id)
    }

    /// Append one non-fatal mapping finding.
    fn push_warning(
        &mut self,
        code: &str,
        message: String,
        severity: ParseWarningSeverity,
        unit_local_id: Option<String>,
    ) {
        self.warnings.push(CandidateWarning {
            code: code.to_string(),
            message,
            severity,
            locator: None,
            unit_local_id,
        });
    }

    /// Build the §19 structural relationship graph over the emitted units.
    ///
    /// Containment scheme (one edge per parented unit, typed by the parent):
    /// page parent -> `physically_contains`; text_section parent ->
    /// `logically_contains`; any other emitted parent (table -> cell,
    /// table/figure -> caption, figure -> inner text) -> `contains`.
    /// `precedes` chains reading-order siblings (units sharing a parent, in
    /// sequence order). `appears_on` links every prov-bearing unit to each
    /// page its prov names — deliberately alongside `physically_contains`
    /// for page-parented units: structure and physical placement are
    /// distinct facts.
    fn build_relationships(&mut self) -> Vec<CandidateUnitRelationship> {
        let mut relationships: Vec<CandidateUnitRelationship> = Vec::new();
        // Local push helper as a macro-free closure is impossible while
        // borrowing self.warnings, so sequence indexes are taken from the
        // vector length at each push.

        // Containment edges, in unit reading order.
        for index in 0..self.units.len() {
            let (unit_id, parent_id) = {
                let unit = &self.units[index];
                (unit.local_id.clone(), unit.parent_local_id.clone())
            };
            let Some(parent_id) = parent_id else {
                continue;
            };
            let relationship_type = match self.unit_types.get(&parent_id) {
                Some(ContentType::Page) => UnitRelationshipType::PhysicallyContains,
                Some(ContentType::TextSection) => UnitRelationshipType::LogicallyContains,
                Some(_) => UnitRelationshipType::Contains,
                // Parents are always emitted before children by
                // construction; a miss is defensive visibility, not a
                // silent drop.
                None => {
                    self.push_warning(
                        "docling_dangling_parent",
                        format!("unit {unit_id} references unemitted parent {parent_id}"),
                        ParseWarningSeverity::Warning,
                        Some(unit_id),
                    );
                    continue;
                }
            };
            relationships.push(CandidateUnitRelationship {
                from_local_id: parent_id,
                to_local_id: unit_id,
                relationship_type,
                relationship_role: None,
                sequence_index: relationships.len() as u64,
            });
        }

        // Reading-order chains among siblings. Units are already in global
        // reading order, so per-parent grouping preserves it; the
        // parentless group (pages, and any unplaceable items) chains too.
        let mut children_by_parent: BTreeMap<Option<String>, Vec<String>> = BTreeMap::new();
        for unit in &self.units {
            children_by_parent
                .entry(unit.parent_local_id.clone())
                .or_default()
                .push(unit.local_id.clone());
        }
        for siblings in children_by_parent.values() {
            for pair in siblings.windows(2) {
                relationships.push(CandidateUnitRelationship {
                    from_local_id: pair[0].clone(),
                    to_local_id: pair[1].clone(),
                    relationship_type: UnitRelationshipType::Precedes,
                    relationship_role: None,
                    sequence_index: relationships.len() as u64,
                });
            }
        }

        // Physical page placement from prov, one edge per distinct page.
        for index in 0..self.units.len() {
            let unit_id = self.units[index].local_id.clone();
            let Some(pages) = self.unit_pages.get(&unit_id).cloned() else {
                continue;
            };
            for page_no in pages {
                let Some(page_id) = self.page_local_ids.get(&page_no).cloned() else {
                    self.warn_missing_page(page_no);
                    continue;
                };
                relationships.push(CandidateUnitRelationship {
                    from_local_id: unit_id.clone(),
                    to_local_id: page_id,
                    relationship_type: UnitRelationshipType::AppearsOn,
                    relationship_role: None,
                    sequence_index: relationships.len() as u64,
                });
            }
        }

        // Caption pairing from the explicit Docling caption arrays, both
        // directions per pair. take() instead of clone(): this is the final
        // use of the index, and moving it out releases the borrow so
        // push_warning can borrow self mutably inside the loop.
        let caption_targets = std::mem::take(&mut self.caption_targets);
        for (caption_ref, targets) in caption_targets {
            if !matches!(
                self.unit_types.get(&caption_ref),
                Some(ContentType::Caption)
            ) {
                self.push_warning(
                    "docling_caption_not_emitted",
                    format!("declared caption {caption_ref} was not emitted as a caption unit"),
                    ParseWarningSeverity::Warning,
                    None,
                );
                continue;
            }
            for target in targets {
                if !self.unit_types.contains_key(&target) {
                    self.push_warning(
                        "docling_caption_target_not_emitted",
                        format!("caption {caption_ref} pairs with unemitted item {target}"),
                        ParseWarningSeverity::Warning,
                        Some(caption_ref.clone()),
                    );
                    continue;
                }
                relationships.push(CandidateUnitRelationship {
                    from_local_id: caption_ref.clone(),
                    to_local_id: target.clone(),
                    relationship_type: UnitRelationshipType::CaptionOf,
                    relationship_role: None,
                    sequence_index: relationships.len() as u64,
                });
                relationships.push(CandidateUnitRelationship {
                    from_local_id: target,
                    to_local_id: caption_ref.clone(),
                    relationship_type: UnitRelationshipType::HasCaption,
                    relationship_role: None,
                    sequence_index: relationships.len() as u64,
                });
            }
        }

        relationships
    }

    /// Measure what this parse produced (spec §12 metrics: report only
    /// what is actually measured).
    fn measure_metrics(&self, relationship_count: u64) -> ParseMetrics {
        let count_of = |content_type: ContentType| -> u64 {
            self.units
                .iter()
                .filter(|unit| unit.content_type == content_type)
                .count() as u64
        };
        ParseMetrics {
            unit_count: Some(self.units.len() as u64),
            relationship_count: Some(relationship_count),
            page_count: Some(count_of(ContentType::Page)),
            table_count: Some(count_of(ContentType::Table)),
            figure_count: Some(count_of(ContentType::Figure)),
            ocr_region_count: None,
            annotation_count: None,
            projection_count: None,
        }
    }
}

/// Text payload of one Docling text item. Docling leaves `text` empty when
/// its sanitizer produces nothing (observed on formula items); `orig` then
/// still carries the raw extraction, which is preferred over losing the
/// content.
fn text_content(item: &DoclingTextItem) -> String {
    if !item.text.is_empty() {
        item.text.clone()
    } else {
        item.orig.clone().unwrap_or_default()
    }
}

/// Distinct page numbers of an item's prov entries, first-seen order.
fn prov_pages(prov: &[DoclingProv]) -> Vec<u64> {
    let mut pages = Vec::new();
    for entry in prov {
        if !pages.contains(&entry.page_no) {
            pages.push(entry.page_no);
        }
    }
    pages
}

/// Span declaration for one header cell, present only when the cell spans
/// more than one row or column (span 1 is the implicit default).
fn table_header_span(cell: &DoclingTableCell) -> Option<TableHeaderSpan> {
    if cell.row_span <= 1 && cell.col_span <= 1 {
        return None;
    }
    Some(TableHeaderSpan {
        row_span: (cell.row_span > 1).then_some(cell.row_span),
        column_span: (cell.col_span > 1).then_some(cell.col_span),
    })
}
