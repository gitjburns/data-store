//! EPUB parser worker (SPEC-epub §3, §5–§12): maps one EPUB archive to the
//! v0.4 content model and stages it as a parser output bundle.
//!
//! Trust boundary (spec §12.1): an untrusted producer whose only write is a
//! staged bundle under the parse staging root via
//! `crate::parse::bundle::BundleWriter`. Nothing here is canonical until the
//! importer validates it; candidate records carry parser-local IDs only.
//!
//! Outcome model (SPEC-epub §11.1), following the plain-text worker: a
//! source-caused failure (`EpubFailure`, stage-tagged) seals a failure
//! bundle and returns `Ok`; only staging faults (`WorkerError::Fault`)
//! return `Err`.
//!
//! Module layout is SPEC-epub §11.1; this file owns the worker entry, the
//! parser identity, the capability profile, the staging `Emitter`, and the
//! §11.2 sequence.

mod archive;
mod blocks;
mod entities;
mod kinds;
mod links;
mod navigation;
mod package;
mod report;
mod structure;
mod text;
mod xhtml;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use tracing::{error, info, warn};

use crate::canonical;
use crate::error::ApiError;
use crate::limits::{EpubLimits, ParsingLimits};
use crate::model::{
    ContentType, Locator, ParseMetrics, ParseWarningSeverity, ParserCapabilityProfile,
    UnitRelationshipType,
};
use crate::parse::bundle::{
    BundleIdentity, BundleWriter, CandidateContentUnit, CandidateUnitRelationship,
    CandidateWarning, ParserExecutionStatus, ParserResult, parse_staging_root,
};
use crate::primitives::utc_now;
use crate::runtime::StorageContext;
use crate::util::truncate_persisted_detail;

use self::navigation::NavigationSource;
use self::report::StructureReport;

/// Parser identity name (SPEC-epub §3.2).
pub(crate) const EPUB_PARSER_NAME: &str = "epub";

/// Parser implementation version (SPEC-epub §3.2). Bump when emission
/// changes in a way the importer or activation can observe.
pub(crate) const EPUB_PARSER_VERSION: &str = "1";

/// Version of the §7.5–§7.11 container and block mapping rules; folded into
/// `parserConfigHash`. Bump on any mapping-rule change.
pub(crate) const MAPPING_VERSION: &str = "1";

/// Version of the §7.4 section tree rules; folded into `parserConfigHash`.
/// Bump on any section-rule change.
pub(crate) const SECTION_RULES_VERSION: &str = "1";

/// Wire name of the `dom_path` locator kind declared in the capability
/// profile. Must stay in sync with the serde `kind` tag of
/// `Locator::DomPath`.
const LOCATOR_KIND_DOM_PATH: &str = "dom_path";

/// Number of `ContentType` variants; sizes the per-type counter arrays.
const CONTENT_TYPE_COUNT: usize = 13;

/// Where in the §11.2 sequence a recorded failure occurred; rendered with
/// the §11.4 stage names.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EpubStage {
    Archive,
    Container,
    Package,
    Navigation,
    /// A content document, by normalized member name.
    Document(String),
    /// A `[parsing]` candidate cap.
    Caps,
}

impl fmt::Display for EpubStage {
    /// The §11.4 stage name: `archive`, `container`, `package`,
    /// `navigation`, `document:<href>`, or `caps`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive => f.write_str("archive"),
            Self::Container => f.write_str("container"),
            Self::Package => f.write_str("package"),
            Self::Navigation => f.write_str("navigation"),
            Self::Document(href) => write!(f, "document:{href}"),
            Self::Caps => f.write_str("caps"),
        }
    }
}

/// A source-caused parse failure: a recorded outcome, never a fault.
/// Helpers that do not know the sequence boundary (archive, xhtml) return
/// stage `Archive`; the caller that knows the boundary re-stages with
/// `with_stage` before propagating.
#[derive(Debug)]
pub(crate) struct EpubFailure {
    pub stage: EpubStage,
    pub detail: String,
}

impl EpubFailure {
    /// Build a failure at `stage` with its detail.
    pub(crate) fn new(stage: EpubStage, detail: impl Into<String>) -> Self {
        Self {
            stage,
            detail: detail.into(),
        }
    }

    /// Replace the stage with the one the calling boundary knows.
    pub(crate) fn with_stage(self, stage: EpubStage) -> Self {
        Self {
            stage,
            detail: self.detail,
        }
    }
}

/// Everything that can stop the worker: a recorded parse failure (sealed
/// as a failure bundle, `Ok` from the entry point) or a staging I/O fault
/// (`Err` from the entry point).
#[derive(Debug)]
pub(crate) enum WorkerError {
    Recorded(EpubFailure),
    /// Staging I/O only; never a statement about the source.
    Fault(ApiError),
}

impl From<EpubFailure> for WorkerError {
    /// Every `EpubFailure` is a recorded outcome.
    fn from(failure: EpubFailure) -> Self {
        Self::Recorded(failure)
    }
}

impl From<ApiError> for WorkerError {
    /// Every `ApiError` reaching the worker is a staging fault.
    fn from(error: ApiError) -> Self {
        Self::Fault(error)
    }
}

/// Result type of every worker step that can fail either way.
pub(crate) type WorkerResult<T> = Result<T, WorkerError>;

/// Per-content-type counters, indexed by an exhaustive slot mapping so a
/// new `ContentType` variant is a compile error here.
#[derive(Default)]
struct TypeCounts([u64; CONTENT_TYPE_COUNT]);

impl TypeCounts {
    /// Array slot of one content type.
    fn slot(content_type: ContentType) -> usize {
        match content_type {
            ContentType::Document => 0,
            ContentType::Page => 1,
            ContentType::TextSection => 2,
            ContentType::TextBlock => 3,
            ContentType::List => 4,
            ContentType::ListItem => 5,
            ContentType::Aside => 6,
            ContentType::Table => 7,
            ContentType::TableRow => 8,
            ContentType::TableCell => 9,
            ContentType::Figure => 10,
            ContentType::Caption => 11,
            ContentType::CodeBlock => 12,
        }
    }

    /// Increment one type's counter and return the new value.
    fn bump(&mut self, content_type: ContentType) -> u64 {
        let slot = &mut self.0[Self::slot(content_type)];
        *slot += 1;
        *slot
    }

    /// Current value of one type's counter.
    fn get(&self, content_type: ContentType) -> u64 {
        self.0[Self::slot(content_type)]
    }

    /// Sum over every type.
    fn total(&self) -> u64 {
        self.0.iter().sum()
    }
}

/// Stream position and parent of one emitted unit; drives the §10.1
/// relationship ordering at flush.
struct UnitPosition {
    sequence: u64,
    parent: Option<String>,
}

/// One aggregated warning: occurrences of one code in one document, with
/// the first instance's locator (§11.5).
#[derive(Default)]
struct WarningAggregate {
    count: u64,
    locator: Option<Locator>,
}

/// Counts the worker measured, carried from `Emitter::finish` to the
/// metrics record and the completion log.
struct EmissionSummary {
    unit_count: u64,
    relationship_count: u64,
    warning_count: u64,
    page_count: u64,
    section_count: u64,
    list_count: u64,
    aside_count: u64,
    table_count: u64,
    figure_count: u64,
    code_block_count: u64,
    image_count: u64,
    image_bytes: u64,
}

/// The worker's single staging surface. Units stream to the bundle as they
/// are finalized; relationships are buffered and written in §10.1 order by
/// `finish`; warnings aggregate per (code, document) and are written by
/// `finish`; images are written to `artifacts/<sha256>` as encountered and
/// deduplicated by hash. The `[parsing]` candidate caps are checked as the
/// counts grow; exceeding one is a recorded failure at stage `Caps`.
///
/// The lifetime is the borrow of the bundle writer and the limits for the
/// whole parse; dropping the emitter returns the writer to the caller for
/// sealing.
pub(crate) struct Emitter<'a> {
    writer: &'a mut BundleWriter,
    limits: &'a EpubLimits,
    parsing: &'a ParsingLimits,
    /// Local ids issued per type by `next_local_id`.
    ids_issued: TypeCounts,
    /// Units streamed per type.
    units_emitted: TypeCounts,
    /// Every streamed unit's position and parent, keyed by local id.
    positions: BTreeMap<String, UnitPosition>,
    /// Buffered relationships in call order; `sequence_index` is assigned
    /// at flush.
    relationships: Vec<CandidateUnitRelationship>,
    /// Warning aggregates keyed by code, then document.
    warnings: BTreeMap<String, BTreeMap<String, WarningAggregate>>,
    /// Hashes already written to `artifacts/`.
    image_hashes: BTreeSet<String>,
    /// Bytes of distinct images written.
    image_bytes: u64,
}

impl<'a> Emitter<'a> {
    /// Open an emitter over a freshly created bundle writer.
    pub(crate) fn new(
        writer: &'a mut BundleWriter,
        limits: &'a EpubLimits,
        parsing: &'a ParsingLimits,
    ) -> Self {
        Self {
            writer,
            limits,
            parsing,
            ids_issued: TypeCounts::default(),
            units_emitted: TypeCounts::default(),
            positions: BTreeMap::new(),
            relationships: Vec::new(),
            warnings: BTreeMap::new(),
            image_hashes: BTreeSet::new(),
            image_bytes: 0,
        }
    }
}

impl Emitter<'_> {
    /// Stream one unit to the bundle. The emitter is the single authority
    /// on stream order, so it overwrites `sequence_index` with the unit's
    /// position in the stream (the importer derives canonical unit IDs from
    /// it). Exceeding `max_candidate_units` is a recorded `Caps` failure.
    pub(crate) fn unit(&mut self, mut unit: CandidateContentUnit) -> WorkerResult<()> {
        let sequence = self.units_emitted.total();
        if sequence >= self.parsing.max_candidate_units as u64 {
            return Err(caps_exceeded(format!(
                "candidate unit count exceeds parsing.max_candidate_units={}",
                self.parsing.max_candidate_units
            )));
        }
        unit.sequence_index = sequence;
        self.writer.append_candidate_unit(&unit)?;
        self.units_emitted.bump(unit.content_type);
        self.positions.insert(
            unit.local_id,
            UnitPosition {
                sequence,
                parent: unit.parent_local_id,
            },
        );
        Ok(())
    }

    /// Buffer one relationship until `finish` orders and writes it.
    /// Exceeding `max_candidate_relationships` is a recorded `Caps` failure.
    pub(crate) fn relationship(
        &mut self,
        from: &str,
        to: &str,
        kind: UnitRelationshipType,
        role: Option<&str>,
    ) -> WorkerResult<()> {
        if self.relationships.len() >= self.parsing.max_candidate_relationships {
            return Err(caps_exceeded(format!(
                "candidate relationship count exceeds parsing.max_candidate_relationships={}",
                self.parsing.max_candidate_relationships
            )));
        }
        self.relationships.push(CandidateUnitRelationship {
            from_local_id: from.to_string(),
            to_local_id: to.to_string(),
            relationship_type: kind,
            relationship_role: role.map(str::to_string),
            // Assigned at flush in §10.1 order.
            sequence_index: 0,
        });
        Ok(())
    }

    /// Record one warning occurrence, aggregated per (code, document) with
    /// the first instance's locator (§11.5). Cannot fail: the aggregate
    /// count is checked against `max_candidate_warnings` at `finish`.
    pub(crate) fn warning(&mut self, code: &str, document: &str, locator: Option<Locator>) {
        let aggregate = self
            .warnings
            .entry(code.to_string())
            .or_default()
            .entry(document.to_string())
            .or_default();
        aggregate.count += 1;
        if aggregate.locator.is_none() {
            aggregate.locator = locator;
        }
    }

    /// Archive one image's bytes as `artifacts/<sha256>` and return the
    /// hash; `Ok(None)` when the image exceeds `max_image_bytes` (the caller
    /// records `epub_image_too_large`). A hash already written is not
    /// rewritten.
    pub(crate) fn image(&mut self, bytes: &[u8]) -> WorkerResult<Option<String>> {
        if bytes.len() > self.limits.max_image_bytes {
            return Ok(None);
        }
        let hash = canonical::sha256_hex_bytes(bytes);
        if self.image_hashes.contains(&hash) {
            return Ok(Some(hash));
        }
        // The writer creates the directory on first use; repeat calls are a
        // cheap existence check, so no path is cached here.
        let path = self.writer.artifacts_dir()?.join(&hash);
        fs::write(&path, bytes).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to write image artifact {}: {source}",
                path.display()
            ),
        })?;
        self.image_bytes += bytes.len() as u64;
        // The dedup set keeps its own key; the caller owns the returned one.
        self.image_hashes.insert(hash.clone());
        Ok(Some(hash))
    }

    /// Issue the next parser-local id for one content type,
    /// `<wire_name>-<n>` with `n` counting from 1 per type.
    pub(crate) fn next_local_id(&mut self, content_type: ContentType) -> String {
        let n = self.ids_issued.bump(content_type);
        format!("{}-{n}", content_type.wire_name())
    }

    /// The bundle's `parser_raw` directory, for the §11.3 report.
    pub(crate) fn parser_raw_dir(&self) -> Result<PathBuf, ApiError> {
        self.writer.parser_raw_dir()
    }

    /// Write the buffered relationships in §10.1 order and the aggregated
    /// warnings, then return the counts the seal needs. Consumes the
    /// emitter, releasing the writer borrow for `BundleWriter::finish`.
    fn finish(mut self) -> WorkerResult<EmissionSummary> {
        let relationships =
            order_relationships(&self.positions, std::mem::take(&mut self.relationships));
        let relationship_count = relationships.len() as u64;
        for relationship in &relationships {
            self.writer.append_candidate_relationship(relationship)?;
        }

        let warnings = std::mem::take(&mut self.warnings);
        let warning_count = warnings
            .values()
            .map(|documents| documents.len() as u64)
            .sum::<u64>();
        if warning_count > self.parsing.max_candidate_warnings as u64 {
            return Err(caps_exceeded(format!(
                "aggregated warning count {warning_count} exceeds parsing.max_candidate_warnings={}",
                self.parsing.max_candidate_warnings
            )));
        }
        for (code, documents) in warnings {
            for (document, aggregate) in documents {
                let message = format!("{code}: {} occurrence(s) in {document}", aggregate.count);
                // Warnings are keyed by document, not unit; the first
                // instance's locator is the only unit-level reference.
                self.writer.append_warning(&CandidateWarning {
                    // One code key serves every document under it.
                    code: code.to_string(),
                    message,
                    severity: ParseWarningSeverity::Warning,
                    locator: aggregate.locator,
                    unit_local_id: None,
                })?;
            }
        }

        Ok(EmissionSummary {
            unit_count: self.units_emitted.total(),
            relationship_count,
            warning_count,
            page_count: self.units_emitted.get(ContentType::Page),
            section_count: self.units_emitted.get(ContentType::TextSection),
            list_count: self.units_emitted.get(ContentType::List),
            aside_count: self.units_emitted.get(ContentType::Aside),
            table_count: self.units_emitted.get(ContentType::Table),
            figure_count: self.units_emitted.get(ContentType::Figure),
            code_block_count: self.units_emitted.get(ContentType::CodeBlock),
            image_count: self.image_hashes.len() as u64,
            image_bytes: self.image_bytes,
        })
    }
}

/// A recorded failure at stage `Caps`.
fn caps_exceeded(detail: String) -> WorkerError {
    WorkerError::Recorded(EpubFailure::new(EpubStage::Caps, detail))
}

/// §10.1 sort key: type rank, primary order, secondary order, call index.
type RelationshipOrder = (u8, u64, u64, usize);

/// Put buffered relationships in §10.1 order and assign sequence indexes:
/// `contains` in child-unit order; `precedes` per sibling group (groups in
/// the order of their first unit) in unit order; `appears_on` in unit
/// order; caption pairs in call order (`pair_caption` emits `caption_of`
/// before `has_caption`); `references` in call order. A local id with no
/// streamed unit is a dangling edge the importer rejects; it sorts last
/// rather than failing here so the bundle still records it.
fn order_relationships(
    positions: &BTreeMap<String, UnitPosition>,
    relationships: Vec<CandidateUnitRelationship>,
) -> Vec<CandidateUnitRelationship> {
    let sequence_of = |local_id: &str| {
        positions
            .get(local_id)
            .map_or(u64::MAX, |position| position.sequence)
    };
    // First-unit sequence per sibling group, keyed by the parent of the
    // `precedes` edge's source unit.
    let mut group_first: BTreeMap<Option<&str>, u64> = BTreeMap::new();
    for relationship in &relationships {
        if relationship.relationship_type != UnitRelationshipType::Precedes {
            continue;
        }
        let Some(position) = positions.get(&relationship.from_local_id) else {
            continue;
        };
        let first = group_first
            .entry(position.parent.as_deref())
            .or_insert(u64::MAX);
        *first = (*first).min(position.sequence);
    }

    let mut keyed: Vec<(RelationshipOrder, CandidateUnitRelationship)> = relationships
        .into_iter()
        .enumerate()
        .map(|(index, relationship)| {
            let order = match relationship.relationship_type {
                UnitRelationshipType::Contains => {
                    (0, sequence_of(&relationship.to_local_id), 0, index)
                }
                UnitRelationshipType::Precedes => {
                    let group = positions
                        .get(&relationship.from_local_id)
                        .and_then(|position| group_first.get(&position.parent.as_deref()))
                        .map_or(u64::MAX, |first| *first);
                    (1, group, sequence_of(&relationship.from_local_id), index)
                }
                UnitRelationshipType::AppearsOn => {
                    (2, sequence_of(&relationship.from_local_id), 0, index)
                }
                UnitRelationshipType::CaptionOf | UnitRelationshipType::HasCaption => {
                    (3, index as u64, 0, index)
                }
                UnitRelationshipType::References => (4, index as u64, 0, index),
            };
            (order, relationship)
        })
        .collect();
    // Stable sort; the trailing call index keeps equal keys in call order.
    keyed.sort_by_key(|(order, _)| *order);
    keyed
        .into_iter()
        .enumerate()
        .map(|(sequence_index, (_, mut relationship))| {
            relationship.sequence_index = sequence_index as u64;
            relationship
        })
        .collect()
}

/// Bind every rule table to parse identity (SPEC-epub §3.2): a change to a
/// mapping rule, section rule, kind pattern, or entity bumps its constant
/// and therefore the hash. Admission caps are deliberately excluded.
fn epub_config_hash() -> Result<String, ApiError> {
    canonical::canonical_sha256_hex(&serde_json::json!({
        "mappingVersion": MAPPING_VERSION,
        "sectionRulesVersion": SECTION_RULES_VERSION,
        "kindPatternsVersion": kinds::KIND_PATTERNS_VERSION,
        "entityTableVersion": entities::ENTITY_TABLE_VERSION,
    }))
}

/// This parser's statically declared capability profile (SPEC-epub §3.3):
/// every content type, the six structural relationship types, and the
/// `dom_path` locator kind, with the profile hash computed over every field
/// except the hash itself.
pub(crate) fn epub_capability_profile() -> Result<ParserCapabilityProfile, ApiError> {
    let mut profile = ParserCapabilityProfile {
        parser_name: EPUB_PARSER_NAME.to_string(),
        parser_version: EPUB_PARSER_VERSION.to_string(),
        parser_config_hash: epub_config_hash()?,
        emits_content_types: vec![
            ContentType::Document,
            ContentType::Page,
            ContentType::TextSection,
            ContentType::TextBlock,
            ContentType::List,
            ContentType::ListItem,
            ContentType::Aside,
            ContentType::Table,
            ContentType::TableRow,
            ContentType::TableCell,
            ContentType::Figure,
            ContentType::Caption,
            ContentType::CodeBlock,
        ],
        emits_relationship_types: vec![
            UnitRelationshipType::Contains,
            UnitRelationshipType::Precedes,
            UnitRelationshipType::AppearsOn,
            UnitRelationshipType::CaptionOf,
            UnitRelationshipType::HasCaption,
            UnitRelationshipType::References,
        ],
        emits_locator_kinds: vec![LOCATOR_KIND_DOM_PATH.to_string()],
        emits_body_fields: None,
        profile_hash: String::new(),
    };
    profile.profile_hash =
        canonical::canonical_sha256_hex_without_field(&profile, canonical::PROFILE_HASH_JSON_KEY)?;
    Ok(profile)
}

/// Facts one parse produced, carried from the inner worker to the terminal
/// boundary log.
struct EpubParseOutcome {
    bundle_dir: PathBuf,
    status: ParserExecutionStatus,
    /// Bounded failure detail and its §11.4 stage name when `status` is
    /// `Failed`; already safe to log and persist.
    failure: Option<(String, String)>,
}

/// Package facts the seal records in `toolIdentity`.
struct ParseFacts {
    package_version: String,
    navigation_source: NavigationSource,
}

/// Run one EPUB parse over `source_absolute_path` and stage the resulting
/// bundle under `{index_root}/fabric/staging/parse`; returns the promoted
/// bundle directory.
///
/// `Ok` covers both parse outcomes: a succeeded bundle, or a sealed failure
/// bundle recording the stage and detail of a source-caused failure. `Err`
/// means the staging workspace itself faulted and no bundle exists.
///
/// This function owns the worker's diagnostic boundary (§11.4): start,
/// completion, recorded failure, and fault, each with elapsed time.
pub(crate) fn run_epub_parse(
    index_root: &StorageContext,
    source_absolute_path: &Path,
    source_id: &str,
    source_hash: &str,
) -> Result<PathBuf, ApiError> {
    let started = Instant::now();
    info!(
        event = "parse.epub_worker.started",
        source_id,
        source_path = %source_absolute_path.display(),
        "EPUB parse starting"
    );

    match run_epub_parse_inner(
        index_root,
        source_absolute_path,
        source_id,
        source_hash,
        started,
    ) {
        Ok(outcome) => {
            let elapsed_ms = started.elapsed().as_millis() as u64;
            match (outcome.status, &outcome.failure) {
                (ParserExecutionStatus::Failed, Some((detail, stage))) => warn!(
                    event = "parse.epub_worker.parse_failed_recorded",
                    source_id,
                    bundle_dir = %outcome.bundle_dir.display(),
                    detail,
                    stage,
                    elapsed_ms,
                    "EPUB parse failed; failure bundle staged"
                ),
                _ => info!(
                    event = "parse.epub_worker.completed",
                    source_id,
                    bundle_dir = %outcome.bundle_dir.display(),
                    elapsed_ms,
                    "EPUB parse completed; bundle staged"
                ),
            }
            Ok(outcome.bundle_dir)
        }
        Err(source) => {
            error!(
                event = "parse.epub_worker.worker_faulted",
                source_id,
                source_path = %source_absolute_path.display(),
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "EPUB parse worker faulted; no bundle staged"
            );
            Err(source)
        }
    }
}

/// The §11.2 sequence behind the logging wrapper: open the bundle with the
/// worker identity, run the parse through an `Emitter`, flush the buffered
/// relationships and warnings, and seal the bundle with metrics and the
/// `ParserResult`. A recorded failure seals a failure bundle; a fault
/// propagates.
fn run_epub_parse_inner(
    index_root: &StorageContext,
    source_absolute_path: &Path,
    source_id: &str,
    source_hash: &str,
    started: Instant,
) -> Result<EpubParseOutcome, ApiError> {
    let started_at = utc_now()?;

    // Identity first: even a bundle that records a failed parse carries the
    // full parser identity claims. The profile is consumed into the
    // identity rather than copied.
    let profile = epub_capability_profile()?;
    let identity = BundleIdentity {
        parser_name: profile.parser_name,
        parser_version: profile.parser_version,
        parser_config_hash: profile.parser_config_hash,
        capability_profile_hash: profile.profile_hash,
        source_id: source_id.to_string(),
        source_hash: source_hash.to_string(),
    };
    let limits = index_root.limits();
    let mut writer = BundleWriter::create(&parse_staging_root(index_root), identity, *limits)?;

    // The emitter's borrow of the writer ends with this block, whichever
    // way the parse went, so the writer can be sealed below.
    let parsed = {
        let mut emitter = Emitter::new(&mut writer, &limits.epub, &limits.parsing);
        match parse_source(source_absolute_path, &limits.epub, &mut emitter) {
            Ok(facts) => emitter.finish().map(|summary| (facts, summary)),
            Err(error) => Err(error),
        }
    };

    match parsed {
        Ok((facts, summary)) => {
            info!(
                event = "epub.mapping.completed",
                unit_count = summary.unit_count,
                relationship_count = summary.relationship_count,
                section_count = summary.section_count,
                page_count = summary.page_count,
                table_count = summary.table_count,
                figure_count = summary.figure_count,
                code_block_count = summary.code_block_count,
                image_count = summary.image_count,
                image_bytes = summary.image_bytes,
                warning_count = summary.warning_count,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "EPUB mapping completed"
            );
            let mut tool_identity = BTreeMap::new();
            tool_identity.insert("packageVersion".to_string(), facts.package_version);
            tool_identity.insert(
                "navigationSource".to_string(),
                facts.navigation_source.wire_name().to_string(),
            );
            let parser_result = ParserResult {
                status: ParserExecutionStatus::Succeeded,
                error: None,
                started_at,
                completed_at: utc_now()?,
                elapsed_ms: started.elapsed().as_millis() as u64,
                tool_identity,
            };
            // In-process worker: the stdout/stderr logs are empty files.
            let bundle_dir =
                writer.finish(&parser_result, &epub_parse_metrics(&summary), &[], &[])?;
            Ok(EpubParseOutcome {
                bundle_dir,
                status: ParserExecutionStatus::Succeeded,
                failure: None,
            })
        }
        Err(WorkerError::Recorded(failure)) => finish_failed(writer, started_at, started, failure),
        Err(WorkerError::Fault(error)) => Err(error),
    }
}

/// §11.2 steps 2 to 6 over one archive: directory, container, package,
/// `mimetype`, navigation, the footnote pass, the emitting walk, and the
/// raw report. Stages are assigned here for the steps whose helpers do not
/// know their boundary; the walk stages its own failures per document.
fn parse_source(
    source_absolute_path: &Path,
    limits: &EpubLimits,
    emitter: &mut Emitter,
) -> WorkerResult<ParseFacts> {
    let mut archive = archive::open(source_absolute_path, limits)
        .map_err(|failure| failure.with_stage(EpubStage::Archive))?;
    // `total_member_bytes` is the central directory's declared sum, a
    // diagnostic only: nothing has been decompressed yet, and caps are
    // enforced on the bytes `Archive::read` actually produces.
    info!(
        event = "epub.archive.opened",
        member_count = archive.member_count(),
        total_member_bytes = archive.declared_total_bytes(),
        "EPUB archive opened"
    );

    let rootfile = package::read_container(&mut archive, limits)
        .map_err(|failure| failure.with_stage(EpubStage::Container))?;
    let package = package::read_package(&mut archive, &rootfile, limits, emitter)
        .map_err(|failure| failure.with_stage(EpubStage::Package))?;
    // After the package so the mimetype warnings are keyed by its href
    // (§11.2 step 3).
    package::check_mimetype(&mut archive, &package.href, emitter)
        .map_err(|failure| failure.with_stage(EpubStage::Archive))?;
    let navigation = navigation::read(&mut archive, &package, limits, emitter)
        .map_err(|failure| failure.with_stage(EpubStage::Navigation))?;
    info!(
        event = "epub.package.read",
        package_version = package.version,
        spine_count = package.spine.len(),
        manifest_count = package.manifest.len(),
        navigation_source = navigation.source.wire_name(),
        "EPUB package read"
    );

    let mut report = StructureReport::new(&package.version, &package.metadata);
    let footnotes = structure::index_footnotes(&mut archive, &package, &navigation, limits)?;
    structure::walk_spine(
        &mut archive,
        &package,
        &navigation,
        &footnotes,
        limits,
        emitter,
        &mut report,
    )?;
    report.write(&emitter.parser_raw_dir()?)?;

    Ok(ParseFacts {
        package_version: package.version,
        navigation_source: navigation.source,
    })
}

/// Seal the open bundle as a recorded parse failure (spec §12.2: failure
/// bundles are preserved for diagnostics) and return the outcome for the
/// terminal log. `Err` from here means the seal itself faulted; the
/// workspace failure supersedes the parse failure it was trying to record.
fn finish_failed(
    writer: BundleWriter,
    started_at: String,
    started: Instant,
    failure: EpubFailure,
) -> Result<EpubParseOutcome, ApiError> {
    let stage = failure.stage.to_string();
    // Bound once here so the identical detail is safe both to persist in
    // the bundle and to emit in the boundary log.
    let detail = truncate_persisted_detail(
        &format!("{stage}: {}", failure.detail),
        &writer.limits().diagnostics,
    );
    let parser_result = ParserResult {
        status: ParserExecutionStatus::Failed,
        // One copy for the persisted record; the original goes to the log.
        error: Some(detail.clone()),
        started_at,
        completed_at: utc_now()?,
        elapsed_ms: started.elapsed().as_millis() as u64,
        // Package facts are unknown or meaningless on failure.
        tool_identity: BTreeMap::new(),
    };
    // A failed parse reports nothing measured; every per-type count is
    // absent rather than a misleading zero.
    let unmeasured = ParseMetrics {
        unit_count: None,
        relationship_count: None,
        page_count: None,
        section_count: None,
        list_count: None,
        aside_count: None,
        table_count: None,
        figure_count: None,
        code_block_count: None,
        annotation_count: None,
        projection_count: None,
    };
    let bundle_dir = writer.finish(&parser_result, &unmeasured, &[], &[])?;
    Ok(EpubParseOutcome {
        bundle_dir,
        status: ParserExecutionStatus::Failed,
        failure: Some((detail, stage)),
    })
}

/// The metrics this worker measures (SPEC-epub §2.5): unit and relationship
/// totals plus the per-type counts the model records.
fn epub_parse_metrics(summary: &EmissionSummary) -> ParseMetrics {
    ParseMetrics {
        unit_count: Some(summary.unit_count),
        relationship_count: Some(summary.relationship_count),
        page_count: Some(summary.page_count),
        section_count: Some(summary.section_count),
        list_count: Some(summary.list_count),
        aside_count: Some(summary.aside_count),
        table_count: Some(summary.table_count),
        figure_count: Some(summary.figure_count),
        code_block_count: Some(summary.code_block_count),
        annotation_count: None,
        projection_count: None,
    }
}
