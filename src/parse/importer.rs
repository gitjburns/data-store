//! Core parse importer (spec §12.3, §13.1): validates staged parser output
//! bundles against the binary structural invariants, assigns canonical
//! parse-scoped IDs, writes the canonical parse artifact bundle to the
//! artifact store, and owns every canonical parse-state write (parse_runs,
//! content_units, unit_relationships, and their events). The only writer of
//! canonical parse state. Implemented by work package C4b.
//!
//! Trust boundary (spec §12.1): parser workers are untrusted producers, and
//! everything in a staged bundle is a claim until it passes `read_bundle`
//! digest verification AND the §13.1 hard gates here. The rejected-vs-fault
//! split mirrors `crate::acquisition`: a bundle that breaks the staged
//! contract, reports a failed parser execution, or fails a hard gate is a
//! RECORDED parse outcome — the parse_runs row moves to `failed` with a
//! bounded error and a parse.failed event (§13.5 rule 2: every failed
//! attempt writes a durable failure record) — never an importer `Err`.
//! `Err` is reserved for faults of the canonical side itself (SQL, artifact
//! store, clock); the importer does not mark the run `failed` for its own
//! infrastructure fault. Failed commits leave final durability unconfirmed.
//! Stuck `building` rows are operator-visible through
//! the error logs here; a dedicated health surface arrives at C10b (the
//! scheduler dispatch warns past stale `building` rows meanwhile).
//!
//! One deliberate boundary of the failure-record guarantee: parse_runs
//! hard-references source_objects and its identity columns are NOT NULL, so
//! a bundle whose manifest is unreadable or whose claimed source does not
//! exist cannot leave ANY durable run row. Those unattributable bundles
//! surface as explicit `BadRequest` errors (logged with the bundle path)
//! instead of failure records.
//!
//! Write policy (D1): one fresh hot-plane connection per import, with the
//! building insert (+ parse.started event) and the terminal ready/failed
//! update (+ its event) each in their own transaction, so state changes and
//! their audit events commit or roll back together (the
//! `crate::events::append_event` invariant). The staged bundle directory is
//! never deleted here — success or failure — because staging lifecycle
//! belongs to the dispatch pipeline (C5); failure bundles stay inspectable
//! per spec §12.2.

use std::{collections::BTreeMap, fs, io, path::Path, time::Instant};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::{Map, Value};
use tracing::{error, info, warn};

use crate::artifact_store::{ArtifactRef, ArtifactStore};
use crate::error::ApiError;
use crate::events::{append_event, entry, new_system_event};
use crate::hot_plane;
use crate::ids::{content_unit_id, new_parse_run_id, unit_relationship_id};
use crate::model::{
    CaptionBody, ConformanceReport, ContentType, ContentUnit, ParseMetrics, ParseRun,
    ParseRunStatus, ParseWarning, ParserCapabilityProfile, ProducerType, Provenance,
    SystemEventType, TableCellBody, TextBlockBody, TextSectionBody, UnitRelationship,
    content_type_body_matches,
};
use crate::parse::bundle::{
    BUNDLE_MANIFEST_FILE_NAME, BundleReadError, CandidateWarning, ParserExecutionStatus,
    ParserOutputBundle, ParserOutputManifest, read_bundle,
};
use crate::parse::conformance;
use crate::primitives::utc_now;
use crate::util::truncate_persisted_detail;

/// Spec §13.1 resource limit: maximum candidate ContentUnits per parse.
/// This is a COUNT, not an index: 999,999 units occupy sequence indices
/// 0..=999,998. It is coupled to the persisted canonical-ID contract:
/// `crate::ids` zero-pads the parse-scoped sequence component to six digits
/// (its private `SEQUENCE_DIGITS`), so 999,999 is the largest sequence
/// index that still orders lexicographically — and unitId-ascending order
/// is a meaningful persisted property (spec §16.4). The limit must not grow
/// past 1,000,000 (index 999,999) without widening the six-digit contract.
const MAX_CANDIDATE_UNITS: usize = 999_999;

/// Spec §13.1 resource limit: maximum candidate UnitRelationships per parse.
/// Relationship IDs share the six-digit zero-padding but carry no ordering
/// contract (only unit IDs do, §16.4), so the limit is a plain resource
/// bound above the ID-width boundary; beyond 999,999 the formatting widens
/// naturally and IDs stay unique and deterministic.
const MAX_CANDIDATE_RELATIONSHIPS: usize = 4_000_000;

/// Spec §13.1 resource limit: maximum candidate warnings per parse; bounds
/// the persisted warnings_json column and the warnings artifact.
const MAX_CANDIDATE_WARNINGS: usize = 10_000;

/// Spec §13.1 resource limit: maximum serialized size of one candidate unit
/// body (4 MiB). Measured over the compact plain-serde serialization of the
/// staged claim; the canonical bytes persisted later may differ slightly
/// (NFC, key order), but this is a resource bound, not an exact contract.
const MAX_UNIT_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Schema version written into the canonical parse bundle manifest (spec
/// §12.3). Bump only with a coordinated change to every bundle consumer
/// (rebuild/export/snapshot paths, C9 onward).
const CANONICAL_PARSE_BUNDLE_SCHEMA_VERSION: u32 = 1;

/// File names inside the canonical parse artifact bundle (spec §12.3 MVP
/// subset). Deliberately absent from the full §12.3 layout:
/// source_object.json — the source is already durable as a hot-plane row
/// plus its content-addressed raw blob, its content-identity fields are
/// immutable per §10 rule 1 (so a bundle-time copy could never diverge from
/// the row), and C9 snapshot manifests carry sourceObjects refs first-class,
/// so the bundle never needs to re-establish source identity;
/// retrieval_projections.jsonl — projection producers arrive at C6, which
/// owns their archival; semantic_annotations.jsonl — annotations are
/// post-MVP per §36; artifacts/ — no large parser binaries are imported yet.
const CANONICAL_PARSE_RUN_FILE_NAME: &str = "parse_run.json";
const CANONICAL_CONFORMANCE_FILE_NAME: &str = "conformance_report.json";
const CANONICAL_UNITS_FILE_NAME: &str = "content_units.jsonl";
const CANONICAL_RELATIONSHIPS_FILE_NAME: &str = "unit_relationships.jsonl";
const CANONICAL_WARNINGS_FILE_NAME: &str = "warnings.jsonl";
const CANONICAL_METRICS_FILE_NAME: &str = "metrics.json";

/// Raw manifest is a sibling of parser_raw/ so no staged raw filename can collide.
const CANONICAL_PARSER_RAW_MANIFEST_FILE_NAME: &str = "parser_raw_manifest.json";
/// Version of the raw path-to-immutable-artifact reference document.
const PARSER_RAW_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Record ordering rules recorded in the bundle manifest per JSONL artifact
/// (spec §16.3: record-set hashes are order-significant, so the ordering
/// rule must be a recorded fact). Unit and relationship records are written
/// in canonical-ID-ascending order, which equals candidate order by the
/// §16.4 assignment rule; warnings keep their staged bundle order.
const RECORD_ORDER_CANONICAL_ID_ASCENDING: &str = "canonical_id_ascending";
const RECORD_ORDER_STAGED_BUNDLE: &str = "staged_bundle_order";

/// SystemEvent object_type for parse_runs rows.
const OBJECT_TYPE_PARSE_RUN: &str = "parse_run";

/// Log-event namespace this module passes to the shared hot-plane
/// transaction helpers, so boundary logs stay attributable to parsing.
const TX_LOG_NAMESPACE: &str = "parse";

/// Existence probe for the manifest's claimed source linkage. parse_runs
/// hard-references source_objects, so the claim must resolve before any run
/// row (and therefore any durable failure record) can exist.
const SELECT_SOURCE_OBJECT_EXISTS_SQL: &str = "
SELECT 1 FROM source_objects WHERE id = ?1";

/// Inserts the durable run record in `building` state (spec §12). Terminal
/// columns (completed_at, conformance, artifact refs, error) are set only by
/// the ready/failed updates below.
const INSERT_PARSE_RUN_BUILDING_SQL: &str = "
INSERT INTO parse_runs (
  id, source_id, parser_name, parser_version, parser_config_hash,
  capability_profile_hash, status, started_at, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'building', ?7, ?8)";

/// Terminal failure update (spec §13.5): bounded error detail, completion
/// time. The status guard makes the importer's only legal transition
/// (building → failed) explicit; a zero-row update is an invariant breach
/// surfaced by the caller's affected-row check.
const UPDATE_PARSE_RUN_FAILED_SQL: &str = "
UPDATE parse_runs
SET status = 'failed', completed_at = ?2, error = ?3, parser_raw_output_uri = ?4
WHERE id = ?1 AND status = 'building'";

/// Terminal success update: ready status plus the measured conformance
/// report, the canonical bundle reference, and the imported warnings and
/// metrics — everything §12 records on a completed run short of activation
/// (which is C5's transition). Status-guarded like the failure update.
const UPDATE_PARSE_RUN_READY_SQL: &str = "
UPDATE parse_runs
SET status = 'ready', completed_at = ?2, conformance_report_json = ?3,
    artifact_bundle_uri = ?4, artifact_bundle_hash = ?5,
    warnings_json = ?6, metrics_json = ?7, parser_raw_output_uri = ?8
WHERE id = ?1 AND status = 'building'";

/// Inserts one canonical ContentUnit row (spec §15). structure_hash is
/// deliberately not listed: it is omitted for MVP (no structural projection
/// is defined yet), so the column stays NULL rather than holding a guess.
const INSERT_CONTENT_UNIT_SQL: &str = "
INSERT INTO content_units (
  id, source_id, parse_id, content_type, body_hash, text_hash,
  primary_parent_id, sequence_index, locators_json, body_json, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// Inserts one canonical UnitRelationship row (spec §19). confidence is not
/// listed: candidate relationships carry no confidence claim, so the column
/// stays NULL.
const INSERT_UNIT_RELATIONSHIP_SQL: &str = "
INSERT INTO unit_relationships (
  id, source_id, parse_id, from_unit_id, to_unit_id, relationship_type,
  relationship_role, sequence_index, provenance_json, created_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";

/// Terminal state of one import. Narrower than `ParseRunStatus` on purpose:
/// the importer's only terminal outcomes are ready and failed, and a closed
/// two-state enum makes that invariant a type instead of a convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportedParseStatus {
    /// Bundle verified, all §13.1 gates passed, canonical state committed.
    Ready,
    /// Recorded parse failure: contract violation, failed parser execution,
    /// or a §13.1 gate breach; the failed run row is the durable record.
    Failed,
}

/// Result of importing one staged parser output bundle. Both arms are
/// recorded outcomes backed by a durable parse_runs row; consumers (C5
/// gating/activation) read this instead of re-querying the row.
#[derive(Debug, Clone)]
pub(crate) struct ImportedParse {
    /// The ParseRun this import created (present on both outcomes).
    pub(crate) parse_run_id: String,
    pub(crate) status: ImportedParseStatus,
    /// Canonical rows committed by this import; both are 0 when failed.
    pub(crate) unit_count: u64,
    pub(crate) relationship_count: u64,
    /// Measured conformance (spec §12.5); None when failed — conformance is
    /// measured only over candidates that passed the hard gates.
    // The activation gate (C5a) re-reads the persisted report inside its
    // own barrier-protected transaction, so no in-process consumer exists;
    // consumed by the C10a inspection surfaces.
    #[allow(dead_code)]
    pub(crate) conformance_report: Option<ConformanceReport>,
    /// The canonical parse bundle's manifest blob (spec §12.3), whose
    /// uri/hash are also persisted as artifact_bundle_uri/_hash; None when
    /// failed.
    // Consumed at C9 (archive-verify-delete and snapshot manifests).
    #[allow(dead_code)]
    pub(crate) artifact_bundle: Option<ArtifactRef>,
    /// Why the parse failed (bounded, persisted verbatim as the run's
    /// error); None when ready.
    pub(crate) rejection_detail: Option<String>,
}

/// The canonical rows one import produces, in persisted order (canonical ID
/// ascending, which equals candidate order by the §16.4 assignment rule).
struct CanonicalRows {
    units: Vec<ContentUnit>,
    relationships: Vec<UnitRelationship>,
}

/// Everything the ready transaction persists, bundled so the transaction
/// body takes one coherent state instead of a long parameter list.
struct ReadyState<'a> {
    parse_run_id: &'a str,
    source_id: &'a str,
    rows: &'a CanonicalRows,
    report: &'a ConformanceReport,
    warnings: &'a [ParseWarning],
    metrics: &'a ParseMetrics,
    bundle_ref: &'a ArtifactRef,
    /// Already archived before this transaction; None means no staged raw files.
    parser_raw_output_uri: Option<&'a str>,
}

/// Validate and import one staged parser output bundle (spec §12.1 rule 2:
/// output is not canonical until validated and imported by the core).
/// Sequence: mint a ParseRun and record it `building` (with parse.started),
/// digest-verify the bundle via `read_bundle`, enforce every §13.1 hard
/// gate, measure conformance, assign canonical IDs, write the canonical
/// parse artifact bundle to the artifact store, then commit all canonical
/// rows plus the `ready` transition (with parse.ready) in one transaction.
/// Producer breaches become recorded failed parses; `Err` means the
/// canonical side itself failed (see the module docs for the split).
pub(crate) fn import_parser_bundle(
    index_root: &Path,
    bundle_dir: &Path,
    capability_profile: &ParserCapabilityProfile,
) -> Result<ImportedParse, ApiError> {
    let started = Instant::now();
    info!(
        event = "parse.import_started",
        bundle_dir = %bundle_dir.display(),
        "staged parser bundle import starting"
    );

    // Identity claims must exist before any verification: §13.5 requires a
    // durable record for every failed attempt, and the record needs parser
    // identity plus source linkage from the manifest. This lenient read is
    // for claims only; read_bundle below performs the real verification.
    let claims = match load_manifest_claims(bundle_dir) {
        Ok(claims) => claims,
        Err(source) => {
            error!(
                event = "parse.import_unattributable",
                bundle_dir = %bundle_dir.display(),
                error = %source,
                "staged bundle carries no usable identity claims; no parse run recorded"
            );
            return Err(source);
        }
    };

    let parse_run_id = new_parse_run_id()?;
    let parse_log = crate::util::LogContext::new("parse_import", &parse_run_id);
    parse_log.record("source_id", claims.source_id.as_str());
    parse_log.record("parse_id", parse_run_id.as_str());
    let _parse_log = parse_log.enter();
    let created_at = utc_now()?;
    let mut connection = hot_plane::open_write(index_root)?;
    create_building_run(&mut connection, &parse_run_id, &claims, &created_at)?;
    info!(
        event = "parse.run_created",
        parse_run_id,
        source_id = claims.source_id,
        parser_name = claims.parser_name,
        parser_version = claims.parser_version,
        "parse run recorded in building state"
    );

    // Attribute faults to the run without claiming its final durable status:
    // a failed commit does not expose a confirmed rollback result.
    let attempt = AttributedImport {
        index_root,
        bundle_dir,
        capability_profile,
        parse_run_id: &parse_run_id,
        created_at: &created_at,
        claims: &claims,
        started,
    };
    match run_attributed_import(&attempt, &mut connection) {
        Ok(imported) => Ok(imported),
        Err(source) => {
            error!(
                event = "parse.import_faulted",
                parse_run_id,
                bundle_dir = %bundle_dir.display(),
                error = %source,
                durable_outcome = "unconfirmed_after_building_insert",
                elapsed_ms = started.elapsed().as_millis() as u64,
                "import failed on the canonical side after recording the building run"
            );
            Err(source)
        }
    }
}

/// Context for one attributed import attempt: by construction the run row
/// already exists in `building`, so every fault from here on is attributable
/// to `parse_run_id`. Grouped so the attempt function keeps a short
/// signature; all fields are borrows or `Copy`, so the struct itself is
/// `Copy` and the attempt body binds them like locals.
#[derive(Clone, Copy)]
struct AttributedImport<'a> {
    index_root: &'a Path,
    bundle_dir: &'a Path,
    capability_profile: &'a ParserCapabilityProfile,
    parse_run_id: &'a str,
    created_at: &'a str,
    claims: &'a ParserOutputManifest,
    started: Instant,
}

/// The import attempt once the `building` run row exists: verified bundle
/// read, manifest cross-check, §13.1 hard gates, conformance measurement,
/// canonical ID assignment, canonical bundle write, and the ready
/// transaction. Producer breaches return `Ok` with a recorded failed parse
/// via `fail_parse_run`; every `Err` is a canonical-side fault whose
/// terminal `parse.import_faulted` log the caller owns.
fn run_attributed_import(
    attempt: &AttributedImport<'_>,
    connection: &mut Connection,
) -> Result<ImportedParse, ApiError> {
    let AttributedImport {
        index_root,
        bundle_dir,
        capability_profile,
        parse_run_id,
        created_at,
        claims,
        started,
    } = *attempt;

    // Verified read (C4a): manifest schema + two-way digest coverage +
    // deny_unknown_fields record deserialization. read_bundle owns its own
    // read/verify boundary logs.
    let bundle = match read_bundle(bundle_dir) {
        Ok(bundle) => bundle,
        Err(BundleReadError::ContractViolation { detail }) => {
            // Producer breach: a meaningful parse outcome, recorded durably.
            return fail_parse_run(
                connection,
                parse_run_id,
                &claims.source_id,
                &detail,
                None,
                started,
            );
        }
        // Canonical-side fault: propagate with no failure record; the run
        // row stays `building` and the caller's terminal
        // parse.import_faulted boundary logs it (see module docs).
        Err(BundleReadError::Internal(source)) => return Err(source),
    };

    // The manifest is the one file its own digest list cannot cover, and it
    // was read twice (lenient claims read for the run row, verified read
    // just now). If the two reads disagree, the run row's identity no longer
    // describes the content being imported — a producer/staging breach,
    // recorded as a failed parse under the identity the row already carries.
    if bundle.manifest != *claims {
        return fail_parse_run(
            connection,
            parse_run_id,
            &claims.source_id,
            "staged bundle manifest changed between the claims read and the verified read",
            None,
            started,
        );
    }

    // Verification and identity checks must precede archival. Preserve raw bytes
    // before either terminal path: even a rejected parse needs its originals.
    // Archival faults remain infrastructure errors; no dangling URI is committed.
    let store = ArtifactStore::open(index_root)?;
    let raw_archive = archive_parser_raw(&store, parse_run_id, &bundle.parser_raw_files)?;

    // A failed parser execution stages a complete bundle for diagnostics
    // (spec §12.2); record its original failure and raw URI without canonical units.
    if bundle.parser_result.status == ParserExecutionStatus::Failed {
        let detail = bundle
            .parser_result
            .error
            .as_deref()
            .unwrap_or("parser execution failed without an error detail");
        return fail_parse_run(
            connection,
            parse_run_id,
            &bundle.manifest.source_id,
            detail,
            raw_archive
                .as_ref()
                .map(|archive| archive.manifest.uri.as_str()),
            started,
        );
    }

    // Spec §13.1 hard gates: definitional structural truths, no thresholds.
    let gates_started = Instant::now();
    if let Err(detail) = validate_hard_gates(&bundle, capability_profile) {
        return fail_parse_run(
            connection,
            parse_run_id,
            &bundle.manifest.source_id,
            &detail,
            raw_archive
                .as_ref()
                .map(|archive| archive.manifest.uri.as_str()),
            started,
        );
    }
    info!(
        event = "parse.validation_succeeded",
        parse_run_id,
        source_id = bundle.manifest.source_id,
        unit_count = bundle.candidate_units.len() as u64,
        relationship_count = bundle.candidate_relationships.len() as u64,
        warning_count = bundle.warnings.len() as u64,
        elapsed_ms = gates_started.elapsed().as_millis() as u64,
        "hard-gate validation passed"
    );

    // Conformance is measured over the validated candidates (spec §12.5:
    // always measured, always reported; it gates nothing here).
    let conformance_started = Instant::now();
    let measured_at = utc_now()?;
    let report = conformance::measure(
        parse_run_id,
        &bundle.candidate_units,
        &bundle.candidate_relationships,
        &measured_at,
    )?;
    info!(
        event = "parse.conformance_measured",
        parse_run_id,
        locator_coverage = report.locator_coverage,
        dimension_count = report.dimensions.len() as u64,
        report_hash = report.report_hash,
        elapsed_ms = conformance_started.elapsed().as_millis() as u64,
        "conformance report measured"
    );

    let rows = assign_canonical_rows(parse_run_id, &bundle)?;
    let warnings = canonical_warnings(&bundle.warnings);

    // Build the parse_run.json snapshot for the canonical bundle. It
    // captures the run's identity and its `building` status as of bundle
    // creation: the terminal status, conformance report, and bundle
    // reference cannot appear inside the bundle they describe (the bundle is
    // sealed before the ready commit), and warnings/metrics live in the
    // authoritative sibling artifacts, so those fields are absent here. The
    // hot-plane row is the authoritative terminal record.
    let mut run_snapshot = ParseRun {
        id: parse_run_id.to_owned(),
        source_id: bundle.manifest.source_id.clone(),
        parser_name: bundle.manifest.parser_name.clone(),
        parser_version: bundle.manifest.parser_version.clone(),
        parser_config_hash: bundle.manifest.parser_config_hash.clone(),
        capability_profile_hash: bundle.manifest.capability_profile_hash.clone(),
        status: ParseRunStatus::Building,
        held_reason: None,
        conformance_report: None,
        started_at: Some(created_at.to_owned()),
        completed_at: None,
        activated_at: None,
        archived_at: None,
        artifact_bundle_uri: None,
        artifact_bundle_hash: None,
        parser_raw_output_uri: None,
        warnings: None,
        metrics: None,
        created_at: created_at.to_owned(),
        error: None,
    };

    // Canonical bundle BEFORE the ready transaction (same crash-orphan
    // rationale as the acquisition importer): the artifact store is
    // content-addressed and write-once idempotent, so a crash between these
    // writes and the commit leaves only orphan blobs a re-import of the same
    // content reuses — never a committed row whose bundle reference dangles.
    // Raw output is already immutable before its URI enters either the canonical
    // run snapshot or the ready transaction. No staged path is persisted.
    run_snapshot.parser_raw_output_uri = raw_archive
        .as_ref()
        .map(|archive| archive.manifest.uri.clone());
    let bundle_ref = write_canonical_parse_bundle(
        &store,
        &run_snapshot,
        &report,
        &rows,
        &warnings,
        &bundle.metrics,
        raw_archive.as_ref(),
    )?;

    commit_ready(
        connection,
        &ReadyState {
            parse_run_id,
            source_id: &bundle.manifest.source_id,
            rows: &rows,
            report: &report,
            warnings: &warnings,
            metrics: &bundle.metrics,
            bundle_ref: &bundle_ref,
            parser_raw_output_uri: run_snapshot.parser_raw_output_uri.as_deref(),
        },
    )?;

    let unit_count = rows.units.len() as u64;
    let relationship_count = rows.relationships.len() as u64;
    info!(
        event = "parse.import_succeeded",
        parse_run_id,
        source_id = bundle.manifest.source_id,
        unit_count,
        relationship_count,
        warning_count = warnings.len() as u64,
        artifact_bundle_hash = bundle_ref.hash,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "parser output bundle imported; parse ready"
    );
    Ok(ImportedParse {
        parse_run_id: parse_run_id.to_owned(),
        status: ImportedParseStatus::Ready,
        unit_count,
        relationship_count,
        conformance_report: Some(report),
        artifact_bundle: Some(bundle_ref),
        rejection_detail: None,
    })
}

/// Lenient claims-only read of the staged manifest, used solely to give the
/// `building` run row its identity before verification. A missing manifest
/// is a producer breach (`BadRequest` — no run row can represent it, see the
/// module docs); any other read fault is a canonical-side `InternalIo`.
fn load_manifest_claims(bundle_dir: &Path) -> Result<ParserOutputManifest, ApiError> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST_FILE_NAME);
    let bytes = fs::read(&manifest_path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            ApiError::BadRequest {
                message: format!(
                    "staged parse bundle has no {BUNDLE_MANIFEST_FILE_NAME} at {}: {source}; \
                     a parse run cannot be recorded without identity claims",
                    manifest_path.display()
                ),
            }
        } else {
            ApiError::InternalIo {
                message: format!(
                    "failed to read staged bundle manifest at {}: {source}",
                    manifest_path.display()
                ),
            }
        }
    })?;
    serde_json::from_slice(&bytes).map_err(|source| ApiError::BadRequest {
        message: format!(
            "staged parse bundle manifest at {} is unparseable: {source}; \
             a parse run cannot be recorded without identity claims",
            manifest_path.display()
        ),
    })
}

/// Record the new ParseRun in `building` state with its parse.started event,
/// in one transaction. Pre-checks the claimed source linkage explicitly so
/// an unknown source surfaces as a precise `BadRequest` instead of an opaque
/// foreign-key failure from the insert.
fn create_building_run(
    connection: &mut Connection,
    parse_run_id: &str,
    claims: &ParserOutputManifest,
    now: &str,
) -> Result<(), ApiError> {
    let tx = hot_plane::begin_write_transaction(connection, TX_LOG_NAMESPACE, "create_parse_run")?;
    let body = (|| -> Result<(), ApiError> {
        let source_exists: Option<i64> = tx
            .query_row(
                SELECT_SOURCE_OBJECT_EXISTS_SQL,
                params![claims.source_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| ApiError::StorageOperation {
                message: format!(
                    "failed to look up source object {}: {source}",
                    claims.source_id
                ),
            })?;
        if source_exists.is_none() {
            return Err(ApiError::BadRequest {
                message: format!(
                    "staged parse bundle claims unknown source object {}; \
                     no parse run can be recorded",
                    claims.source_id
                ),
            });
        }

        tx.execute(
            INSERT_PARSE_RUN_BUILDING_SQL,
            params![
                parse_run_id,
                claims.source_id,
                claims.parser_name,
                claims.parser_version,
                claims.parser_config_hash,
                claims.capability_profile_hash,
                now,
                now,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!("failed to insert parse run {parse_run_id}: {source}"),
        })?;

        let payload = Map::from_iter([
            entry("sourceId", &claims.source_id),
            entry("sourceHash", &claims.source_hash),
            entry("parserName", &claims.parser_name),
            entry("parserVersion", &claims.parser_version),
        ]);
        let event = new_system_event(
            SystemEventType::ParseStarted,
            OBJECT_TYPE_PARSE_RUN,
            parse_run_id,
            Some(payload),
        )?;
        append_event(&tx, &event)
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "create_parse_run",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "create_parse_run")
}

/// Record one parse failure durably (spec §13.5 rule 2): building → failed
/// with the bounded detail, plus the parse.failed event, in one transaction.
/// This function owns detail bounding so no caller can persist unbounded
/// producer output. Returns the failed `ImportedParse` — a recorded outcome,
/// not an error; the staged bundle stays on disk for diagnostics.
/// A raw URI is supplied only after verified bytes have been archived, so the
/// original failure and its raw evidence commit together without dangling refs.
fn fail_parse_run(
    connection: &mut Connection,
    parse_run_id: &str,
    source_id: &str,
    detail: &str,
    parser_raw_output_uri: Option<&str>,
    started: Instant,
) -> Result<ImportedParse, ApiError> {
    let detail = truncate_persisted_detail(detail);
    let completed_at = utc_now()?;

    let tx = hot_plane::begin_write_transaction(connection, TX_LOG_NAMESPACE, "fail_parse_run")?;
    let body = (|| -> Result<(), ApiError> {
        let updated = tx
            .execute(
                UPDATE_PARSE_RUN_FAILED_SQL,
                params![parse_run_id, completed_at, detail, parser_raw_output_uri],
            )
            .map_err(|source| ApiError::StorageOperation {
                message: format!("failed to mark parse run {parse_run_id} failed: {source}"),
            })?;
        // The status-guarded update matching no row means the building row
        // vanished or already left building — an invariant breach, never a
        // silent success.
        if updated != 1 {
            return Err(ApiError::StorageOperation {
                message: format!(
                    "parse run {parse_run_id} was not in building state; \
                     failure transition updated {updated} rows"
                ),
            });
        }

        let payload = Map::from_iter([entry("sourceId", source_id), entry("error", &detail)]);
        let event = new_system_event(
            SystemEventType::ParseFailed,
            OBJECT_TYPE_PARSE_RUN,
            parse_run_id,
            Some(payload),
        )?;
        append_event(&tx, &event)
    })();
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "fail_parse_run",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "fail_parse_run")?;

    warn!(
        event = "parse.import_rejected",
        parse_run_id,
        source_id,
        detail,
        bundle_kept = true,
        parser_raw_output_uri,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "parse failed validation; failure recorded, staged bundle kept for diagnostics"
    );
    Ok(ImportedParse {
        parse_run_id: parse_run_id.to_owned(),
        status: ImportedParseStatus::Failed,
        unit_count: 0,
        relationship_count: 0,
        conformance_report: None,
        artifact_bundle: None,
        rejection_detail: Some(detail),
    })
}

/// Enforce every spec §13.1 hard gate over the verified bundle. `Err` is the
/// violation detail — one distinct message per gate, naming the gate and the
/// first offending record — which the caller records as the parse failure.
/// Gate (e), locator shape sanity, needs no code here: the typed locator
/// model already enforced it during `read_bundle` deserialization (bbox is
/// `[f64; 4]`, page numbers and ranges are unsigned, and JSON cannot encode
/// NaN/Infinity), so malformed locators cannot reach this function.
fn validate_hard_gates(
    bundle: &ParserOutputBundle,
    profile: &ParserCapabilityProfile,
) -> Result<(), String> {
    let manifest = &bundle.manifest;

    // Gate (b) precondition: the profile being enforced must be the profile
    // the bundle declares it was produced under. Validating against a
    // different declaration would make the subset checks below meaningless,
    // and doing so silently is exactly the hidden fallback the §12.1 trust
    // boundary forbids.
    if manifest.capability_profile_hash != profile.profile_hash
        || manifest.parser_name != profile.parser_name
        || manifest.parser_version != profile.parser_version
        || manifest.parser_config_hash != profile.parser_config_hash
    {
        return Err(format!(
            "§12.4 capability profile mismatch: bundle declares parser {}/{} \
             (config {}, profile {}), importer is validating against {}/{} \
             (config {}, profile {})",
            manifest.parser_name,
            manifest.parser_version,
            manifest.parser_config_hash,
            manifest.capability_profile_hash,
            profile.parser_name,
            profile.parser_version,
            profile.parser_config_hash,
            profile.profile_hash
        ));
    }

    // Gate (d): resource limits, as documented code constants.
    if bundle.candidate_units.len() > MAX_CANDIDATE_UNITS {
        return Err(format!(
            "§13.1 gate resource_limits: {} candidate units exceed the maximum of \
             {MAX_CANDIDATE_UNITS}",
            bundle.candidate_units.len()
        ));
    }
    if bundle.candidate_relationships.len() > MAX_CANDIDATE_RELATIONSHIPS {
        return Err(format!(
            "§13.1 gate resource_limits: {} candidate relationships exceed the maximum of \
             {MAX_CANDIDATE_RELATIONSHIPS}",
            bundle.candidate_relationships.len()
        ));
    }
    if bundle.warnings.len() > MAX_CANDIDATE_WARNINGS {
        return Err(format!(
            "§13.1 gate resource_limits: {} candidate warnings exceed the maximum of \
             {MAX_CANDIDATE_WARNINGS}",
            bundle.warnings.len()
        ));
    }

    let mut local_ids: BTreeMap<&str, ContentType> = BTreeMap::new();
    for (index, unit) in bundle.candidate_units.iter().enumerate() {
        // Gate: §13.1 "ID assignment rules fail". Canonical unit IDs are
        // derived from candidate order (§16.4), so a claimed sequence index
        // disagreeing with that order means the producer's ordering claim
        // and the deterministic assignment rule diverge — ambiguous content
        // the importer must reject, not silently reinterpret.
        if unit.sequence_index != index as u64 {
            return Err(format!(
                "§13.1 gate id_assignment: unit {} claims sequenceIndex {} at candidate \
                 position {index}",
                unit.local_id, unit.sequence_index
            ));
        }
        // Gate (c): duplicate parser-local IDs make references ambiguous.
        if local_ids
            .insert(unit.local_id.as_str(), unit.content_type)
            .is_some()
        {
            return Err(format!(
                "§13.1 gate local_ref_integrity: duplicate unit localId {}",
                unit.local_id
            ));
        }
        // Gate (d): single-body size bound over the staged serialization.
        let body_bytes = serde_json::to_vec(&unit.body).map_err(|source| {
            format!(
                "§13.1 gate resource_limits: unit {} body is not serializable: {source}",
                unit.local_id
            )
        })?;
        if body_bytes.len() > MAX_UNIT_BODY_BYTES {
            return Err(format!(
                "§13.1 gate resource_limits: unit {} body serializes to {} bytes, exceeding \
                 the maximum of {MAX_UNIT_BODY_BYTES}",
                unit.local_id,
                body_bytes.len()
            ));
        }
        // Gate (a): the §15.2 contentType-to-body mapping.
        if let Err(source) = content_type_body_matches(unit.content_type, &unit.body) {
            return Err(format!(
                "§13.1 gate body_type_mapping: unit {}: {source}",
                unit.local_id
            ));
        }
        // Gate (b): §12.4 — emitting undeclared structure fails validation.
        if !profile.emits_content_types.contains(&unit.content_type) {
            return Err(format!(
                "§13.1 gate capability_profile: unit {} emits undeclared content type {}",
                unit.local_id,
                unit.content_type.wire_name()
            ));
        }
    }

    // Gate (c): parent references, checked in a second pass so a parent
    // appearing later in the candidate stream is legal.
    for unit in &bundle.candidate_units {
        if let Some(parent) = &unit.parent_local_id
            && !local_ids.contains_key(parent.as_str())
        {
            return Err(format!(
                "§13.1 gate local_ref_integrity: unit {} references unknown parent localId \
                 {parent}",
                unit.local_id
            ));
        }
    }

    for (index, relationship) in bundle.candidate_relationships.iter().enumerate() {
        // Same §16.4 assignment rule as units.
        if relationship.sequence_index != index as u64 {
            return Err(format!(
                "§13.1 gate id_assignment: relationship at candidate position {index} claims \
                 sequenceIndex {}",
                relationship.sequence_index
            ));
        }
        // Gate (b) for relationship emissions.
        if !profile
            .emits_relationship_types
            .contains(&relationship.relationship_type)
        {
            return Err(format!(
                "§13.1 gate capability_profile: relationship at sequenceIndex {index} emits \
                 undeclared relationship type {}",
                relationship.relationship_type.wire_name()
            ));
        }
        // Gate (c): both endpoints must resolve to candidate units.
        if !local_ids.contains_key(relationship.from_local_id.as_str()) {
            return Err(format!(
                "§13.1 gate local_ref_integrity: relationship at sequenceIndex {index} \
                 references unknown fromLocalId {}",
                relationship.from_local_id
            ));
        }
        if !local_ids.contains_key(relationship.to_local_id.as_str()) {
            return Err(format!(
                "§13.1 gate local_ref_integrity: relationship at sequenceIndex {index} \
                 references unknown toLocalId {}",
                relationship.to_local_id
            ));
        }
    }

    Ok(())
}

/// Assign canonical parse-scoped IDs (spec §16.4) and build the canonical
/// rows: unit `i` in candidate order becomes `<parseId>:unit:<i>`,
/// relationship `i` becomes `<parseId>:rel:<i>`, and every parser-local
/// reference in envelope fields (primary parent, relationship endpoints) is
/// remapped to the canonical IDs. Typed bodies are copied verbatim: refs
/// embedded INSIDE a body are never remapped, because bodyHash must stay
/// purely content-derived (§16.1) and canonical unit IDs embed the parseId.
/// Workers must therefore omit body fields that would carry unit
/// references (e.g. `captionForUnitIds`, `headerRefs`) and express pairing
/// as UnitRelationship edges instead. Unit-level parser provenance lives on
/// the ParseRun itself (spec §15 defines no per-unit provenance field);
/// relationships carry a full §20 Provenance naming the parser from the
/// verified manifest.
fn assign_canonical_rows(
    parse_run_id: &str,
    bundle: &ParserOutputBundle,
) -> Result<CanonicalRows, ApiError> {
    let manifest = &bundle.manifest;
    // One shared creation timestamp for every row of the parse: the rows
    // become durable together in one transaction, and createdAt is volatile
    // metadata excluded from content hashes (§16.1).
    let created_at = utc_now()?;

    // Full local→canonical map first, so a parent reference to a unit later
    // in the candidate stream resolves.
    let mut canonical_unit_ids: BTreeMap<&str, String> = BTreeMap::new();
    for (index, candidate) in bundle.candidate_units.iter().enumerate() {
        canonical_unit_ids.insert(
            candidate.local_id.as_str(),
            content_unit_id(parse_run_id, index as u64),
        );
    }

    let mut units = Vec::with_capacity(bundle.candidate_units.len());
    for (index, candidate) in bundle.candidate_units.iter().enumerate() {
        let primary_parent_id = candidate
            .parent_local_id
            .as_deref()
            .map(|parent| resolve_local_id(&canonical_unit_ids, parent))
            .transpose()?;
        units.push(ContentUnit {
            id: content_unit_id(parse_run_id, index as u64),
            source_id: manifest.source_id.clone(),
            parse_id: parse_run_id.to_owned(),
            content_type: candidate.content_type,
            body_hash: crate::canonical::canonical_sha256_hex(&candidate.body)?,
            text_hash: text_projection_hash(candidate.content_type, &candidate.body)?,
            // structureHash is omitted for MVP: no structural projection
            // (§16.1 "structure excluding volatile metadata") is defined
            // yet, and an absent hash is honest where a guessed one is not.
            structure_hash: None,
            primary_parent_id,
            sequence_index: Some(index as u64),
            // Empty locator sets persist as absent, matching the §16.2
            // omission convention the model shapes use.
            locators: (!candidate.locators.is_empty()).then(|| candidate.locators.clone()),
            body: candidate.body.clone(),
            created_at: created_at.clone(),
            deleted_at: None,
        });
    }

    // Spec §20: parser-derived structure references parser identity and
    // configuration — from the verified manifest, never from worker claims
    // inside the records.
    let provenance = Provenance {
        producer_type: ProducerType::Parser,
        producer_name: manifest.parser_name.clone(),
        producer_version: Some(manifest.parser_version.clone()),
        config_hash: Some(manifest.parser_config_hash.clone()),
        model_name: None,
        model_version: None,
        prompt_hash: None,
        temperature: None,
        confidence: None,
        memoized: None,
        memoized_from: None,
        memoization_key_hash: None,
        input_refs: None,
    };

    let mut relationships = Vec::with_capacity(bundle.candidate_relationships.len());
    for (index, candidate) in bundle.candidate_relationships.iter().enumerate() {
        relationships.push(UnitRelationship {
            id: unit_relationship_id(parse_run_id, index as u64),
            source_id: manifest.source_id.clone(),
            parse_id: parse_run_id.to_owned(),
            from_unit_id: resolve_local_id(&canonical_unit_ids, &candidate.from_local_id)?,
            to_unit_id: resolve_local_id(&canonical_unit_ids, &candidate.to_local_id)?,
            relationship_type: candidate.relationship_type,
            relationship_role: candidate.relationship_role.clone(),
            sequence_index: Some(index as u64),
            confidence: None,
            provenance: Some(provenance.clone()),
            created_at: created_at.clone(),
            deleted_at: None,
        });
    }

    Ok(CanonicalRows {
        units,
        relationships,
    })
}

/// Resolve one parser-local unit reference to its canonical ID. Validation
/// already proved every reference resolves, so a miss here is an internal
/// invariant breach between the gate pass and ID assignment, not producer
/// input.
fn resolve_local_id(
    canonical_unit_ids: &BTreeMap<&str, String>,
    local_id: &str,
) -> Result<String, ApiError> {
    canonical_unit_ids
        .get(local_id)
        .cloned()
        .ok_or_else(|| ApiError::InternalIo {
            message: format!(
                "validated local unit reference {local_id} has no canonical ID; \
                 gate validation and ID assignment disagree"
            ),
        })
}

/// Compute the §16.1 `textHash` for bodies with a natural text projection:
/// the normalized text when present, else the raw text field. Exactly the
/// spec §18 `text`/`normalizedText` fields qualify (text_block, caption,
/// table_cell, and text_section's normalizedText); code, OCR text, and
/// captions embedded in other bodies are not text projections of THIS unit.
/// The hash is the canonical SHA-256 of the projection as a JSON string
/// (NFC-normalized per §16.2). The body was already gate-validated, so a
/// typed deserialization failure here is an internal invariant breach.
fn text_projection_hash(
    content_type: ContentType,
    body: &Value,
) -> Result<Option<String>, ApiError> {
    let projection: Option<String> = match content_type {
        ContentType::TextBlock => {
            let typed: TextBlockBody = validated_body(body, "text_block")?;
            Some(typed.normalized_text.unwrap_or(typed.text))
        }
        ContentType::Caption => {
            let typed: CaptionBody = validated_body(body, "caption")?;
            Some(typed.normalized_text.unwrap_or(typed.text))
        }
        ContentType::TableCell => {
            let typed: TableCellBody = validated_body(body, "table_cell")?;
            typed.normalized_text.or(typed.text)
        }
        ContentType::TextSection => {
            let typed: TextSectionBody = validated_body(body, "text_section")?;
            typed.normalized_text
        }
        ContentType::Page
        | ContentType::Table
        | ContentType::TableRow
        | ContentType::Figure
        | ContentType::ImageRegion
        | ContentType::CodeBlock => None,
    };
    projection
        .map(|text| crate::canonical::canonical_sha256_hex_of(&text))
        .transpose()
}

/// Deserialize one already-gate-validated body as its typed shape; failure
/// means validation and projection disagree — an internal fault, never a
/// producer outcome.
fn validated_body<T: serde::de::DeserializeOwned>(
    body: &Value,
    what: &'static str,
) -> Result<T, ApiError> {
    T::deserialize(body).map_err(|source| ApiError::InternalIo {
        message: format!(
            "validated {what} body failed typed deserialization during text projection: {source}"
        ),
    })
}

/// Map staged candidate warnings to the canonical §12 ParseWarning shape.
/// Two deliberate reductions, both recoverable from the preserved staged
/// bundle: messages are bounded before persistence, and the parser-local
/// `unit_local_id` is dropped because the canonical ParseWarning defines no
/// unit reference field.
fn canonical_warnings(candidates: &[CandidateWarning]) -> Vec<ParseWarning> {
    candidates
        .iter()
        .map(|warning| ParseWarning {
            code: warning.code.clone(),
            message: truncate_persisted_detail(&warning.message),
            severity: warning.severity,
            locator: warning.locator.clone(),
        })
        .collect()
}

/// Immutable raw files and the manifest that the ready parse row points to.
struct ArchivedParserRaw {
    manifest: ArtifactRef,
    files: BTreeMap<String, ArtifactRef>,
}

/// Path-preserving raw-output index; its content hash is supplied by ArtifactStore.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ParserRawManifest<'a> {
    schema_version: u32,
    parse_run_id: &'a str,
    files: &'a BTreeMap<String, ArtifactRef>,
}

/// Archive only verified in-memory bytes, then publish their manifest last.
/// Empty historical bundles retain None; archival faults remain infrastructure errors.
fn archive_parser_raw(
    store: &ArtifactStore,
    parse_run_id: &str,
    raw_files: &BTreeMap<String, Vec<u8>>,
) -> Result<Option<ArchivedParserRaw>, ApiError> {
    let started = Instant::now();
    let raw_bytes: u64 = raw_files.values().map(|bytes| bytes.len() as u64).sum();
    info!(
        event = "parse.parser_raw_archive_started",
        parse_run_id,
        file_count = raw_files.len(),
        raw_bytes,
        "verified parser raw-output archival started"
    );
    let archived = (|| {
        if raw_files.is_empty() {
            return Ok(None);
        }
        let mut files = BTreeMap::new();
        for (path, bytes) in raw_files {
            // put_bytes preserves the original extraction encoding and byte order;
            // canonical JSON serialization is reserved for the manifest itself.
            let artifact = store.put_bytes(bytes).map_err(|source| ApiError::InternalIo {
                message: format!(
                    "failed to archive verified raw file {path} for parse {parse_run_id}: {source}"
                ),
            })?;
            files.insert(path.clone(), artifact);
        }
        let manifest = store.put_json(&json_value_of(
            &ParserRawManifest {
                schema_version: PARSER_RAW_MANIFEST_SCHEMA_VERSION,
                parse_run_id,
                files: &files,
            },
            "parser raw-output manifest",
        )?)?;
        Ok::<_, ApiError>(Some(ArchivedParserRaw { manifest, files }))
    })();
    match archived {
        Ok(archive) => {
            info!(
                event = "parse.parser_raw_archive_completed",
                parse_run_id,
                file_count = raw_files.len(),
                raw_bytes,
                manifest_uri = archive.as_ref().map(|value| value.manifest.uri.as_str()),
                manifest_hash = archive.as_ref().map(|value| value.manifest.hash.as_str()),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "verified parser raw-output archival completed"
            );
            Ok(archive)
        }
        Err(source) => {
            error!(
                event = "parse.parser_raw_archive_failed",
                parse_run_id,
                file_count = raw_files.len(),
                raw_bytes,
                error = %source,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "verified parser raw-output archival failed"
            );
            Err(source)
        }
    }
}

/// One artifact entry in the canonical bundle manifest: content-addressed
/// reference plus the artifact type, and — for JSONL record sets — the
/// record ordering rule the hash depends on (§16.3 requires ordering to be
/// a recorded manifest fact).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalBundleFile {
    artifact_type: &'static str,
    hash: String,
    uri: String,
    size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_order: Option<&'static str>,
}

/// Canonical parse bundle manifest (spec §12.3): every artifact's path,
/// type, hash, and size, plus schema version, source and parse identity,
/// parser identity/configuration, creation time, and the manifest's own
/// hash computed over the body without the hash field.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalParseBundleManifest {
    schema_version: u32,
    parse_run_id: String,
    source_id: String,
    parser_name: String,
    parser_version: String,
    parser_config_hash: String,
    capability_profile_hash: String,
    created_at: String,
    files: BTreeMap<String, CanonicalBundleFile>,
    /// Serialized as an empty placeholder, removed from the hash input, and
    /// filled with the computed hash before the manifest blob is stored.
    manifest_hash: String,
}

/// Build one manifest entry from a stored artifact's reference.
fn bundle_file(
    artifact_type: &'static str,
    artifact: &ArtifactRef,
    record_order: Option<&'static str>,
) -> CanonicalBundleFile {
    CanonicalBundleFile {
        artifact_type,
        hash: artifact.hash.clone(),
        uri: artifact.uri.clone(),
        size_bytes: artifact.size_bytes,
        record_order,
    }
}

/// Write the canonical parse artifact bundle (spec §12.3) into the artifact
/// store: each record file as a canonical JSON/JSONL blob (via
/// `crate::canonical` inside the store helpers), then the manifest listing
/// them all, written last. Returns the stored manifest's reference — the
/// bundle's single durable entry point, persisted as the run's
/// artifact_bundle_uri/_hash. Metrics are stored as the parser reported
/// them: they are producer claims, and the conformance report is the
/// measured truth beside them.
fn write_canonical_parse_bundle(
    store: &ArtifactStore,
    run: &ParseRun,
    report: &ConformanceReport,
    rows: &CanonicalRows,
    warnings: &[ParseWarning],
    metrics: &ParseMetrics,
    raw_archive: Option<&ArchivedParserRaw>,
) -> Result<ArtifactRef, ApiError> {
    let started = Instant::now();
    info!(
        event = "parse.artifact_bundle_write_started",
        parse_run_id = run.id,
        unit_count = rows.units.len() as u64,
        relationship_count = rows.relationships.len() as u64,
        "canonical parse bundle write starting"
    );

    let mut files: BTreeMap<String, CanonicalBundleFile> = BTreeMap::new();

    let run_artifact = store.put_json(&json_value_of(run, "parse run snapshot")?)?;
    files.insert(
        CANONICAL_PARSE_RUN_FILE_NAME.to_owned(),
        bundle_file("parse_run", &run_artifact, None),
    );

    let report_artifact = store.put_json(&json_value_of(report, "conformance report")?)?;
    files.insert(
        CANONICAL_CONFORMANCE_FILE_NAME.to_owned(),
        bundle_file("conformance_report", &report_artifact, None),
    );

    let unit_values = json_values_of(&rows.units, "content unit")?;
    let units_artifact = store.put_jsonl(&unit_values)?;
    files.insert(
        CANONICAL_UNITS_FILE_NAME.to_owned(),
        bundle_file(
            "content_units",
            &units_artifact,
            Some(RECORD_ORDER_CANONICAL_ID_ASCENDING),
        ),
    );

    let relationship_values = json_values_of(&rows.relationships, "unit relationship")?;
    let relationships_artifact = store.put_jsonl(&relationship_values)?;
    files.insert(
        CANONICAL_RELATIONSHIPS_FILE_NAME.to_owned(),
        bundle_file(
            "unit_relationships",
            &relationships_artifact,
            Some(RECORD_ORDER_CANONICAL_ID_ASCENDING),
        ),
    );

    let warning_values = json_values_of(warnings, "parse warning")?;
    let warnings_artifact = store.put_jsonl(&warning_values)?;
    files.insert(
        CANONICAL_WARNINGS_FILE_NAME.to_owned(),
        bundle_file(
            "warnings",
            &warnings_artifact,
            Some(RECORD_ORDER_STAGED_BUNDLE),
        ),
    );

    let metrics_artifact = store.put_json(&json_value_of(metrics, "parse metrics")?)?;
    files.insert(
        CANONICAL_METRICS_FILE_NAME.to_owned(),
        bundle_file("metrics", &metrics_artifact, None),
    );

    // List every raw blob directly as well as its manifest so the canonical
    // bundle is a complete reference index. Snapshot refs keep this bundle
    // reachable; their current verifier does not recursively hash these children.
    if let Some(raw) = raw_archive {
        for (path, artifact) in &raw.files {
            files.insert(
                path.clone(),
                bundle_file("parser_raw_output", artifact, None),
            );
        }
        files.insert(
            CANONICAL_PARSER_RAW_MANIFEST_FILE_NAME.to_owned(),
            bundle_file("parser_raw_manifest", &raw.manifest, None),
        );
    }

    let artifact_count = files.len();
    let mut manifest = CanonicalParseBundleManifest {
        schema_version: CANONICAL_PARSE_BUNDLE_SCHEMA_VERSION,
        parse_run_id: run.id.clone(),
        source_id: run.source_id.clone(),
        parser_name: run.parser_name.clone(),
        parser_version: run.parser_version.clone(),
        parser_config_hash: run.parser_config_hash.clone(),
        capability_profile_hash: run.capability_profile_hash.clone(),
        created_at: utc_now()?,
        files,
        manifest_hash: String::new(),
    };
    // manifestHash covers the manifest body WITHOUT its own hash field — a
    // record cannot contain its own hash (§12.3, §16.2). The shared
    // self-hash helper removes the placeholder before hashing and fails
    // loudly if a struct rename ever makes the field disappear.
    manifest.manifest_hash =
        crate::canonical::canonical_sha256_hex_without_field(&manifest, "manifestHash")?;
    let manifest_artifact = store.put_json(&json_value_of(
        &manifest,
        "canonical parse bundle manifest",
    )?)?;

    info!(
        event = "parse.artifact_bundle_written",
        parse_run_id = run.id,
        artifact_count = artifact_count as u64,
        bundle_uri = manifest_artifact.uri,
        bundle_hash = manifest_artifact.hash,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "canonical parse bundle written"
    );
    Ok(manifest_artifact)
}

/// Commit the ready state in one transaction: all canonical unit and
/// relationship rows, the building → ready transition with its terminal
/// columns, and the parse.ready event — so the parse becomes ready
/// atomically or not at all.
fn commit_ready(connection: &mut Connection, state: &ReadyState<'_>) -> Result<(), ApiError> {
    let commit_started = Instant::now();
    let tx =
        hot_plane::begin_write_transaction(connection, TX_LOG_NAMESPACE, "commit_parse_ready")?;
    let body = ready_transaction_body(&tx, state);
    if let Err(source) = body {
        return Err(hot_plane::abort_transaction(
            tx,
            TX_LOG_NAMESPACE,
            "commit_parse_ready",
            source,
        ));
    }
    hot_plane::commit_transaction(tx, TX_LOG_NAMESPACE, "commit_parse_ready")?;
    info!(
        event = "parse.ready_committed",
        parse_run_id = state.parse_run_id,
        unit_count = state.rows.units.len() as u64,
        relationship_count = state.rows.relationships.len() as u64,
        elapsed_ms = commit_started.elapsed().as_millis() as u64,
        "ready transaction durable"
    );
    Ok(())
}

/// The persistence phases of the ready transaction, each phase logged with
/// counts per the diagnostics standard (rows are logged as counts and IDs
/// only — never unit contents).
fn ready_transaction_body(tx: &Transaction<'_>, state: &ReadyState<'_>) -> Result<(), ApiError> {
    for unit in &state.rows.units {
        insert_content_unit(tx, unit)?;
    }
    tracing::debug!(
        event = "parse.units_persisted",
        committed = false,
        parse_run_id = state.parse_run_id,
        unit_count = state.rows.units.len() as u64,
        "content unit rows staged in the ready transaction"
    );

    for relationship in &state.rows.relationships {
        insert_unit_relationship(tx, relationship)?;
    }
    tracing::debug!(
        event = "parse.relationships_persisted",
        committed = false,
        parse_run_id = state.parse_run_id,
        relationship_count = state.rows.relationships.len() as u64,
        "unit relationship rows staged in the ready transaction"
    );

    let completed_at = utc_now()?;
    let conformance_json = canonical_json_string_of(state.report, "conformance report")?;
    let warnings_json = canonical_json_string_of(&state.warnings, "parse warnings")?;
    let metrics_json = canonical_json_string_of(state.metrics, "parse metrics")?;
    let updated = tx
        .execute(
            UPDATE_PARSE_RUN_READY_SQL,
            params![
                state.parse_run_id,
                completed_at,
                conformance_json,
                state.bundle_ref.uri,
                state.bundle_ref.hash,
                warnings_json,
                metrics_json,
                state.parser_raw_output_uri,
            ],
        )
        .map_err(|source| ApiError::StorageOperation {
            message: format!(
                "failed to mark parse run {} ready: {source}",
                state.parse_run_id
            ),
        })?;
    // Status-guarded update: matching no row means the building row vanished
    // or already left building — an invariant breach, never a silent success.
    if updated != 1 {
        return Err(ApiError::StorageOperation {
            message: format!(
                "parse run {} was not in building state; ready transition updated {updated} rows",
                state.parse_run_id
            ),
        });
    }
    tracing::debug!(
        event = "parse.run_ready",
        committed = false,
        parse_run_id = state.parse_run_id,
        "parse ready transition staged in the transaction"
    );

    let mut payload = Map::from_iter([
        entry("sourceId", state.source_id),
        entry("artifactBundleHash", &state.bundle_ref.hash),
    ]);
    payload.insert(
        "unitCount".to_owned(),
        Value::Number((state.rows.units.len() as u64).into()),
    );
    payload.insert(
        "relationshipCount".to_owned(),
        Value::Number((state.rows.relationships.len() as u64).into()),
    );
    let event = new_system_event(
        SystemEventType::ParseReady,
        OBJECT_TYPE_PARSE_RUN,
        state.parse_run_id,
        Some(payload),
    )?;
    append_event(tx, &event)
}

/// Insert one canonical ContentUnit row on the caller's transaction.
/// body_json holds the exact canonical bytes whose SHA-256 is body_hash, so
/// the persisted body is always re-verifiable against its hash. Per-row
/// logging is deliberately absent (up to a million rows); the caller logs
/// the phase count.
fn insert_content_unit(tx: &Transaction<'_>, unit: &ContentUnit) -> Result<(), ApiError> {
    let sequence_index = unit
        .sequence_index
        .map(|value| sql_integer(value, "sequence_index"))
        .transpose()?;
    let locators_json = unit
        .locators
        .as_ref()
        .map(|locators| canonical_json_string_of(locators, "content unit locators"))
        .transpose()?;
    let body_json = canonical_json_string_of(&unit.body, "content unit body")?;
    tx.execute(
        INSERT_CONTENT_UNIT_SQL,
        params![
            unit.id,
            unit.source_id,
            unit.parse_id,
            unit.content_type.wire_name(),
            unit.body_hash,
            unit.text_hash,
            unit.primary_parent_id,
            sequence_index,
            locators_json,
            body_json,
            unit.created_at,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!("failed to insert content unit {}: {source}", unit.id),
    })?;
    Ok(())
}

/// Insert one canonical UnitRelationship row on the caller's transaction;
/// same count-not-content logging policy as unit inserts.
fn insert_unit_relationship(
    tx: &Transaction<'_>,
    relationship: &UnitRelationship,
) -> Result<(), ApiError> {
    let sequence_index = relationship
        .sequence_index
        .map(|value| sql_integer(value, "sequence_index"))
        .transpose()?;
    let provenance_json = relationship
        .provenance
        .as_ref()
        .map(|provenance| canonical_json_string_of(provenance, "relationship provenance"))
        .transpose()?;
    tx.execute(
        INSERT_UNIT_RELATIONSHIP_SQL,
        params![
            relationship.id,
            relationship.source_id,
            relationship.parse_id,
            relationship.from_unit_id,
            relationship.to_unit_id,
            relationship.relationship_type.wire_name(),
            relationship.relationship_role,
            sequence_index,
            provenance_json,
            relationship.created_at,
        ],
    )
    .map_err(|source| ApiError::StorageOperation {
        message: format!(
            "failed to insert unit relationship {}: {source}",
            relationship.id
        ),
    })?;
    Ok(())
}

/// Render any serializable shape as a `serde_json::Value` with error
/// context, for the artifact-store JSON/JSONL helpers.
fn json_value_of<T: Serialize>(value: &T, what: &str) -> Result<Value, ApiError> {
    serde_json::to_value(value).map_err(|source| ApiError::InternalIo {
        message: format!("{what} is not representable as JSON: {source}"),
    })
}

/// Render a record slice as JSON values in order, for JSONL artifacts.
fn json_values_of<T: Serialize>(records: &[T], what: &str) -> Result<Vec<Value>, ApiError> {
    records
        .iter()
        .map(|record| json_value_of(record, what))
        .collect()
}

/// Render any model shape as a canonical JSON string for a *_json column
/// (deterministic bytes per spec §16.2; mirrors the private helper in
/// `crate::acquisition` — a shared-util consolidation candidate once a third
/// caller appears).
fn canonical_json_string_of<T: Serialize>(value: &T, what: &str) -> Result<String, ApiError> {
    let bytes = crate::canonical::canonical_json_bytes_of(value)?;
    // Canonical bytes are valid UTF-8 by construction (spec §16.2); the
    // error arm keeps the panic-free Result policy instead of unwrapping.
    String::from_utf8(bytes).map_err(|source| ApiError::InternalIo {
        message: format!("canonical bytes for {what} are not UTF-8: {source}"),
    })
}

/// Convert an unsigned count into an SQLite INTEGER, rejecting values beyond
/// i64 range as a shape violation instead of panicking or wrapping (mirrors
/// the private helper in `crate::acquisition`).
fn sql_integer(value: u64, what: &'static str) -> Result<i64, ApiError> {
    i64::try_from(value).map_err(|_| ApiError::BadRequest {
        message: format!("{what} value {value} exceeds the SQLite integer range"),
    })
}
