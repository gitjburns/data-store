//! Parser output bundle contract (spec §12.1–§12.2): the staged, untrusted
//! artifact set one parser execution emits, and the writer/reader shared by
//! the parser workers (producers, C4c/C4d) and the core importer (consumer,
//! C4b). Implemented by work package C4a.
//!
//! Trust boundary (spec §12.1): parser workers are untrusted producers.
//! They write staged bundles only — never canonical storage or hot indexes —
//! and nothing in a bundle is canonical until the importer validates and
//! imports it. The contract is therefore claims-based, mirroring
//! `crate::connectors`: every manifest field is a producer claim, and
//! `read_bundle` independently recomputes every file digest before the
//! importer trusts any bundle content.
//!
//! Serialization asymmetry: staged bundle files use PLAIN `serde_json`
//! output, not `crate::canonical` serialization. Staged bundles are
//! untrusted producer claims, not hash-input canonical state;
//! canonicalization (NFC, sorted keys, §16.2) happens exactly once, at
//! import, when the core builds the canonical parse bundle (§12.3). The
//! per-file SHA-256 digests in the manifest are integrity checks over the
//! exact staged bytes, not content-identity hashes.
//!
//! IDs: candidate records carry parser-local string IDs only. Canonical
//! parse-scoped IDs (§16.4) are assigned by the importer, never by workers;
//! even the bundle directory name is a worker-chosen unique name with no
//! canonical meaning.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{error, info, warn};

use crate::error::ApiError;
use crate::model::{
    ContentType, Locator, ParseMetrics, ParseWarningSeverity, UnitRelationshipType,
};
use crate::primitives::{current_time_ms, utc_now};
use crate::util::truncate_persisted_detail;

/// Schema version written into and required from `manifest.json`. Bump only
/// with a coordinated producer/consumer change; the reader rejects any other
/// version instead of guessing at field semantics.
pub(crate) const PARSER_OUTPUT_BUNDLE_SCHEMA_VERSION: u32 = 1;

/// Manifest file name inside one bundle (spec §12.2). The manifest is the
/// only bundle file NOT covered by the manifest's own digest map: a file
/// cannot contain its own hash (writing the digest would change the bytes
/// the digest covers), so the manifest is validated structurally instead —
/// parse, schema version, and full digest coverage of every other file.
pub(crate) const BUNDLE_MANIFEST_FILE_NAME: &str = "manifest.json";

/// Parser execution outcome record (`ParserResult`) file name.
pub(crate) const BUNDLE_PARSER_RESULT_FILE_NAME: &str = "parser_result.json";

/// Candidate ContentUnit JSONL file name (one `CandidateContentUnit` per
/// line, sequence order).
pub(crate) const BUNDLE_CANDIDATE_UNITS_FILE_NAME: &str = "candidate_content_units.jsonl";

/// Candidate UnitRelationship JSONL file name (one
/// `CandidateUnitRelationship` per line, sequence order).
pub(crate) const BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME: &str =
    "candidate_unit_relationships.jsonl";

/// Reserved file name from the spec §12.2 layout. Semantic annotations are
/// post-MVP: no writer support and no typed record exist yet, and
/// `read_bundle` does not deserialize this file. If a bundle contains it,
/// the file is digest-verified like any other listed file but its content is
/// ignored at import.
// Consumed by the post-MVP annotation cluster (CAd, spec §36) when
// annotation-emitting parsers arrive.
#[allow(dead_code)]
pub(crate) const BUNDLE_CANDIDATE_ANNOTATIONS_FILE_NAME: &str =
    "candidate_semantic_annotations.jsonl";

/// Parse warning JSONL file name (one `CandidateWarning` per line).
pub(crate) const BUNDLE_WARNINGS_FILE_NAME: &str = "warnings.jsonl";

/// Parser-reported metrics (`crate::model::ParseMetrics`) file name.
pub(crate) const BUNDLE_METRICS_FILE_NAME: &str = "metrics.json";

/// Captured external-process stdout log file name (bounded, see
/// [`BUNDLE_STREAM_LOG_CAP_BYTES`]).
pub(crate) const BUNDLE_STDOUT_LOG_FILE_NAME: &str = "stdout.log";

/// Captured external-process stderr log file name (bounded, see
/// [`BUNDLE_STREAM_LOG_CAP_BYTES`]).
pub(crate) const BUNDLE_STDERR_LOG_FILE_NAME: &str = "stderr.log";

/// Optional subdirectory for preserved parser raw output (spec §12.1 rule
/// 6): diagnostic evidence only, never canonical unless imported.
pub(crate) const BUNDLE_PARSER_RAW_DIR_NAME: &str = "parser_raw";

/// Optional subdirectory for large binary artifacts referenced by hash from
/// candidate records (spec §12.2).
// Consumed (with artifacts_dir below) by the first worker that stages large
// binary artifacts — no cluster is assigned yet; neither MVP worker (C4c
// PDF, C4d text) emits any.
#[allow(dead_code)]
pub(crate) const BUNDLE_ARTIFACTS_DIR_NAME: &str = "artifacts";

/// Files every promoted bundle must contain and list in its manifest. The
/// writer guarantees this set by construction (`finish` writes each one);
/// the reader enforces it so a hand-rolled or corrupted bundle cannot skip
/// a record file. The reserved annotations file and the two optional
/// directories are deliberately absent.
const REQUIRED_BUNDLE_FILES: [&str; 7] = [
    BUNDLE_PARSER_RESULT_FILE_NAME,
    BUNDLE_CANDIDATE_UNITS_FILE_NAME,
    BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME,
    BUNDLE_WARNINGS_FILE_NAME,
    BUNDLE_METRICS_FILE_NAME,
    BUNDLE_STDOUT_LOG_FILE_NAME,
    BUNDLE_STDERR_LOG_FILE_NAME,
];

/// Cap on the PRESERVED payload of each captured process log stream
/// (stdout.log, stderr.log). A truncated log gets an explicit marker line
/// appended after the preserved bytes, so the file may exceed the cap by
/// the marker's length and a capped log is never mistaken for a complete
/// one. 64 KiB keeps failure diagnostics useful without letting a chatty
/// external tool bloat staging.
pub(crate) const BUNDLE_STREAM_LOG_CAP_BYTES: usize = 64 * 1024;

/// Prefix of every bundle directory name this writer creates, promoted and
/// temp alike (`bundle-{epoch_ms}-{seq}`). Shared with the scheduler's
/// startup temp sweep so the sweep and the writer can never drift apart on
/// what a bundle workspace is called.
pub(crate) const BUNDLE_DIR_NAME_PREFIX: &str = "bundle-";

/// Suffix distinguishing an in-progress temp workspace from a promoted
/// bundle. Promoted names never contain a dot, so this suffix can never
/// collide with a contract bundle name; the scheduler's startup sweep
/// targets exactly `{prefix}...{suffix}` directories.
pub(crate) const BUNDLE_TEMP_DIR_SUFFIX: &str = ".tmp";

/// Process-unique sequence for bundle directory names, so many bundles
/// created in the same millisecond stay distinct (same pattern as the
/// Docling conversion-directory counter).
static BUNDLE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Staging root for parser output bundles under the service-owned index
/// root: `{index_root}/fabric/staging/parse`. Staging is not canonical
/// storage: bundles here are untrusted until validated and imported by the
/// core importer (`crate::parse::importer`).
pub(crate) fn parse_staging_root(index_root: &Path) -> PathBuf {
    index_root.join("fabric").join("staging").join("parse")
}

/// Digest claim for one file inside a bundle: SHA-256 (lowercase hex) and
/// byte size over the exact staged bytes. `read_bundle` recomputes both
/// before the importer trusts the file. `Eq` supports the importer's
/// claims-vs-verified manifest cross-check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FileDigest {
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

/// Bundle manifest (spec §12.2): parser identity, source linkage, schema
/// version, creation time, and a digest for EVERY file in the bundle except
/// the manifest itself (which cannot self-hash — see
/// [`BUNDLE_MANIFEST_FILE_NAME`]). Written last by the writer, so a
/// manifest's presence inside a promoted bundle implies the listed files
/// were fully written first. The manifest is the one file its own digest
/// list cannot cover, and the importer reads it twice (lenient claims read,
/// then verified read); `Eq` lets the importer fail the parse if the two
/// reads disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParserOutputManifest {
    /// Must equal [`PARSER_OUTPUT_BUNDLE_SCHEMA_VERSION`]; any other value
    /// is rejected at read.
    pub(crate) schema_version: u32,

    pub(crate) parser_name: String,
    pub(crate) parser_version: String,
    pub(crate) parser_config_hash: String,
    pub(crate) capability_profile_hash: String,

    /// Canonical SourceObject ID the parse ran over (assigned by the core
    /// before dispatch; the worker only echoes it).
    pub(crate) source_id: String,
    /// Canonical `sourceHash` of the parsed source bytes, binding the
    /// bundle to exact input content, not just a source ID.
    pub(crate) source_hash: String,

    /// Bundle creation time (RFC3339 UTC milliseconds).
    pub(crate) created_at: String,

    /// Digest per bundle file, keyed by `/`-separated path relative to the
    /// bundle directory (e.g. `parser_raw/page_1.json`). BTreeMap keeps the
    /// serialized manifest deterministic. Covers every file except
    /// `manifest.json`.
    pub(crate) files: BTreeMap<String, FileDigest>,
}

/// Terminal outcome of one parser execution. Failed executions still stage
/// a complete bundle (spec §12.2: failure bundles may be preserved for
/// diagnostics); the importer records the failure without importing
/// candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParserExecutionStatus {
    Succeeded,
    Failed,
}

/// Parser execution record (`parser_result.json`): outcome, timing, and
/// external-tool identity facts for the ParseRun the importer will create.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ParserResult {
    pub(crate) status: ParserExecutionStatus,
    /// Failure detail when `status` is `failed`. The writer bounds this via
    /// `truncate_persisted_detail` before staging, so it is always safe to
    /// persist into ParseRun failure records as-is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,

    /// Execution start/end (RFC3339 UTC milliseconds).
    pub(crate) started_at: String,
    pub(crate) completed_at: String,
    pub(crate) elapsed_ms: u64,

    /// External-tool identity facts observed during execution (e.g.
    /// `"docling_version" -> "2.x"`). Free-form claims for diagnostics and
    /// provenance; BTreeMap keeps serialization deterministic.
    pub(crate) tool_identity: BTreeMap<String, String>,
}

/// One candidate ContentUnit as emitted by a parser worker. Candidate =
/// untrusted claim: IDs are parser-local, and `body` is untyped JSON that
/// the importer validates against the §15.2 content-type-to-body mapping
/// (`crate::model::body::content_type_body_matches`) before any canonical
/// unit is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CandidateContentUnit {
    /// Parser-local identifier, unique within this bundle. Replaced by a
    /// canonical parse-scoped unit ID (§16.4) at import; never persisted.
    pub(crate) local_id: String,

    pub(crate) content_type: ContentType,

    /// Untyped body payload; validated against `content_type` at import.
    pub(crate) body: serde_json::Value,

    /// Parser-local ID of the primary parent unit, when the unit has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_local_id: Option<String>,

    /// Position in the parse's stable reading order; the importer derives
    /// the deterministic canonical unit ID from it.
    pub(crate) sequence_index: u64,

    /// Source-position locators (§17). May be empty when the parser cannot
    /// locate the unit; conformance measures the resulting coverage.
    pub(crate) locators: Vec<Locator>,
}

/// One candidate UnitRelationship between two parser-local unit IDs. The
/// importer resolves both endpoints to canonical unit IDs; a dangling local
/// reference fails validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CandidateUnitRelationship {
    pub(crate) from_local_id: String,
    pub(crate) to_local_id: String,

    pub(crate) relationship_type: UnitRelationshipType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relationship_role: Option<String>,

    /// Position in the parse's stable relationship order; the importer
    /// derives the deterministic canonical relationship ID from it.
    pub(crate) sequence_index: u64,
}

/// One non-fatal parse finding, mirroring `crate::model::ParseWarning` but
/// with a parser-local unit reference instead of a canonical one. The
/// importer maps `unit_local_id` to the canonical unit ID (or drops the
/// reference when the offending unit itself failed validation).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CandidateWarning {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) severity: ParseWarningSeverity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) locator: Option<Locator>,
    /// Parser-local ID of the unit this warning concerns, when unit-scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unit_local_id: Option<String>,
}

/// Producer identity a worker declares when opening a bundle; folded
/// verbatim into the manifest at `finish`. Captured at creation so even a
/// bundle that fails mid-parse carries full identity claims.
#[derive(Debug, Clone)]
pub(crate) struct BundleIdentity {
    pub(crate) parser_name: String,
    pub(crate) parser_version: String,
    pub(crate) parser_config_hash: String,
    pub(crate) capability_profile_hash: String,
    pub(crate) source_id: String,
    pub(crate) source_hash: String,
}

/// A fully verified in-memory bundle, as returned by [`read_bundle`]. Every
/// file digest has been recomputed and every record deserialized through
/// `deny_unknown_fields` shapes; content is still CANDIDATE data — §13.1
/// structural validation happens in the importer, not here.
#[derive(Debug)]
pub(crate) struct ParserOutputBundle {
    /// Promoted bundle directory the content was read from.
    // The importer names bundle paths from its own parameter today; this
    // self-describing field is consumed at C9 (archive-verify reads of
    // preserved failure bundles).
    #[allow(dead_code)]
    pub(crate) bundle_dir: PathBuf,
    pub(crate) manifest: ParserOutputManifest,
    pub(crate) parser_result: ParserResult,
    pub(crate) candidate_units: Vec<CandidateContentUnit>,
    pub(crate) candidate_relationships: Vec<CandidateUnitRelationship>,
    pub(crate) warnings: Vec<CandidateWarning>,
    pub(crate) metrics: ParseMetrics,
    /// Original parser output and cleanup reports, keyed by bundle-relative path.
    /// These are the exact bytes already checked against the staged manifest.
    pub(crate) parser_raw_files: BTreeMap<String, Vec<u8>>,
}

/// Why a bundle read failed, discriminated by which side of the trust
/// boundary faulted (same split as `crate::connectors::ScanError`). The
/// importer routes the arms differently: a `ContractViolation` is a
/// meaningful parse outcome — the producer staged a bundle that breaks the
/// contract — recorded as a durable failed ParseRun, while `Internal` is a
/// canonical-side fault (importer environment) that propagates as an error
/// with no failure record blamed on the parse.
#[derive(Debug)]
pub(crate) enum BundleReadError {
    /// Local infrastructure failed while reading (I/O fault other than a
    /// missing file); not a statement about the bundle's content.
    Internal(ApiError),
    /// The bundle violates the staged-output contract: missing or unlisted
    /// files, digest/size mismatch, bad schema version, or malformed
    /// records. `detail` names the offending file, is bounded via
    /// `truncate_persisted_detail`, and is safe to persist as a ParseRun
    /// failure detail as-is.
    ContractViolation { detail: String },
}

/// Build a bounded `ContractViolation`, applying the persisted-detail cap at
/// construction so no caller can accidentally persist unbounded producer
/// output.
fn violation(detail: impl AsRef<str>) -> BundleReadError {
    BundleReadError::ContractViolation {
        detail: truncate_persisted_detail(detail.as_ref()),
    }
}

/// Wrap a local I/O fault as an internal read error with the failing path.
fn internal_io(context: &str, path: &Path, source: &io::Error) -> BundleReadError {
    BundleReadError::Internal(ApiError::InternalIo {
        message: format!("{context} at {}: {source}", path.display()),
    })
}

/// One open JSONL output stream inside the writer: buffered file handle plus
/// the record count reported in boundary logs.
struct JsonlStream {
    writer: BufWriter<File>,
    records_written: u64,
}

impl JsonlStream {
    /// Open (create/truncate) one JSONL stream file inside the temp bundle.
    fn create(path: &Path) -> Result<Self, ApiError> {
        let file = File::create(path).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create bundle stream file at {}: {source}",
                path.display()
            ),
        })?;
        Ok(Self {
            writer: BufWriter::new(file),
            records_written: 0,
        })
    }

    /// Append one record as a plain serde_json line (LF-terminated). Plain,
    /// not canonical, serialization by design: staged records are untrusted
    /// claims, and compact serde_json never emits a raw newline, so one
    /// line is always one record.
    fn append<T: Serialize>(&mut self, record: &T, file_name: &str) -> Result<(), ApiError> {
        let mut line = serde_json::to_vec(record).map_err(|source| ApiError::InternalIo {
            message: format!("failed to serialize {file_name} record: {source}"),
        })?;
        line.push(b'\n');
        self.writer
            .write_all(&line)
            .map_err(|source| ApiError::InternalIo {
                message: format!("failed to append to {file_name}: {source}"),
            })?;
        self.records_written += 1;
        Ok(())
    }

    /// Flush buffered records to the file before hashing/promotion.
    fn flush(&mut self, file_name: &str) -> Result<(), ApiError> {
        self.writer.flush().map_err(|source| ApiError::InternalIo {
            message: format!("failed to flush {file_name}: {source}"),
        })
    }
}

/// Worker-side bundle writer. Streams candidate records to JSONL files in a
/// temp directory, then `finish` seals the bundle: outcome + metrics +
/// bounded logs written, every file digested, manifest written LAST, temp
/// directory atomically renamed to the contract bundle name. A
/// contract-named bundle directory is therefore only ever absent or
/// complete, never partial — the same promotion invariant as the C3
/// acquisition staging.
///
/// Bundle names are worker-chosen and unique (`bundle-{epoch_ms}-{seq}`,
/// like Docling conversion directories); they carry no canonical meaning.
/// If the process crashes before `finish`, the leftover `.tmp` directory is
/// inert: its name never matches a promoted bundle name, so no consumer
/// will ever read it, and the scheduler removes such orphans at thread
/// start (its sweep keys on BUNDLE_DIR_NAME_PREFIX/BUNDLE_TEMP_DIR_SUFFIX).
pub(crate) struct BundleWriter {
    identity: BundleIdentity,
    /// In-progress directory (`<bundle>.tmp`); renamed to `bundle_dir` on
    /// successful `finish`.
    temp_dir: PathBuf,
    /// Contract-named destination; must not exist until promotion.
    bundle_dir: PathBuf,
    units: JsonlStream,
    relationships: JsonlStream,
    warnings: JsonlStream,
    started: Instant,
}

impl BundleWriter {
    /// Open a new bundle under `staging_root`: create the staging root and
    /// a unique temp directory, and eagerly create the three JSONL stream
    /// files so even a zero-record bundle contains the full required file
    /// set.
    pub(crate) fn create(staging_root: &Path, identity: BundleIdentity) -> Result<Self, ApiError> {
        fs::create_dir_all(staging_root).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create parse staging root at {}: {source}",
                staging_root.display()
            ),
        })?;

        // Worker-chosen unique name: epoch-millisecond timestamp plus a
        // process-unique counter (same scheme as Docling conversion dirs).
        let timestamp_ms = current_time_ms()?;
        let sequence = BUNDLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let bundle_dir =
            staging_root.join(format!("{BUNDLE_DIR_NAME_PREFIX}{timestamp_ms}-{sequence}"));
        // Promoted names never contain a dot, so the temp suffix can never
        // collide with a contract bundle name (see BUNDLE_TEMP_DIR_SUFFIX).
        let temp_dir = staging_root.join(format!(
            "{BUNDLE_DIR_NAME_PREFIX}{timestamp_ms}-{sequence}{BUNDLE_TEMP_DIR_SUFFIX}"
        ));
        fs::create_dir(&temp_dir).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create temp bundle directory at {}: {source}",
                temp_dir.display()
            ),
        })?;

        let units = JsonlStream::create(&temp_dir.join(BUNDLE_CANDIDATE_UNITS_FILE_NAME))?;
        let relationships =
            JsonlStream::create(&temp_dir.join(BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME))?;
        let warnings = JsonlStream::create(&temp_dir.join(BUNDLE_WARNINGS_FILE_NAME))?;

        info!(
            event = "parse.bundle.write_started",
            temp_dir = %temp_dir.display(),
            parser_name = identity.parser_name,
            source_id = identity.source_id,
            "parser output bundle write starting"
        );

        Ok(Self {
            identity,
            temp_dir,
            bundle_dir,
            units,
            relationships,
            warnings,
            started: Instant::now(),
        })
    }

    /// Stream one candidate unit to `candidate_content_units.jsonl`.
    pub(crate) fn append_candidate_unit(
        &mut self,
        unit: &CandidateContentUnit,
    ) -> Result<(), ApiError> {
        self.units.append(unit, BUNDLE_CANDIDATE_UNITS_FILE_NAME)
    }

    /// Stream one candidate relationship to
    /// `candidate_unit_relationships.jsonl`.
    pub(crate) fn append_candidate_relationship(
        &mut self,
        relationship: &CandidateUnitRelationship,
    ) -> Result<(), ApiError> {
        self.relationships
            .append(relationship, BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME)
    }

    /// Stream one warning to `warnings.jsonl`.
    pub(crate) fn append_warning(&mut self, warning: &CandidateWarning) -> Result<(), ApiError> {
        self.warnings.append(warning, BUNDLE_WARNINGS_FILE_NAME)
    }

    /// Directory for preserved parser raw output (created on first use).
    /// The worker may use it as a disposable workspace; every regular file
    /// left inside at `finish` is digested into the manifest.
    pub(crate) fn parser_raw_dir(&self) -> Result<PathBuf, ApiError> {
        self.optional_dir(BUNDLE_PARSER_RAW_DIR_NAME)
    }

    /// Directory for large binary artifacts referenced from candidate
    /// records (created on first use); files inside are digested into the
    /// manifest at `finish`.
    // Consumed by the first worker that stages large binary artifacts (see
    // BUNDLE_ARTIFACTS_DIR_NAME); neither MVP worker emits any.
    #[allow(dead_code)]
    pub(crate) fn artifacts_dir(&self) -> Result<PathBuf, ApiError> {
        self.optional_dir(BUNDLE_ARTIFACTS_DIR_NAME)
    }

    /// Create-on-demand for the two optional bundle subdirectories.
    fn optional_dir(&self, name: &str) -> Result<PathBuf, ApiError> {
        let dir = self.temp_dir.join(name);
        fs::create_dir_all(&dir).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create bundle subdirectory at {}: {source}",
                dir.display()
            ),
        })?;
        Ok(dir)
    }

    /// Seal and promote the bundle. Writes `parser_result.json` (failure
    /// detail bounded), `metrics.json`, the bounded stdout/stderr logs,
    /// digests every file in the temp tree, writes `manifest.json` LAST,
    /// then atomically renames the temp directory to the contract bundle
    /// name and returns it. On any failure the temp directory is removed
    /// best-effort so no partial bundle lingers; the contract name is never
    /// created on failure.
    pub(crate) fn finish(
        mut self,
        parser_result: &ParserResult,
        metrics: &ParseMetrics,
        stdout_log: &[u8],
        stderr_log: &[u8],
    ) -> Result<PathBuf, ApiError> {
        let unit_count = self.units.records_written;
        let relationship_count = self.relationships.records_written;
        let warning_count = self.warnings.records_written;

        match self.finish_inner(parser_result, metrics, stdout_log, stderr_log) {
            Ok((bundle_dir, file_count)) => {
                info!(
                    event = "parse.bundle.promoted",
                    bundle_dir = %bundle_dir.display(),
                    parser_name = self.identity.parser_name,
                    source_id = self.identity.source_id,
                    status = ?parser_result.status,
                    unit_count,
                    relationship_count,
                    warning_count,
                    file_count,
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "parser output bundle promoted"
                );
                Ok(bundle_dir)
            }
            Err(finish_error) => {
                // Best-effort cleanup so a failed seal does not leave temp
                // litter; the failure being reported is the seal error, not
                // the cleanup.
                if self.temp_dir.exists()
                    && let Err(cleanup_error) = fs::remove_dir_all(&self.temp_dir)
                {
                    warn!(
                        event = "parse.bundle.temp_cleanup_failed",
                        temp_dir = %self.temp_dir.display(),
                        error = %cleanup_error,
                        "temp bundle directory cleanup failed after seal error"
                    );
                }
                error!(
                    event = "parse.bundle.write_failed",
                    temp_dir = %self.temp_dir.display(),
                    parser_name = self.identity.parser_name,
                    source_id = self.identity.source_id,
                    error = %finish_error,
                    elapsed_ms = self.started.elapsed().as_millis() as u64,
                    "parser output bundle write failed"
                );
                Err(finish_error)
            }
        }
    }

    /// Seal steps shared by the success and cleanup paths of `finish`;
    /// returns the promoted directory and its digested file count.
    fn finish_inner(
        &mut self,
        parser_result: &ParserResult,
        metrics: &ParseMetrics,
        stdout_log: &[u8],
        stderr_log: &[u8],
    ) -> Result<(PathBuf, usize), ApiError> {
        self.units.flush(BUNDLE_CANDIDATE_UNITS_FILE_NAME)?;
        self.relationships
            .flush(BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME)?;
        self.warnings.flush(BUNDLE_WARNINGS_FILE_NAME)?;

        // Bound the failure detail defensively at the staging boundary so a
        // pathological tool error can never bloat the persisted record the
        // importer builds from it.
        let mut bounded_result = parser_result.clone();
        if let Some(error_detail) = &bounded_result.error {
            bounded_result.error = Some(truncate_persisted_detail(error_detail));
        }
        write_json_file(
            &self.temp_dir.join(BUNDLE_PARSER_RESULT_FILE_NAME),
            BUNDLE_PARSER_RESULT_FILE_NAME,
            &bounded_result,
        )?;
        write_json_file(
            &self.temp_dir.join(BUNDLE_METRICS_FILE_NAME),
            BUNDLE_METRICS_FILE_NAME,
            metrics,
        )?;
        write_bounded_log(&self.temp_dir.join(BUNDLE_STDOUT_LOG_FILE_NAME), stdout_log)?;
        write_bounded_log(&self.temp_dir.join(BUNDLE_STDERR_LOG_FILE_NAME), stderr_log)?;

        // Digest the exact bytes on disk (read back after flush) so every
        // manifest claim describes what the bundle actually contains,
        // including files the worker dropped into parser_raw/ or artifacts/
        // directly.
        let disk_files = collect_regular_files(&self.temp_dir).map_err(|walk| match walk {
            WalkError::Io { detail } | WalkError::Contract { detail } => ApiError::InternalIo {
                message: format!("bundle file enumeration failed: {detail}"),
            },
        })?;
        let mut files = BTreeMap::new();
        for (rel_path, abs_path) in &disk_files {
            let bytes = fs::read(abs_path).map_err(|source| ApiError::InternalIo {
                message: format!(
                    "failed to read staged bundle file {rel_path} at {}: {source}",
                    abs_path.display()
                ),
            })?;
            files.insert(
                rel_path.clone(),
                FileDigest {
                    sha256: crate::canonical::sha256_hex_bytes(&bytes),
                    size_bytes: bytes.len() as u64,
                },
            );
        }
        let file_count = files.len();

        let manifest = ParserOutputManifest {
            schema_version: PARSER_OUTPUT_BUNDLE_SCHEMA_VERSION,
            parser_name: self.identity.parser_name.clone(),
            parser_version: self.identity.parser_version.clone(),
            parser_config_hash: self.identity.parser_config_hash.clone(),
            capability_profile_hash: self.identity.capability_profile_hash.clone(),
            source_id: self.identity.source_id.clone(),
            source_hash: self.identity.source_hash.clone(),
            created_at: utc_now()?,
            files,
        };
        // Manifest written last: its presence marks the file set complete.
        write_json_file(
            &self.temp_dir.join(BUNDLE_MANIFEST_FILE_NAME),
            BUNDLE_MANIFEST_FILE_NAME,
            &manifest,
        )?;

        // Atomic promotion: the contract name appears only once the bundle
        // is complete. Names are unique per attempt (no coalescing, unlike
        // acquisition staging), so the destination never pre-exists and a
        // rename failure is a real fault.
        fs::rename(&self.temp_dir, &self.bundle_dir).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to promote bundle {} -> {}: {source}",
                self.temp_dir.display(),
                self.bundle_dir.display()
            ),
        })?;

        Ok((self.bundle_dir.clone(), file_count))
    }
}

/// Write one plain serde_json file into the temp bundle.
fn write_json_file<T: Serialize>(path: &Path, file_name: &str, value: &T) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec(value).map_err(|source| ApiError::InternalIo {
        message: format!("failed to serialize {file_name}: {source}"),
    })?;
    fs::write(path, bytes).map_err(|source| ApiError::InternalIo {
        message: format!(
            "failed to write {file_name} at {}: {source}",
            path.display()
        ),
    })
}

/// Write one captured process log stream, preserving at most
/// [`BUNDLE_STREAM_LOG_CAP_BYTES`] bytes and appending an explicit
/// truncation marker when anything was omitted. Logs are opaque bytes (a
/// child process owns their encoding), so the cut may split a UTF-8
/// sequence; the marker makes the truncation unmistakable either way.
fn write_bounded_log(path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
    let io_error = |source: &io::Error| ApiError::InternalIo {
        message: format!(
            "failed to write bounded log at {}: {source}",
            path.display()
        ),
    };
    if bytes.len() <= BUNDLE_STREAM_LOG_CAP_BYTES {
        return fs::write(path, bytes).map_err(|source| io_error(&source));
    }
    let omitted = bytes.len() - BUNDLE_STREAM_LOG_CAP_BYTES;
    let mut bounded = bytes[..BUNDLE_STREAM_LOG_CAP_BYTES].to_vec();
    bounded.extend_from_slice(
        format!("\n--- log truncated: {omitted} bytes omitted ---\n").as_bytes(),
    );
    fs::write(path, bounded).map_err(|source| io_error(&source))
}

/// How a bundle-tree walk failed. Kept distinct from `BundleReadError`
/// because the walker serves both sides of the trust boundary: the writer
/// maps every arm to an internal error (its own output tree is service
/// state), while the reader maps `Contract` to a contract violation
/// (untrusted producer content) and `Io` to an internal fault.
enum WalkError {
    /// Filesystem fault while enumerating (unreadable directory, etc.).
    Io { detail: String },
    /// The tree contains something the contract forbids: a symlink, a
    /// special file, or a non-UTF-8 file name.
    Contract { detail: String },
}

/// Recursively enumerate every regular file under `root`, keyed by
/// `/`-separated relative path. Symlinks and special files are rejected
/// rather than followed: a staged bundle must be self-contained, and a
/// symlink could smuggle bytes from outside the bundle past digest
/// verification.
fn collect_regular_files(root: &Path) -> Result<BTreeMap<String, PathBuf>, WalkError> {
    let mut files = BTreeMap::new();
    collect_regular_files_into(root, root, &mut files)?;
    Ok(files)
}

/// Depth-first worker for `collect_regular_files`; `root` stays fixed so
/// relative keys are always bundle-relative.
fn collect_regular_files_into(
    root: &Path,
    dir: &Path,
    files: &mut BTreeMap<String, PathBuf>,
) -> Result<(), WalkError> {
    let entries = fs::read_dir(dir).map_err(|source| WalkError::Io {
        detail: format!("failed to read directory {}: {source}", dir.display()),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| WalkError::Io {
            detail: format!("failed to read entry in {}: {source}", dir.display()),
        })?;
        let path = entry.path();
        // symlink_metadata (not metadata) so links are detected, never
        // followed.
        let metadata = fs::symlink_metadata(&path).map_err(|source| WalkError::Io {
            detail: format!("failed to stat {}: {source}", path.display()),
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(WalkError::Contract {
                detail: format!("bundle contains a symlink: {}", path.display()),
            });
        }
        if file_type.is_dir() {
            collect_regular_files_into(root, &path, files)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(WalkError::Contract {
                detail: format!("bundle contains a non-regular file: {}", path.display()),
            });
        }
        // Relative key with `/` separators; the path is under `root` by
        // construction, so strip_prefix cannot fail.
        let rel_path = path
            .strip_prefix(root)
            .map_err(|_| WalkError::Io {
                detail: format!("file escaped the bundle root: {}", path.display()),
            })?
            .to_str()
            .ok_or_else(|| WalkError::Contract {
                detail: format!("bundle file name is not valid UTF-8: {}", path.display()),
            })?
            .to_string();
        files.insert(rel_path, path);
    }
    Ok(())
}

/// Importer-side bundle read: load and structurally validate the manifest,
/// verify EVERY listed file's recomputed SHA-256 and byte size against the
/// bytes on disk, reject unlisted files, and deserialize all record files
/// through `deny_unknown_fields` shapes. This is the verification boundary
/// of the §12.1 trust split: nothing from a bundle reaches the importer's
/// validation logic without passing it. Content is still candidate claims —
/// §13.1 structural validation is the importer's job, not this function's.
pub(crate) fn read_bundle(bundle_dir: &Path) -> Result<ParserOutputBundle, BundleReadError> {
    let started = Instant::now();
    info!(
        event = "parse.bundle.read_started",
        bundle_dir = %bundle_dir.display(),
        "parser output bundle read starting"
    );
    match read_bundle_inner(bundle_dir) {
        Ok(bundle) => {
            info!(
                event = "parse.bundle.read_succeeded",
                bundle_dir = %bundle_dir.display(),
                parser_name = bundle.manifest.parser_name,
                source_id = bundle.manifest.source_id,
                status = ?bundle.parser_result.status,
                file_count = bundle.manifest.files.len(),
                unit_count = bundle.candidate_units.len(),
                relationship_count = bundle.candidate_relationships.len(),
                warning_count = bundle.warnings.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parser output bundle read and verified"
            );
            Ok(bundle)
        }
        Err(read_error) => {
            // Two log severities for the two failure arms: a rejected
            // bundle is an expected untrusted-producer outcome (warn), an
            // internal fault is our own infrastructure failing (error).
            match &read_error {
                BundleReadError::ContractViolation { detail } => warn!(
                    event = "parse.bundle.rejected",
                    bundle_dir = %bundle_dir.display(),
                    detail,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "parser output bundle violates the staged-output contract"
                ),
                BundleReadError::Internal(source) => error!(
                    event = "parse.bundle.read_failed",
                    bundle_dir = %bundle_dir.display(),
                    error = %source,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "parser output bundle read failed on the canonical side"
                ),
            }
            Err(read_error)
        }
    }
}

/// Read/verify steps shared by the logging wrapper `read_bundle`.
fn read_bundle_inner(bundle_dir: &Path) -> Result<ParserOutputBundle, BundleReadError> {
    let manifest = load_manifest(bundle_dir)?;
    let record_bytes = verify_listed_files(bundle_dir, &manifest)?;

    // Deserialize the record files out of the already-verified bytes; every
    // shape carries deny_unknown_fields, so undeclared fields are rejected
    // here rather than silently dropped.
    let parser_result: ParserResult =
        parse_json_record(BUNDLE_PARSER_RESULT_FILE_NAME, &record_bytes)?;
    let metrics: ParseMetrics = parse_json_record(BUNDLE_METRICS_FILE_NAME, &record_bytes)?;
    let candidate_units = parse_jsonl_records(BUNDLE_CANDIDATE_UNITS_FILE_NAME, &record_bytes)?;
    let candidate_relationships =
        parse_jsonl_records(BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME, &record_bytes)?;
    let warnings = parse_jsonl_records(BUNDLE_WARNINGS_FILE_NAME, &record_bytes)?;
    // Transfer verified bytes to the importer; reopening staging paths after
    // digest validation would archive potentially different, unverified data.
    let parser_raw_files = record_bytes
        .into_iter()
        .filter(|(path, _)| is_parser_raw_path(path))
        .collect();

    Ok(ParserOutputBundle {
        bundle_dir: bundle_dir.to_path_buf(),
        manifest,
        parser_result,
        candidate_units,
        candidate_relationships,
        warnings,
        metrics,
        parser_raw_files,
    })
}

/// Load and structurally validate `manifest.json`: parse through
/// deny_unknown_fields, check the schema version, and require every
/// contract-mandated file to be listed. A missing manifest is a contract
/// violation (the atomic promotion invariant means a visible bundle is
/// complete), while any other read fault is an internal error.
fn load_manifest(bundle_dir: &Path) -> Result<ParserOutputManifest, BundleReadError> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST_FILE_NAME);
    let manifest_bytes = fs::read(&manifest_path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            violation(format!(
                "missing {BUNDLE_MANIFEST_FILE_NAME} at {}",
                manifest_path.display()
            ))
        } else {
            internal_io("failed to read bundle manifest", &manifest_path, &source)
        }
    })?;
    let manifest: ParserOutputManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|source| violation(format!("invalid {BUNDLE_MANIFEST_FILE_NAME}: {source}")))?;

    if manifest.schema_version != PARSER_OUTPUT_BUNDLE_SCHEMA_VERSION {
        return Err(violation(format!(
            "unsupported bundle schema version {} (expected {PARSER_OUTPUT_BUNDLE_SCHEMA_VERSION})",
            manifest.schema_version
        )));
    }
    for required in REQUIRED_BUNDLE_FILES {
        if !manifest.files.contains_key(required) {
            return Err(violation(format!(
                "manifest does not list required file {required}"
            )));
        }
    }
    Ok(manifest)
}

/// Verify the manifest's digest coverage against the bytes on disk, both
/// directions: every listed file must exist with matching recomputed
/// SHA-256 and size, and every on-disk file (except the manifest itself)
/// must be listed — an unlisted file is unaccounted producer content and
/// fails verification. Retains record files and parser_raw/ bytes so deserialization
/// and archival both consume the exact bytes checked here, with one read per file.
fn verify_listed_files(
    bundle_dir: &Path,
    manifest: &ParserOutputManifest,
) -> Result<BTreeMap<String, Vec<u8>>, BundleReadError> {
    let disk_files = collect_regular_files(bundle_dir).map_err(|walk| match walk {
        WalkError::Io { detail } => BundleReadError::Internal(ApiError::InternalIo {
            message: format!("bundle file enumeration failed: {detail}"),
        }),
        WalkError::Contract { detail } => violation(detail),
    })?;

    // Coverage check: nothing on disk may escape the manifest.
    for rel_path in disk_files.keys() {
        if rel_path != BUNDLE_MANIFEST_FILE_NAME && !manifest.files.contains_key(rel_path) {
            return Err(violation(format!(
                "bundle file not covered by manifest: {rel_path}"
            )));
        }
    }

    // Record files feed canonical import; parser_raw/ files feed lossless archival.
    // Logs and optional artifacts retain their existing verification-only behavior.
    let retained: [&str; 5] = [
        BUNDLE_PARSER_RESULT_FILE_NAME,
        BUNDLE_METRICS_FILE_NAME,
        BUNDLE_CANDIDATE_UNITS_FILE_NAME,
        BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME,
        BUNDLE_WARNINGS_FILE_NAME,
    ];
    let mut record_bytes = BTreeMap::new();
    for (rel_path, digest) in &manifest.files {
        // The walk already proved every disk path is inside the bundle, so
        // resolving listed files through it also blocks any traversal via
        // hostile manifest keys (`../`, absolute paths).
        let Some(abs_path) = disk_files.get(rel_path) else {
            return Err(violation(format!("listed file missing: {rel_path}")));
        };
        let bytes = fs::read(abs_path).map_err(|source| {
            internal_io("failed to read listed bundle file", abs_path, &source)
        })?;
        if bytes.len() as u64 != digest.size_bytes {
            return Err(violation(format!(
                "size mismatch for {rel_path}: manifest claims {} bytes, found {}",
                digest.size_bytes,
                bytes.len()
            )));
        }
        // Trust boundary: the recomputed digest is what verification means;
        // the manifest value is only a claim checked against it.
        let recomputed = crate::canonical::sha256_hex_bytes(&bytes);
        if recomputed != digest.sha256 {
            return Err(violation(format!(
                "sha256 mismatch for {rel_path}: manifest claims {}, bytes hash to {recomputed}",
                digest.sha256
            )));
        }
        if retained.contains(&rel_path.as_str()) || is_parser_raw_path(rel_path) {
            record_bytes.insert(rel_path.clone(), bytes);
        }
    }
    Ok(record_bytes)
}

/// Match only descendants of the dedicated raw-output directory, not sibling names.
fn is_parser_raw_path(path: &str) -> bool {
    path.strip_prefix(BUNDLE_PARSER_RAW_DIR_NAME)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Deserialize one singular JSON record file out of the verified bytes; a
/// listed-but-unretained file is a caller bug, not producer input, hence
/// the internal arm.
fn parse_json_record<T: DeserializeOwned>(
    file_name: &str,
    record_bytes: &BTreeMap<String, Vec<u8>>,
) -> Result<T, BundleReadError> {
    let bytes = record_bytes.get(file_name).ok_or_else(|| {
        BundleReadError::Internal(ApiError::InternalIo {
            message: format!("verified bytes for {file_name} were not retained"),
        })
    })?;
    serde_json::from_slice(bytes)
        .map_err(|source| violation(format!("invalid {file_name}: {source}")))
}

/// Deserialize one JSONL record file out of the verified bytes: UTF-8 text,
/// one record per LF-separated line (a trailing LF is tolerated; blank
/// lines are not). An empty file is a valid empty record set.
fn parse_jsonl_records<T: DeserializeOwned>(
    file_name: &str,
    record_bytes: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<T>, BundleReadError> {
    let bytes = record_bytes.get(file_name).ok_or_else(|| {
        BundleReadError::Internal(ApiError::InternalIo {
            message: format!("verified bytes for {file_name} were not retained"),
        })
    })?;
    let text = std::str::from_utf8(bytes)
        .map_err(|source| violation(format!("{file_name} is not valid UTF-8: {source}")))?;
    let mut records = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let record: T = serde_json::from_str(line).map_err(|source| {
            violation(format!(
                "invalid record in {file_name} line {}: {source}",
                line_index + 1
            ))
        })?;
        records.push(record);
    }
    Ok(records)
}
