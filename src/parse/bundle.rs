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
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use crate::error::ApiError;
use crate::limits::RuntimeLimits;
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

/// Captured process stdout log file name (bounded, see
/// [`PROCESS_LOG_CAPTURE_BYTES`]).
pub(crate) const BUNDLE_STDOUT_LOG_FILE_NAME: &str = "stdout.log";

/// Captured process stderr log file name (bounded, see
/// [`PROCESS_LOG_CAPTURE_BYTES`]).
pub(crate) const BUNDLE_STDERR_LOG_FILE_NAME: &str = "stderr.log";

/// Upper bound on captured process output retained per stdout/stderr log in
/// the bundle contract; bytes beyond it are dropped, never a failure. The
/// stdout/stderr logs remain part of the contract as empty files: in-process
/// workers pass empty slices to `BundleWriter::finish`.
pub(crate) const PROCESS_LOG_CAPTURE_BYTES: usize = 65536;

/// Optional subdirectory for preserved parser raw output (spec §12.1 rule
/// 6): diagnostic evidence only, never canonical unless imported.
pub(crate) const BUNDLE_PARSER_RAW_DIR_NAME: &str = "parser_raw";

/// Optional subdirectory for large binary artifacts referenced by hash from
/// candidate records (spec §12.2). The EPUB worker stages image bytes here
/// as `artifacts/<sha256>`; the importer enforces the name-equals-hash
/// contract (SPEC-epub §2.7).
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
/// created in the same millisecond stay distinct.
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

    /// Tool identity facts observed during execution (e.g.
    /// `"parser_version" -> "2.x"`). Free-form claims for diagnostics and
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
    /// Staged binary artifacts (`artifacts/<name>`), keyed by bundle-relative
    /// path to the verified on-disk file. Paths, not bytes: images can be
    /// large, so the importer reads and stores them one at a time. Every
    /// listed file already passed digest and size verification against the
    /// staged manifest; the name-equals-hash contract is the importer's gate.
    pub(crate) artifact_files: BTreeMap<String, PathBuf>,
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
    /// records. The public read boundary bounds detail before logging/returning it.
    ContractViolation { detail: String },
    /// Current resource admission refused valid-or-unverified bytes; not corruption.
    ResourceLimit {
        detail: String,
        /// Present only after complete file verification; importer must archive
        /// these bytes before recording the admission refusal.
        verified_parser_raw_files: Option<VerifiedParserRaw>,
    },
}

/// Tie rejected-bundle raw bytes to the verified manifest for the importer's claims check.
#[derive(Debug)]
pub(crate) struct VerifiedParserRaw {
    pub(crate) manifest_hash: String,
    pub(crate) files: BTreeMap<String, Vec<u8>>,
}

/// Classify an intermediate contract failure; the public read boundary bounds detail.
fn violation(detail: impl AsRef<str>) -> BundleReadError {
    BundleReadError::ContractViolation {
        detail: detail.as_ref().to_owned(),
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
/// Bundle names are worker-chosen and unique (`bundle-{epoch_ms}-{seq}`);
/// they carry no canonical meaning.
/// If the process crashes before `finish`, the leftover `.tmp` directory is
/// inert: its name never matches a promoted bundle name, so no consumer
/// will ever read it, and the scheduler removes such orphans at thread
/// start (its sweep keys on BUNDLE_DIR_NAME_PREFIX/BUNDLE_TEMP_DIR_SUFFIX).
pub(crate) struct BundleWriter {
    /// Immutable admission/diagnostic limits survive failed producer completion.
    limits: RuntimeLimits,
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
    pub(crate) fn create(
        staging_root: &Path,
        identity: BundleIdentity,
        limits: RuntimeLimits,
    ) -> Result<Self, ApiError> {
        fs::create_dir_all(staging_root).map_err(|source| ApiError::InternalIo {
            message: format!(
                "failed to create parse staging root at {}: {source}",
                staging_root.display()
            ),
        })?;

        // Worker-chosen unique name: epoch-millisecond timestamp plus a
        // process-unique counter.
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
            limits,
            identity,
            temp_dir,
            bundle_dir,
            units,
            relationships,
            warnings,
            started: Instant::now(),
        })
    }

    /// Keep failure summaries and process capture on the writer's original settings.
    pub(crate) fn limits(&self) -> &RuntimeLimits {
        &self.limits
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

    /// Stream one parse warning to `warnings.jsonl`. Like the unit and
    /// relationship appenders, this does not enforce the `[parsing]` record
    /// caps; the worker checks `max_candidate_warnings` once against its
    /// aggregated (code, document) count before streaming them at finish,
    /// and the reader re-checks at import.
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
    /// manifest at `finish`. The EPUB worker writes images here; the
    /// plain-text worker emits none.
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
            bounded_result.error = Some(truncate_persisted_detail(
                error_detail,
                &self.limits.diagnostics,
            ));
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
        write_bounded_log(
            &self.temp_dir.join(BUNDLE_STDOUT_LOG_FILE_NAME),
            stdout_log,
            PROCESS_LOG_CAPTURE_BYTES,
        )?;
        write_bounded_log(
            &self.temp_dir.join(BUNDLE_STDERR_LOG_FILE_NAME),
            stderr_log,
            PROCESS_LOG_CAPTURE_BYTES,
        )?;

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
            let digest = file_digest(abs_path, self.limits.resources.embedding_read_buffer_bytes)
                .map_err(|source| match source {
                BundleReadError::Internal(source) => source,
                BundleReadError::ContractViolation { detail }
                | BundleReadError::ResourceLimit { detail, .. } => {
                    ApiError::InternalIo { message: detail }
                }
            })?;
            files.insert(rel_path.clone(), digest);
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
/// the configured process-log byte cap and appending an explicit
/// truncation marker when anything was omitted. Logs are opaque bytes (a
/// child process owns their encoding), so the cut may split a UTF-8
/// sequence; the marker makes the truncation unmistakable either way.
fn write_bounded_log(path: &Path, bytes: &[u8], max_bytes: usize) -> Result<(), ApiError> {
    let io_error = |source: &io::Error| ApiError::InternalIo {
        message: format!(
            "failed to write bounded log at {}: {source}",
            path.display()
        ),
    };
    if bytes.len() <= max_bytes {
        return fs::write(path, bytes).map_err(|source| io_error(&source));
    }
    let omitted = bytes.len() - max_bytes;
    let mut bounded = bytes[..max_bytes].to_vec();
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
pub(crate) fn read_bundle(
    bundle_dir: &Path,
    limits: &RuntimeLimits,
) -> Result<ParserOutputBundle, BundleReadError> {
    let started = Instant::now();
    info!(
        event = "parse.bundle.read_started",
        bundle_dir = %bundle_dir.display(),
        "parser output bundle read starting"
    );
    match read_bundle_inner(bundle_dir, limits) {
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
                artifact_count = bundle.artifact_files.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "parser output bundle read and verified"
            );
            Ok(bundle)
        }
        Err(mut read_error) => {
            // Intermediate validation keeps full context; only this externally
            // observable boundary applies the configured persisted-detail budget.
            if let BundleReadError::ContractViolation { detail }
            | BundleReadError::ResourceLimit { detail, .. } = &mut read_error
            {
                *detail = truncate_persisted_detail(detail, &limits.diagnostics);
            }
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
                BundleReadError::ResourceLimit { detail, .. } => warn!(
                    event = "parse.bundle.resource_limited",
                    bundle_dir = %bundle_dir.display(),
                    detail,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "configured resource limit refused parser bundle admission"
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
fn read_bundle_inner(
    bundle_dir: &Path,
    limits: &RuntimeLimits,
) -> Result<ParserOutputBundle, BundleReadError> {
    let manifest = load_manifest(bundle_dir, limits)?;
    let reader = BundleReader {
        limits,
        paths: listed_files(bundle_dir, &manifest)?,
        manifest: &manifest,
    };

    // JSONL is decoded and counted while reading; no full encoded plane is
    // retained beside the typed rows. Results escape only after their digest passes.
    let parser_result = reader.json_record(BUNDLE_PARSER_RESULT_FILE_NAME)?;
    let metrics = reader.json_record(BUNDLE_METRICS_FILE_NAME)?;
    let candidate_units = reader.jsonl_records(
        BUNDLE_CANDIDATE_UNITS_FILE_NAME,
        limits.parsing.max_candidate_units,
        |unit: &CandidateContentUnit| {
            let body = serde_json::to_vec(&unit.body).map_err(|source| {
                violation(format!(
                    "unit {} body serialization: {source}",
                    unit.local_id
                ))
            })?;
            if body.len() > limits.parsing.max_unit_body_bytes {
                return Err(resource_limit(format!(
                    "unit {} body exceeds parsing.max_unit_body_bytes={}",
                    unit.local_id, limits.parsing.max_unit_body_bytes
                )));
            }
            Ok(())
        },
    )?;
    let candidate_relationships = reader.jsonl_records(
        BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME,
        limits.parsing.max_candidate_relationships,
        |_| Ok(()),
    )?;
    let warnings = reader.jsonl_records(
        BUNDLE_WARNINGS_FILE_NAME,
        limits.parsing.max_candidate_warnings,
        |_| Ok(()),
    )?;
    let mut parser_raw_files = BTreeMap::new();
    let mut artifact_files = BTreeMap::new();
    for name in manifest.files.keys() {
        if matches!(
            name.as_str(),
            BUNDLE_PARSER_RESULT_FILE_NAME
                | BUNDLE_METRICS_FILE_NAME
                | BUNDLE_CANDIDATE_UNITS_FILE_NAME
                | BUNDLE_CANDIDATE_RELATIONSHIPS_FILE_NAME
                | BUNDLE_WARNINGS_FILE_NAME
        ) {
            continue;
        }
        let (path, expected) = reader.file(name)?;
        if is_parser_raw_path(name) {
            // Forensic payloads retain their original bytes and ownership. Record
            // admission limits do not cap or silently abbreviate opaque raw evidence.
            let bytes = fs::read(path)
                .map_err(|source| internal_io("read parser raw evidence", path, &source))?;
            verify_digest(
                name,
                expected,
                bytes.len() as u64,
                &crate::canonical::sha256_hex_bytes(&bytes),
            )?;
            parser_raw_files.insert(name.clone(), bytes);
        } else {
            let actual = file_digest(path, limits.resources.embedding_read_buffer_bytes)?;
            verify_digest(name, expected, actual.size_bytes, &actual.sha256)?;
            // Binary artifacts stay on disk: the importer reads each one when
            // it enforces the name-equals-hash contract and stores it.
            if is_artifacts_path(name) {
                artifact_files.insert(name.clone(), path.to_path_buf());
            }
        }
    }

    if let Some(detail) = candidate_units
        .refusal
        .or(candidate_relationships.refusal)
        .or(warnings.refusal)
    {
        // Rejected typed planes never reach canonicalization. Complete verified
        // raw bytes still reach the importer's durable failed-run archival path.
        let manifest_hash = crate::canonical::canonical_sha256_hex_of(&manifest)
            .map_err(BundleReadError::Internal)?;
        return Err(BundleReadError::ResourceLimit {
            detail,
            verified_parser_raw_files: Some(VerifiedParserRaw {
                manifest_hash,
                files: parser_raw_files,
            }),
        });
    }
    Ok(ParserOutputBundle {
        bundle_dir: bundle_dir.to_path_buf(),
        manifest,
        parser_result,
        candidate_units: candidate_units.records,
        candidate_relationships: candidate_relationships.records,
        warnings: warnings.records,
        metrics,
        parser_raw_files,
        artifact_files,
    })
}

/// Load and structurally validate `manifest.json`: parse through
/// deny_unknown_fields, check the schema version, and require every
/// contract-mandated file to be listed. A missing manifest is a contract
/// violation (the atomic promotion invariant means a visible bundle is
/// complete), while any other read fault is an internal error.
fn load_manifest(
    bundle_dir: &Path,
    limits: &RuntimeLimits,
) -> Result<ParserOutputManifest, BundleReadError> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST_FILE_NAME);
    let manifest_bytes = read_capped_bytes(&manifest_path, limits.resources.max_manifest_bytes)?;
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

/// Prove two-way file coverage and safe path containment before opening any listed file.
/// Each consumer subsequently verifies the bytes from its single read of that file.
fn listed_files(
    bundle_dir: &Path,
    manifest: &ParserOutputManifest,
) -> Result<BTreeMap<String, PathBuf>, BundleReadError> {
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

    for rel_path in manifest.files.keys() {
        // The walk already proved every disk path is inside the bundle, so
        // resolving listed files through it also blocks any traversal via
        // hostile manifest keys (`../`, absolute paths).
        if !disk_files.contains_key(rel_path) {
            return Err(violation(format!("listed file missing: {rel_path}")));
        }
    }
    Ok(disk_files)
}

/// Match only descendants of the dedicated raw-output directory, not sibling names.
fn is_parser_raw_path(path: &str) -> bool {
    is_under_bundle_dir(path, BUNDLE_PARSER_RAW_DIR_NAME)
}

/// Match only descendants of the binary artifacts directory, not sibling names.
fn is_artifacts_path(path: &str) -> bool {
    is_under_bundle_dir(path, BUNDLE_ARTIFACTS_DIR_NAME)
}

/// True when `path` is `<dir_name>/…`; a bare `<dir_name>` or a sibling
/// sharing the prefix (`parser_raw_x`) does not match.
fn is_under_bundle_dir(path: &str, dir_name: &str) -> bool {
    path.strip_prefix(dir_name)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

/// One verified file inventory and immutable admission contract for a bundle read.
struct BundleReader<'a> {
    paths: BTreeMap<String, PathBuf>,
    manifest: &'a ParserOutputManifest,
    limits: &'a RuntimeLimits,
}

impl BundleReader<'_> {
    /// Resolve only paths already proven to be covered and contained by the bundle walk.
    fn file(&self, name: &str) -> Result<(&Path, &FileDigest), BundleReadError> {
        let path = self
            .paths
            .get(name)
            .ok_or_else(|| violation(format!("listed file missing: {name}")))?;
        let digest = self
            .manifest
            .files
            .get(name)
            .ok_or_else(|| violation(format!("file not listed: {name}")))?;
        Ok((path, digest))
    }

    /// Bound one JSON document before decoding, then verify the same bytes it supplies.
    fn json_record<T: DeserializeOwned>(&self, name: &str) -> Result<T, BundleReadError> {
        let (path, expected) = self.file(name)?;
        let bytes = read_capped_bytes(path, self.limits.resources.max_json_cell_bytes)?;
        verify_digest(
            name,
            expected,
            bytes.len() as u64,
            &crate::canonical::sha256_hex_bytes(&bytes),
        )?;
        serde_json::from_slice(&bytes)
            .map_err(|source| violation(format!("invalid {name}: {source}")))
    }

    /// Stream complete JSONL records under byte/count guards; decoded rows remain
    /// provisional until EOF and the full original-file digest have been checked.
    fn jsonl_records<T: DeserializeOwned>(
        &self,
        name: &str,
        max_records: usize,
        mut validate: impl FnMut(&T) -> Result<(), BundleReadError>,
    ) -> Result<BoundedRecords<T>, BundleReadError> {
        let (path, expected) = self.file(name)?;
        let file =
            File::open(path).map_err(|source| internal_io("open parser JSONL", path, &source))?;
        let mut reader = BufReader::new(file);
        let mut digest = Sha256::new();
        let mut bytes_read = 0_u64;
        let mut records = Vec::new();
        let mut line = Vec::new();
        let mut refusal = None;
        loop {
            line.clear();
            let read = Read::by_ref(&mut reader)
                .take(self.limits.resources.max_json_cell_bytes as u64 + 1)
                .read_until(b'\n', &mut line)
                .map_err(|source| internal_io("read parser JSONL", path, &source))?;
            if read == 0 {
                break;
            }
            digest.update(&line);
            bytes_read = bytes_read
                .checked_add(read as u64)
                .ok_or_else(|| resource_limit(format!("{name} byte count overflow")))?;
            // A refusal stops decoding/allocation but continues byte verification.
            // This preserves forensic archival without admitting an oversized plane.
            if refusal.is_some() {
                continue;
            }
            if line.len() > self.limits.resources.max_json_cell_bytes {
                refusal = Some(format!(
                    "resource limit: {name} line {} exceeds resources.max_json_cell_bytes={}",
                    records.len() + 1,
                    self.limits.resources.max_json_cell_bytes
                ));
                records = Vec::new();
                continue;
            }
            if records.len() == max_records {
                refusal = Some(format!(
                    "resource limit: {name} exceeds configured record count {max_records}"
                ));
                records = Vec::new();
                continue;
            }
            let record = serde_json::from_slice(&line).map_err(|source| {
                violation(format!(
                    "invalid {name} line {}: {source}",
                    records.len() + 1
                ))
            })?;
            match validate(&record) {
                Ok(()) => {}
                Err(BundleReadError::ResourceLimit { detail, .. }) => {
                    refusal = Some(detail);
                    records = Vec::new();
                    continue;
                }
                Err(source) => return Err(source),
            }
            if let Err(source) = records.try_reserve(1) {
                refusal = Some(format!("resource limit: allocate {name} records: {source}"));
                records = Vec::new();
                continue;
            }
            records.push(record);
        }
        verify_digest(
            name,
            expected,
            bytes_read,
            &format!("{:x}", digest.finalize()),
        )?;
        Ok(BoundedRecords { records, refusal })
    }
}

/// Rejected rows stay private until all file digests and preserved raw bytes are verified.
struct BoundedRecords<T> {
    records: Vec<T>,
    refusal: Option<String>,
}

/// Refuse current admission without falsely declaring the stored content corrupt.
fn resource_limit(detail: String) -> BundleReadError {
    BundleReadError::ResourceLimit {
        detail: format!("resource limit: {detail}"),
        verified_parser_raw_files: None,
    }
}

/// Read at most the configured document bytes plus one overflow sentinel.
pub(crate) fn read_capped_bytes(path: &Path, max_bytes: usize) -> Result<Vec<u8>, BundleReadError> {
    let file = File::open(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            violation(format!("missing {}: {source}", path.display()))
        } else {
            internal_io("open bounded parser record", path, &source)
        }
    })?;
    let mut bytes = Vec::new();
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| internal_io("read bounded parser record", path, &source))?;
    if bytes.len() > max_bytes {
        return Err(resource_limit(format!(
            "{} exceeds configured JSON document bytes {max_bytes}",
            path.display()
        )));
    }
    Ok(bytes)
}

/// Check both complete-byte identity claims; neither a decoded subset nor a prefix suffices.
fn verify_digest(
    name: &str,
    expected: &FileDigest,
    bytes: u64,
    hash: &str,
) -> Result<(), BundleReadError> {
    if bytes != expected.size_bytes {
        return Err(violation(format!(
            "size mismatch for {name}: expected {}, found {bytes}",
            expected.size_bytes
        )));
    }
    if hash != expected.sha256 {
        return Err(violation(format!(
            "sha256 mismatch for {name}: expected {}, found {hash}",
            expected.sha256
        )));
    }
    Ok(())
}

/// Hash files that need no retained payload using one configured read buffer.
fn file_digest(path: &Path, buffer_bytes: usize) -> Result<FileDigest, BundleReadError> {
    let mut file =
        File::open(path).map_err(|source| internal_io("open parser bundle file", path, &source))?;
    let mut buffer = vec![0_u8; buffer_bytes];
    let mut digest = Sha256::new();
    let mut size_bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| internal_io("hash parser bundle file", path, &source))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        size_bytes = size_bytes
            .checked_add(read as u64)
            .ok_or_else(|| resource_limit(format!("{} size overflow", path.display())))?;
    }
    Ok(FileDigest {
        sha256: format!("{:x}", digest.finalize()),
        size_bytes,
    })
}
